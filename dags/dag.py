from airflow import DAG
from datetime import datetime
from airflow.providers.amazon.aws.operators.emr import (
    EmrServerlessCreateApplicationOperator,
    EmrServerlessStartJobOperator,
    EmrServerlessDeleteApplicationOperator,
)
from airflow.providers.amazon.aws.sensors.emr import EmrServerlessJobSensor

# Define your AWS and EMR Serverless parameters
APP_NAME = 'aa_teams'
EMR_APPLICATION_NAME = f"emr_serverless_app_{APP_NAME}"
RELEASE_LABEL = "emr-7.1.0" # Specify the EMR release version
JOB_ROLE_ARN = "arn:aws:iam::656435923965:role/role-IN-apps-dev-aa_teams" # Replace with your job runtime role ARN
S3_EMR_ARTIFACT = f"s3://emr-app-artifacts-dev/{APP_NAME}" # Replace with your S3 script location

# Define the Spark job driver configuration
SPARK_JOB_DRIVER={
    "sparkSubmit": {
        "entryPoint": f"{S3_EMR_ARTIFACT}/main.py",
        "sparkSubmitParameters":f"--conf spark.submit.pyFiles={S3_EMR_ARTIFACT}/pyfiles.zip --conf spark.emr-serverless.driverEnv.app_env=dev --conf spark.executorEnv.app_env=dev --conf spark.sql.catalogImplementation=hive --conf spark.hadoop.hive.metastore.client.factory.class=com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory"

        # "sparkSubmitParameters":f"--conf spark.archives={S3_EMR_ARTIFACT}/venv-aws_data_pipeline.tar.gz#environment --conf spark.submit.pyFiles={S3_EMR_ARTIFACT}/pyfiles.zip --conf spark.emr-serverless.driverEnv.PYSPARK_DRIVER_PYTHON=/home/hadoop/environment/bin/python --conf spark.emr-serverless.driverEnv.PYSPARK_PYTHON=/home/hadoop/environment/bin/python --conf spark.executorEnv.PYSPARK_PYTHON=/home/hadoop/environment/bin/python --conf spark.emr-serverless.driverEnv.app_env=dev --conf spark.executorEnv.app_env=dev"
    }
}

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2022, 1, 1),
    'depends_on_past': False,
    'tags': ['emr_serverless_airflow'],
}

with DAG(
    dag_id='emr_serverless_job_dag',
    default_args=default_args,
    schedule=None,
    catchup=False,
) as dag:
    create_emr_app = EmrServerlessCreateApplicationOperator(
        task_id="create_emr_serverless_app",
        config={'name': EMR_APPLICATION_NAME},
        release_label=RELEASE_LABEL,
        job_type="SPARK",
    )

    start_emr_job = EmrServerlessStartJobOperator(
        task_id="start_emr_serverless_job",
        application_id=create_emr_app.output, # Dynamically get the application ID
        execution_role_arn=JOB_ROLE_ARN,
        job_driver=SPARK_JOB_DRIVER,
        configuration_overrides={
        "monitoringConfiguration": {
            "cloudWatchLoggingConfiguration": {
                "enabled": True, 
                "logGroupName": "/emr-serverless/jobs", 
                "logStreamNamePrefix": "test-teams-emr-serverless" 
                } 
            } 
        }
    )
    
    # Use the sensor to wait for the job to complete
    # wait_for_job_completion = EmrServerlessJobSensor(
    #     task_id="wait_for_job_completion",
    #     application_id=create_emr_app.output,
    #     job_run_id=start_emr_job.output, # Dynamically get the job run ID
    #     target_states={"SUCCESS"},
    # )

    # delete_emr_app = EmrServerlessDeleteApplicationOperator(
    #     task_id="delete_emr_serverless_app",
    #     application_id=create_emr_app.output,
    #     trigger_rule="all_done", # Ensures this runs even if the job fails
    # )

    # Define the workflow
    create_emr_app >> start_emr_job 
