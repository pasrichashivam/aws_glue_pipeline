from airflow import DAG
from datetime import datetime
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2022, 1, 1),
    'depends_on_past': False,
    'tags': ['emr_serverless_airflow'],
}

job_name = "teams_glue_job"

with DAG(
    dag_id='glue_job_dag',
    default_args=default_args,
    schedule=None,
    catchup=False,
) as dag:
    start_emr_job = GlueJobOperator(
        task_id="start_glue_job",
        job_name=job_name,
        script_args={
            "script_args": "argument_1"
        }
    )
    start_emr_job 
