import os
from dq_process.data_quality import DataQualityProcess 
from pyspark.sql import SparkSession
import yaml

dq_alert_recipients = ['pasrichashivam@gmail.com']

class DataQuality:
    def __init__(self):
        print("Starting Data Quality Process")
        self.output_db = f"dev_analytics"
        self.output_table = "teams_processed"
        self.spark = SparkSession.builder.appName("dq-job").enableHiveSupport().getOrCreate()

        with open('metadata/da_rules.yaml') as file_:
            self.da_metadata =  yaml.safe_load(file_.read())['']
    
    def run (self):
        table = f"{self.output_table}"
        iceberg_table = f"{self.output_db}.{table}"
        data = self.spark.sql(f"SELECT * FROM {iceberg_table} VERSION AS OF 'staging'")
        
        self.dq_metadata['expectations']['expectation_suite_name'] = f"{self.output_db}_{self.output_table}"
        dq_process = DataQualityProcess(
            self.dq_metadata, 'glue-job-artifacts-dev', self.output_db,
            self.output_table)
        dq_process.run(data)
        
        if dq_process.parsed_results['success_critical_percentage'] < 1 and dq_alert_recipients:
            template_path = f"{os.environ.get('PROJECT_PATH')}/templates/dq.html"
            dq_process.send_report(template_path, dq_alert_recipients)
        print(f"Completed Data Quality process with the following results {dq_process.parsed_results}")