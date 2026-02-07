import os
from datetime import datetime
import yaml
from pyspark.sql import DataFrame
import inspect
import traceback
from great_expectations.data_context.data_context.abstract_data_context import AbstractDataContext
from dq_process.gx_helper import *

class DataQualityProcess:
    def __init__(self, data_quality_meta: dict,
                 gx_bucket: str,
                 database: str,
                 table: str):
        if not isinstance(data_quality_meta, dict):
            raise ValueError("data_quality_meta parameter should be a dict type")
        if 's3_checkpoints_uri' not in data_quality_meta:
            raise ValueError("s3_checkpoints_uri is not provided in data_quality_meta dict")
        if 's3_expectations_uri' not in data_quality_meta:
            raise ValueError("s3_expectations_uri is not provided in data_quality_meta dict")
        if 's3_validations_uri' not in data_quality_meta:
            raise ValueError("s3_validations_uri is not provided in data_quality_meta dict")
        if 'expectations' not in data_quality_meta:
            raise ValueError("expectations is not provided in data_quality_meta dict")
        if 'expectation_suite_name' not in data_quality_meta[ 'expectations']:
            raise ValueError("expectation_suite_name is not provided under expectations")
        if 'rules' not in data_quality_meta['expectations']:
            raise ValueError("rules is not provided under expectations")
        if 'data_docs_prefix' not in data_quality_meta:
            raise ValueError("data_docs_prefix is not provided in data_quality _meta dict")
        if 'repository' not in data_quality_meta:
            raise ValueError("repository is not provided in data_quality_meta dict")
        
        self.database = database
        self.table = table
        self.data_quality_meta = data_quality_meta
        
        self.da_dict = parse_expectations_metadata(data_quality_meta, gx_bucket)
        self.expectation_suite_name = self.dq_dict["expectations_suite_name"]
        self.gx_bucket = self.dq_dict["gx_bucket"]
        self.expectations_suite = self.dq_dict["expectations_suite"]
        self.table_expectation_prefix = self.dq_dict["table_expectations _prefix"]
        self.table_validation_prefix = self.dq_dict["table_validations_prefix"]
        self.table_checkpoints_prefix = self.dq_dict["table_checkpoints_prefix"]
        self.critical_expectation_suite = self.dq_dict["critical_expectation_suite"]
        self.data_docs_prefix = self.dq_dict["data_docs_prefix"]
        self.repository = self.dq_dict ["repository"]

    def run(self, data: DataFrame):
        context = self.create_and_save_expectation_suite(data)
        quality_results = self.execute_rules_and_save_results(context, data)
        return quality_results
    
    def create_and_save_expectation_suite(self, data: DataFrame):
        """Creates initializes and saves an expectations suite
            Parameters:
                data (DataFrame): Spark dataframe
            Returns:
                context (AbstractDataContext): GX data context"""
        print("Creating GreatExpectations rules with efpectation suite") 
        try:
            context: AbstractDataContext = create_context(
                source_name=self.database,
                gx_bucket_sourcee=self.gx_bucket,
                table_expectations_prefix=self.table_expectation_prefix, 
                table_validation_prefix=self.table_validation_prefix, 
                table_cheokpoints_prefix=self.table_checkpoints_prefix, 
                data_docs_prefix=self.data_docs_prefix)
            
            template_df = build_template_df(data.schema)
            batch_request = build_batch_request(batch_name=f'{self.database}_{self.table}', df=template_df)

            context.add_or_update_expectation_suite(self.expectation_suite_name)
            validator = context.get_validator(batch_request=batch_request, 
                                              expectation_suite_name=self.expectation_suite_name
                                              )
            validator = add_expectation_suite_to_validator(expectations_suite=self.expectations_suite, validator=validator)
            validator.save_expectation_suite(discard_failed_xpectations=False)
            print(f"Saved expectation suite with name {self.expectation_suite_name}")
            return context
        except Exception:
            raise Exception(f'ERROR in {__class__.__name__}.{inspect.curzentframe().f_code.co_name}: {traceback.format_exc()}')
        
    def execute_rules_and_save_results(self, context: AbstractDataContext, data: DataFrame) :
        try:
            print("Initiating validation execution process") 
            timestamp = datetime.now().strftime("%Y-%m-%d %H:&M:%S")
            checkpoint_name = f"checkpoint_{timestamp}"
            config_yaml = build_yaml_config(checkpoint_name=checkpoint_name, 
                                            expectation_suite_name=self.expectation_suite_name
                                            )

            context.test_yaml_config(yaml_config=config_yaml)
            context.add_checkpoint(**yaml.safe_load(config_yaml))
            batch_request = build_batch_request(
                batch_name=f'{self.database}_{self.table}',df=data)
            checkpoint_result = context.run_checkpoint(
                checkpoint_name=checkpoint_name, 
                run_name=f'Run_{timestamp}', 
                validations=[{"batch_request": batch_request,
                              "expectation_suite_name": self.expectation_suite_name}]
                              )
            self.parsed_results = parse_critical_expectations_results(
                critical_exp_from_meta=self.critical_expectation_suite, 
                checkpoint_result=checkpoint_result, 
                gx_bucket_source=self.gx_bucket
            )
            print("Successfully completed execution of validation process")
        except Exception:
            raise Exception(f'ERROR in {__class__.__name__}.{inspect.curzentframe().f_code.co_name}: {traceback.format_exc()}')
