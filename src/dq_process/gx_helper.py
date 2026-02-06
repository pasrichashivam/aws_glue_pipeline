import great_expectations as gx 
from great_expectations.checkpoint.types.checkpoint_result import CheckpointResult 
from great_expectations.core.batch import RuntimeBatchRequest 
from great_expectations.data_context import BaseDataContext
from great_expectations.data_context.types.base import DataContextConfig, S3StoreBackendDefaults 
from great_expectations.validator.validator import Validator 
from pyspark.sql import DataFrame 
from dq_process.utils import *
import inspect
import traceback

def create_context(
        source_name: str, 
        gx_bucket_source: str, 
        table_expectations_prefix: str,
        table_validation_prefix: str, 
        table_checkpoints_prefix: str, 
        data_docs_prefix: str
) :
    try:
        print("Setting up context for Great Expectations")
        _ = gx.get_context()
        backend_defaults = S3StoreBackendDefaults(
            default_bucket_name=gx_bucket_source, 
            checkpoint_store_prefix=table_checkpoints_prefix,
            validations_store_prefix=table_validation_prefix, 
            expectations_store_prefix=table_expectations_prefix, 
            data_docs_prefix=data_docs_prefix
        )
        data_context_config = DataContextConfig(store_backend_defaults=backend_defaults)
        context = BaseDataContext(project_config=data_context_config)
        datasource_name = f'gx_{source_name}_dq'
        datasource_config = build_datasource_config(
            datasource_name=datasource_name, 
            gx_bucket_source=gx_bucket_source
        )
        context.add_datasource(**datasource_config)
        print("Great Expectations context setup successfully")
        return context 
    except Exception:
        raise Exception(f'ERROR in {inspect.currentframe().f_code.co_name}: {traceback.format_exc()}')

def build_batch_request(
        batch_name: str, 
        df: DataFrame
    ):
    """Builds the batch RuntimeBatchRequest obj
        Parameters:
            batch_name (str): Name of source table 
            df (DataFrame): DataFrame containing the data
        Returns:
            dict: RuntimeBatchRequest created
    """
    try:
        print("Great Expectations: built runtime batch request") 
        return RuntimeBatchRequest(
            datasource_name=f'gx_{batch_name}_dq',
            data_connector_name="default_runtime_data_connector_name", 
            data_asset_name="version-0.15.50", 
            runtime_parameters={"batch_data": df},
            batch_identifiers={f"default_identifier_name": "template_batch_request"}
    )
    except Exception:
        raise Exception(f'ERROR in {inspect.currentframe().f_code.co_name}: {traceback.format_exc()}')

def add_expectation_suite_to_validator(
    expectations_suite: dict, 
    validator: Validator
):
    try:
        for rule in expectations_suite["rules"]:
            expectation_kwargs = rule["rule_definition"]["kwargs"]
            expectation_type = rule["rule_definition"]["expectation_type"]
            getattr(validator, expectation_type) (**expectation_kwargs)
            print("Great Expectations added expectation suite to validator") 
            return validator
    except Exception:
        raise Exception(f'ERROR in {inspect.currentframe().f_code.co_name}: {traceback.format_exc()}')

def parse_critical_expectations_results(
    critical_exp_from_meta: list, 
    checkpoint_result: CheckpointResult, 
    gx_bucket_source: str
) :
    """Extracts the desired metrics from the critical expectations validation results
        Parameters:
            critical_exp_from_meta (list): List of critical expectations from expectation rules 
            checkpoint_result (dict): Results of execution the validation 
            gx_bucket_source (str): Source bucket in S3
        Returns:
            Dict: Critical validation metrics such as: 
                  validation_s3_üri, critical_rules_success, critical_rules ko, 
                  success_total_percentage, success_critical percentage,
                  success_non_critical_percentage"""
    try:
        print("Great Expectations initiating parsing validation results")
        critical_rules_ko = []
        number_total_expectations= 0
        success_total_percentage = 1
        number_total_successful_expectations = 0
        run_results = checkpoint_result.get("run_results")
        key_run_result = list(run_results.keys())[0]
        s3_results_subprefix: str = str(key_run_result).replace(
            "ValidationResultIdentifier::", "")
        validation_s3_uri = gx_bucket_source + "/validations/" + s3_results_subprefix
        report_location_key = \
            run_results.get(key_run_result).get("actions_results").get('update_data_docs').get('s3_site').split(gx_bucket_source)[-1][1:]
        
        for rule in critical_exp_from_meta:
            expectation_kwargs = rule["kwargs"]
            expectation_type = rule["expectation_type"]
            rules_success = run_results.get(key_run_result).get("validation_result").get ("success")
            
            results = run_results.get(key_run_result).get("validation_result").get("results")
            success_total_percentage = get_success_total_percentage(
                run_results.get(key_run_result)
            )
            number_total_expectations = get_number_total_expectations(
                run_results.get(key_run_result)
            )
            number_total_successful_expectations = get_number_total_successful_expectations(
                run_results.get(key_run_result)
            )
            # If all expectations have passed return success response
            if rules_success is True:
                return format_response(
                    validation_s3_uri, report_location_key, rules_success, [], 1, 1, 1, 
                    number_total_expectations, number_total_successful_expectations,
                    len(critical_exp_from_meta), len(critical_rules_ko)
                )
            
            for expectation in results:
                is_failure = expectation["success"] is False
                is_expected_type = (expectation["expectation_config"]["expectation_type"] == expectation_type)
            
            # Delete 'batch_id' if present
            if "batch_id" in expectation ["expectation_config"]["kwargs"]:
                del expectation ["expectation_config"]["kwargs"]["batch_id"]

            has_expected_kwargs = expectation["expectation_config"]["kwargs"] == expectation_kwargs
            if is_failure and is_expected_type and has_expected_kwargs:
                critical_rules_ko.append(expectation)
                # No need to keep searching once found
                break
        success_critical_percentage = calculate_critical_success_percentages(
            critical_exp_from_meta=critical_exp_from_meta, 
            critical_rules_ko=critical_rules_ko
        )
        success_non_critical_percentage = calculate_non_critical_success_percentages(
            critical_exp_from_meta=critical_exp_from_meta, 
            critical_rules_ko=critical_rules_ko, 
            number_total_successful_expectations=number_total_successful_expectations, 
            number_total_rules=number_total_expectations
        )
        print("Great Expectations Successfully parsed validation results")
        return format_response(
            validation_s3_uri, 
            report_location_key, 
            len(critical_rules_ko) == 0,
            critical_rules_ko, 
            success_total_percentage, 
            success_critical_percentage, 
            success_non_critical_percentage, 
            number_total_expectations, 
            number_total_successful_expectations, 
            len(critical_exp_from_meta),
            len(critical_rules_ko)
        )
    except Exception:
        raise Exception(f'ERROR in {inspect.currentframe().f_code.co_name}: {traceback.format_exc()}')