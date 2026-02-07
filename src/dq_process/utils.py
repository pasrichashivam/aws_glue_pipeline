import base64 
import inspect 
import traceback
import boto3
from pyspark.sql import SparkSession

def get_critical_expectations(expectations_suite: dict):
    """Retrieves the critical expectations given a complete expectation suite
        Parameters:
            expectations_suite (dict): Complete expectation suite
        Returns:
            dict: Critical expectation suite
    """
    try:
        # Loop all rules in the expectation suite and return the critical ones
        critical_expectation_suite = []
        for rule in expectations_suite["rules"]:
            if rule["critical"]:
                critical_expectation_suite.append(rule["rule_definition"])
            return critical_expectation_suite
    except Exception:
        raise Exception(f'ERROR in {inspect.currentframe().f_code.co_name}: {traceback.format_exc()}')
    
def parse_expectations_metadata(
    dq_metadata: dict, gx_bucket: str
):
    """Parse data quality configuration information from Json for the requested table
        Parameters:
            dq_metadata (dict): Data Quality Metadata having information required to execute DQ rules
        Returns:
            dict: Table information such as: expectations_suite_name, expectations_suite,
                  gx_bucket, table_expectations_prefix, table_validations_prefix, table_checkpoints_prefix,
                  critical_expectation_suite
    """
    try:
        expectation_suite_name = dq_metadata['expectations']['expectation_suite_name']
        expectations_suite = dq_metadata['expectations']
        critical_expectation_suite = get_critical_expectations(expectations_suite=expectations_suite)
        expectations_prefix = dq_metadata['s3_expectations_uri']
        validation_prefix = dq_metadata['s3_validations_uri']
        checkpoints_prefix = dq_metadata['s3_checkpoints_uri']
        data_docs_prefix = dq_metadata['data_docs_prefix'] 
        repository = dq_metadata['repository']
        return {
            "expectations_suite_name": expectation_suite_name,
            "expectations_suite": expectations_suite,
            "gx bucket": gx_bucket,
            "table_expectations_prefix": expectations_prefix,
            "table_validations_prefix": validation_prefix,
            "table_checkpoints_prefix": checkpoints_prefix,
            "critical_expectation_suite": critical_expectation_suite,
            "data_docs_prefix": data_docs_prefix,
            "repository": repository
        }
    except Exception:
        raise Exception(f'ERROR in {inspect.currentframe().f_code.co_name}: {traceback. format_exc()}')

def build_datasource_config(datasource_name: str, gx_bucket_source: str):
    """Builds the datasource config structure
        Parameters:
            datasource_name (str): Name for the datasource 
            gx_bucket_source (str): Name of the source bucket
        Returns:
            dict: Datasource config dictionary
    """
    try:
        return {
            "name": datasource_name,
            "class_name": "Datasource",
            "execution_engine": {
                "class_name": "SparkDFExecutionEngine",
                "force_reuse_spark_context": "true"},
            "data_connectors": {
                "default_runtime_data_connector_name": {
                    "class _name": "RuntimeDataConnector",
                    "batch_identifiers": ["default_identifier_name"],
                },
                "default_inferred_data_connector_name": {
                    "class_name": "InferredAssetS3DataConnector",
                    "bucket": gx_bucket_source,
                    "default_regex": {
                        "pattern" : "(.*)\\.csv",
                        "group_names": ["data_asset_name"], 
                    },
                },
            },
        }
    except Exception:
        raise Exception(f'ERROR in {inspect.currentframe().f_code.co_name}: {traceback.format_exc()}')

def build_yaml_config(checkpoint_name: str, expectation_suite_name: str):
    """Builds the checkpoint configuration in YAML format
        Parameters:
            checkpoint_name (str): Name of the checkpoint 
            expectation_suite_name (str): Name of the expectation suite
        Returns:
            str: Checkpoint config in YAML format
    """
    try:
        return f"""
            name: {checkpoint_name}
            config_version: 1.0
            class_name: SimpleCheckpoint
            expectation_suite_name: {expectation_suite_name}
        """
    except Exception:
        raise Exception(f'ERROR in {inspect.currentframe().f_code.co_name}: {traceback.format_exc()}')
    
def build_template_df(schema_map):
    """Builds a template DataFrame given a schema
        Parameters:
            schema_map (dict): Schema for the DataFrame
        Returns:
            DataFrame: Template DataFrame created
    """
    try:
        spark = SparkSession.builder.appName('GreatExpectationsSparkApp').getOrCreate()
        empty_rdd = spark.sparkContext.emptyRDD()
        return spark.createDataFrame(empty_rdd, schema_map)
    except Exception:
        raise Exception(f'ERROR in {inspect.currentframe().f_code.co_name}: {traceback.format_exc()}')

def get_success_total_percentage(results: dict):
    metric = results.get("validation_result").get("statistics").get("success_percent") * 0.01
    return metric

def get_number_total_expectations(results: dict):
    metric = results.get("validation_result").get("statistics").get("evaluated_expectations")
    return metric

def get_number_total_successful_expectations(results: dict):
    metric = results.get("validation_result").get("statistics").get("successful_expectations")
    return metric

def calculate_critical_success_percentages(
        critical_exp_from_meta: list, 
        critical_rules_ko: list
) :
    try:
        if not critical_exp_from_meta:
            return 1
        number_total_critical_rules = len(critical_exp_from_meta)
        number_critical_failed_rules = len(critical_rules_ko)
        number_critical_successful_rules = (
            number_total_critical_rules - number_critical_failed_rules)
        percentage_critical_successful_rules = (number_critical_successful_rules / number_total_critical_rules)
        return percentage_critical_successful_rules
    except Exception:
        raise Exception(f'ERROR in {inspect.currentframe().f_code.co_name}: {traceback.format_exc()}')

def calculate_non_critical_success_percentages(
    critical_exp_from_meta: list, 
    critical_rules_ko: list, 
    number_total_successful_expectations: int, 
    number_total_rules: int
):
    try:
        number_total_critical_rules = len(critical_exp_from_meta)
        number_critical_failed_rules = len(critical_rules_ko)
        number_total_non_critical_rules = (number_total_rules - number_total_critical_rules)
        
        if number_total_non_critical_rules == 0:
            return 1
        number_critical_successful_rules = (
            number_total_critical_rules - number_critical_failed_rules
        )
        number_non_critical_successful_rules = (
            number_total_successful_expectations - number_critical_successful_rules)
        percentage_non_critical_successful_rules = (
            number_non_critical_successful_rules / number_total_non_critical_rules
        )
        return percentage_non_critical_successful_rules 
    except Exception:
        raise Exception(f'ERROR in {inspect.currentframe().f_code.co_name}: {traceback.format_exc()}')
        
def format_response(
    validations_s3_uri: str, 
    report_location_key: str,
    critical_rules_success: bool,
    critical_rules_ko: list, 
    success_total_percentage: float, 
    success_critical_percentage: float,
    success_non_critical_percentage: float,
    number_total_expectations: int = None,
    number_total_successful_expectations: int = None, 
    number_critical_expectations: int = None,
    number_failed_critical_expectations: int = None
):
    """Formats the provided fields in a response dictionary
        Parameters:
            validations_s3_uri (str): S3 URI for the validations table 
            report_location_key (str): S3 location of final HTML report 
            critical_ rules_success (bool): Whether all critical rules have passed 
            critical_rules_ko (list): List of failed critical rules 
            success_total_percentage (float): Percentage of general success 
            success_critical_percentage (float): Percentage of critical 
            success success_non_critical_percentage (float): Percentage of non-critical success
            number_total_expectations (int): Total number of expectations 
            number_total_successful_expectations (int): Total number of successful expectations 
            number_critical_expectations (int): Total number of critical expectations 
            number_failed_critical_expectations (int): Total number of failed critical expectations 
        Returns:
            dict: All provided fields in a formatted response"""
    try:
        response = {
            "validation_results_uri": validations_s3_uri,
            "report_location_key": report_location_key,
            "critical_rules_success": critical_rules_success,
            "critical_rules_ko": critical_rules_ko,
            "success_total_percentage": success_total_percentage,
            "success_critical_percentage": success_critical_percentage,
            "success_non_critical_percentage": success_non_critical_percentage,
            "number_total_expectations": number_total_expectations,
            "number_total_successful_expectations": number_total_successful_expectations,
            "number_total_failed_expectations": number_total_expectations - number_total_successful_expectations,
            "number_critical_expectations": number_critical_expectations,
            "number_failed_critical_expectations": number_failed_critical_expectations
        }
        return response
    except Exception:
        raise Exception(f'ERROR in {inspect.currentframe().f_code.co_name}: {traceback.format_exc()}')
        
def get_alert_message_body(template_path: str, parsed_information: dict, html=True):
    def str_failed_critical_checks():
        critical_checks = '<ol>'
        for check in parsed_information['critical_rules_ko']:
            try:
                critical_checks += '<li><ul>'
                critical_checks += '<li><b>Quality Check: </b>' + check[ 'expectation_config']['expectation_type'] + '</li>'
                critical_checks += '<li><b>Column Name: </b>' + check['expectation_config']['kwargs']['column'] + '</li>'
                critical_checks += '<li><b>Parameters: </b>' + str(check['expectation_config']['kwargs']) + '</li>'
                
                critical_checks += '<li><b>Total Records: </b>' + str(check['result']['element_count']) + '</li>'
                critical_checks += '<li><b>Unexpected Records: </b>' + str(check['result']['unexpected_count']) + '</li>'
                critical_checks +='<li><b>Unexpected Precent: </b>' + str(check['result']['unexpected_percent']) + '</li>'
                critical_checks +='<li><b>Sample Unexpected Values: </b>' + str(
                    list(set(check['result']['partial_unexpected_list']))[:10])  + '</li>'
                critical_checks += '</ul></li>'
            except KeyError:
                critical_checks += '</ul></li>'
        critical_checks += '</ol>' 
        return critical_checks
    
    def str_downstreams():
        downstreams = '<ol>'
        for pipeline in parsed_information['repository']:
            downstreams += f"<li><a href= {pipeline['url']}>" + pipeline['name'] + '</a></1i>'
        downstreams += '</ol>' 
        return downstreams
    
    parsed_information['total_rules'] = parsed_information[' number_total_expectations']
    parsed_information['total_failed_rules'] = parsed_information['number_total_failed_expectations']
    parsed_information['critical_rules'] = parsed_information['number_critical_expectations']
    parsed_information['failed_critical_rules'] = parsed_information[ 'number_failed_critical_expectations']
    parsed_information['total_success_percentage'] = int(round(parsed_information['success_total_percentage'] * 100, 0))
    parsed_information['critical_success_percentage'] = int(
        round(parsed_information['success_critical_percentage'] * 100, 0))
    parsed_information['non_critical_success_percentage'] = int(round(parsed_information['success_non_critical_percentage'] * 100, 0))
    parsed_information['downstreams'] = str_downstreams()
    parsed_information['critical_checks'] = str_failed_critical_checks()
    
    with open(template_path, 'r') as file:
        message_body = f"{file.read()}".format(**parsed_information)
        if html:
            encoded_body = base64.b64encode(bytes(message_body, 'utf-8'))
            message_body = encoded_body.decode('utf-8')
            return message_body
        
def get_base64_da_report(gx_bucket: str, report_location_key: str):
    s3 = boto3.client('s3')
    data = s3.get_object(Bucket=gx_bucket, Key=report_location_key)
    html_report = data['Body'].read()
    report_base64 = base64.b64encode(html_report).decode('utf-8')
    return report_base64