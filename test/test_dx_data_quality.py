import unittest
from unittest.mock import MagicMock, patch 
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType 
from dq_process.data_quality import DataQualityProcess


class TestMain(unittest.TestCase):
    """Unittest suite for main class"""

    @classmethod
    def setUpClass (cls) -> None:
        cls. spark = SparkSession.builder.appName("UnitTesting") \
            .master ("local[2]") \
            .getOrCreate()
    
    @patch('dq_process.data_quality.data_quality.parse_expectations_metadata')
    @patch('dq_process.data_quality.data_quality.create_context')
    @patch('dq_process.data_quality.data_quality.build_template_df')
    @patch('dq_process.data_quality.data_quality.build_batch_request')
    @patch('dq_process.data_quality.data_quality.add_expectation_suite_to_validator')
    def test_create_gx_rules(self, mock_add_expectation_suite_to_validator,
        mock_build_batch_request, mock_build_template_df, 
        mock_create_context, mock_get_table_information):
    
        mock_get_table_information.return_value = {
        "expectations_suite_name": "test_suite",
        "expectations_suite": [],
        "gx_bucket": "test_bucket",
        "table_expectations_prefix": "expectations_prefix",
        "table_validations_prefix": "validation_prefix",
        "table_checkpoints_prefix": "checkpoints_prefix",
        "data_docs_prefix": "data_docs_prefix",
        "downstream_dataproducts": [{'url': 'test_url', 'name': 'test_name'}],
        "critical_expectation_suite": [],
        }
        mock_context = MagicMock()
        mock_validator = MagicMock()
        mock_create_context.return_value = mock_context
        mock_context.get_validator.return_value = mock_validator
        mock_add_expectation_suite_to_validator.return_value = mock_validator
        s3_bucket = 'test bucket'
        database = 'test_db'
        table = 'test_table'
        alert_recipients = ['test_abc.com']
        data_quality_meta = {
            "s3_checkpoints_uri": "checkpoints_prefix",
            "s3_expectations_uri": "expectations_prefix",
            "s3_validations_uri": "validations_prefix",
            "data_docs_prefix": "data_docs_prefix",
            "downstream dataproducts": [{'url': 'test_url', 'name': 'test_name'}],
            "expectations": {
                "expectation_suite_name": "test_suite",
                "rules": []
            }
        }
        da_proces = DataQualityProcess(data_quality_meta, s3_bucket, database, table)
        empty_rdd = self.spark.sparkContext.emptyRDD()
        schema_map = StructType([StructField( 'col1', StringType(), True)])
        df = self.spark.createDataFrame(empty_rdd, schema_map)
        da_proces. create_and_save_expectation_suite(df)
        table_name = f'{database}_{table}'
        
        mock_create_context.assert_called_once_with(
            source_name=table_name, 
            gx_bucket_source="test_bucket", 
            table_expectations_prefix="expectations_prefix", 
            table_validation_prefix="validation_prefix", 
            table_checkpoints_prefix="checkpoints_prefix", 
            data_docs_prefix='data_docs_prefix'
        )
        mock_build_template_df.assert_called_once()
        mock_build_batch_request.assert_called_once_with(
            batch_name=table_name, df=mock_build_template_df())
        mock_add_expectation_suite_to_validator.assert_called_once_with(
        expectations_suite=[], validator=mock_validator)

        mock_validator.save_expectation_suite.assert_called_once_with(
        discard_failed_expectations=False)


if __name__=='__main__':
    unittest.main()

