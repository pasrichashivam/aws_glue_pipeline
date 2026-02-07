
import sys
from pyspark.sql import SparkSession
from awsglue.utils import getResolvedOptions
from zipfile import ZipFile

args = getResolvedOptions(sys.argv, ['ENV', 'task'])
spark = SparkSession.builder.appName("glue-job").enableHiveSupport().getOrCreate()
env = args['ENV']
task = args['task']

print("ENV:", env)
print("script_args:", task)

base_path = '/tmp/'
with ZipFile(f'{base_path}pyfiles.zip', 'r') as zip_ref:
    zip_ref.extractall(f'{base_path}extracted')

sys.path.append(f'{base_path}extracted')
from transform import transform_data
from dq_process.dq_process import DataQuality


output_db = f"{env}_analytics"
output_table = "teams_processed"

if task != 'dq':
    input_path = f"s3://raw-bucket-{env}-source/data/leagues.csv"
    df = spark.read.csv(input_path, header=True, inferSchema=True)
    result_df = transform_data.transform(df)
    
    result_df.write.mode("append").format("parquet") \
        .insertInto(f"{output_db}.{output_table}")

else:
    DataQuality().run()
