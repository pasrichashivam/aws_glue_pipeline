
import sys
from pyspark.sql import SparkSession
from awsglue.utils import getResolvedOptions
from zipfile import ZipFile
import chispa

print("Chispa: ", chispa)

args = getResolvedOptions(sys.argv, ['ENV', 'script_args'])

spark = SparkSession.builder.appName("glue-job").enableHiveSupport().getOrCreate()
env = args['ENV']
script_args = args['script_args']

print("ENV:", env)
print("script_args:", script_args)

base_path = '/tmp/'
with ZipFile(f'{base_path}pyfiles.zip', 'r') as zip_ref:
    zip_ref.extractall(f'{base_path}extracted')

sys.path.append(f'{base_path}extracted')

input_path = f"s3://raw-bucket-{env}-source/data/leagues.csv"
output_db = f"{env}_analytics"
output_table = "teams_processed"

from transform import transform_data
df = spark.read.csv(input_path, header=True)
result_df = transform_data.transform(df)

result_df.write.mode("append").format("parquet") \
    .insertInto(f"{output_db}.{output_table}")
