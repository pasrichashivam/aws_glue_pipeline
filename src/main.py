
import sys
import os
from pyspark.sql import SparkSession
from zipfile import ZipFile

print("sys.executable:", sys.executable)
print("PYSPARK_PYTHON:", os.environ.get("PYSPARK_PYTHON"))
print("PYSPARK_DRIVER_PYTHON:", os.environ.get("PYSPARK_DRIVER_PYTHON"))
print("ENV:", os.environ.get("app_env"))

with ZipFile('/home/hadoop/pyfiles.zip', 'r') as zip_ref:
    zip_ref.extractall("/home/hadoop/")

env = os.environ.get("app_env")
spark = SparkSession.builder.appName("emr-serverless-job").enableHiveSupport().getOrCreate()

input_path = f"s3://raw-bucket-{env}-source/data/leagues.csv"
output_db = f"{env}_analytics"
output_table = "teams_processed"

from transform import transform_data
df = spark.read.csv(input_path, header=True)
result_df = transform_data.transform(df)

result_df.write.mode("append").format("parquet") \
    .insertInto(f"{output_db}.{output_table}")
