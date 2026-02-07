from pyspark.sql import functions as f

def transform(df):
    df = df.withColumn('execution_date', f.current_date())
    return df
