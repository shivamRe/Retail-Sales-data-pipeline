# small helper to run ingest logic for unit tests
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import input_file_name, current_timestamp

def run(raw_input, silver_output):
    spark = SparkSession.builder.appName("test-ingest").getOrCreate()
    df = spark.read.option("header", True).csv(raw_input)
    df = df.withColumn("_source_file", input_file_name()).withColumn("_ingested_at", current_timestamp())
    bronze = silver_output  # for tests we shortcut bronze to silver path
    df.write.mode("overwrite").parquet(bronze)
