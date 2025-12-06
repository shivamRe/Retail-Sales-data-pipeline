# 01_ingest_bronze.py
# Simulated Auto Loader ingestion into Bronze Delta (local-mode simulation)
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import input_file_name, current_timestamp

spark = SparkSession.builder.appName("ingest_bronze").getOrCreate()

RAW_INPUT = os.getenv("RAW_INPUT", "data/raw/")
BRONZE_OUTPUT = os.getenv("BRONZE_OUTPUT", "local_output/bronze/")

def ingest():
    df = spark.read.option("header", True).option("inferSchema", True).csv(RAW_INPUT)
    df = df.withColumn("_source_file", input_file_name()).withColumn("_ingested_at", current_timestamp())
    df.write.mode("append").parquet(BRONZE_OUTPUT)
    print("Ingested rows:", df.count())

if __name__ == "__main__":
    ingest()
