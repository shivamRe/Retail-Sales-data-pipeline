# Databricks notebook - 01_ingest_bronze_streaming.py
# Uses Auto Loader (cloudFiles) to continuously ingest files from ADLS into Bronze Delta
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType
from pyspark.sql.functions import input_file_name, current_timestamp
import os

raw_path = dbutils.widgets.get("raw_path") if 'dbutils' in globals() else os.getenv("RAW_INPUT", "/mnt/adls/retail/raw/")
bronze_table = dbutils.widgets.get("bronze_table") if 'dbutils' in globals() else os.getenv("BRONZE_TABLE", "/mnt/adls/retail/bronze/")
checkpoint = dbutils.widgets.get("checkpoint") if 'dbutils' in globals() else os.getenv("BRONZE_CHK", "/mnt/adls/checkpoints/bronze/")

# Define schema (example)
schema = StructType([
    StructField("transaction_id", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("unit_price", DoubleType(), True),
    StructField("sales_date", StringType(), True),
    StructField("region", StringType(), True),
    StructField("customer_id", StringType(), True)
])

df_stream = (spark.readStream
             .format("cloudFiles")
             .option("cloudFiles.format", "csv")
             .option("header", "true")
             .option("cloudFiles.schemaLocation", checkpoint + "schema/")
             .schema(schema)
             .load(raw_path)
            )

df_stream = df_stream.withColumn("_source_file", input_file_name()).withColumn("_ingested_at", current_timestamp())

(df_stream.writeStream
 .format("delta")
 .option("checkpointLocation", checkpoint)
 .outputMode("append")
 .option("mergeSchema", "true")
 .start(bronze_table)
)

print("Auto Loader streaming started. Monitor the stream in Databricks UI.")
