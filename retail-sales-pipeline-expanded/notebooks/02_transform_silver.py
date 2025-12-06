# 02_transform_silver.py
# Transform Bronze -> Silver: cleaning, type casting, dedupe, DQUT hooks
import os, sys, datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, current_timestamp

spark = SparkSession.builder.appName("transform_silver").getOrCreate()

BRONZE_INPUT = os.getenv("BRONZE_INPUT", "local_output/bronze/")
SILVER_OUTPUT = os.getenv("SILVER_OUTPUT", "local_output/silver/")

def transform():
    df = spark.read.parquet(BRONZE_INPUT)
    # Cast & clean
    df2 = (df.withColumn("quantity", col("quantity").cast("int"))
             .withColumn("unit_price", col("unit_price").cast("double"))
             .withColumn("sales_date", to_date(col("sales_date"), "yyyy-MM-dd"))
             .filter((col("quantity").isNotNull()) & (col("unit_price").isNotNull()))
          )
    # drop duplicates based on transaction_id, product_id
    df3 = df2.dropDuplicates(["transaction_id", "product_id"])
    df3 = df3.withColumn("_processed_ts", current_timestamp())
    # Write silver
    df3.write.mode("overwrite").parquet(SILVER_OUTPUT)
    print("Silver rows:", df3.count())

if __name__ == "__main__":
    transform()
