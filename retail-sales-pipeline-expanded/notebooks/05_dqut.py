# 05_dqut.py - Data Quality Unit Tests (basic checks)
import os, datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.appName("dqut").getOrCreate()

SILVER_PATH = os.getenv("SILVER_PATH", "local_output/silver/")

def run_checks():
    df = spark.read.parquet(SILVER_PATH)
    null_check = df.filter(col("sales_date").isNull() | col("product_id").isNull())
    if null_check.count() > 0:
        raise SystemExit("DQUT FAILED: Nulls in key columns")
    bad_qty = df.filter((col("quantity") <= 0) | (col("quantity") > 100000))
    if bad_qty.count() > 0:
        raise SystemExit("DQUT FAILED: Invalid quantity ranges")
    print("DQUT PASSED")
if __name__ == '__main__':
    run_checks()
