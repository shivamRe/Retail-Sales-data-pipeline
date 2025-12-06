# 03_curate_gold.py
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import year, month, dayofmonth, sum as _sum, avg as _avg, countDistinct

spark = SparkSession.builder.appName("curate_gold").getOrCreate()

SILVER_INPUT = os.getenv("SILVER_INPUT", "local_output/silver/")
GOLD_OUTPUT = os.getenv("GOLD_OUTPUT", "local_output/gold/")

def curate():
    df = spark.read.parquet(SILVER_INPUT)
    agg = (df.withColumn("year", year("sales_date"))
             .withColumn("month", month("sales_date"))
             .withColumn("day", dayofmonth("sales_date"))
             .groupBy("year","month","day","region","product_id")
             .agg(
                 _sum("quantity").alias("total_quantity"),
                 _sum("quantity"*col("unit_price")).alias("total_sales"),
                 _avg("unit_price").alias("avg_price"),
                 countDistinct("customer_id").alias("unique_customers")
             )
          )
    agg.write.mode("overwrite").parquet(GOLD_OUTPUT)
    print("Gold rows:", agg.count())

if __name__ == "__main__":
    curate()
