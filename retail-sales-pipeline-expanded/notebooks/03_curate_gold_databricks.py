# Databricks notebook - 03_curate_gold_databricks.py
# Aggregates silver into gold tables and runs OPTIMIZE + ZORDER (Databricks only)
from pyspark.sql.functions import year, month, dayofmonth, sum as _sum, avg as _avg, countDistinct, col
import os, time

silver_table = dbutils.widgets.get("silver_table") if 'dbutils' in globals() else os.getenv("SILVER_TABLE", "/mnt/adls/retail/silver/")
gold_path = dbutils.widgets.get("gold_path") if 'dbutils' in globals() else os.getenv("GOLD_PATH", "/mnt/adls/retail/gold/")
gold_table = dbutils.widgets.get("gold_table") if 'dbutils' in globals() else None

silver_df = spark.read.format("delta").load(silver_table)
agg_df = (silver_df.withColumn("year", year("sales_date"))
          .withColumn("month", month("sales_date"))
          .withColumn("day", dayofmonth("sales_date"))
          .groupBy("year","month","day","region","product_id")
          .agg(
              _sum(col("quantity")*col("unit_price")).alias("total_sales"),
              _sum("quantity").alias("total_quantity"),
              _avg("unit_price").alias("avg_price"),
              countDistinct("customer_id").alias("unique_customers")
          )
         )

# write to gold
agg_df.write.format("delta").mode("overwrite").partitionBy("year","month").save(gold_path)

# Optional: register as a table for Databricks SQL
if gold_table:
    spark.sql(f"CREATE TABLE IF NOT EXISTS {gold_table} USING DELTA LOCATION '{gold_path}'")

# Run OPTIMIZE + ZORDER (Databricks command)
try:
    spark.sql(f"OPTIMIZE delta.`{gold_path}` ZORDER BY (product_id, region)")
except Exception as e:
    print('OPTIMIZE failed in this environment:', e)

print("Gold curation complete. Rows:", agg_df.count())
