# Databricks notebook - 02_transform_silver_databricks.py
# Performs transformations and writes to a Silver Delta table. Demonstrates MERGE to be idempotent.
from delta.tables import DeltaTable
from pyspark.sql.functions import col, to_date, current_timestamp
import os

bronze_table = dbutils.widgets.get("bronze_table") if 'dbutils' in globals() else os.getenv("BRONZE_TABLE", "/mnt/adls/retail/bronze/")
silver_table = dbutils.widgets.get("silver_table") if 'dbutils' in globals() else os.getenv("SILVER_TABLE", "/mnt/adls/retail/silver/")

bronze_df = spark.read.format("delta").load(bronze_table)

clean_df = (bronze_df
            .withColumn("quantity", col("quantity").cast("int"))
            .withColumn("unit_price", col("unit_price").cast("decimal(10,2)"))
            .withColumn("sales_date", to_date(col("sales_date"), "yyyy-MM-dd"))
            .filter((col("quantity") > 0) & (col("unit_price") > 0))
            .dropDuplicates(["transaction_id", "product_id"])
            .withColumn("_processed_ts", current_timestamp())
           )

# If silver path exists, merge, else write as new table
if DeltaTable.isDeltaTable(spark, silver_table):
    deltaTable = DeltaTable.forPath(spark, silver_table)
    deltaTable.alias("t").merge(
        clean_df.alias("s"),
        "t.transaction_id = s.transaction_id AND t.product_id = s.product_id")                .whenMatchedUpdateAll()                .whenNotMatchedInsertAll()                .execute()
else:
    clean_df.write.format("delta").mode("overwrite").partitionBy("sales_date").save(silver_table)

print("Silver transformation complete. Rows:", clean_df.count())
