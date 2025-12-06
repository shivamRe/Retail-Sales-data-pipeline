# tests/test_transform.py
# Simple pytest to validate transformation logic on a tiny sample dataset
import os
import pandas as pd
from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local[2]").appName("test").getOrCreate()

def create_sample_csv(path):
    os.makedirs(path, exist_ok=True)
    df = pd.DataFrame([
        {'transaction_id':'t1','product_id':'p1','quantity':2,'unit_price':10.0,'sales_date':'2025-01-01','region':'north','customer_id':'c1'},
        {'transaction_id':'t2','product_id':'p2','quantity':1,'unit_price':5.0,'sales_date':'2025-01-02','region':'south','customer_id':'c2'}
    ])
    df.to_csv(os.path.join(path,'sample.csv'), index=False)

def test_transform_pipeline(tmp_path):
    raw = tmp_path/"data"/"raw"
    create_sample_csv(str(raw))
    os.environ['RAW_INPUT'] = str(raw)
    os.environ['BRONZE_OUTPUT'] = str(tmp_path/"bronze")
    os.environ['BRONZE_INPUT'] = str(tmp_path/"bronze")
    os.environ['SILVER_OUTPUT'] = str(tmp_path/"silver")
    from notebooks import _run_ingest_transform_for_test as runner
    runner.run_all(str(raw), str(tmp_path/"silver"))
    # read silver and assert
    df = spark.read.parquet(str(tmp_path/"silver"))
    assert df.count() == 2
