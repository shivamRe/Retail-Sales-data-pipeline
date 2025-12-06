# 04_manifest.py
import os, json, datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import count, sum as _sum

spark = SparkSession.builder.appName("manifest").getOrCreate()

GOLD_INPUT = os.getenv("GOLD_INPUT", "local_output/gold/")
MANIFEST_OUTPUT = os.getenv("MANIFEST_OUTPUT", "local_output/manifests/")

def generate_manifest():
    df = spark.read.parquet(GOLD_INPUT)
    stats = df.select(_sum("total_sales").alias("total_sales")).collect()[0].asDict()
    record_count = df.count()
    manifest = {
        "generated_at": datetime.datetime.utcnow().isoformat(),
        "record_count": record_count,
        "total_sales": float(stats.get("total_sales") or 0)
    }
    os.makedirs(MANIFEST_OUTPUT, exist_ok=True)
    path = os.path.join(MANIFEST_OUTPUT, f"manifest_{datetime.datetime.utcnow().strftime('%Y%m%dT%H%M%SZ')}.json")
    with open(path, "w") as f:
        json.dump(manifest, f, indent=2)
    print("Wrote manifest:", path)

if __name__ == "__main__":
    generate_manifest()
