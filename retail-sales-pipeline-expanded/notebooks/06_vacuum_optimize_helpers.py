# Helpers notebook: vacuum and optimize scheduling
import os
from datetime import datetime, timedelta
gold_path = dbutils.widgets.get("gold_path") if 'dbutils' in globals() else os.getenv("GOLD_PATH", "/mnt/adls/retail/gold/")
retention_hours = int(dbutils.widgets.get("retention_hours")) if 'dbutils' in globals() else 168  # 7 days default

try:
    spark.sql(f"VACUUM delta.`{gold_path}` RETAIN {retention_hours} HOURS")
    print("VACUUM triggered with retention:", retention_hours)
except Exception as e:
    print("VACUUM failed:", e)
