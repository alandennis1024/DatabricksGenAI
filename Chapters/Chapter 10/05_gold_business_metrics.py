# Databricks notebook source
# MAGIC %md
# MAGIC # 05 — Pattern 3: publish Gold metrics with explicit business definitions
# MAGIC
# MAGIC Input: quality-checked `supplier_shipments_silver` and
# MAGIC `apex_shipments_silver`. Gold metrics are **deterministically computed**;
# MAGIC no LLM decides whether a shipment is on time or a load is heavy.
# MAGIC
# MAGIC - Steel reliability: observed arrival <= ETA + **two hours**. Open
# MAGIC   deliveries are excluded from the completed-delivery denominator.
# MAGIC - Chicago heavy load: strictly > **5,000 kg**, destination in ORD/MDW/GYY.
# MAGIC - Week is a Monday-start calendar week in the workspace's configured time
# MAGIC   zone. The sample is seeded for the running week by notebook 00.

# COMMAND ----------
from pyspark.sql import functions as F
from cobalt_ch10 import name, notebook_settings

s = notebook_settings(dbutils)
supplier_silver = name(s, "supplier_shipments_silver")
apex_silver = name(s, "apex_shipments_silver")
supplier_gold = name(s, "ch10_supplier_reliability_gold")
apex_gold = name(s, "ch10_apex_heavy_loads_gold")

# COMMAND ----------
# Spark SQL uses timestamp arithmetic, not a prompted LLM estimate.
supplier_gold_frame = spark.sql(f"""
SELECT
  'New Steel Supplier' AS supplier_name,
  COUNT(*) AS valid_supplier_rows,
  SUM(CASE WHEN actual_arrival_timestamp IS NOT NULL THEN 1 ELSE 0 END)
    AS completed_deliveries,
  SUM(CASE WHEN actual_arrival_timestamp IS NOT NULL
             AND actual_arrival_timestamp <= eta_timestamp + INTERVAL 2 HOURS
           THEN 1 ELSE 0 END) AS on_time_deliveries,
  ROUND(AVG(CASE WHEN actual_arrival_timestamp IS NULL THEN NULL
                 WHEN actual_arrival_timestamp <= eta_timestamp + INTERVAL 2 HOURS
                   THEN 1.0 ELSE 0.0 END), 4) AS on_time_rate,
  2 AS grace_hours
FROM {supplier_silver}
""")
supplier_gold_frame.write.format("delta").mode("overwrite").saveAsTable(supplier_gold)

# COMMAND ----------
apex_gold_frame = spark.sql(f"""
SELECT
  DATE_TRUNC('WEEK', start_timestamp) AS week_start,
  COUNT(*) AS total_valid_shipments,
  SUM(CASE WHEN cargo_weight_kg > 5000
               AND destination_code IN ('ORD', 'MDW', 'GYY')
           THEN 1 ELSE 0 END) AS chicago_heavy_loads
FROM {apex_silver}
GROUP BY DATE_TRUNC('WEEK', start_timestamp)
""")
apex_gold_frame.write.format("delta").mode("overwrite").saveAsTable(apex_gold)

# COMMAND ----------
row = spark.table(supplier_gold).first().asDict()
print("Supplier Gold:", row)
assert int(row["completed_deliveries"]) >= 5
assert 0.0 <= float(row["on_time_rate"]) <= 1.0
print(f"[PASS] {supplier_gold} computed from validated Silver records")
for result in spark.table(apex_gold).collect():
    print("Apex weekly Gold:", result.asDict())
print(f"[PASS] {apex_gold} computed; 06 will test the specific Genie benchmark queries")
