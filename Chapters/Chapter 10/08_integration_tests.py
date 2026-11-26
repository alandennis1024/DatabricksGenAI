# Databricks notebook source
# MAGIC %md
# MAGIC # 08 — Workspace acceptance tests for the connected Chapter 10 lab
# MAGIC
# MAGIC This **does** read the actual Delta tables from your Databricks workspace.
# MAGIC It is separate from offline unit tests and from running benchmarks in Genie.
# MAGIC
# MAGIC Run after `00`, `01`, `02`, `03`, `04`, `05`, and `06` in the same
# MAGIC catalog/schema for the same current-week synthetic dataset. The suite
# MAGIC verifies the exact known sample result, so it may fail if unrelated data
# MAGIC was appended into the demo tables; never point it at production tables.

# COMMAND ----------
from pyspark.sql import functions as F
from cobalt_ch10 import (
    name, notebook_settings, verified_apex_sql, verified_supplier_sql,
)

s = notebook_settings(dbutils)
supplier_bronze = name(s, "supplier_shipments_bronze")
apex_bronze = name(s, "apex_shipping_logs")
supplier_silver = name(s, "supplier_shipments_silver")
apex_silver = name(s, "apex_shipments_silver")
supplier_gold = name(s, "ch10_supplier_reliability_gold")
apex_gold = name(s, "ch10_apex_heavy_loads_gold")

# COMMAND ----------
# Live Spark assertions: these raise on incorrect outputs; no fake PASS status.
bronze_counts = {"supplier": spark.table(supplier_bronze).count(),
                 "apex": spark.table(apex_bronze).count()}
assert bronze_counts == {"supplier": 7, "apex": 10}, bronze_counts
print("[PASS] Bronze row counts match the source sample:", bronze_counts)

silver_counts = {"supplier": spark.table(supplier_silver).count(),
                 "apex": spark.table(apex_silver).count()}
assert silver_counts == {"supplier": 6, "apex": 8}, silver_counts
print("[PASS] Silver data quality removed one supplier and two carrier records")

# COMMAND ----------
invalid_supplier = spark.table(supplier_silver).filter(
    (F.col("quantity") <= 0) | F.col("transaction_id").isNull()
).count()
invalid_apex = spark.table(apex_silver).filter(
    (F.col("cargo_weight_kg") <= 0)
    | (~F.col("destination_code").rlike("^[A-Z]{3}$"))
    | F.col("start_timestamp").isNull()
).count()
assert invalid_supplier == 0
assert invalid_apex == 0
assert spark.table(name(s, "ch10_supplier_quarantine")).count() == 1
assert spark.table(name(s, "ch10_apex_quarantine")).count() == 2
print("[PASS] Negative-case records excluded and available in quarantine")

# COMMAND ----------
expected_supplier = spark.sql(verified_supplier_sql(s)).first().asDict()
expected_apex = spark.sql(verified_apex_sql(s)).first().asDict()
assert expected_supplier["completed_deliveries"] == 5
assert expected_supplier["on_time_deliveries"] == 3
assert abs(float(expected_supplier["on_time_rate"]) - 0.6) < 1e-9
assert expected_apex["chicago_heavy_loads"] == 4
print("[PASS] Verified SQL: steel on-time rate = 3/5 = 60%; Chicago loads = 4")

# COMMAND ----------
gold_supplier = spark.table(supplier_gold).first().asDict()
gold_apex_current = spark.sql(f"""
SELECT chicago_heavy_loads
FROM {apex_gold}
WHERE week_start = DATE_TRUNC('WEEK', CURRENT_TIMESTAMP())
""").first()
assert gold_supplier["completed_deliveries"] == 5
assert gold_supplier["on_time_deliveries"] == 3
assert abs(float(gold_supplier["on_time_rate"]) - 0.6) < 1e-9
assert gold_apex_current is not None and gold_apex_current["chicago_heavy_loads"] == 4
print("[PASS] Gold business metrics agree with independent Silver queries")

# COMMAND ----------
telemetry_count = spark.table(name(s, "ch10_synthetic_telemetry")).count()
assert telemetry_count == 100
print("[PASS] 100 fictional telemetry rows generated deterministically")
print("[SKIP] Genie accuracy is NOT tested by these SQL checks. Run the UI benchmarks")
print("[SKIP] Optional Lakeflow pipeline is NOT tested unless separately deployed and run")
print("[PASS] Chapter 10 Databricks batch-lab integration tests completed")
