# Databricks notebook source
# MAGIC %md
# MAGIC # 99 — Controlled cleanup of Chapter 10 tables
# MAGIC
# MAGIC **Read before running.** Nothing is dropped by default. Set
# MAGIC `confirm=yes` only when you are sure the configured chapter schema
# MAGIC contains **your** Chapter 10 synthetic lab tables.
# MAGIC
# MAGIC This notebook never drops catalogs, schemas, volumes, external locations,
# MAGIC other chapters' assets, or Genie Agents. Volume files and the optional
# MAGIC Lakeflow pipeline must be reviewed and deleted separately, if appropriate.

# COMMAND ----------
from cobalt_ch10 import name, notebook_settings, volume_path

s = notebook_settings(dbutils)
try:
    dbutils.widgets.get("confirm")
except Exception:
    dbutils.widgets.dropdown("confirm", "no", ["no", "yes"])
confirm = dbutils.widgets.get("confirm").lower()
if confirm not in ("no", "yes"):
    raise ValueError("confirm must be yes or no")

short_names = [
    "ch10_review_evidence", "ch10_synthetic_telemetry",
    "ch10_supplier_reliability_gold", "ch10_apex_heavy_loads_gold",
    "ch10_supplier_quarantine", "ch10_apex_quarantine",
    "supplier_shipments_silver", "apex_shipments_silver",
    "supplier_shipments_bronze", "apex_shipping_logs",
]
print("Tables proposed for deletion:")
for short in short_names:
    print("  ", name(s, short))
print("The UC volume will be RETAINED:", volume_path(s))

# COMMAND ----------
if confirm != "yes":
    print("[SKIP] Nothing deleted. Set confirm=yes explicitly to drop only these tables")
else:
    # Do not drop objects whose names are chosen by the user or a broader pattern.
    for short in short_names:
        full = name(s, short)
        spark.sql(f"DROP TABLE IF EXISTS {full}")
        print(f"[PASS] Dropped lab table if present: {full}")
    print("[PASS] Listed Chapter 10 tables dropped; schema and volume remain untouched")
    print("Manually review retained source CSVs and pipeline/Genie assets before deleting")
