# Databricks notebook source
# MAGIC %md
# MAGIC # 06 — Pattern 3: Genie Agent data product and verified answers
# MAGIC
# MAGIC This notebook runs **reviewed SQL** against the real sample Silver tables
# MAGIC and checks the expected outputs. It does **not** claim to create a Genie
# MAGIC Agent via an undocumented SDK, nor to verify Genie's generated answers.
# MAGIC
# MAGIC A data owner must configure Genie in the Databricks UI, add the governed
# MAGIC tables, apply `genie/GENERAL_INSTRUCTIONS.md`, register the SQL examples
# MAGIC from `genie/verified_queries.sql`, and run the benchmark questions in
# MAGIC `genie/benchmarks.csv`. Use a SQL warehouse and appropriate permissions.

# COMMAND ----------
from cobalt_ch10 import (
    name, notebook_settings, verified_apex_sql, verified_supplier_sql,
)

s = notebook_settings(dbutils)
supplier_sql = verified_supplier_sql(s)
apex_sql = verified_apex_sql(s)
print("Verified SQL candidate #1 — completed steel-delivery on-time rate:\n")
print(supplier_sql)
print("\nVerified SQL candidate #2 — Chicago heavy loads during the current week:\n")
print(apex_sql)

# COMMAND ----------
supplier = spark.sql(supplier_sql).first().asDict()
apex = spark.sql(apex_sql).first().asDict()
print("Observed supplier benchmark:", supplier)
print("Observed Apex benchmark:", apex)

# These expected results apply when 00–05 have been run for the current sample
# week with no additional real or foreign records in the same table names.
expected = {
    "completed_deliveries": 5, "on_time_deliveries": 3,
    "on_time_rate": 0.6, "chicago_heavy_loads": 4,
}
assert supplier["completed_deliveries"] == expected["completed_deliveries"]
assert supplier["on_time_deliveries"] == expected["on_time_deliveries"]
assert abs(float(supplier["on_time_rate"]) - expected["on_time_rate"]) < 1e-9
assert apex["chicago_heavy_loads"] == expected["chicago_heavy_loads"]
print("[PASS] Both reviewed benchmark SQL statements matched seeded data")
print("[REVIEW REQUIRED] Configure Genie and run UI benchmarks; this step is not automated")

# COMMAND ----------
print("Genie Agent configuration checklist:")
print(f"  Connect {name(s, 'supplier_shipments_silver')}")
print(f"  Connect {name(s, 'apex_shipments_silver')}")
print("  Optional: add Gold tables created by 05")
print("  Define Heavy Load as STRICTLY > 5,000 kg, Chicago Hub as ORD/MDW/GYY")
print("  Define supplier on time as observed arrival no later than ETA + 2 hours")
print("  Add both SQL examples, benchmark each query, and review results with data owner")
print("  Verify Genie credentials, Unity Catalog privileges, and row-level access")
