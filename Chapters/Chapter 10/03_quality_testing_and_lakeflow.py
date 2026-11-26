# Databricks notebook source
# MAGIC %md
# MAGIC # 03 — Pattern 2: tested Silver transformations and quality gates
# MAGIC
# MAGIC The chapter's code uses Lakeflow `@dp.expect_or_drop` / `@dp.expect_or_fail`.
# MAGIC Here we demonstrate **equivalent business predicates as a regular Spark
# MAGIC batch transformation**, runnable without provisioning a Lakeflow pipeline.
# MAGIC The actual optional Lakeflow source is `pipelines/lakeflow_supplier_apex.py`.
# MAGIC
# MAGIC **Important:** Use this notebook OR configure the optional Lakeflow
# MAGIC pipeline in the same target schema, not both. They create the same Silver
# MAGIC table names and should not compete for ownership. Run `02` first.
# MAGIC Invalid IDs fail the notebook; invalid measurements are quarantined.

# COMMAND ----------
from pyspark.sql import functions as F
from cobalt_ch10 import name, notebook_settings

s = notebook_settings(dbutils)
supplier_bronze = name(s, "supplier_shipments_bronze")
apex_bronze = name(s, "apex_shipping_logs")
supplier_silver = name(s, "supplier_shipments_silver")
apex_silver = name(s, "apex_shipments_silver")

supplier = spark.table(supplier_bronze)
apex = spark.table(apex_bronze)

# COMMAND ----------
# Mirrors @dp.expect_or_fail("valid_id", "transaction_id IS NOT NULL")
# with an additional guard against blank strings.
for label, frame in (("supplier", supplier), ("apex", apex)):
    missing_id = frame.filter(
        F.col("transaction_id").isNull() | (F.length(F.trim("transaction_id")) == 0)
    ).count()
    if missing_id:
        raise RuntimeError(f"[FAIL] {label}: {missing_id} empty transaction IDs (fail update)")
    print(f"[PASS] {label} transaction IDs are not blank")

# COMMAND ----------
# Rules authored by a person after considering the assistant's proposed checks.
supplier_valid = (
    F.col("quantity").isNotNull() & (F.col("quantity") > 0)
    & F.col("eta_timestamp").isNotNull()
)
supplier_rejected = supplier.filter(~supplier_valid)
supplier_clean = supplier.filter(supplier_valid)

apex_valid = (
    F.col("cargo_weight_kg").isNotNull() & (F.col("cargo_weight_kg") > 0)
    & F.col("start_timestamp").isNotNull()
    & F.coalesce(F.col("destination_code").rlike("^[A-Z]{3}$"), F.lit(False))
)
apex_rejected = apex.filter(~apex_valid)
apex_clean = apex.filter(apex_valid)

# Each rerun deterministically rebuilds the same sample Gold inputs.
# Overwrite is intentional in this teaching lab; do not target real datasets.
supplier_clean.write.format("delta").mode("overwrite").saveAsTable(supplier_silver)
apex_clean.write.format("delta").mode("overwrite").saveAsTable(apex_silver)
supplier_rejected.write.format("delta").mode("overwrite").saveAsTable(
    name(s, "ch10_supplier_quarantine")
)
apex_rejected.write.format("delta").mode("overwrite").saveAsTable(
    name(s, "ch10_apex_quarantine")
)

# COMMAND ----------
counts = {
    "supplier_silver": spark.table(supplier_silver).count(),
    "apex_silver": spark.table(apex_silver).count(),
    "supplier_rejected": supplier_rejected.count(),
    "apex_rejected": apex_rejected.count(),
}
print("Observed Data Quality results:", counts)
assert counts["supplier_silver"] >= 6
assert counts["apex_silver"] >= 8
assert counts["supplier_rejected"] >= 1
assert counts["apex_rejected"] >= 2
print("[PASS] Silver data and quarantined rows verified")
print("Next: 04 synthetic anomaly scenarios; 05 reviewed Gold business metrics")
