# Databricks notebook source
# MAGIC %md
# MAGIC # 07 — Simulated unsafe manual incident and containment
# MAGIC This notebook **does not delete real files or shut down any endpoint**.
# MAGIC It creates reviewable evidence that a retired document is excluded from
# MAGIC the synthetic public retrieval set. Real containment needs incident approval.

# COMMAND ----------
from cobalt_ch11 import (MANUALS, incident_disposition,
                         qualified_schema)

dbutils.widgets.text("catalog", "cobalt_ops")
dbutils.widgets.text("schema", "ch11")
ns = qualified_schema(dbutils.widgets.get("catalog"),
                      dbutils.widgets.get("schema"))

retired = next(m for m in MANUALS if m.manual_id == "XM400-RETIRED")
assert incident_disposition(retired) == "QUARANTINE"
source = f"{ns}.ch11_public_manual_chunks"
assert spark.table(source).where("manual_id = 'XM400-RETIRED'").count() == 0

# COMMAND ----------
incident = [{
    "incident_id": "CH11-SYNTHETIC-001",
    "affected_manual_id": retired.manual_id,
    "manual_revision": retired.revision,
    "observed_risk": "Synthetic obsolete instructions could enter retrieval",
    "containment": "Excluded from PUBLIC chunk table",
    "evidence_status": "DEMO_CHECK_PASSED",
    "restore_service_status": "NOT_APPLICABLE_SYNTHETIC",
}]
incident_table = f"{ns}.ch11_incident_reviews"
(spark.createDataFrame(incident).write.format("delta")
 .mode("overwrite").saveAsTable(incident_table))
print("[PASS] Retired manual excluded and synthetic incident documented")
print("[REVIEW] Real incident: contain, preserve evidence, verify index sync and approvals")
