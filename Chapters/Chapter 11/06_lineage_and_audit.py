# Databricks notebook source
# MAGIC %md
# MAGIC # 06 — Observed lineage and audit-evidence boundaries
# MAGIC System table lineage covers supported captured events; it does **not**
# MAGIC reconstruct an entire model's training path. Time-based audit joins do
# MAGIC **not** establish caller identity or a shared request.

# COMMAND ----------
from cobalt_ch11 import audited_request_match, qualified_schema

dbutils.widgets.text("catalog", "cobalt_ops")
dbutils.widgets.text("schema", "ch11")
dbutils.widgets.dropdown("query_system_lineage", "no", ["no", "yes"])
ns = qualified_schema(dbutils.widgets.get("catalog"),
                      dbutils.widgets.get("schema"))

# COMMAND ----------
# Synthetic fixture demonstrates matching on a shared verified request key.
inference = {"request_id": "demo-001", "endpoint": "cobalt-demo"}
audit = {"request_id": "demo-001", "endpoint": "cobalt-demo",
         "authenticated_principal": "synthetic.user@example.invalid"}
assert audited_request_match(inference, audit)
assert not audited_request_match(inference, {**audit, "request_id": "other"})
print("[PASS] Exact-key fixture matching; time proximity alone is insufficient")

# COMMAND ----------
if dbutils.widgets.get("query_system_lineage") == "yes":
    # Requires access to system.access.table_lineage and recent captured events.
    query = f"""
    SELECT source_table_full_name, target_table_full_name, event_time
    FROM system.access.table_lineage
    WHERE source_table_full_name = '{ns}.ch11_source_manuals'
       OR target_table_full_name = '{ns}.ch11_public_manual_chunks'
    ORDER BY event_time DESC LIMIT 100
    """
    evidence = spark.sql(query)
    display(evidence)
    print("[REVIEW] These are observed lineage events, not a complete training audit")
else:
    print("[SKIP] System table lineage access not requested")
