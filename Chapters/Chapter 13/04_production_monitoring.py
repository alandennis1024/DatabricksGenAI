# Databricks notebook source

# MAGIC %md
# MAGIC # Chapter 13 — Production Monitoring with Inference Tables
# MAGIC
# MAGIC This notebook demonstrates how to enable production monitoring on a
# MAGIC deployed serving endpoint using inference tables. Every request and response
# MAGIC is logged to Delta tables automatically — no application code changes needed.
# MAGIC
# MAGIC **Continues from:** `08_model_serving` (endpoint already created)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Set Up Variables

# COMMAND ----------

CATALOG = "demo"
SCHEMA  = "finance"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Enable Inference Tables on the Serving Endpoint
# MAGIC
# MAGIC When enabled, Databricks automatically logs every request, response, and
# MAGIC trace to a Delta table. This gives us a complete audit trail without
# MAGIC adding any instrumentation to our application code.

# COMMAND ----------

# MAGIC %sh
# MAGIC # Enable inference tables when updating the serving endpoint
# MAGIC databricks serving-endpoints update-config data-quality-agent-endpoint \
# MAGIC   --json '{
# MAGIC     "served_entities": [{
# MAGIC       "entity_name": "dev_catalog.finance.data_quality_agent",
# MAGIC       "entity_version": "5",
# MAGIC       "workload_size": "Small",
# MAGIC       "scale_to_zero_enabled": true
# MAGIC     }],
# MAGIC     "auto_capture_config": {
# MAGIC       "catalog_name": "'"${CATALOG}"'",
# MAGIC       "schema_name": "'"${SCHEMA}"'",
# MAGIC       "table_name_prefix": "dq_agent"
# MAGIC     }
# MAGIC   }'

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Query the Inference Table
# MAGIC
# MAGIC Once enabled, a set of Delta tables (such as `dq_agent_payload`) is created
# MAGIC in the specified catalog and schema. We can query this data for auditing,
# MAGIC monitoring drift, identifying edge cases, and building evaluation datasets.

# COMMAND ----------

# Query recent inference payloads
df = spark.sql(f"""
    SELECT *
    FROM {CATALOG}.{SCHEMA}.dq_agent_payload
    ORDER BY timestamp DESC
    LIMIT 10
""")
display(df)
