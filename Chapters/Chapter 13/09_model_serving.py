# Databricks notebook source

# MAGIC %md
# MAGIC # Chapter 13 — Model Serving Endpoint Deployment
# MAGIC
# MAGIC This notebook demonstrates how to deploy a Model Serving endpoint for
# MAGIC the data quality agent. We show both the DAB configuration approach
# MAGIC and the CLI approach.
# MAGIC
# MAGIC **Continues from:** `01_model_packaging` (model registered in Unity Catalog)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. DAB Configuration — Serving Endpoint
# MAGIC
# MAGIC We define the serving endpoint in the DAB so it deploys alongside the
# MAGIC model, app, and pipelines as a single unit.
# MAGIC
# MAGIC ```yaml
# MAGIC resources:
# MAGIC   model_serving_endpoints:
# MAGIC     data_quality_agent_endpoint:
# MAGIC       name: "data-quality-agent-endpoint"
# MAGIC       config:
# MAGIC         served_entities:
# MAGIC           - entity_name: "${var.catalog}.${var.schema}.data_quality_agent"
# MAGIC             entity_version: "1"
# MAGIC             workload_size: "Small"
# MAGIC             scale_to_zero_enabled: true
# MAGIC ```
# MAGIC
# MAGIC - **workload_size:** Small, Medium, or Large (compute resources)
# MAGIC - **scale_to_zero_enabled:** No compute cost when the endpoint is idle

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Create a Serving Endpoint via CLI
# MAGIC
# MAGIC We can also create and manage endpoints directly with the Databricks CLI.

# COMMAND ----------

# MAGIC %sh
# MAGIC # Create a new endpoint
# MAGIC databricks serving-endpoints create --json '{
# MAGIC   "name": "data-quality-agent-endpoint",
# MAGIC   "config": {
# MAGIC     "served_entities": [{
# MAGIC       "entity_name": "dev_catalog.finance.data_quality_agent",
# MAGIC       "entity_version": "1",
# MAGIC       "workload_size": "Small",
# MAGIC       "scale_to_zero_enabled": true
# MAGIC     }]
# MAGIC   }
# MAGIC }'

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Check Endpoint Status

# COMMAND ----------

# MAGIC %sh
# MAGIC databricks serving-endpoints get data-quality-agent-endpoint
