# Databricks notebook source

# MAGIC %md
# MAGIC # Chapter 13 — Integration with Deployment Pipeline
# MAGIC
# MAGIC This notebook demonstrates how MLflow artifacts and the Unity Catalog
# MAGIC registry integrate with CI/CD automation. We cover the model URI pattern,
# MAGIC DAB configuration for cross-environment deployment, and direct endpoint
# MAGIC updates via the CLI.
# MAGIC
# MAGIC **Continues from:** `02_versioning_aliases` (aliases assigned)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Model URI Pattern
# MAGIC
# MAGIC When we reference a model in a DAB or notebook, we use a URI that includes
# MAGIC the alias. This always resolves to whichever version the alias currently
# MAGIC points to.

# COMMAND ----------

# The model URI pattern — always resolves to the current champion
model_uri = "models:/dev_catalog.finance.data_quality_agent@champion"
print(f"Model URI: {model_uri}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. DAB Configuration — Cross-Environment Inference Job
# MAGIC
# MAGIC The same YAML works across development, staging, and production.
# MAGIC The only thing that changes is the value of the variables.
# MAGIC
# MAGIC ```yaml
# MAGIC variables:
# MAGIC   catalog:
# MAGIC     default: dev_catalog
# MAGIC   schema:
# MAGIC     default: finance
# MAGIC
# MAGIC resources:
# MAGIC   jobs:
# MAGIC     inference_job:
# MAGIC       name: "data-quality-agent-inference"
# MAGIC       tasks:
# MAGIC         - task_key: run_inference
# MAGIC           notebook_task:
# MAGIC             notebook_path: ./notebooks/run_agent.py
# MAGIC             base_parameters:
# MAGIC               model_uri: "models:/${var.catalog}.${var.schema}.data_quality_agent@champion"
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Update a Serving Endpoint via CLI
# MAGIC
# MAGIC We can update a serving endpoint to serve a new model version with a
# MAGIC single command. This enables blue-green deployment and champion-challenger
# MAGIC testing.

# COMMAND ----------

# MAGIC %sh
# MAGIC databricks serving-endpoints update-config data-quality-agent-endpoint \
# MAGIC   --json '{
# MAGIC     "served_entities": [{
# MAGIC       "entity_name": "dev_catalog.finance.data_quality_agent",
# MAGIC       "entity_version": "5",
# MAGIC       "workload_size": "Small",
# MAGIC       "scale_to_zero_enabled": true
# MAGIC     }]
# MAGIC   }'
