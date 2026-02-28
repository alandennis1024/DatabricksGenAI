# Databricks notebook source

# MAGIC %md
# MAGIC # Chapter 13 — Model Versioning and Aliases
# MAGIC
# MAGIC This notebook demonstrates how to use aliases as stable pointers for
# MAGIC deployment pipelines. Aliases let us promote models without changing
# MAGIC deployment configuration.
# MAGIC
# MAGIC **Continues from:** `01_model_packaging` (model registered in Unity Catalog)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Set Up Variables
# MAGIC
# MAGIC These should match the catalog and schema used during model registration.

# COMMAND ----------

CATALOG = "demo"
SCHEMA  = "finance"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Assign a Model Alias
# MAGIC
# MAGIC Aliases act as stable pointers that deployment configurations reference.
# MAGIC Common aliases include "champion" (current production), "challenger"
# MAGIC (candidate under evaluation), and "archived" (retired version).

# COMMAND ----------

from mlflow import MlflowClient

client = MlflowClient()

model_name = f"{CATALOG}.{SCHEMA}.data_quality_agent"

# Point the "champion" alias at version 4
client.set_registered_model_alias(
    name=model_name,
    alias="champion",
    version=4
)

print(f"Alias 'champion' now points to version 4 of {model_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. DAB Configuration — Alias-Based Job
# MAGIC
# MAGIC In a DAB configuration, we reference the alias rather than a version number.
# MAGIC The deployment configuration stays the same across environments.
# MAGIC
# MAGIC ```yaml
# MAGIC resources:
# MAGIC   jobs:
# MAGIC     inference_job:
# MAGIC       name: "data-quality-agent-inference"
# MAGIC       tasks:
# MAGIC         - task_key: run_inference
# MAGIC           notebook_task:
# MAGIC             notebook_path: ./notebooks/run_agent.py
# MAGIC             base_parameters:
# MAGIC               model_name: "${var.catalog}.${var.schema}.data_quality_agent@champion"
# MAGIC ```
# MAGIC
# MAGIC The only thing that changes between development and production is the value
# MAGIC of the variables, not the structure of the configuration.
