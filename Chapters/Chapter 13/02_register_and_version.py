# Databricks notebook source

# MAGIC %md
# MAGIC # Chapter 13 - Step 2: Register, Version, and Alias
# MAGIC
# MAGIC This notebook corresponds to the **Unity Catalog Integration** and **Versioning**
# MAGIC sections of Chapter 13. It demonstrates:
# MAGIC
# MAGIC 1. Registering the data quality agent in Unity Catalog
# MAGIC 2. Creating a new version by re-registering
# MAGIC 3. Setting aliases (champion, challenger) for deployment
# MAGIC 4. Loading a model by alias
# MAGIC
# MAGIC **Prerequisites:**
# MAGIC - Run **01_create_table_and_log_model.py** first to create the model run

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

CATALOG = "demo"
SCHEMA  = "finance"
TABLE   = "sales_transactions"

# IMPORTANT: Replace this with the run_id from notebook 01.
# You can find it in the output of the log_model cell or in the MLflow UI.
run_id = "<REPLACE_WITH_RUN_ID_FROM_NOTEBOOK_01>"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Register the model in Unity Catalog
# MAGIC
# MAGIC We point MLflow at the Unity Catalog registry and register the model
# MAGIC using the three-level namespace: catalog.schema.model_name.

# COMMAND ----------

import mlflow

mlflow.set_registry_uri("databricks-uc")

model_name = f"{CATALOG}.{SCHEMA}.data_quality_agent"

# Register the model - this creates version 1
result = mlflow.register_model(
    model_uri=f"runs:/{run_id}/data_quality_agent",
    name=model_name
)

print(f"Registered model: {result.name}")
print(f"Version: {result.version}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create a second version
# MAGIC
# MAGIC Every call to `register_model` for an existing model creates a new version.
# MAGIC In practice, this would be a new run with updated code or prompts.
# MAGIC Here we re-register the same run to illustrate the versioning behavior.

# COMMAND ----------

result_v2 = mlflow.register_model(
    model_uri=f"runs:/{run_id}/data_quality_agent",
    name=model_name
)

print(f"Version: {result_v2.version}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Set aliases
# MAGIC
# MAGIC Aliases are named pointers to specific versions. They allow our deployment
# MAGIC configuration to reference a stable name ("champion") rather than a version
# MAGIC number that changes with every registration.

# COMMAND ----------

from mlflow import MlflowClient

client = MlflowClient()

# Point "champion" at version 1 (our validated production model)
client.set_registered_model_alias(
    name=model_name,
    alias="champion",
    version=1
)
print(f"Set 'champion' alias -> version 1")

# Point "challenger" at version 2 (the candidate we want to evaluate)
client.set_registered_model_alias(
    name=model_name,
    alias="challenger",
    version=2
)
print(f"Set 'challenger' alias -> version 2")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Inspect aliases
# MAGIC
# MAGIC We can look up which version an alias points to, and which aliases
# MAGIC are set on a given model.

# COMMAND ----------

# Get the version that "champion" points to
champion_version = client.get_model_version_by_alias(
    name=model_name,
    alias="champion"
)
print(f"'champion' points to version {champion_version.version}")

# List all versions
versions = client.search_model_versions(f"name='{model_name}'")
for v in versions:
    aliases = v.aliases if v.aliases else []
    print(f"  Version {v.version}: aliases={aliases}, status={v.status}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load a model by alias
# MAGIC
# MAGIC In production, we reference the alias rather than a specific version.
# MAGIC This way, promoting a new version is as simple as moving the alias.

# COMMAND ----------

import pandas as pd

champion_model = mlflow.pyfunc.load_model(f"models:/{model_name}@champion")

result = champion_model.predict(pd.DataFrame({
    "table_name": [TABLE],
    "catalog": [CATALOG],
    "schema": [SCHEMA],
}))

import json
rules = json.loads(result["rules"].iloc[0])
print(f"Champion model generated {len(rules)} rules")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Promote the challenger
# MAGIC
# MAGIC If the challenger performs well, we promote it by moving the alias.
# MAGIC Rollback is equally simple: just point the alias back.

# COMMAND ----------

# Promote: move champion to version 2
client.set_registered_model_alias(
    name=model_name,
    alias="champion",
    version=2
)
print("Promoted version 2 to 'champion'")

# Rollback: move champion back to version 1
# client.set_registered_model_alias(name=model_name, alias="champion", version=1)
# print("Rolled back to version 1")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next Steps
# MAGIC
# MAGIC The model is now registered, versioned, and aliased in Unity Catalog.
# MAGIC Use **03_tracking_and_tracing.py** to explore MLflow Tracking and LLM Tracing.
