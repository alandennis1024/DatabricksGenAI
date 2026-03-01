# Databricks notebook source

# MAGIC %md
# MAGIC # Chapter 13 — Validate and Register the Agent Model
# MAGIC
# MAGIC This notebook demonstrates the first gate in the deployment pipeline:
# MAGIC validate the packaged model and register it in Unity Catalog.
# MAGIC
# MAGIC **Continues from:** `00_create_table_and_log_model` (table created, model logged)
# MAGIC
# MAGIC **Prerequisites:**
# MAGIC - Run `00_create_table_and_log_model` first to create the seed data and log the model
# MAGIC - Unity Catalog enabled with appropriate permissions

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Configuration
# MAGIC
# MAGIC These must match the values used in `00_create_table_and_log_model`.

# COMMAND ----------

CATALOG = "demo"
SCHEMA  = "finance"
TABLE   = "sales_transactions"

username = spark.conf.get("spark.databricks.notebook.userName")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Retrieve the Run ID from the Previous Notebook
# MAGIC
# MAGIC We look up the most recent run in the experiment that `00` created.
# MAGIC In a CI/CD pipeline, this would be passed as a parameter.

# COMMAND ----------

import mlflow

mlflow.set_experiment(f"/Users/{username}/data_quality_agent_experiment")

experiment = mlflow.get_experiment_by_name(
    f"/Users/{username}/data_quality_agent_experiment"
)

# Get the most recent run
runs = mlflow.search_runs(
    experiment_ids=[experiment.experiment_id],
    order_by=["start_time DESC"],
    max_results=1
)
run_id = runs.iloc[0]["run_id"]
print(f"Using run_id from 00_create_table_and_log_model: {run_id}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Validate the Model Before Registration
# MAGIC
# MAGIC This is the first gate in the deployment pipeline. We load the model
# MAGIC from the run, pass it a known input, and verify the output meets our
# MAGIC expectations. If the validation fails, the pipeline stops and nothing
# MAGIC gets promoted.

# COMMAND ----------

import pandas as pd

# Load the model from the run and validate before registration
model = mlflow.pyfunc.load_model(f"runs:/{run_id}/data_quality_agent")

test_input = pd.DataFrame({
    "table_name": [TABLE],
    "catalog": [CATALOG],
    "schema": [SCHEMA]
})
result = model.predict(test_input)

# Gate: the agent must produce at least one rule without errors
assert len(result) > 0, "Agent must produce at least one rule"

print(f"Validation passed — {len(result)} rules generated")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Register in Unity Catalog
# MAGIC
# MAGIC After validation passes, we register the model in Unity Catalog using the
# MAGIC three-level namespace: `catalog.schema.model_name`. Unity Catalog assigns
# MAGIC the version number automatically.

# COMMAND ----------

mlflow.set_registry_uri("databricks-uc")
model_name = f"{CATALOG}.{SCHEMA}.data_quality_agent"
mv = mlflow.register_model(
    model_uri=f"runs:/{run_id}/data_quality_agent",
    name=model_name
)
print(f"Registered {model_name} version {mv.version}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next Steps
# MAGIC
# MAGIC The model is now registered in Unity Catalog. Use **02_versioning_aliases**
# MAGIC to assign an alias (such as "champion") for deployment.
