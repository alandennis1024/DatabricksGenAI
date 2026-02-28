# Databricks notebook source

# MAGIC %md
# MAGIC # Chapter 13 — MLflow Model Packaging
# MAGIC
# MAGIC This notebook demonstrates how to package a generative AI agent using MLflow's
# MAGIC standardized model format, validate it before registration, and register it in
# MAGIC Unity Catalog. This is the first gate in the deployment pipeline.
# MAGIC
# MAGIC **Prerequisites:**
# MAGIC - An MLflow experiment configured in your workspace
# MAGIC - The agent source code in the expected directory structure
# MAGIC - Unity Catalog enabled with appropriate permissions

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Define Environment Variables
# MAGIC
# MAGIC These variables are used throughout the notebook. In a DAB deployment,
# MAGIC they would be passed as parameters via `${var.catalog}` and `${var.schema}`.

# COMMAND ----------

CATALOG = "demo"
SCHEMA  = "finance"
TABLE   = "sales_transactions"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Package the Agent with `mlflow.pyfunc.log_model`
# MAGIC
# MAGIC We pass the custom model class, code dependencies, artifacts, and an
# MAGIC input/output signature. MLflow logs everything as a single versioned unit
# MAGIC to an experiment run.

# COMMAND ----------

import mlflow
from mlflow.models.signature import infer_signature
import pandas as pd

# Define a sample input and output for signature inference
sample_input = pd.DataFrame({
    "table_name": [TABLE],
    "catalog": [CATALOG],
    "schema": [SCHEMA]
})
sample_output = pd.DataFrame({
    "rules": ['[{"column": "amount", "rule": "amount > 0"}]']
})
signature = infer_signature(sample_input, sample_output)

with mlflow.start_run() as run:
    mlflow.pyfunc.log_model(
        artifact_path="data_quality_agent",
        python_model="python_model.py",           # The PyFunc model class
        code_paths=["code/data_quality_agent"],    # Custom Python modules
        artifacts={                                 # Non-code assets
            "system_prompt": "artifacts/system_prompt.txt",
            "example_rules": "artifacts/example_rules.json"
        },
        signature=signature,                        # Input/output schema
        pip_requirements="requirements.txt"         # Pinned dependencies
    )
run_id = run.info.run_id
print(f"Logged model in run: {run_id}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Validate the Model Before Registration
# MAGIC
# MAGIC This is the first gate in the deployment pipeline. We load the model,
# MAGIC run a test inference, and only register it if the output meets expectations.
# MAGIC If validation fails, the pipeline stops and nothing gets promoted.

# COMMAND ----------

import mlflow

# Load the model from the run and validate before registration
model = mlflow.pyfunc.load_model(f"runs:/{run_id}/data_quality_agent")

test_input = pd.DataFrame({
    "table_name": ["sales_transactions"],
    "catalog": ["dev_catalog"],
    "schema": ["finance"]
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
# MAGIC three-level namespace: `catalog.schema.model_name`.

# COMMAND ----------

# Passed validation — register in Unity Catalog
mlflow.set_registry_uri("databricks-uc")
model_name = f"{CATALOG}.{SCHEMA}.data_quality_agent"
mv = mlflow.register_model(
    model_uri=f"runs:/{run_id}/data_quality_agent",
    name=model_name
)
print(f"Registered version {mv.version}")
