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

# MAGIC %pip install mlflow
# MAGIC %restart_python

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

try:
    username = spark.conf.get("spark.databricks.notebook.userName")
except:
    username = "unknown"
if username == "unknown":
    username = spark.sql("SELECT current_user()").collect()[0][0]
print(f"Current user: {username}")


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
import sys

# Add the code path to sys.path so we can import the model
sys.path.insert(0, "data_quality_agent_model/code")
# Import and instantiate the model class
from data_quality_agent_model.python_model import DataQualityAgentModel
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

# Set experiment
mlflow.set_experiment(f"/Users/{username}/data_quality_agent_experiment")

# Log model with instance instead of file path
with mlflow.start_run(run_name="data_quality_agent_v1") as run:
    mlflow.pyfunc.log_model(
        name="data_quality_agent",
        python_model=DataQualityAgentModel(),
        code_paths=["data_quality_agent_model/code/data_quality_agent"],
        artifacts={
            "system_prompt": "data_quality_agent_model/artifacts/system_prompt.txt",
            "example_rules": "data_quality_agent_model/artifacts/example_rules.json",
        },
        signature=signature,
        input_example=sample_input,
        pip_requirements="data_quality_agent_model/requirements.txt",
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

# COMMAND ----------

