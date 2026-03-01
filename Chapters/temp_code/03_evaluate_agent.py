# Databricks notebook source

# MAGIC %md
# MAGIC # Chapter 13 — Evaluate the Agent and Log Metrics
# MAGIC
# MAGIC This notebook runs the registered agent against test data, measures
# MAGIC performance, and logs metrics to the experiment. These metrics are
# MAGIC what the promotion decision in `04_tracking_promotion` queries.
# MAGIC
# MAGIC Run this notebook multiple times with different parameters (temperature,
# MAGIC prompt version, LLM endpoint) to create candidate runs for comparison.
# MAGIC
# MAGIC **Continues from:** `01_model_packaging` (model registered in Unity Catalog)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Configuration

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
# MAGIC ## 2. Load the Registered Model
# MAGIC
# MAGIC We load the model from Unity Catalog rather than from a run,
# MAGIC since it has already been validated and registered.

# COMMAND ----------

import mlflow
import pandas as pd
import time

mlflow.set_experiment(f"/Users/{username}/data_quality_agent_experiment")

model_name = f"{CATALOG}.{SCHEMA}.data_quality_agent"
model = mlflow.pyfunc.load_model(f"models:/{model_name}@champion")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Run the Agent and Log Metrics
# MAGIC
# MAGIC We time the prediction, count the rules generated, and log everything
# MAGIC as an MLflow run. The parameters record the configuration that produced
# MAGIC these results; the metrics record how the agent performed.

# COMMAND ----------

with mlflow.start_run(run_name="agent_eval_v1") as run:
    # Log the parameters that shape agent behavior
    mlflow.log_param("llm_endpoint", "databricks-meta-llama-3-3-70b-instruct")
    mlflow.log_param("temperature", 0.1)
    mlflow.log_param("max_tokens", 1024)
    mlflow.log_param("model_name", model_name)

    # Run the agent and measure performance
    test_input = pd.DataFrame({
        "table_name": [TABLE],
        "catalog": [CATALOG],
        "schema": [SCHEMA]
    })

    start = time.time()
    result = model.predict(test_input)
    elapsed = time.time() - start

    import json
    rules = json.loads(result["rules"].iloc[0])

    # Log metrics
    mlflow.log_metric("rules_generated", len(rules))
    mlflow.log_metric("latency_seconds", elapsed)

    print(f"Generated {len(rules)} rules in {elapsed:.2f}s")
    print(f"Run ID: {run.info.run_id}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. (Optional) Run Again with Different Parameters
# MAGIC
# MAGIC To create multiple candidate runs for the promotion decision in
# MAGIC `04_tracking_promotion`, re-run the cell above after changing parameters
# MAGIC (e.g., use a different model version, temperature, or prompt).

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next Steps
# MAGIC
# MAGIC Multiple evaluation runs now exist in the experiment with `rules_generated`
# MAGIC and `latency_seconds` metrics. Use **04_tracking_promotion** to query these
# MAGIC runs and promote the best one.
