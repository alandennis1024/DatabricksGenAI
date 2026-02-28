# Databricks notebook source

# MAGIC %md
# MAGIC # Chapter 13 — Tracking-Based Promotion Decision
# MAGIC
# MAGIC This notebook demonstrates how to use MLflow Tracking data to make
# MAGIC automated promotion decisions. We query the experiment for the
# MAGIC best-performing run, compare it against thresholds, and promote
# MAGIC only if it qualifies.
# MAGIC
# MAGIC **Continues from:** `03_evaluate_agent` (evaluation runs logged with metrics)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Set Up Variables

# COMMAND ----------

CATALOG = "demo"
SCHEMA  = "finance"
username = spark.sql("SELECT current_user()").first()[0]

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Query Experiment Runs and Promote the Winner
# MAGIC
# MAGIC This turns promotion from a manual judgment call into an automated gate
# MAGIC in the CI/CD pipeline. We filter for runs that meet quality thresholds,
# MAGIC rank by latency, and promote the best one.

# COMMAND ----------

import mlflow
from mlflow import MlflowClient

client = MlflowClient()
experiment = mlflow.get_experiment_by_name(
    f"/Users/{username}/data_quality_agent_experiment"
)

# Find the run with the lowest latency that produced enough rules
best_runs = client.search_runs(
    experiment_ids=[experiment.experiment_id],
    filter_string="metrics.rules_generated >= 5",
    order_by=["metrics.latency_seconds ASC"],
    max_results=1
)

best = best_runs[0]
print(f"Best run: {best.info.run_id}")
print(f"  Latency: {best.data.metrics['latency_seconds']:.2f}s")
print(f"  Rules:   {int(best.data.metrics['rules_generated'])}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Register and Promote the Winning Run
# MAGIC
# MAGIC The alias update is what causes the production serving endpoint to pick
# MAGIC up the new version — no redeployment needed.

# COMMAND ----------

model_name = f"{CATALOG}.{SCHEMA}.data_quality_agent"
mv = mlflow.register_model(
    f"runs:/{best.info.run_id}/data_quality_agent", model_name
)
client.set_registered_model_alias(model_name, "champion", mv.version)

print(f"Promoted version {mv.version} to 'champion'")
