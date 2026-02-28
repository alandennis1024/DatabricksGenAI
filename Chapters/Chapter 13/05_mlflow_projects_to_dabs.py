# Databricks notebook source

# MAGIC %md
# MAGIC # Chapter 13 — From MLflow Projects to DABs
# MAGIC
# MAGIC MLflow Projects is being superseded by Databricks Asset Bundles (DABs).
# MAGIC This notebook shows how the same parameterized train-and-evaluate pipeline
# MAGIC translates from an MLproject file to a DAB configuration.
# MAGIC
# MAGIC The key difference: DABs focus on deployment (getting code and infrastructure
# MAGIC into the right environment), while Projects focused on reproducible execution.
# MAGIC DABs can do everything Projects can do and more.

# COMMAND ----------

# MAGIC %md
# MAGIC ## DAB Configuration — Train and Evaluate Pipeline
# MAGIC
# MAGIC The parameterized entry points, pinned environment, and dependency ordering
# MAGIC all carry over from MLflow Projects, but now the pipeline is a deployable
# MAGIC resource rather than something we run from the command line.
# MAGIC
# MAGIC ```yaml
# MAGIC resources:
# MAGIC   jobs:
# MAGIC     train_and_evaluate:
# MAGIC       name: "train-data-quality-agent"
# MAGIC       tasks:
# MAGIC         - task_key: train
# MAGIC           notebook_task:
# MAGIC             notebook_path: ./notebooks/train.py
# MAGIC             base_parameters:
# MAGIC               catalog: "${var.catalog}"
# MAGIC               schema: "${var.schema}"
# MAGIC               temperature: "0.1"
# MAGIC         - task_key: evaluate
# MAGIC           depends_on:
# MAGIC             - task_key: train
# MAGIC           notebook_task:
# MAGIC             notebook_path: ./notebooks/evaluate.py
# MAGIC             base_parameters:
# MAGIC               catalog: "${var.catalog}"
# MAGIC               schema: "${var.schema}"
# MAGIC ```
# MAGIC
# MAGIC The evaluate task only runs after training completes, just like calling
# MAGIC separate entry points in an MLproject.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Deploy the Pipeline
# MAGIC
# MAGIC We deploy with a single CLI command. The pipeline runs in the target
# MAGIC environment with the right variables.

# COMMAND ----------

# MAGIC %sh
# MAGIC # Deploy to the development environment
# MAGIC databricks bundle deploy --target dev
