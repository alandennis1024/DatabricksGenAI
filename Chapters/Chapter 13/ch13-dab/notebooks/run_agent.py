# Databricks notebook source
# MAGIC %md
# MAGIC # Data quality agent - batch inference
# MAGIC
# MAGIC Loads the registered agent by alias and generates data quality rules
# MAGIC for the configured table. Parameters come from the DAB job definition
# MAGIC (`resources.jobs.inference_job` in `databricks.yml`), so the same
# MAGIC notebook runs unchanged in every environment.

# COMMAND ----------

dbutils.widgets.text("model_name", "", "Registered model (name@alias)")
dbutils.widgets.text("data_catalog", "demo", "Catalog of the table to analyze")
dbutils.widgets.text("data_schema", "finance", "Schema of the table to analyze")
dbutils.widgets.text("table", "sales_transactions", "Table to analyze")

model_name = dbutils.widgets.get("model_name")
data_catalog = dbutils.widgets.get("data_catalog")
data_schema = dbutils.widgets.get("data_schema")
table = dbutils.widgets.get("table")

assert model_name, "model_name parameter is required (name@alias)"

# COMMAND ----------

import mlflow
import pandas as pd

# The alias reference resolves to the currently promoted version,
# so this job picks up promotions without any configuration change.
model = mlflow.pyfunc.load_model(f"models:/{model_name}")

result = model.predict(pd.DataFrame({
    "table_name": [table],
    "catalog": [data_catalog],
    "schema": [data_schema],
}))

rules = result["rules"].iloc[0]
print(f"Agent produced rules for {data_catalog}.{data_schema}.{table}")

# COMMAND ----------

import pyspark.sql.functions as F

output = spark.createDataFrame(pd.DataFrame({
    "table_name": [table],
    "catalog": [data_catalog],
    "schema": [data_schema],
    "model": [model_name],
    "rules_json": [rules],
})).withColumn("generated_at", F.current_timestamp())

output_table = f"{data_catalog}.{data_schema}.data_quality_rules"
output.write.mode("append").saveAsTable(output_table)

print(f"Rules appended to {output_table}")
display(output)
