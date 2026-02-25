# Databricks notebook source

# MAGIC %md
# MAGIC # Data Quality Agent — End-to-End Demo
# MAGIC
# MAGIC This notebook walks through three steps:
# MAGIC 1. **Create** a `sales_transactions` table with synthetic data
# MAGIC 2. **Log** the data quality agent as an MLflow PyFunc model
# MAGIC 3. **Load** the model and generate data quality rules for the table

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration
# MAGIC
# MAGIC Set the catalog and schema where the demo table and model will live.
# MAGIC Change these to match your workspace.

# COMMAND ----------

CATALOG = "demo"
SCHEMA  = "finance"
TABLE   = "sales_transactions"

spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"USE SCHEMA {SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1 — Create the sales_transactions table with synthetic data
# MAGIC
# MAGIC The table is designed to give the agent interesting columns to reason about:
# MAGIC numeric ranges, date ordering, email formats, enum values, and cross-column
# MAGIC relationships.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    DoubleType, DateType, TimestampType
)
import random
from datetime import date, timedelta

NUM_ROWS = 2000

# ---------- helpers ----------
random.seed(42)

start_date = date(2024, 1, 1)
statuses = ["completed", "pending", "cancelled", "refunded"]
payment_methods = ["credit_card", "debit_card", "wire_transfer", "paypal"]
domains = ["example.com", "testmail.org", "acme.co", "bigcorp.net"]

def random_email(i):
    return f"customer_{i}@{random.choice(domains)}"

def random_date_pair():
    """Return an (order_date, ship_date) pair where ship >= order most of the time."""
    order = start_date + timedelta(days=random.randint(0, 365))
    # ~5 % of rows intentionally have ship_date before order_date
    # so the agent has something real to catch
    if random.random() < 0.05:
        ship = order - timedelta(days=random.randint(1, 5))
    else:
        ship = order + timedelta(days=random.randint(0, 14))
    return order, ship

# ---------- generate rows ----------
rows = []
for i in range(1, NUM_ROWS + 1):
    original_price = round(random.uniform(5.0, 500.0), 2)
    # ~3 % of rows have discount > original (data quality issue)
    if random.random() < 0.03:
        discount_price = round(original_price + random.uniform(1, 50), 2)
    else:
        discount_price = round(original_price * random.uniform(0.5, 1.0), 2)

    quantity = random.randint(1, 200)
    # ~2 % negative quantities (another quality issue)
    if random.random() < 0.02:
        quantity = -random.randint(1, 10)

    order_dt, ship_dt = random_date_pair()

    rows.append((
        f"TXN-{i:06d}",                        # transaction_id
        random_email(i),                         # customer_email
        round(original_price * quantity, 2),     # amount
        original_price,                          # original_price
        discount_price,                          # discount_price
        quantity,                                # quantity
        order_dt,                                # order_date
        ship_dt,                                 # ship_date
        random.choice(statuses),                 # status
        random.choice(payment_methods),          # payment_method
    ))

schema = StructType([
    StructField("transaction_id",  StringType(),  False),
    StructField("customer_email",  StringType(),  True),
    StructField("amount",          DoubleType(),  True),
    StructField("original_price",  DoubleType(),  True),
    StructField("discount_price",  DoubleType(),  True),
    StructField("quantity",        IntegerType(), True),
    StructField("order_date",      DateType(),    True),
    StructField("ship_date",       DateType(),    True),
    StructField("status",          StringType(),  True),
    StructField("payment_method",  StringType(),  True),
])

df = spark.createDataFrame(rows, schema)
df.write.mode("overwrite").saveAsTable(TABLE)

print(f"Created {CATALOG}.{SCHEMA}.{TABLE} with {df.count()} rows")
display(df.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2 — Log the data quality agent as an MLflow model
# MAGIC
# MAGIC We point `log_model` at the files in the `data_quality_agent_model/`
# MAGIC directory.  Upload that folder to your workspace (or use a Git Folder)
# MAGIC and adjust the `MODEL_DIR` path below.

# COMMAND ----------

import mlflow
from mlflow.models.signature import infer_signature
import pandas as pd
import os

# Path to the model directory — adjust if your layout differs.
# When this notebook lives alongside data_quality_agent_model/ in the same
# repo / workspace folder, the relative path works directly.
MODEL_DIR = os.path.join(
    os.path.dirname(dbutils.notebook.entry_point.getDbutils()
        .notebook().getContext().notebookPath().get()),
    "data_quality_agent_model"
)

# -- signature --
sample_input = pd.DataFrame({
    "table_name": ["sales_transactions"],
    "catalog":    ["dev_catalog"],
    "schema":     ["finance"]
})
sample_output = pd.DataFrame({
    "rules": ['[{"column": "amount", "rule": "amount > 0"}]']
})
signature = infer_signature(sample_input, sample_output)

# -- log model --
mlflow.set_experiment(f"/Users/{spark.conf.get('spark.databricks.notebook.userName')}/data_quality_agent_experiment")

with mlflow.start_run(run_name="data_quality_agent_v1") as run:
    mlflow.pyfunc.log_model(
        artifact_path="data_quality_agent",
        python_model=f"{MODEL_DIR}/python_model.py",
        code_paths=[f"{MODEL_DIR}/code/data_quality_agent"],
        artifacts={
            "system_prompt": f"{MODEL_DIR}/artifacts/system_prompt.txt",
            "example_rules": f"{MODEL_DIR}/artifacts/example_rules.json",
        },
        signature=signature,
        pip_requirements=f"{MODEL_DIR}/requirements.txt",
    )
    run_id = run.info.run_id
    print(f"Model logged — run_id: {run_id}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3 — Load the model and generate rules
# MAGIC
# MAGIC We load the model we just logged and ask it to generate data quality
# MAGIC rules for our `sales_transactions` table.

# COMMAND ----------

import json

model = mlflow.pyfunc.load_model(f"runs:/{run_id}/data_quality_agent")

result = model.predict(pd.DataFrame({
    "table_name": [TABLE],
    "catalog":    [CATALOG],
    "schema":     [SCHEMA],
}))

rules = json.loads(result["rules"].iloc[0])

print(f"Generated {len(rules)} data quality rules:\n")
for r in rules:
    source = r.get("source", "?")
    print(f"  [{source:6s}]  {r['column']:20s}  →  {r['rule']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## (Optional) Apply the rules with Databricks Expectations
# MAGIC
# MAGIC You can take the generated rules and apply them as Delta Live Table
# MAGIC expectations or as direct SQL checks:

# COMMAND ----------

print("Sample SQL checks you can run against the table:\n")
for r in rules[:5]:
    col = r["column"]
    rule_expr = r["rule"]
    print(f"SELECT COUNT(*) AS violations FROM {CATALOG}.{SCHEMA}.{TABLE} WHERE NOT ({rule_expr});")
    print()
