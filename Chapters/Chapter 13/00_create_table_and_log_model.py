# Databricks notebook source
# MAGIC %md
# MAGIC # Chapter 13 - Step 0: Create Table and Log Model
# MAGIC
# MAGIC This notebook corresponds to the **Models - Standardized Packaging Format** section
# MAGIC of Chapter 13. It demonstrates:
# MAGIC
# MAGIC 1. Creating a `sales_transactions` table with synthetic data
# MAGIC 2. Logging the data quality agent as an MLflow PyFunc model
# MAGIC 3. Loading the model and generating data quality rules
# MAGIC
# MAGIC **Prerequisites:**
# MAGIC - Upload the `data_quality_agent_model/` folder to the same workspace directory as this notebook
# MAGIC - The `databricks-meta-llama-3-3-70b-instruct` foundation model endpoint must be available

# COMMAND ----------

# MAGIC %pip install mlflow
# MAGIC %restart_python

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

try:
    username = spark.conf.get("spark.databricks.notebook.userName")
except:
    username = "unknown"
if username == "unknown":
    username = spark.sql("SELECT current_user()").collect()[0][0]
print(f"Current user: {username}")

spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"USE SCHEMA {SCHEMA}")

print(f"Using {CATALOG}.{SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1 - Create the sales_transactions table with synthetic data
# MAGIC
# MAGIC The table is designed to give the agent interesting columns to reason about:
# MAGIC numeric ranges, date ordering, email formats, enum values, and cross-column
# MAGIC relationships. A small percentage of rows intentionally contain data quality
# MAGIC issues so the agent has real violations to detect.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    DoubleType, DateType
)
import random
from datetime import date, timedelta

NUM_ROWS = 2000

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
    # ~5% of rows intentionally have ship_date before order_date
    if random.random() < 0.05:
        ship = order - timedelta(days=random.randint(1, 5))
    else:
        ship = order + timedelta(days=random.randint(0, 14))
    return order, ship

rows = []
for i in range(1, NUM_ROWS + 1):
    original_price = round(random.uniform(5.0, 500.0), 2)
    # ~3% of rows have discount > original (data quality issue)
    if random.random() < 0.03:
        discount_price = round(original_price + random.uniform(1, 50), 2)
    else:
        discount_price = round(original_price * random.uniform(0.5, 1.0), 2)

    quantity = random.randint(1, 200)
    # ~2% negative quantities (data quality issue)
    if random.random() < 0.02:
        quantity = -random.randint(1, 10)

    order_dt, ship_dt = random_date_pair()

    rows.append((
        f"TXN-{i:06d}",
        random_email(i),
        round(original_price * quantity, 2),
        original_price,
        discount_price,
        quantity,
        order_dt,
        ship_dt,
        random.choice(statuses),
        random.choice(payment_methods),
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
# MAGIC ## Step 2 - Log the data quality agent as an MLflow model
# MAGIC
# MAGIC We use `mlflow.pyfunc.log_model` to package the agent code, artifacts, and
# MAGIC dependencies into a single versioned unit.

# COMMAND ----------

# DBTITLE 1,Cell 8
import mlflow
from mlflow.models.signature import infer_signature
import pandas as pd
import sys

# Add the code path to sys.path so we can import the model
sys.path.insert(0, "data_quality_agent_model/code")

# Import and instantiate the model class
from data_quality_agent_model.python_model import DataQualityAgentModel

# Signature: what the model expects and returns
sample_input = pd.DataFrame({
    "table_name": [TABLE],
    "catalog": [CATALOG],
    "schema": [SCHEMA]
})
sample_output = pd.DataFrame({
    "rules": ['{"column": "amount", "rule": "amount > 0"}']
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
    print(f"Model logged - run_id: {run_id}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3 - Load the model and generate rules
# MAGIC
# MAGIC We load the model we just logged and ask it to generate data quality
# MAGIC rules for our sales_transactions table.

# COMMAND ----------

import json

model = mlflow.pyfunc.load_model(f"runs:/{run_id}/data_quality_agent")

result = model.predict(pd.DataFrame({
    "table_name": [TABLE],
    "catalog": [CATALOG],
    "schema": [SCHEMA],
}))

rules = json.loads(result["rules"].iloc[0])

print(f"Generated {len(rules)} data quality rules:\n")
for r in rules:
    source = r.get("source", "?")
    print(f"  [{source:6s}]  {r['column']:20s}  ->  {r['rule']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## (Optional) Sample SQL checks
# MAGIC
# MAGIC The generated rules can be applied as SQL checks against the table.

# COMMAND ----------

print("Sample SQL checks:\n")
for r in rules[:5]:
    rule_expr = r["rule"]
    print(f"SELECT COUNT(*) AS violations FROM {CATALOG}.{SCHEMA}.{TABLE} WHERE NOT ({rule_expr});")
    print()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next Steps
# MAGIC
# MAGIC The `run_id` variable contains the ID of the run we just created.
# MAGIC Use **01_model_packaging** next to validate and register this model in Unity Catalog.