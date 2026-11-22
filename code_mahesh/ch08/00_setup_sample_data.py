# Databricks notebook source
# MAGIC %md
# MAGIC # 00 - Set up the sample data
# MAGIC
# MAGIC **Book section:** Chapter 8, *Agents, Tools and Function Calling*. This notebook
# MAGIC creates the Cobalt Works sales data that every later notebook in this folder uses.
# MAGIC
# MAGIC **What it does:** creates a catalog and schema (defaults `cobalt_ops` and `sales`) and fills
# MAGIC them with small, made-up tables. The notebook is safe to re-run: it rebuilds the sample
# MAGIC tables and keeps the approval and ledger tables you already have.
# MAGIC
# MAGIC | Table | Used by | Purpose |
# MAGIC |---|---|---|
# MAGIC | `sales` | 01, 02, 03 | Sales by region, read by the `region_revenue` function |
# MAGIC | `help_articles` | 03 | Help articles the customer support specialist searches |
# MAGIC | `orders` | 04 | Orders a refund request is checked against |
# MAGIC | `customer_records` | 05 | Source rows for the data deletion demo |
# MAGIC | `refund_approvals` | 04 | Audit trail of every refund decision |
# MAGIC | `refund_ledger` | 04 | The (pretend) refunds that were issued |
# MAGIC
# MAGIC **Prerequisites:** a Unity Catalog workspace and permission to create tables in a schema.
# MAGIC If you cannot create catalogs, set the `catalog` widget to a catalog you own (for example
# MAGIC `workspace` on a new workspace or Free Edition).
# MAGIC
# MAGIC **Compute:** serverless or DBR 17+. **Expected runtime:** under a minute.
# MAGIC
# MAGIC **No model endpoint is needed here.** The sample data is invented.

# COMMAND ----------

# Widgets: the same names and defaults are used in every notebook in this folder.
dbutils.widgets.text("catalog", "cobalt_ops", "Catalog")
dbutils.widgets.text("schema", "sales", "Schema")

import re

catalog = dbutils.widgets.get("catalog").strip()
schema = dbutils.widgets.get("schema").strip()
for _label, _value in (("catalog", catalog), ("schema", schema)):
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", _value):
        raise ValueError(
            f"Widget '{_label}' must use letters, digits and underscores only "
            f"(got '{_value}')."
        )

FQ = f"{catalog}.{schema}"
print(f"Working in {FQ}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create the catalog and schema
# MAGIC
# MAGIC Creating a catalog needs the `CREATE CATALOG` privilege, and some workspaces have no default
# MAGIC storage for new catalogs. If this fails, the notebook tells you what to do.

# COMMAND ----------

try:
    spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog}")
    print(f"Catalog '{catalog}' is ready.")
except Exception as exc:
    print(
        f"Could not create catalog '{catalog}': {str(exc)[:300]}\n"
        "Trying to use it anyway. If the next step fails, set the 'catalog' widget "
        "to a catalog you own (for example 'workspace') and run the notebook again."
    )

try:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {FQ}")
    print(f"Schema '{FQ}' is ready.")
except Exception as exc:
    raise RuntimeError(
        f"Cannot create or use schema '{FQ}'. Set the 'catalog' widget to a catalog "
        "you own (and have CREATE SCHEMA on), then run this notebook again. "
        f"Original error: {str(exc)[:300]}"
    ) from exc

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sales table
# MAGIC
# MAGIC Four regions, a few sales each. `EMEA` is the region the book uses in its examples.

# COMMAND ----------

spark.sql(
    f"""
    CREATE OR REPLACE TABLE {FQ}.sales (
      sale_id INT,
      region STRING,
      product STRING,
      sale_date DATE,
      revenue DOUBLE
    )
    """
)
spark.sql(
    f"""
    INSERT INTO {FQ}.sales VALUES
      (1,  'EMEA',  'Laptop Pro',      DATE'2026-01-14', 12500.00),
      (2,  'EMEA',  'Docking Station', DATE'2026-02-03',  3200.50),
      (3,  'EMEA',  'Monitor 27',      DATE'2026-02-21',  8800.00),
      (4,  'EMEA',  'Laptop Pro',      DATE'2026-03-09', 15600.00),
      (5,  'AMER',  'Laptop Pro',      DATE'2026-01-20', 21000.00),
      (6,  'AMER',  'Monitor 27',      DATE'2026-02-11',  9400.00),
      (7,  'AMER',  'Docking Station', DATE'2026-03-02',  4100.00),
      (8,  'AMER',  'Laptop Air',      DATE'2026-03-18', 17250.00),
      (9,  'APAC',  'Laptop Air',      DATE'2026-01-29', 11800.00),
      (10, 'APAC',  'Monitor 27',      DATE'2026-02-17',  6700.00),
      (11, 'APAC',  'Docking Station', DATE'2026-03-05',  2950.00),
      (12, 'LATAM', 'Laptop Pro',      DATE'2026-02-08',  7300.00),
      (13, 'LATAM', 'Monitor 27',      DATE'2026-03-12',  3900.00)
    """
)
n_sales = spark.sql(f"SELECT COUNT(*) AS n FROM {FQ}.sales").collect()[0]["n"]
print("sales:", n_sales, "rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Help articles, orders and customer records

# COMMAND ----------

spark.sql(
    f"""
    CREATE OR REPLACE TABLE {FQ}.help_articles (
      article_id INT,
      title STRING,
      body STRING,
      keywords STRING
    )
    """
)
spark.sql(
    f"""
    INSERT INTO {FQ}.help_articles VALUES
      (1, 'Reset your device password',
          'Open Settings, choose Security, then Reset password. We email a link that '
          'expires after 30 minutes. If the email does not arrive, check your spam folder.',
          'password reset forgot login device security'),
      (2, 'Turn on two-factor authentication',
          'Open Settings, choose Security, then Two-factor authentication. Scan the QR '
          'code with an authenticator app and enter the six digit code to finish.',
          'two-factor 2fa authentication security code login'),
      (3, 'Update your billing address',
          'Go to Account, then Billing, then Edit address. Changes apply to your next '
          'invoice. Past invoices are not changed.',
          'billing address invoice payment account'),
      (4, 'Track an order',
          'Open Orders, select the order, and choose Track shipment. You can also send '
          'the order number to support to get the latest status.',
          'track order shipping delivery status shipment'),
      (5, 'Return an item and request a refund',
          'Start a return within 30 days of delivery from Orders, then Return item. '
          'Refunds go back to the original payment method after we receive the item.',
          'return refund money back order policy')
    """
)

spark.sql(
    f"""
    CREATE OR REPLACE TABLE {FQ}.orders (
      order_id STRING,
      customer STRING,
      order_total DOUBLE,
      status STRING
    )
    """
)
spark.sql(
    f"""
    INSERT INTO {FQ}.orders VALUES
      ('10482', 'Northwind Retail',   12000.00, 'delivered'),
      ('10490', 'Contoso Logistics',  15000.00, 'delivered'),
      ('10503', 'Fabrikam Services',   8000.00, 'delivered'),
      ('10517', 'Adventure Works',       59.00, 'delivered')
    """
)
print("help_articles and orders are ready.")

# COMMAND ----------

# MAGIC %md
# MAGIC `customer_records` holds a mix of `test` and `production` rows with creation dates spread
# MAGIC over the last two years. Notebook 05 copies it into a scratch table before it deletes
# MAGIC anything, so this source table is never changed after this cell.

# COMMAND ----------

from datetime import date, timedelta

today = date.today()
# (record_type, age in days, how many rows)
plan = [
    ("test", 400, 12),        # old test rows: the ones a deletion request targets
    ("test", 20, 6),          # recent test rows: must survive
    ("production", 400, 6),   # old production rows: must survive
    ("production", 20, 6),    # recent production rows: must survive
]
rows = []
record_id = 1
for record_type, age_days, count in plan:
    for i in range(count):
        created = today - timedelta(days=age_days + i)
        rows.append(
            f"({record_id}, '{record_type}', DATE'{created.isoformat()}', "
            f"'owner_{record_id % 5}')"
        )
        record_id += 1

spark.sql(
    f"""
    CREATE OR REPLACE TABLE {FQ}.customer_records (
      record_id INT,
      record_type STRING,
      created_date DATE,
      owner STRING
    )
    """
)
spark.sql(f"INSERT INTO {FQ}.customer_records VALUES " + ", ".join(rows))
print("customer_records:", len(rows), "rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Approval and ledger tables (kept between runs)
# MAGIC
# MAGIC These two use `CREATE TABLE IF NOT EXISTS`, so re-running this notebook does not erase
# MAGIC earlier decisions.

# COMMAND ----------

spark.sql(
    f"""
    CREATE TABLE IF NOT EXISTS {FQ}.refund_approvals (
      request_id STRING,
      order_id STRING,
      requested_amount DOUBLE,
      reason STRING,
      status STRING,
      requested_at TIMESTAMP,
      decided_by STRING,
      decided_at TIMESTAMP,
      decision_note STRING,
      approved_amount DOUBLE
    )
    """
)
spark.sql(
    f"""
    CREATE TABLE IF NOT EXISTS {FQ}.refund_ledger (
      request_id STRING,
      order_id STRING,
      amount DOUBLE,
      issued_at TIMESTAMP
    )
    """
)
print("refund_approvals and refund_ledger are ready.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Check the result

# COMMAND ----------

for table in (
    "sales", "help_articles", "orders", "customer_records",
    "refund_approvals", "refund_ledger",
):
    n = spark.sql(f"SELECT COUNT(*) AS n FROM {FQ}.{table}").collect()[0]["n"]
    print(f"{FQ}.{table:<18} {n} rows")

display(
    spark.sql(
        f"SELECT region, ROUND(SUM(revenue), 2) AS revenue FROM {FQ}.sales "
        "GROUP BY region ORDER BY region"
    )
)
print("Done. Next: run 01_uc_function_tool.")
