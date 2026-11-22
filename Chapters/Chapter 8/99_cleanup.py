# Databricks notebook source
# MAGIC %md
# MAGIC # 99 - Clean up
# MAGIC
# MAGIC **Book section:** none. This removes what the Chapter 8 notebooks created.
# MAGIC
# MAGIC **What it does:** drops the function `region_revenue` and the tables created by notebooks
# MAGIC 00, 04 and 05. It does nothing unless you type `yes` in the `confirm` widget.
# MAGIC
# MAGIC * **Tables dropped:** `sales`, `help_articles`, `orders`, `customer_records`,
# MAGIC   `refund_approvals`, `refund_ledger`, `deletion_demo_records`.
# MAGIC * **Schema and catalog:** kept, unless you also type `yes` in `drop_schema`. The schema is
# MAGIC   dropped only if it is empty, and the catalog is never dropped. If you used a schema
# MAGIC   that existed before this folder, leave `drop_schema` as `no`.
# MAGIC * **MLflow traces** are kept. Delete them from the Experiments page if you want them gone.
# MAGIC
# MAGIC **Warning:** the approval table is an audit trail. Dropping it removes the record of
# MAGIC every refund decision from the demo.
# MAGIC
# MAGIC **Compute:** serverless or DBR 17+. **Expected runtime:** under a minute.

# COMMAND ----------

dbutils.widgets.text("catalog", "cobalt_ops", "Catalog")
dbutils.widgets.text("schema", "sales", "Schema")
dbutils.widgets.dropdown("confirm", "no", ["no", "yes"], "Set to yes to drop everything")
dbutils.widgets.dropdown("drop_schema", "no", ["no", "yes"], "Also drop the schema")

import re

catalog = dbutils.widgets.get("catalog").strip()
schema = dbutils.widgets.get("schema").strip()
confirm = dbutils.widgets.get("confirm").strip().lower()
drop_schema = dbutils.widgets.get("drop_schema").strip().lower()
for _label, _value in (("catalog", catalog), ("schema", schema)):
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", _value):
        raise ValueError(
            f"Widget '{_label}' must use letters, digits and underscores only "
            f"(got '{_value}')."
        )

FQ = f"{catalog}.{schema}"
TABLES = [
    "sales", "help_articles", "orders", "customer_records",
    "refund_approvals", "refund_ledger", "deletion_demo_records",
]
print(f"Target: {FQ}")
print(f"confirm = '{confirm}', drop_schema = '{drop_schema}'")

# COMMAND ----------

if confirm != "yes":
    print(
        "Nothing was dropped. To clean up, set the 'confirm' widget to 'yes' and run "
        "this notebook again. These objects would be removed:"
    )
    print(f"  function {FQ}.region_revenue")
    for table in TABLES:
        print(f"  table    {FQ}.{table}")
else:
    try:
        spark.sql(f"DROP FUNCTION IF EXISTS {FQ}.region_revenue")
        print(f"Dropped function {FQ}.region_revenue (if it existed).")
    except Exception as exc:
        print(f"Could not drop the function: {str(exc)[:300]}")
    for table in TABLES:
        try:
            spark.sql(f"DROP TABLE IF EXISTS {FQ}.{table}")
            print(f"Dropped table {FQ}.{table} (if it existed).")
        except Exception as exc:
            print(f"Could not drop {FQ}.{table}: {str(exc)[:300]}")

    if drop_schema == "yes":
        try:
            spark.sql(f"DROP SCHEMA IF EXISTS {FQ}")
            print(f"Dropped schema {FQ} (if it existed).")
        except Exception as exc:
            print(
                f"Schema {FQ} was not dropped, probably because it still holds other "
                f"objects: {str(exc)[:300]}"
            )
    print("Cleanup finished.")
