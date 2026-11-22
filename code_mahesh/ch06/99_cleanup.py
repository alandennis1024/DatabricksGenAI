# Databricks notebook source
# MAGIC %md
# MAGIC # Chapter 6 - Cleanup
# MAGIC
# MAGIC **What it does:** removes what notebooks `00` and `01` created: the AI
# MAGIC Search index, the AI Search endpoint (only if no other index still uses
# MAGIC it), and the `policy_chunks` table. Optionally drops the schema, but
# MAGIC only if it is empty. It never drops a catalog.
# MAGIC
# MAGIC **AI Search endpoints are billed while they exist, so run this when you
# MAGIC are done.**
# MAGIC
# MAGIC **Safety:** nothing is deleted unless the `confirm` widget equals
# MAGIC `yes` (the default is `no`). Set the other widgets to the same values
# MAGIC you used in `00` and `01`. If you reused an endpoint that held other
# MAGIC indexes, the endpoint is kept.
# MAGIC
# MAGIC **Runtime:** 1 to 5 minutes. Safe to re-run: missing resources are
# MAGIC skipped.

# COMMAND ----------

# MAGIC %pip install -q -r requirements.txt

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import re
import time

from databricks.ai_search.client import AISearchClient

dbutils.widgets.text("confirm", "no", "Type yes to delete")
dbutils.widgets.text("catalog", "cobalt_ops", "Catalog")
dbutils.widgets.text("schema", "serving", "Schema")
dbutils.widgets.text("endpoint_name", "cobalt-policy-search", "AI Search endpoint")
dbutils.widgets.text("index_name", "policy_search", "Index name (short)")
dbutils.widgets.text("drop_schema", "no", "Also drop the schema if empty (yes/no)")

CONFIRM = dbutils.widgets.get("confirm").strip().lower()
CATALOG = dbutils.widgets.get("catalog").strip()
SCHEMA = dbutils.widgets.get("schema").strip()
ENDPOINT_NAME = dbutils.widgets.get("endpoint_name").strip()
INDEX_SHORT_NAME = dbutils.widgets.get("index_name").strip()
DROP_SCHEMA = dbutils.widgets.get("drop_schema").strip().lower() == "yes"

for label, value in (("catalog", CATALOG), ("schema", SCHEMA),
                     ("index_name", INDEX_SHORT_NAME)):
    if not re.fullmatch(r"[A-Za-z0-9_]+", value):
        raise ValueError(f"Widget '{label}' must be letters, digits, _.")
if not re.fullmatch(r"[A-Za-z0-9_-]+", ENDPOINT_NAME):
    raise ValueError("Widget 'endpoint_name' is not valid.")

INDEX_NAME = f"{CATALOG}.{SCHEMA}.{INDEX_SHORT_NAME}"
TABLE = f"{CATALOG}.{SCHEMA}.policy_chunks"

print("Would delete:")
print(f"  index    {INDEX_NAME}")
print(f"  endpoint {ENDPOINT_NAME} (only if no other index uses it)")
print(f"  table    {TABLE}")
if DROP_SCHEMA:
    print(f"  schema   {CATALOG}.{SCHEMA} (only if empty)")
if CONFIRM != "yes":
    print("\nNothing was deleted. Set the 'confirm' widget to yes and run the "
          "remaining cells again.")

# COMMAND ----------

def wait_until_no_indexes(client, endpoint, attempts=30, pause_seconds=10):
    for _ in range(attempts):
        remaining = client.list_indexes(endpoint).get("vector_indexes", [])
        if not remaining:
            return []
        time.sleep(pause_seconds)
    return remaining


def delete_search_resources():
    client = AISearchClient()
    if not client.endpoint_exists(ENDPOINT_NAME):
        print(f"Endpoint {ENDPOINT_NAME} does not exist; nothing to remove.")
        return
    if client.index_exists(endpoint_name=ENDPOINT_NAME, index_name=INDEX_NAME):
        client.delete_index(endpoint_name=ENDPOINT_NAME, index_name=INDEX_NAME)
        print(f"Deleted index {INDEX_NAME}.")
    else:
        print(f"Index {INDEX_NAME} does not exist; skipping.")
    remaining = wait_until_no_indexes(client, ENDPOINT_NAME)
    if remaining:
        names = [i.get("name") for i in remaining]
        print(f"Keeping endpoint {ENDPOINT_NAME}: it still has indexes {names}.")
        return
    client.delete_endpoint(ENDPOINT_NAME)
    print(f"Deleted endpoint {ENDPOINT_NAME}.")

# COMMAND ----------

if CONFIRM == "yes":
    delete_search_resources()
    try:
        spark.sql(f"DROP TABLE IF EXISTS {TABLE}")
        print(f"Dropped table {TABLE} (if it existed).")
    except Exception as err:
        print(f"Could not drop table {TABLE}: {err}")
    if DROP_SCHEMA:
        try:
            spark.sql(f"DROP SCHEMA IF EXISTS `{CATALOG}`.`{SCHEMA}`")
            print(f"Dropped schema {CATALOG}.{SCHEMA} (if it existed).")
        except Exception as err:
            print(f"Schema {CATALOG}.{SCHEMA} was kept: {err}")
    print("Cleanup finished.")
else:
    print("Skipped: 'confirm' is not 'yes'.")
