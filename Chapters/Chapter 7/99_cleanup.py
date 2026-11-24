# Databricks notebook source
# MAGIC %md
# MAGIC # 99 — Optional, guarded cleanup
# MAGIC Deletes **only** namespaced Chapter 7 resources. Destructive actions require
# MAGIC explicit confirm=yes. The catalog, schema and Chapter 6 assets are never deleted.

# COMMAND ----------
# MAGIC %pip install -q -r requirements.txt

# COMMAND ----------
dbutils.library.restartPython()

# COMMAND ----------
from databricks.ai_search.client import AISearchClient
from cobalt_ch07 import notebook_settings,table,index_resource
s=notebook_settings(dbutils)
dbutils.widgets.dropdown("confirm","no",["no","yes"])
dbutils.widgets.dropdown("delete_endpoint","no",["no","yes"])
dbutils.widgets.dropdown("delete_sample_volume","no",["no","yes"])
if dbutils.widgets.get("confirm")!="yes":
    print("[SKIP] No resources deleted. Set confirm=yes to remove Chapter 7 resources.")
else:
    client=AISearchClient()
    index_name=index_resource(s)
    if client.index_exists(endpoint_name=s["search_endpoint"],index_name=index_name):
        client.delete_index(endpoint_name=s["search_endpoint"],index_name=index_name)
        print("[DELETED] Chapter 7 index",index_name)
    else:
        print("[SKIP] No Chapter 7 index found")
    for short in ("manual_chunks","parsed_manuals","sample_telemetry","operator_clearances"):
        spark.sql(f"DROP TABLE IF EXISTS {table(s,short)}")
        print("[DELETED]",table(s,short))
    if dbutils.widgets.get("delete_sample_volume")=="yes":
        spark.sql(f"DROP VOLUME IF EXISTS {s['catalog']}.{s['schema']}.ch07_manuals")
        print("[DELETED] Chapter 7 sample volume and files")
    else:
        print("[KEPT] Sample volume; choose delete_sample_volume=yes if you want it removed")
    if (dbutils.widgets.get("delete_endpoint")=="yes" and s["search_endpoint"].endswith("-ch07") and client.endpoint_exists(s["search_endpoint"])):
        # Even in a dedicated endpoint, make sure it has no remaining indexes.
        remaining=client.list_indexes(s["search_endpoint"])
        if isinstance(remaining, dict) and not remaining.get("vector_indexes",[]) and not remaining.get("indexes",[]):
            client.delete_endpoint(s["search_endpoint"])
            print("[DELETED] Dedicated empty Chapter 7 search endpoint")
        else:
            print("[KEPT] Endpoint still contains indexes; not deleting")
    else:
        print("[KEPT] Search endpoint; explicitly opt in to delete the empty dedicated endpoint")
    print("[INFO] UC registered models and MLflow experiments are intentionally retained.")
