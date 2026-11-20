# Databricks notebook source
# MAGIC %md
# MAGIC # Set up the AI Search endpoint and Delta Sync index
# MAGIC
# MAGIC Creates the endpoint if it does not exist, then creates the Delta Sync
# MAGIC index over the knowledge-base table (or syncs it if it already exists).
# MAGIC Index creation is asynchronous; the polling cell blocks until the index
# MAGIC reports ready, so downstream deployment steps can rely on it.

# COMMAND ----------

dbutils.widgets.text("catalog", "dev_catalog", "Catalog")
dbutils.widgets.text("schema", "finance", "Schema")
dbutils.widgets.text("source_table", "dq_rules_knowledge_base", "Source table")
dbutils.widgets.text("index_name", "dq_rules_index", "Index name")
dbutils.widgets.text("endpoint_name", "dq-vector-endpoint", "Endpoint name")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
source_table = f"{catalog}.{schema}.{dbutils.widgets.get('source_table')}"
index_name = f"{catalog}.{schema}.{dbutils.widgets.get('index_name')}"
endpoint_name = dbutils.widgets.get("endpoint_name")

# COMMAND ----------

import time

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.vectorsearch import EndpointType

workspace = WorkspaceClient()

existing = [e.name for e in workspace.vector_search_endpoints.list_endpoints()]
if endpoint_name not in existing:
    workspace.vector_search_endpoints.create_endpoint(
        name=endpoint_name,
        endpoint_type=EndpointType.STANDARD,
    )
    print(f"Creating endpoint {endpoint_name}; this can take several minutes.")
else:
    print(f"Endpoint {endpoint_name} already exists.")

# COMMAND ----------

indexes = workspace.vector_search_indexes.list_indexes(endpoint_name)
if index_name not in [i.name for i in indexes]:
    workspace.vector_search_indexes.create_delta_sync_index(
        name=index_name,
        endpoint_name=endpoint_name,
        primary_key="rule_id",
        source_table=source_table,
        pipeline_type="TRIGGERED",
        embedding_source_column="content",
        embedding_model_endpoint_name="databricks-gte-large-en",
    )
    print(f"Index {index_name} created; initial sync is asynchronous.")
else:
    print(f"Index {index_name} already exists; triggering a sync.")
    workspace.vector_search_indexes.sync_index(index_name)

# COMMAND ----------

deadline = time.time() + 1800
while time.time() < deadline:
    status = workspace.vector_search_indexes.get_index(index_name).status
    if getattr(status, "ready", False):
        print(f"Index {index_name} is ready.")
        break
    state = getattr(status, "detailed_state", None) or "UNKNOWN"
    print(f"Index state: {state}; waiting 30s...")
    time.sleep(30)
else:
    raise TimeoutError(f"Index {index_name} not ready after 1800s")
