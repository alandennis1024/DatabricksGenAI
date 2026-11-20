# Databricks notebook source
# MAGIC %md
# MAGIC # Chapter 6 - Create the AI Search endpoint and Delta Sync index
# MAGIC
# MAGIC Hardened version of the index-creation listing in the chapter:
# MAGIC checks whether resources already exist, then polls for readiness.
# MAGIC Index creation and synchronization are asynchronous, so anything
# MAGIC downstream (queries, tests) must wait for the index to report ready.

# COMMAND ----------

import time

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.vectorsearch import EndpointType

ENDPOINT_NAME = "cobalt-policy-search"
INDEX_NAME = "cobalt_ops.serving.policy_search"
SOURCE_TABLE = "cobalt_ops.serving.policy_chunks"
EMBEDDING_ENDPOINT = "databricks-gte-large-en"

workspace = WorkspaceClient()

# COMMAND ----------

existing = [e.name for e in workspace.vector_search_endpoints.list_endpoints()]
if ENDPOINT_NAME not in existing:
    workspace.vector_search_endpoints.create_endpoint(
        name=ENDPOINT_NAME,
        endpoint_type=EndpointType.STANDARD,
    )
    print(f"Creating endpoint {ENDPOINT_NAME}; this can take several minutes.")
else:
    print(f"Endpoint {ENDPOINT_NAME} already exists.")

# COMMAND ----------

indexes = workspace.vector_search_indexes.list_indexes(ENDPOINT_NAME)
if INDEX_NAME not in [i.name for i in indexes]:
    workspace.vector_search_indexes.create_delta_sync_index(
        name=INDEX_NAME,
        endpoint_name=ENDPOINT_NAME,
        primary_key="chunk_id",
        source_table=SOURCE_TABLE,
        pipeline_type="TRIGGERED",
        embedding_source_column="content",
        embedding_model_endpoint_name=EMBEDDING_ENDPOINT,
    )
    print(f"Index {INDEX_NAME} created; initial sync is asynchronous.")
else:
    print(f"Index {INDEX_NAME} already exists; triggering a sync.")
    workspace.vector_search_indexes.sync_index(INDEX_NAME)

# COMMAND ----------

def wait_for_index(name, timeout_seconds=1800, poll_seconds=30):
    """Poll until the index reports ready, or raise after the timeout."""
    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        status = workspace.vector_search_indexes.get_index(name).status
        state = getattr(status, "detailed_state", None) or "UNKNOWN"
        if getattr(status, "ready", False):
            print(f"Index {name} is ready.")
            return
        print(f"Index state: {state}; waiting {poll_seconds}s...")
        time.sleep(poll_seconds)
    raise TimeoutError(f"Index {name} not ready after {timeout_seconds}s")

wait_for_index(INDEX_NAME)
