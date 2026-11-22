# Databricks notebook source
# MAGIC %md
# MAGIC # Chapter 6 - Create the AI Search endpoint and Delta Sync index
# MAGIC
# MAGIC **Book section:** "Configure and query an index" (the index-creation
# MAGIC listing), hardened for repeat runs.
# MAGIC
# MAGIC **What it does:** checks that the source table from `00_setup_sample_data`
# MAGIC is ready, checks that the embedding model endpoint exists (falling back
# MAGIC to an available `databricks-*` embedding endpoint if it does not),
# MAGIC creates a **STANDARD** AI Search endpoint if needed, creates a
# MAGIC **TRIGGERED** Delta Sync index with Databricks-managed embeddings, waits
# MAGIC until the index is online, and confirms that a search returns rows.
# MAGIC Index creation and synchronization are asynchronous, so anything
# MAGIC downstream must wait for the index to report ready.
# MAGIC
# MAGIC **Prerequisites:** `00_setup_sample_data` has run; you can create AI
# MAGIC Search endpoints and have `CREATE TABLE` on the schema; a Foundation
# MAGIC Model API embedding endpoint is available.
# MAGIC
# MAGIC **Runtime:** a new endpoint takes about 5 to 15 minutes, and the first
# MAGIC index sync a few more. A re-run with existing resources takes 1 to 3
# MAGIC minutes.
# MAGIC
# MAGIC **Creates:** AI Search endpoint `cobalt-policy-search` and index
# MAGIC `<catalog>.<schema>.policy_search`. **The endpoint is billed while it
# MAGIC exists. Run `99_cleanup` when you are done.** Free Edition allows a
# MAGIC single AI Search endpoint; if you already have one, set the
# MAGIC `endpoint_name` widget to its name to reuse it.

# COMMAND ----------

# MAGIC %pip install -q -r requirements.txt

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import re
import time

from databricks.ai_search.client import AISearchClient
from databricks.sdk import WorkspaceClient

dbutils.widgets.text("catalog", "cobalt_ops", "Catalog")
dbutils.widgets.text("schema", "serving", "Schema")
dbutils.widgets.text("endpoint_name", "cobalt-policy-search", "AI Search endpoint")
dbutils.widgets.text("index_name", "policy_search", "Index name (short)")
dbutils.widgets.text(
    "embedding_endpoint", "databricks-gte-large-en", "Embedding endpoint"
)

CATALOG = dbutils.widgets.get("catalog").strip()
SCHEMA = dbutils.widgets.get("schema").strip()
ENDPOINT_NAME = dbutils.widgets.get("endpoint_name").strip()
INDEX_SHORT_NAME = dbutils.widgets.get("index_name").strip()
EMBEDDING_ENDPOINT = dbutils.widgets.get("embedding_endpoint").strip()

for label, value in (("catalog", CATALOG), ("schema", SCHEMA),
                     ("index_name", INDEX_SHORT_NAME)):
    if not re.fullmatch(r"[A-Za-z0-9_]+", value):
        raise ValueError(f"Widget '{label}' must be letters, digits, _: {value!r}")
if not re.fullmatch(r"[A-Za-z0-9_-]+", ENDPOINT_NAME):
    raise ValueError(f"Widget 'endpoint_name' is not valid: {ENDPOINT_NAME!r}")

SOURCE_TABLE = f"{CATALOG}.{SCHEMA}.policy_chunks"
INDEX_NAME = f"{CATALOG}.{SCHEMA}.{INDEX_SHORT_NAME}"
REQUIRED_COLUMNS = {"chunk_id", "content", "document_id", "section_title",
                    "effective_date", "plant_id", "access_group"}

workspace = WorkspaceClient()
client = AISearchClient()
print(f"Endpoint: {ENDPOINT_NAME}\nIndex:    {INDEX_NAME}\nSource:   {SOURCE_TABLE}")

# COMMAND ----------

# Preflight: model endpoint names change over time, so check before use.
def resolve_endpoint(configured, task, widget, prefer=()):
    """Return a serving endpoint that exists and serves `task`.

    Falls back to the first suitable `databricks-*` endpoint if the
    configured one is missing, and says so.
    """
    try:
        available = list(workspace.serving_endpoints.list())
    except Exception as err:
        print(f"WARNING: could not list serving endpoints ({err}). "
              f"Continuing with '{configured}' unchecked.")
        return configured
    by_name = {e.name: e for e in available}
    found = by_name.get(configured)
    if found is not None and found.task in (None, task):
        print(f"Using {task} endpoint '{configured}'.")
        return configured
    reason = "is not available in this workspace"
    if found is not None:
        reason = f"serves task '{found.task}', not '{task}'"
    candidates = sorted(
        (e.name for e in available
         if e.name.startswith("databricks-") and e.task == task),
        key=lambda n: (not any(p in n for p in prefer), n),
    )
    if not candidates:
        raise RuntimeError(
            f"Endpoint '{configured}' {reason}, and no databricks-* endpoint "
            f"for task '{task}' was found. Open Serving in the sidebar, pick "
            f"a {task} endpoint, and put its name in the '{widget}' widget."
        )
    print(f"WARNING: endpoint '{configured}' {reason}.\n"
          f"Falling back to '{candidates[0]}'. To choose another, set the "
          f"'{widget}' widget (candidates: {candidates[:5]}).")
    return candidates[0]


EMBEDDING_ENDPOINT = resolve_endpoint(
    EMBEDDING_ENDPOINT, "llm/v1/embeddings", "embedding_endpoint"
)

# COMMAND ----------

# The index needs the source table, its columns and Change Data Feed.
try:
    source_df = spark.table(SOURCE_TABLE)
    source_rows = source_df.count()
except Exception as err:
    raise RuntimeError(
        f"Source table {SOURCE_TABLE} is not readable: {err}\n"
        "Run 00_setup_sample_data first, and check the catalog and schema "
        "widgets match the ones you used there."
    ) from err

missing = REQUIRED_COLUMNS - set(source_df.columns)
if missing:
    raise RuntimeError(f"{SOURCE_TABLE} is missing columns {sorted(missing)}.")
if source_rows == 0:
    raise RuntimeError(f"{SOURCE_TABLE} is empty; run 00_setup_sample_data.")

props = {r[0]: r[1] for r in spark.sql(f"SHOW TBLPROPERTIES {SOURCE_TABLE}").collect()}
if str(props.get("delta.enableChangeDataFeed")).lower() != "true":
    print("Enabling Change Data Feed on the source table.")
    spark.sql(
        f"ALTER TABLE {SOURCE_TABLE} "
        "SET TBLPROPERTIES (delta.enableChangeDataFeed = true)"
    )
print(f"Source table OK: {source_rows} rows, Change Data Feed enabled.")

# COMMAND ----------

def ensure_endpoint(name):
    if client.endpoint_exists(name):
        print(f"Endpoint {name} already exists; waiting until it is ONLINE.")
        client.wait_for_endpoint(name, verbose=True)
        return
    try:
        others = [e["name"] for e in client.list_endpoints().get("endpoints", [])]
    except Exception:
        others = []
    if others:
        print(f"Other AI Search endpoints in this workspace: {others}. If "
              "creation fails with a limit error (Free Edition allows one "
              "endpoint), set the 'endpoint_name' widget to one of them.")
    print(f"Creating endpoint {name}; this takes several minutes.")
    try:
        client.create_endpoint_and_wait(
            name=name, endpoint_type="STANDARD", verbose=True
        )
    except Exception as err:
        raise RuntimeError(
            f"Could not create AI Search endpoint '{name}': {err}\n"
            "Common causes: no permission to create endpoints, the "
            "endpoint limit for your workspace or edition, or AI Search "
            "not being enabled in this region."
        ) from err


ensure_endpoint(ENDPOINT_NAME)

# COMMAND ----------

def ensure_index():
    if client.index_exists(endpoint_name=ENDPOINT_NAME, index_name=INDEX_NAME):
        print(f"Index {INDEX_NAME} already exists; syncing it with the table.")
        index = client.get_index(endpoint_name=ENDPOINT_NAME, index_name=INDEX_NAME)
        index.wait_until_ready(verbose=True)
        try:
            index.sync()
        except Exception as err:
            print(f"Could not start a sync (one may already be running): {err}")
        time.sleep(15)
        index.wait_until_ready(verbose=True, wait_for_updates=True)
        return index
    print(f"Creating index {INDEX_NAME} with embeddings from {EMBEDDING_ENDPOINT}.")
    index = client.create_delta_sync_index(
        endpoint_name=ENDPOINT_NAME,
        index_name=INDEX_NAME,
        source_table_name=SOURCE_TABLE,
        primary_key="chunk_id",
        pipeline_type="TRIGGERED",
        embedding_source_column="content",
        embedding_model_endpoint_name=EMBEDDING_ENDPOINT,
    )
    index.wait_until_ready(verbose=True)
    return index


index = ensure_index()
status = index.describe().get("status", {})
print(f"Index state: {status.get('detailed_state', 'UNKNOWN')}")

# COMMAND ----------

# ONLINE can precede the first searchable rows, so poll with a real query.
def wait_until_searchable(index, expected_rows, attempts=40, pause_seconds=15):
    found = 0
    for attempt in range(1, attempts + 1):
        results = index.similarity_search(
            query_text="discount approval",
            columns=["chunk_id"],
            num_results=max(expected_rows, 1),
            query_type="hybrid",
        )
        found = len((results.get("result") or {}).get("data_array") or [])
        print(f"Attempt {attempt}: search returned {found} of {expected_rows} rows.")
        if found >= expected_rows:
            return True
        time.sleep(pause_seconds)
    return False


if wait_until_searchable(index, source_rows):
    print("The index is searchable. Continue with 02_query_index.")
else:
    print("WARNING: the index is online but not fully populated yet. Wait a "
          "few minutes and run 02_query_index; if rows are still missing, "
          "check the index's sync status in Catalog Explorer.")
