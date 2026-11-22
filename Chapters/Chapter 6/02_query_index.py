# Databricks notebook source
# MAGIC %md
# MAGIC # Chapter 6 - Query the policy index
# MAGIC
# MAGIC **Book sections:** "Configure and query an index" (the connection,
# MAGIC `search_policy` and filtered-search listings) and "Reranking and context
# MAGIC selection" (the `DatabricksReranker` listing).
# MAGIC
# MAGIC **What it does:** connects to the index, runs a filtered
# MAGIC `similarity_search` with name-mapped row parsing (column names come from
# MAGIC the response manifest, falling back to the requested column order),
# MAGIC compares ANN with hybrid search, and tries the built-in reranker. An
# MAGIC empty result is a normal outcome, not an error.
# MAGIC
# MAGIC **Prerequisites:** `01_create_index` finished and the index is online.
# MAGIC
# MAGIC **Runtime:** under a minute.
# MAGIC
# MAGIC **Creates:** nothing. This notebook only reads.
# MAGIC
# MAGIC **Note:** this notebook searches with a plant filter only, as the
# MAGIC chapter does at this point. A plant filter is *not* access control; see
# MAGIC `04_search_as_user`. The reranker is a Databricks Designated Service and
# MAGIC may be unavailable in your region or settings; the notebook prints a
# MAGIC message and carries on if so.

# COMMAND ----------

# MAGIC %pip install -q -r requirements.txt

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

from databricks.ai_search.client import AISearchClient
from databricks.ai_search.reranker import DatabricksReranker

dbutils.widgets.text("catalog", "cobalt_ops", "Catalog")
dbutils.widgets.text("schema", "serving", "Schema")
dbutils.widgets.text("endpoint_name", "cobalt-policy-search", "AI Search endpoint")
dbutils.widgets.text("index_name", "policy_search", "Index name (short)")

CATALOG = dbutils.widgets.get("catalog").strip()
SCHEMA = dbutils.widgets.get("schema").strip()
ENDPOINT_NAME = dbutils.widgets.get("endpoint_name").strip()
INDEX_NAME = f"{CATALOG}.{SCHEMA}.{dbutils.widgets.get('index_name').strip()}"

COLUMNS = ["chunk_id", "content", "document_id", "section_title",
           "effective_date", "plant_id", "access_group"]

# COMMAND ----------

client = AISearchClient()
if not client.index_exists(endpoint_name=ENDPOINT_NAME, index_name=INDEX_NAME):
    raise RuntimeError(
        f"Index {INDEX_NAME} was not found on endpoint {ENDPOINT_NAME}. Run "
        "01_create_index first, and check that the catalog, schema, "
        "endpoint_name and index_name widgets match the ones you used there."
    )
index = client.get_index(
    endpoint_name=ENDPOINT_NAME,
    index_name=INDEX_NAME,
)
print(f"Connected to {INDEX_NAME}.")

# COMMAND ----------

def rows_from(results, requested_columns):
    """Turn a similarity_search response into dicts keyed by column name."""
    if not isinstance(results, dict) or "result" not in results:
        raise ValueError(f"Unexpected response shape: {type(results)}")
    data = results["result"].get("data_array") or []
    manifest = results.get("manifest") or {}
    names = [c.get("name") for c in manifest.get("columns", [])]
    if not names:
        # No manifest: data_array follows the requested column order
        names = requested_columns
    return [dict(zip(names, row)) for row in data]


def search_policy(question, filters=None, num_results=5, query_type="hybrid"):
    """Search the policy index and return rows as dicts keyed by column name."""
    results = index.similarity_search(
        query_text=question,
        columns=COLUMNS,
        filters=filters or {},
        num_results=num_results,
        query_type=query_type,
    )
    return rows_from(results, COLUMNS)

# COMMAND ----------

rows = search_policy(
    "discount approval for a Chicago platinum supplier",
    filters={"plant_id": "CHI"},
)

if not rows:
    print("No evidence found; the application should say so explicitly. "
          "If the index was just created, wait a few minutes and re-run.")
for row in rows:
    print(f"{row['document_id']} | {row['section_title']} "
          f"| {row['effective_date']} | {row['content'][:80]}...")

# COMMAND ----------

# Compare semantic-only (ANN) with hybrid search on an exact identifier.
for query_type in ("ANN", "hybrid"):
    hits = search_policy("POL-002", filters={"plant_id": "DET"},
                         query_type=query_type, num_results=3)
    labels = [f"{h['document_id']}/{h['section_title']}" for h in hits]
    print(f"{query_type:>6}: {labels}")

# COMMAND ----------

# The built-in reranker, as in the chapter's listing.
try:
    reranked = index.similarity_search(
        query_text="discount approval for a Chicago platinum supplier",
        num_results=5,
        query_type="hybrid",
        columns=["chunk_id", "content", "document_id", "section_title"],
        reranker=DatabricksReranker(columns_to_rerank=["content"]),
    )
    names = ["chunk_id", "content", "document_id", "section_title"]
    for row in rows_from(reranked, names):
        print(f"{row['document_id']} | {row['section_title']}")
except Exception as err:
    print("The reranker call failed, so this step is skipped. It is a "
          "Designated Service and may be disabled for your workspace or "
          f"region.\nDetails: {err}")
