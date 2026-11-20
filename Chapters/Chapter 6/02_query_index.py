# Databricks notebook source
# MAGIC %md
# MAGIC # Chapter 6 - Query the policy index
# MAGIC
# MAGIC Filtered `similarity_search` with defensive, name-mapped row parsing:
# MAGIC column names come from the response manifest (falling back to the
# MAGIC requested column order), the response shape is validated, and an
# MAGIC empty result is a first-class outcome rather than an error.
# MAGIC
# MAGIC Version-dependent: whether the response includes a `manifest` key
# MAGIC varies by SDK/endpoint version. If your run shows no manifest, the
# MAGIC fallback keeps working - record that in `requirements.txt`/README.

# COMMAND ----------

from databricks.vector_search.client import VectorSearchClient

ENDPOINT_NAME = "cobalt-policy-search"
INDEX_NAME = "cobalt_ops.serving.policy_search"

COLUMNS = ["chunk_id", "content", "document_id", "section_title",
           "effective_date", "plant_id", "access_group"]

client = VectorSearchClient()
index = client.get_index(
    endpoint_name=ENDPOINT_NAME,
    index_name=INDEX_NAME,
)

# COMMAND ----------

def search_policy(question, filters=None, num_results=5, query_type="hybrid"):
    """Search the policy index and return rows as dicts keyed by column name."""
    results = index.similarity_search(
        query_text=question,
        columns=COLUMNS,
        filters=filters or {},
        num_results=num_results,
        query_type=query_type,
    )
    if not isinstance(results, dict) or "result" not in results:
        raise ValueError(f"Unexpected response shape: {type(results)}")

    data = results["result"].get("data_array") or []

    manifest = results.get("manifest") or {}
    names = [c.get("name") for c in manifest.get("columns", [])]
    if not names:
        # No manifest: data_array follows the requested column order
        names = COLUMNS

    return [dict(zip(names, row)) for row in data]

# COMMAND ----------

rows = search_policy(
    "discount approval for a Chicago platinum supplier",
    filters={"plant_id": "CHI"},
)

if not rows:
    print("No evidence found; the application should say so explicitly.")
for row in rows:
    print(f"{row['document_id']} | {row['section_title']} "
          f"| {row['effective_date']} | {row['content'][:80]}...")
