# Chapter 6 - From Prompt Engineering to Retrieval-Augmented Generation

Runnable companions to the code listings in Chapter 6 of
*Generative AI Solutions with Databricks*.

These notebooks build the CobaltWorks procurement-policy RAG example end to end:
sample data, an AI Search Delta Sync index, filtered queries, a simple RAG
chain, the metadata-filter access-control pattern, and the two-user
authorized/unauthorized validation test.

## Notebooks

| Notebook | Book listing | What it does |
|---|---|---|
| `00_setup_sample_data.py` | (new) | Creates `cobalt_ops.serving.policy_chunks` with synthetic policy content, including a leadership-restricted document |
| `01_create_index.py` | "Databricks AI Search" index-creation listing | Creates the search endpoint and Delta Sync index, hardened with existence checks and status polling |
| `02_query_index.py` | Filtered `similarity_search` listing | Queries the index; name-maps rows via the response manifest instead of positional parsing |
| `03_rag_chain.py` | "Building the Simple RAG Chain" listing | Retrieve -> render prompt -> query a serving endpoint, with corrected column names |
| `04_search_as_user.py` | "Enforcing access control with metadata filters" listing | Resolves identity/groups from the platform (SCIM), applies `access_group` and plant filters |
| `05_access_control_test.py` | (new) | Two-user PASS/FAIL validation: authorized retrieval, unauthorized denial, and proof a user-supplied `plant_id` cannot widen scope |

All notebooks are in Databricks `.py` notebook format: import them into a
workspace (Repos / Workspace > Import) or run them as job tasks.

## Prerequisites

* A Unity Catalog-enabled workspace with serverless compute available.
* Privileges to create catalog `cobalt_ops` (or an existing catalog you own -
  adjust the names in each notebook), and `USE CATALOG` / `USE SCHEMA` /
  `CREATE TABLE` on it.
* Permission to create AI Search endpoints and indexes.
* Access to the foundation model endpoint `databricks-gte-large-en`
  (embeddings) and `databricks-meta-llama-3-3-70b-instruct` (chat), or edit
  the endpoint names in `01_create_index.py` and `03_rag_chain.py`.
* For `05_access_control_test.py`: two workspace users with different group
  memberships - one in the `leadership` group and one in `procurement-chi`
  only (or equivalent groups; edit `GROUP_PLANTS` in `04_search_as_user.py`
  to match your environment). The test resolves their real group membership
  through SCIM.

Run order: `00` -> `01` -> (`02`, `03`) -> `04` -> `05`.

## Endpoint types

The examples use a **STANDARD** endpoint with a `TRIGGERED` Delta Sync
pipeline and dict-style `filters`. Dict filter syntax and the response
`manifest` shape are version- and endpoint-dependent; if you use a
storage-optimized endpoint, re-run the validation in
`05_access_control_test.py` before relying on the filter behavior.

## Version pinning

Per reviewer guidance, the tested versions must be recorded after a live
run. `requirements.txt` currently lists the packages without pins on
purpose: run the notebooks against your workspace runtime, then pin the
versions that passed and delete this note. The APIs marked
version-dependent in the text (dict `filters` vs `filters_json`, response
`manifest`) are exactly the ones to confirm during that run.

## A note on the two SDK clients

The examples intentionally use both `databricks.sdk.WorkspaceClient`
(endpoint/index management, serving queries, SCIM) and
`databricks.vector_search.client.VectorSearchClient` (high-level index
queries). This mirrors the chapter. If you standardize on one client in
your own code, keep the name-mapped row parsing from `02_query_index.py`
either way.
