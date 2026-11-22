# Chapter 6 - From Prompt Engineering to Retrieval-Augmented Generation

Runnable companions to the code listings in Chapter 6 of
*Generative AI Solutions with Databricks*.

These notebooks build the CobaltWorks procurement-policy RAG example end to
end: sample data, a Databricks AI Search Delta Sync index, filtered queries,
a simple RAG chain, the metadata-filter access-control pattern, and the
two-user authorized/unauthorized validation test.

## Notebooks

| Notebook | Book listing | What it does |
|---|---|---|
| `00_setup_sample_data.py` | (new) | Creates `cobalt_ops.serving.policy_chunks` with synthetic policy content (Change Data Feed on), including a leadership-restricted document |
| `01_create_index.py` | "Configure and query an index" index-creation listing | Creates the STANDARD AI Search endpoint and TRIGGERED Delta Sync index with `AISearchClient`, hardened with existence checks, a model-endpoint preflight and readiness polling |
| `02_query_index.py` | Connection, `search_policy` and reranking listings | Filtered `similarity_search`; name-maps rows via the response manifest; compares ANN and hybrid; tries `DatabricksReranker` |
| `03_rag_chain.py` | "Building the Simple RAG Chain" listing | Retrieve, render the prompt, query a chat endpoint with `ChatMessage`, and refuse to answer on empty evidence |
| `04_search_as_user.py` | "Enforcing access control with metadata filters" listing | Resolves identity and groups from the platform (SCIM) and applies `access_group` and plant filters, failing closed |
| `05_access_control_test.py` | (new) | Two-user PASS/FAIL validation: authorized retrieval, unauthorized denial, and proof that a user-supplied `plant_id` cannot widen scope |
| `99_cleanup.py` | (new) | Deletes the index, endpoint and table, only when `confirm` is `yes` |

## How to run

1. In your workspace choose **Workspace > Create > Git folder** and paste this
   repository's URL. (Importing single files also works, but keep
   `requirements.txt` next to the notebooks.)
2. Open the notebooks from `Chapters/Chapter 6/` and attach **serverless**
   compute (or a DBR 17+ cluster).
3. Run `00`, then `01`, then `02` and `03` in any order, then `04`, then `05`.
   Run `99_cleanup` last.

Every notebook starts with `%pip install -q -r requirements.txt` and a
Python restart, so run the cells from the top. Settings are widgets at the
top of each notebook with the same names and defaults everywhere:

| Widget | Default | Used by |
|---|---|---|
| `catalog` | `cobalt_ops` | all |
| `schema` | `serving` | all |
| `endpoint_name` | `cobalt-policy-search` | 01-05, 99 |
| `index_name` | `policy_search` (short name) | 01-05, 99 |
| `embedding_endpoint` | `databricks-gte-large-en` | 01 |
| `chat_endpoint` | `databricks-meta-llama-3-3-70b-instruct` | 03 |
| `leadership_group`, `chi_group`, `det_group` | `leadership`, `procurement-chi`, `procurement-det` | 00, 04, 05 |
| `authorized_user`, `unauthorized_user` | empty | 05 |
| `confirm`, `drop_schema` | `no`, `no` | 99 |

If you change a widget in one notebook, change it in the others too.

## Prerequisites

* A Unity Catalog-enabled workspace with serverless compute.
* A catalog you can write to. `00` uses `cobalt_ops` if it exists and tries to
  create it if not. If catalog creation is not allowed for you (or your
  metastore has no default storage), set the `catalog` widget to a catalog you
  own. You need `USE CATALOG`, `CREATE SCHEMA` (unless the schema exists) and
  `CREATE TABLE`.
* **Change Data Feed** on the source table. `00` creates the table with
  `delta.enableChangeDataFeed = true`, and `01` re-enables it if missing. A
  standard endpoint requires it. If you point the notebooks at your own table,
  run `ALTER TABLE <table> SET TBLPROPERTIES (delta.enableChangeDataFeed = true)`.
* Permission to create AI Search endpoints and indexes in the workspace.
* A foundation model endpoint for embeddings and one for chat. Model names
  change; `01` and `03` check that the configured endpoint exists and, if it
  does not, print a message and fall back to an available `databricks-*`
  endpoint of the right type. Set the `embedding_endpoint` and `chat_endpoint`
  widgets to pin your own choice. If you serve your own embedding endpoint,
  turn off **Scale to zero**.
* A **SQL warehouse is not required**. Everything runs on the notebook's
  compute.
* For `05_access_control_test.py`: **two workspace users** and group
  membership you set up beforehand. `authorized_user` must be in the group
  named by `leadership_group`; `unauthorized_user` must be in `chi_group` and
  not in `leadership_group`. Groups are resolved through SCIM, so you need
  permission to read users and groups (account or workspace admins can create
  the groups; the defaults are `leadership` and `procurement-chi`). You can
  reuse existing groups by setting the group widgets to their names in `00`
  (re-run it), `04` and `05`. With the user widgets left empty, `05` prints
  guidance and skips instead of failing.

Run times: a new endpoint takes about 5 to 15 minutes and the first index sync
a few more; everything else is under a minute.

## Cost warning

**An AI Search endpoint is billed for as long as it exists, even when idle.**
Model calls in `01` (embeddings) and `03` (chat) are billed per token, and the
index sync uses serverless compute. Run `99_cleanup` as soon as you finish:
set `confirm` to `yes`. It deletes the index, then the endpoint (kept if other
indexes still use it), then the table. The schema is dropped only if you set
`drop_schema` to `yes` and it is empty. Catalogs are never dropped.

## Free Edition notes

* Free Edition allows **one** AI Search endpoint. If you already have one, set
  `endpoint_name` to its name in every notebook so `01` reuses it, and
  `99_cleanup` will not delete it while other indexes remain.
* **Direct Vector Access** indexes are not available; these notebooks use only
  a Delta Sync index, so this does not affect them.
* Creating new catalogs is often restricted on Free Edition. Set the
  `catalog` widget to your default catalog (often `workspace`).
* Account-level groups and a second user may not be available, in which case
  `05` stays in skip mode. Use `04` to check your own groups instead.
* Available foundation models differ by region and edition; rely on the
  preflight fallback or set the endpoint widgets.

## Endpoint types

The examples use a **STANDARD** endpoint, a `TRIGGERED` Delta Sync pipeline and
dictionary-style `filters`. Storage-optimized endpoints take SQL-style filter
strings such as `"plant_id = 'CHI'"` and filter after retrieving candidates, so
a restrictive filter can return fewer rows. If you switch endpoint type, adapt
the `filters` arguments and re-run `05_access_control_test.py` before relying on
the filter behavior.

## Tested versions and the SDK clients

`requirements.txt` sets lower bounds from the releases the code was checked
against offline with mocks built from the real classes:

| Package | Verified version |
|---|---|
| `databricks-ai-search` | 0.83 |
| `databricks-sdk` | 0.169.0 |
| Python (offline checks) | 3.12 |

MLflow is not used by these notebooks and is not listed.

The notebooks use two clients, as the chapter does:

* `databricks.ai_search.client.AISearchClient` (package `databricks-ai-search`)
  creates and deletes the endpoint and index, waits for them, and runs
  `similarity_search`. This is the current client. Older code imports
  `VectorSearchClient` from `databricks.vector_search`; that comes from the
  deprecated `databricks-vectorsearch` package, a thin shim over
  `databricks-ai-search`, and is not used here.
* `databricks.sdk.WorkspaceClient` lists serving endpoints, queries the chat
  model with `ChatMessage` objects, and reads users and groups from SCIM.

Index creation through `WorkspaceClient` is a different API from
`AISearchClient.create_delta_sync_index`; do not mix their arguments.

## Troubleshooting

| Symptom | Likely cause and fix |
|---|---|
| `Could not create catalog ...` in `00` | No `CREATE CATALOG` privilege or no default storage. Set the `catalog` widget to a catalog you own |
| `Could not create schema ...` | Missing `CREATE SCHEMA` on the catalog. Ask for it or use another catalog |
| `Source table ... is not readable` in `01` | `00` has not run, or the `catalog` / `schema` widgets differ from the ones used in `00` |
| `Could not create AI Search endpoint` | No permission, endpoint limit reached (Free Edition: one), or AI Search not available in the region. Reuse an endpoint via `endpoint_name` |
| `Endpoint ... is OFFLINE` / `STOPPED` | The endpoint cannot serve. Start it from Compute > AI Search, or delete it with `99` and re-run `01` |
| `WARNING: endpoint 'databricks-...' is not available` | Model names change. The notebook falls back to another `databricks-*` endpoint; set `embedding_endpoint` or `chat_endpoint` to choose |
| `no databricks-* endpoint for task ...` | No suitable foundation model endpoint in this workspace. Create or pick one under Serving |
| Index created but searches return nothing | The first sync is still running. `01` polls and warns; wait and re-run `02` |
| Changed embedding model but index unchanged | An existing index keeps its embedding model. Run `99_cleanup`, then `01` again |
| Reranker step prints `reranker call failed` | It is a Designated Service that may be disabled for your workspace or region. The rest is unaffected |
| `Index ... was not found on endpoint ...` in 02-05 | `01` has not finished, or the widgets differ. Match all widget values |
| `04` returns nothing and prints `Your groups: [...]` | You are in none of the groups the sample data uses. Add yourself to one, or change the group widgets |
| `05`: `Test setup is wrong: ...` | The two users do not have the required group membership, or a user name is mistyped. Check with `04` |
| `05`: `PERMISSION_DENIED` on users | You cannot read other users through SCIM. Ask a workspace admin, or run it as one |
| `05` shows `[FAIL] negative ...` | The index returned restricted rows to a user outside the group. Re-run `00` and `01`, then check the filter behavior for your endpoint type |
| `99` prints `Nothing was deleted` | Set `confirm` to `yes` and run the remaining cells |

Cleanup: run `99_cleanup` with `confirm` set to `yes` using the same
`catalog`, `schema`, `endpoint_name` and `index_name` values you used earlier.
