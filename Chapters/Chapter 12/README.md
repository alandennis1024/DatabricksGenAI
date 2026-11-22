# Chapter 12: Model and System Evaluation

Runnable notebooks for Chapter 12 of *Generative AI Solutions with Databricks*. You evaluate a small CobaltWorks procurement-policy assistant with MLflow 3 on Databricks: build an evaluation dataset, run built-in and custom judges, debug a judge, compare two versions, apply a release gate, and (optionally) turn on production monitoring.

The assistant calls a Databricks chat endpoint with a policy system prompt and a short policy text that is embedded in the code, so **no AI Search index, agent deployment, or private data is needed**.

## Notebooks

| Notebook | Book section | What it does | Time | Beta |
|---|---|---|---|---|
| `00_setup_eval_dataset` | Why Building Evaluation Datasets Is Really Hard; What a Good Evaluation Row Contains | Sets the MLflow experiment, creates a Unity Catalog evaluation dataset, merges 20 CobaltWorks records (lookups, discounts, refusals, prompt-injection cases), reads it back | 1 min | |
| `01_run_evaluation` | MLflow Evaluation on Databricks; Built-in Judges | Runs `mlflow.genai.evaluate()` with `Correctness`, `RelevanceToQuery`, `Safety`, `RetrievalGroundedness`, `Guidelines`, and a custom code scorer; reads per-row verdicts from traces | 5-15 min | |
| `02_judge_debugging` | Calling the Judge Directly for Debugging | Calls judges with `judge(trace=trace)`; handles one `Feedback` versus a list; shows rationales | 2 min | |
| `03_custom_judge_make_judge` | Building a Custom Judge with `make_judge()`; Judge Alignment | Builds the discount-policy judge, runs it on a few rows, optional `judge.align()` | 3-5 min | |
| `04_compare_versions_and_gate` | Comparing Versions; Evaluation in CI/CD | Evaluates two system-prompt versions on the same data, compares metrics, applies a PASS/FAIL release gate; the job wiring is described in markdown only | 10-25 min | |
| `05_production_monitoring` | Production Monitoring with Scheduled Scorers | Registers and starts monitoring scorers with sampling. **Off by default** | 1 min, then a 15-20 min wait | Yes |
| `06_traces_audit` | Interpretability: Making the Reasoning Visible | `search_traces()` and a summary table with per-scorer pass rates and an audit trail | 1 min | |
| `99_cleanup` | | Deletes what the notebooks created, only when `confirm = yes` | 1 min | |

`cobalt_eval_common.py` holds the shared pieces: the 20 records, the embedded policy text, the two system prompts, `build_predict_fn()`, the policy check used by the code scorers, endpoint selection, and the gate logic. Open it when a notebook says "see `cobalt_eval_common.py`".

## Prerequisites

- A Databricks workspace with Unity Catalog and **serverless compute** (or a cluster on Databricks Runtime 17.0 or later).
- Permission to query two model serving endpoints: a chat model for the assistant and a different model for the judges. Foundation Model API endpoints named `databricks-*` work.
- Permission to use or create a Unity Catalog catalog and schema (default `cobalt_ops.agent_audit`). Notebook 00 tries to create them; if catalog creation is not allowed, set the `catalog` widget to one you can write to.

## How to run

1. In your workspace, go to **Workspace > Create > Git folder** and clone this repository.
2. Open `Chapters/Chapter 12/`.
3. Open each notebook, attach **Serverless** compute, and choose **Run all**. Run them in this order:

   `00` -> `01` -> `02` -> `03` -> `04` -> `05` (optional) -> `06` -> `99`

   Notebooks 02, 03, and 06 do not need 01 to have run, but they read more interesting data if it has. Notebook 04 does not need 01.

The first cell of each notebook installs `requirements.txt` (MLflow with the Databricks extras, the Databricks SDK, and `dspy` for the optional judge alignment) and restarts Python. That is expected. The assistant calls its model with the SDK's `serving_endpoints.query()`, so no LangChain package is needed.

### Widgets

The same widget names are used everywhere. If you change one, set it to the same value in every notebook.

| Widget | Default | Meaning |
|---|---|---|
| `catalog`, `schema` | `cobalt_ops`, `agent_audit` | Where the evaluation dataset lives |
| `dataset_name` | `cobalt_eval_dataset_v1` | Dataset table name |
| `chat_endpoint` | `databricks-meta-llama-3-3-70b-instruct` | Endpoint for the assistant |
| `judge_endpoint` | `databricks-claude-sonnet-4-6` | Endpoint for the LLM judges, used as `databricks:/<name>` |
| `experiment_name` | `/Users/<you>/cobaltworks-ch12-evaluation` | MLflow experiment for every run and trace |

Endpoint names change as models are retired. Each notebook lists the chat endpoints you can see and, if a configured name is missing, prints a message and uses the first suitable `databricks-*` chat endpoint.

Judges use `databricks:/<endpoint>` model URIs. The `endpoints:/` form is for `mlflow.genai.to_predict_fn()`, not for judges.

## Beta features

- **Production monitoring** (`05_production_monitoring`): `Scorer.register()`, `Scorer.start()`, `ScorerSamplingConfig`, and `set_databricks_monitoring_sql_warehouse_id()` are labeled Beta in the Databricks documentation. The notebook does nothing unless you set `enable_monitoring = yes` and supply a `sql_warehouse_id`. Results appear 15-20 minutes after you start the scorers.
- **Judge alignment** (`judge.align()` in notebook 03): optional, off by default, needs `dspy` and at least 10 human-labeled traces.
- **Review queues** (mentioned in the chapter) are a Beta workspace feature; no notebook depends on them.

## Free Edition notes

Databricks Free Edition is serverless-only and limits model serving (fewer endpoints, quotas on Foundation Model APIs, and some models are not available).

- Use `max_rows` (for example 5) in notebooks 01, 03, and 04 to stay inside quotas.
- If a configured endpoint is not available, the preflight picks another one; if none works, set `chat_endpoint` and `judge_endpoint` yourself.
- If you cannot create a catalog, set `catalog` to the one your workspace provides. If dataset creation is not available at all, the notebooks fall back to the same 20 records held in `cobalt_eval_common.py`.
- Production monitoring (notebook 05) may not be available. It is off by default and fails with a message, not an error, if the feature is missing.

## Cost

Everything runs on serverless compute plus Foundation Model API calls. With the defaults:

| Notebook | Agent calls | Judge calls |
|---|---|---|
| 01 | 20 | about 100 |
| 03 | 6 | about 9 |
| 04 | 40 | about 200 |
| 05 (when enabled) | 5, then every sampled production trace | continuing until you stop the scorers |

Use `max_rows` to cut the cost of a trial. Stop monitoring scorers with `99_cleanup` when you are done.

## Cleanup

Run `99_cleanup`. With the default `confirm = no` it only lists what it would remove. With `confirm = yes` it stops and deletes the `cobalt_ch12_*` monitoring scorers, deletes the evaluation dataset, and deletes the runs these notebooks created (tagged `cobalt_ch12 = true`). The experiment and its traces are kept unless you also set `delete_experiment = yes`. The catalog and schema are never dropped.

## Troubleshooting

| Symptom | Likely cause | What to do |
|---|---|---|
| `Could not create or use <catalog>.<schema>` | You cannot create catalogs | Set the `catalog` widget to a catalog you can write to, and re-run notebook 00 |
| "Could not create the evaluation dataset" | No Unity Catalog write access, or the feature is unavailable | Carry on: later notebooks use the 20 local records |
| "Endpoint '...' was not found. Falling back to ..." | The model was retired or is not enabled for you | Set `chat_endpoint` / `judge_endpoint` to an endpoint listed in the preflight output |
| `RuntimeError: ... no databricks-* chat endpoint is available` | No chat endpoint is visible to your user | Open **Serving**, find a chat endpoint you can query, and set both endpoint widgets |
| `PermissionError` / 403 in the smoke-test cell | You cannot query that endpoint | Pick another endpoint, or ask for `CAN QUERY` |
| 429 or timeouts during evaluation | Rate limits (common in Free Edition) | Lower `max_rows`; wait and re-run |
| A judge metric is missing from `results.metrics` | The judge returned an error for every row | Look at the `error` field in notebook 02; check the judge endpoint |
| `ModuleNotFoundError: cobalt_eval_common` | The notebook was imported without its folder | Clone the whole repository into a Git folder so the `.py` file sits next to the notebooks |
| No assessments after starting monitoring | The monitoring service is still catching up | Wait 15-20 minutes; check the warehouse ID and your permissions |
| `ValueError: ... may only contain letters, digits and underscores` | A widget value is not a simple identifier | Fix `catalog`, `schema`, or `dataset_name` |
| Gate prints FAIL | A metric is below its minimum, regressed, or missing | Read the per-metric lines; with 20 rows one answer moves a score by 5 points |

## What was verified, and where

The notebooks were tested offline against MLflow 3.18 with a local tracking store, a stubbed chat model, and a stubbed judge model: the dataset, evaluation, scorer, trace, and registry code paths ran for real. Calls to hosted models, Unity Catalog tables, and the Beta monitoring service can only be confirmed in a workspace, so run `00` and `01` with `max_rows = 3` first.
