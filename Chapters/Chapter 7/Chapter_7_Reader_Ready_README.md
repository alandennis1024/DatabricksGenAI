# Chapter 7 — Application Development with AI Models

Runnable companion notebooks for Chapter 7 of *Generative AI Solutions with Databricks* (O’Reilly).

This chapter extends the **Chapter 6** learning pattern: Databricks source-format (`.py`) notebooks, consistent widgets, synthetic CobaltWorks data, step-by-step setup, tests, troubleshooting, and a guarded cleanup notebook. It illustrates **Pydantic contracts, MLflow model packaging, Auto Loader PDF ingestion, Databricks AI Search, a real chat-model call, deterministic safety validation, LLM evaluation, MLflow tracing, and defensive security patterns**.

> **Safety and authenticity:** CobaltWorks and the XM-400 turbine are fictional. The sample PDF and all telemetry are **synthetic training material** and **not approved equipment procedures**. Nothing in this lab authorizes maintenance work. The lab receives a **precomputed acoustic anomaly score** and a Boolean thermal-risk flag; it does **not** run a trained acoustic classifier, a VLM, or four production models. Real physical maintenance decisions belong to qualified staff and approved site procedures.

## Notebooks

| Notebook | Chapter 7 section / book listing | Purpose | Evidence / exit condition |
|---|---|---|---|
| `00_setup_sample_data.py` | Development loop; data/knowledge/behavior | Creates synthetic clearance and telemetry Delta tables and a small PDF in a Unity Catalog volume | `[PASS]` source tables and the synthetic PDF exist |
| `01_models_as_dependencies.py` | Models as Dependencies / PyFunc | Validates Pydantic input, packages a **fail-closed guardrail PyFunc**, logs and reloads it with MLflow. Optional Unity Catalog registration | `[PASS]` model logged, reloaded, and returns `INSUFFICIENT_CONTEXT` (not a manufactured diagnosis) |
| `02_ingestion_and_vector_search.py` | Ingestion / indexing | Auto Loader reads the sample PDF; creates Delta chunks with Change Data Feed, a STANDARD AI Search endpoint, and a TRIGGERED Delta Sync index | `[PASS]` machine-filtered retrieval returns an indexed sample manual |
| `03_compound_ai_application.py` | Use-case orchestration | Demonstrates clearance lookup → real retrieval → optional real LLM → deterministic validation and safe fallback | `[PASS]` retrieval; when `live_inference=yes`, records the actual model response or a rejection |
| `04_testing_and_evaluation.py` | Unit and integration testing; MLflow scorers | Runs the offline tests; optionally runs MLflow GenAI evaluation against real model responses | `pytest` results; with live mode, a genuine MLflow evaluation run ID/metrics |
| `05_tracing_and_debugging.py` | Tracing and Debugging | Wraps actual clearance, search, and LLM calls in MLflow spans | Experiment trace with observed spans (no made-up latencies) |
| `06_security_and_redteaming.py` | Defensive Engineering Patterns | Tests fail-closed JSON Boolean decisions; optionally submits adversarial prompts to a live classifier | Local strict-type tests; optional observed classifier PASS/FAIL by case |
| `07_integration_tests.py` | Integration Testing | Verifies sample clearance and real index access; optionally invokes the full real model pipeline | Explicit `[PASS]`, `[REVIEW]`, or `[SKIP]` with no false claims of accuracy |
| `99_cleanup.py` | Operational Readiness | Opt-in deletion of the Chapter 7 index, tables, sample volume and, optionally, the dedicated empty search endpoint | Default is **no deletion**; requires `confirm=yes` |

**Supporting files:** `cobalt_ch07.py` (pure contracts, safety rules, injectable orchestration), `cobalt_runtime.py` (Databricks SDK/AI Search adapters), `cobalt_model.py` (MLflow adapter), `tests/test_contracts.py`, `requirements.txt`, and optional `databricks.yml`. An optional root-level GitHub Actions workflow is included at `.github/workflows/ch07-offline-checks.yml` and should be copied to the **repository root**, not inside `Chapter 7`.

## Run in a Databricks workspace

1. Open the same repository used for Chapter 6 using **Workspace → Git folders**. Keep this folder intact: `Chapters/Chapter 7/`. The Python notebooks use files in this folder, and `requirements.txt` must remain next to them.
2. Attach **serverless** notebook compute (or a compatible recent Databricks Runtime). Select a **Unity Catalog-enabled** workspace and an existing writable catalog; the default is `cobalt_ops`, with an isolated `ch07` schema.
3. Run **`00`**, then **`01`** (PyFunc), then **`02`** (Auto Loader and index); after `02` completes, run **`03`**, **`04`**, **`05`**, **`06`**, and **`07`**. Finally, run **`99_cleanup`** when you are finished.
4. Every notebook begins with `%pip install -q -r requirements.txt` and a Python restart. Run the notebook **from the top**, including the restart cell, because later cells depend on its scoped environment.
5. First use the default `live_inference=no`. This validates data, retrieval, the packaged guardrail and deterministic tests without creating model-generation charges. To test a real chat model in `03–07`, set **`live_inference=yes` in each relevant notebook**. The chat endpoint must exist and be accessible; these calls can be billed.
6. **The operator clearance table is demonstrative, not authentication.** `operator_id` is a widget value in this lab. Production must bind identity to verified SSO/service-principal context before checking clearance. The examples do not implement that identity proof.

### Shared widgets

| Widget | Default | Used in |
|---|---|---|
| `catalog` | `cobalt_ops` | all |
| `schema` | `ch07` | all |
| `search_endpoint` | `cobalt-turbine-search-ch07` | 02–07, 99 |
| `index_name` | `turbine_manual_index` | 02–07, 99 (fully qualified automatically) |
| `embedding_endpoint` | `databricks-gte-large-en` | 02 |
| `chat_endpoint` | `databricks-meta-llama-3-3-70b-instruct` | 03–06 |
| `model_name` | `turbine_diagnostic_ch07` | 01 (UC registration opt-in) |
| `operator_id` | `TECH-104` | 03 |
| `register_model` | `no` | 01 |
| `live_inference` | `no` | 03–07 |
| `confirm`, `delete_endpoint`, `delete_sample_volume` | `no` | 99 |

**Keep widget values consistent across notebooks.** `schema=ch07` ensures Chapter 6's default `cobalt_ops.serving` tables and `cobalt-policy-search` endpoint are not touched.

## Prerequisites and permissions

- A supported Databricks workspace with **Unity Catalog**, a preexisting writable catalog, and privileges to `CREATE SCHEMA`, `CREATE TABLE`, `CREATE VOLUME`, read/write the Chapter 7 tables and volume, and create MLflow experiments. This lab does **not** automatically create a catalog.
- Serverless notebook compute or a compatible Databricks cluster. The Auto Loader binary-file stream uses a governed volume and a checkpoint directory; workspace features/privileges may vary.
- AI Search endpoint/index create privileges and a usable **embedding-serving endpoint**. The `02` notebook uses the supported `databricks.ai_search.client.AISearchClient`, a STANDARD endpoint, a TRIGGERED Delta Sync index, and `delta.enableChangeDataFeed = true` on the source chunks table. Check regional availability.
- For `03–07` live tests, access to a chat endpoint that accepts the SDK `ChatMessage` format. Foundation model names can change. Set `chat_endpoint` to an available, compatible model. **There is no silent endpoint substitution:** if it does not exist, fix the widget and rerun.
- For `04` custom LLM judges, enable `enable_judges=yes`. Your workspace must provide MLflow's designated judge model or a configured substitute; model-judge support/permissions vary.
- `01` logs an MLflow model by default. If `register_model=yes`, you also need the requisite Unity Catalog model creation permissions. **Registration is not required for the local load-and-predict example.**
- For the optional bundle, install the Databricks CLI and authenticate to the target workspace before running `databricks bundle validate -t dev`.

## Trust and validation boundary

| Component | Proven by supplied package | Still needs a live workspace run |
|---|---|---|
| Pydantic contracts, machine-family parser, fail-closed validation | **Offline pytest** (see results below) | No |
| `.py` notebook source syntax, YAML parsing | Static checks only | Notebook execution |
| MLflow PyFunc | Uses real MLflow API pattern | **Yes:** log/reload/optional UC registration |
| Auto Loader / PDF / AI Search | Full SDK code and synthetic PDF generator provided | **Yes:** ingestion, endpoint, index creation/query |
| Chat-model synthesis | Actual SDK chat invocation (not a hardcoded answer) | **Yes:** requires compatible endpoint |
| MLflow GenAI evaluation and tracing | Real API calls, no fabricated scores | **Yes:** runtime and endpoints |
| Security and adversarial tests | Offline Boolean contract tests; real LLM classifier optional | **Yes:** test behavior against real inputs and multiple models |
| Four-model diagram (embedding, acoustic, VLM, LLM) | **Architectural concept only** | Custom acoustic model and VLM not provided |

**Validation recorded while preparing this package:** the pure Python test suite passed locally with Python 3.13.5, Pydantic 2.13.5 and pytest 9.0.2 (13 passing tests). That does *not* validate Databricks network calls, Spark, serving endpoints, or bundle deployment. Once you run the notebooks, replace this statement with the actual workspace cloud, Databricks Runtime, model endpoints, library versions, date and observed outcome. The repository's `requirements.txt` uses compatibility ranges; pin precise versions after the workspace acceptance run for publication reproducibility.

### Optional CLI and CI

At the root of this chapter folder:

```bash
PYTHONPATH=. python -m pytest -q tests
python -m compileall -q cobalt_ch07.py cobalt_model.py cobalt_runtime.py
# Databricks CLI credentials required:
databricks bundle validate -t dev
# Deploy only the optional setup/smoke job, not paid model/index resources:
databricks bundle deploy -t dev
```

The optional GitHub Actions workflow executes only offline Python tests and syntax checks. It **does not** call Databricks or certify live integrations. Only merge or deploy after the `07` notebook has been reviewed in a configured non-production workspace.

## Operational and cost warnings

**AI Search endpoints may continue to incur charges when idle.** Embedding generation, endpoint creation/sync, serverless compute and chat-model calls may incur costs. Review workspace billing and quotas. Run `99_cleanup` after the lab; the notebook never drops your catalog or schema. It only drops the index and specially named Chapter 7 tables after `confirm=yes`, and deletes the sample volume/empty endpoint only with their separate opt-in flags. **MLflow experiments and UC model registrations are intentionally retained** so a reader's audit trail is not silently erased.

## Known limitations (not hidden)

1. The lab does not supply a trained acoustic anomaly model or thermal VLM. The four-model dependency graph is an *architectural illustration*, whereas the runnable path is **precomputed synthetic telemetry + governed text retrieval + a compatible LLM endpoint + deterministic safety validation**.
2. There is no verified login identity binding in these notebooks. The demo clearance table demonstrates filtering only; real production authorization requires authenticated identity, least privilege and server-side checks.
3. The code returns *draft diagnostic text*; it does not authorize maintenance, control equipment or produce calibrated diagnosis probabilities.
4. The PyFunc registered in `01` is a **standalone safety gate** that deliberately returns `INSUFFICIENT_CONTEXT`; the connected live orchestration is in `03` and the `cobalt_runtime.py` module. Do not mistake the guardrail PyFunc for the whole deployed agent.
5. An LLM prompt-injection classifier is a best-effort detection layer, not a sandbox or guaranteed firewall. A false negative is possible. Indirect injection through retrieved content requires independent source screening and access controls.
6. MLflow GenAI evaluation uses a tiny teaching dataset. Any resulting scores are **sample-specific** and should not be claimed as plant failure-prediction accuracy.
7. `databricks.yml` defines an optional *setup smoke job*, not full automated multi-environment model and gateway deployment. This keeps the bundle from accidentally provisioning costly infrastructure.

## Troubleshooting

| Symptom | What to check |
|---|---|
| `CREATE SCHEMA` denied | Choose a preexisting catalog with `CREATE SCHEMA` or ask an administrator. |
| `CREATE VOLUME` denied | Verify UC privileges or use a writable catalog/schema. |
| Python cannot import `cobalt_ch07` | Open `.py` notebooks from the Git folder with the three helper `.py` files alongside them. Run from the notebook's folder. |
| PDF not found | Run `00` and verify `.../ch07_manuals/XM-400-SYNTHETIC-MANUAL.pdf`. |
| Auto Loader stream fails | Check serverless/DBR support, volume privileges and `_checkpoint_pdf` location. |
| Endpoint creation fails | Check AI Search entitlement, regional limits, model endpoint name and account billing. |
| Index not ready | Inspect Delta CDF, source table, endpoint state and the `02` wait/sync output. |
| `manifest` missing columns | Inspect source table/index schema and rerun `02`; never assume index column order. |
| Chat model returns schema-invalid text | The diagnostic must be rejected with `SAFETY_ABORT` and manual review; inspect MLflow tracing and model compatibility. |
| All 04/05/06 live steps skipped | Set `live_inference=yes` in that notebook. The default is deliberately safe. |
| Real red-team classifier approves attack text | Record `[FAIL]`, tighten controls and include additional independent guardrails; do not claim resilience. |
| Cleanup says nothing deleted | Set `confirm=yes`; optional endpoint/volume deletion need additional explicit flags. |

### In-book cross-reference

The printed Chapter 7 should contain short excerpts plus a reference to **`Chapters/Chapter 7/README.md`**. All full code is in the notebooks and helper modules. When citing execution, say **exactly which environment and version were tested**; do not label mocked tests as end-to-end validation.

**Reference:** [Databricks AI Search Python SDK](https://api-docs.databricks.com/python/ai-search/index.html), [Databricks source-format notebooks](https://docs.databricks.com/aws/en/notebooks/notebook-format), [MLflow GenAI evaluation](https://mlflow.org/docs/latest/genai/eval-monitor/).
