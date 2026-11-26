# Chapter 10 — Other GenAI patterns on Databricks: Code generation, testing, and AI/BI data products

Runnable companion notebooks for Chapter 10 of *Generative AI Solutions with Databricks*.

This chapter follows the same hands-on format as Chapters 6 and 7: **Databricks source-format (`.py`) notebooks, consistent widgets, synthetic CobaltWorks data, step-by-step setup, tests, troubleshooting, and a guarded cleanup notebook**. It demonstrates three complementary patterns: **AI-assisted code generation, AI-driven data-quality testing, and AI/BI data products**. The running example connects steel-supplier onboarding and Apex Haulers carrier records for a fictional heavy-machinery manufacturer.

> **Safety and authenticity:** CobaltWorks, its suppliers, and Apex Haulers are fictional, and all records are **synthetic teaching data**. The lab demonstrates reviewed code and Spark transformations, not an automatically operated coding assistant. Notebook `01` provides prompts and review checks; it does not call Genie Code/Databricks Assistant programmatically. Notebook `04` generates 100 **deterministic** telemetry fixtures by default and can make **one optional** `ai_query()` call. Notebook `06` executes candidate SQL and explains Genie configuration, but it does **not** create or certify a Genie Agent. Nothing in this lab is a validated manufacturing safety or logistics decision.

## Notebooks

| **Notebook** | **Chapter 10 section / book example** | **Purpose** | **Evidence / exit condition** |
|---|---|---|---|
| `00_setup_sample_data.py` | Supplier and carrier onboarding | Creates a chapter-specific Unity Catalog schema/volume and synthetic supplier and Apex Haulers CSVs | `[PASS]` source files created: 7 supplier records and 10 carrier records |
| `01_ai_assisted_code_review.py` | Pattern 1: intelligent code generation | Explains prompts, schema mapping, timestamp review, and human approval of generated code | `[PASS]` mapping checks; no claim that an assistant API ran |
| `02_auto_loader_ingestion.py` | Pattern 1: Auto Loader ingestion | Reads both feeds using Auto Loader, independent checkpoints, and a streaming completion wait | `[PASS]` 7 supplier and 10 carrier rows in Bronze tables |
| `03_quality_testing_and_lakeflow.py` | Pattern 2: testing and quality | Applies the **Spark batch equivalent** of reviewed Lakeflow expectations and writes rejected records to quarantine | `[PASS]` 6 supplier / 8 carrier Silver rows; 3 quarantined rows |
| `04_synthetic_telemetry.py` | Pattern 2: synthetic failure scenarios | Creates 100 deterministic, fictional rising-temperature cases; optional **one** billable `ai_query()` request | `[PASS]` fixture invariants; optional recorded model response or rejection |
| `05_gold_business_metrics.py` | Pattern 3: governed business metrics | Builds Gold tables for supplier on-time deliveries and Apex heavy-load counts | `[PASS]` chapter-specific Gold tables created |
| `06_genie_data_product.py` | Pattern 3: Genie and trusted SQL | Evaluates reviewed SQL against sample tables and describes the separate manual Genie setup | `[PASS]` 3/5 completed deliveries on time (**60%**); **4** Chicago heavy loads |
| `07_human_review.py` | Human-in-the-loop governance | Records review requirements for code, metrics, quality, access, and Genie | `[PASS]` five `PENDING_REVIEW` entries—**not** sign-offs |
| `08_integration_tests.py` | End-to-end lab verification | Checks the actual workspace Bronze, Silver, quarantine, Gold, and metric outputs | `[PASS]` only when actual Spark/Delta assertions succeed; Genie UI not certified |
| `99_cleanup.py` | Controlled resource cleanup | Opt-in deletion of the named Chapter 10 sample **tables** | Defaults to **no deletion**; requires `confirm=yes` |

**Supporting files:** `cobalt_ch10.py` (pure contracts, synthetic-data generator, reviewed SQL), `pipelines/lakeflow_supplier_apex.py` (optional declarative Lakeflow pipeline source), `genie/` (instructions, SQL and benchmarks for manual Genie setup), `prompts/assistant_prompts.md`, `sample_data/` (sample CSV snapshots), `tests/`, `requirements-dev.txt`, and optional `databricks.yml`. The optional root-level workflow `.github/workflows/ch10-offline-checks.yml` must go at the **repository root**, not under `Chapter 10`.

[MANUSCRIPT_ALIGNMENT.md](MANUSCRIPT_ALIGNMENT.md) maps the book's concepts and selected listings to these runnable files; it contains no editorial review history.

## Run in a Databricks workspace

1. Open this repository in **Workspace → Git folders**. Keep the files together under `Chapters/Chapter 10/` so the notebooks can import `cobalt_ch10.py`.
2. Use a **Unity Catalog-enabled** workspace with compatible serverless notebook compute or a supported Databricks Runtime. Select an **existing writable catalog**; the default is `cobalt_ops`. The lab uses the isolated `ch10` schema and `ch10_demo_files` managed volume.
3. Run notebooks **`00` → `01` → `02` → `03` → `04` → `05` → `06` → `07` → `08`**, in order and with consistent widget values. Run **`99_cleanup`** only after reviewing its proposed deletions.
4. Keep `live_ai=no` initially. This performs **zero foundation-model calls** and checks the deterministic dataset, mappings, metrics, and real Spark data flow. If you want optional model inference in `04`, set `live_ai=yes` there and configure an accessible `ai_endpoint` first; model calls can incur charges.
5. **Genie requires a separate manual step:** after `05`–`06`, follow `genie/GENERAL_INSTRUCTIONS.md`, connect the permitted tables, configure the instructions/SQL assets, and run the questions in `genie/benchmarks.csv` in a real Genie Agent. Successful SQL checks in notebook `06` do **not** prove Genie answer accuracy.
6. The printed Lakeflow examples correspond to **`pipelines/lakeflow_supplier_apex.py`**, which must run inside a separately configured Lakeflow pipeline. Notebook `03` is a simpler Spark **batch alternative**. **Do not run both as active writers of the same Silver tables.** The `08` notebook assumes the batch route, including its quarantine tables.

### Shared widgets

| **Widget** | **Default** | **Used in** |
|---|---|---|
| `catalog` | `cobalt_ops` | 00–08, 99 |
| `schema` | `ch10` | 00–08, 99 |
| `volume` | `ch10_demo_files` | 00–08, 99 |
| `live_ai` | `no` | Shared config; optional model invocation in 04 |
| `ai_endpoint` | `system.ai.meta-llama-3-3-70b-instruct` | Optional 04; example only, not guaranteed available |
| `confirm` | `no` | 99 only; must be `yes` to delete tables |

**Keep the catalog/schema/volume values consistent across notebooks.** The lab does **not** create a catalog. If `cobalt_ops` does not exist or you lack write permission, select an existing catalog you can use. The static SQL in `genie/verified_queries.sql` uses `cobalt_ops.ch10` and must be adapted to a changed namespace.

### Sample data and expected results

The steel-supplier CSV has 7 rows. One nonpositive quantity is rejected, so 6 rows pass Silver quality checks. Of those, one lacks an observed arrival; among the remaining **5 completed** deliveries, **3** arrive within **ETA + 2 hours**, for an on-time rate of **60%**.

The Apex Haulers CSV has 10 rows. Two are excluded by quality checks, leaving 8 Silver rows. Four qualifying shipments this week have **cargo weight strictly greater than 5,000 kg** and a destination of **ORD, MDW, or GYY** (the Chicago Hub). Exactly 5,000 kg does **not** qualify. The source field `TS_INIT` uses the unusual date format **`yyyy-dd-MM HH:mm`**; verify this rather than guessing `yyyy-MM-dd`.

Notebook `00` generates synthetic records for the **current Monday–Sunday week** on the Python driver. The committed `sample_data/` snapshots show the week of **2026-11-23**. When running near a week boundary, verify the Python driver and SQL session time zones agree. The supplier and carrier data are *not joined* without documented shared keys.

| **Layer** | **Steel supplier table** | **Apex Haulers table** |
|---|---|---|
| Bronze | `cobalt_ops.ch10.supplier_shipments_bronze` | `cobalt_ops.ch10.apex_shipping_logs` |
| Silver | `cobalt_ops.ch10.supplier_shipments_silver` | `cobalt_ops.ch10.apex_shipments_silver` |
| Quarantine (batch route) | `cobalt_ops.ch10.ch10_supplier_quarantine` | `cobalt_ops.ch10.ch10_apex_quarantine` |
| Gold | `cobalt_ops.ch10.ch10_supplier_reliability_gold` | `cobalt_ops.ch10.ch10_apex_heavy_loads_gold` |

## Prerequisites and permissions

- A supported **Databricks workspace with Unity Catalog** and Spark/Delta/Auto Loader access. Use an existing catalog with `USE CATALOG` and `CREATE SCHEMA` (or an already writable chapter schema), plus appropriate `USE SCHEMA`, `CREATE TABLE`, and `CREATE VOLUME` privileges. Source/checkpoint volume reads and writes also require permissions.
- Compatible notebook compute. Notebooks `00–08` use workspace-provided Spark libraries; **`requirements-dev.txt` is for local offline tests**, not a full Databricks runtime lockfile.
- A SQL warehouse and the relevant Unity Catalog data permissions for the **manual** Genie exercise. Grant only the intended principal access. Genie instructions, examples, and benchmark questions do not enforce authorization themselves.
- For the **optional Lakeflow route**, rights to create/configure a pipeline and privileges on its source and target tables. Do not have the optional pipeline and batch notebook `03` write to the same Silver tables concurrently.
- For the **optional `ai_query()` experiment**, access to a compatible serving endpoint and support for the function in your runtime/region. `live_ai=no` is the default. No model endpoint is provisioned automatically.
- For the optional bundle, an authenticated Databricks CLI. Its `ch10_setup_smoke` job is not a full Auto Loader/Lakeflow/Genie deployment.

## Trust and validation boundary

| **Component** | **Verified in supplied package** | **Needs workspace execution** |
|---|---|---|
| Python data-generation and business rules | Offline `pytest` suite and static fixture consistency | No |
| Notebook and Lakeflow source syntax; workflow/bundle YAML | Static checks only | Yes, for actual execution and bundle schema validation |
| Auto Loader, Bronze/Silver/Gold tables, quarantine | Real Spark code included, but not locally executed | **Yes:** run 00, 02, 03, 05, 06, 08 |
| Lakeflow expectation decorators | Real pipeline source included as **alternative** | **Yes:** create/run the pipeline separately |
| Assistant/Genie Code prompts | Reader-operated prompting and code-review workflow | **Yes:** actual assistant interaction and human assessment |
| Optional `ai_query()` | Guarded call and JSON validation logic supplied | **Yes:** accessible endpoint and observed output |
| Genie Agent and semantic benchmarks | SQL candidates, instructions, and expected fixture outputs | **Yes:** configure Genie and run actual UI benchmarks |
| Human sign-off | Five `PENDING_REVIEW` checklist records only | **Yes:** independent approval required |

**Validation recorded for this package:** 10 notebook files passed Python syntax checks; the shared module and optional Lakeflow source parsed; **38 offline tests passed**. Static sample data reproduce **60%** steel-supplier on-time completion and **4** qualifying Chicago heavy loads. This does **not** establish that Auto Loader, Spark, Lakeflow, model endpoints, or Genie worked in a Databricks workspace. After workspace acceptance, record the cloud/region, compute runtime, actual permissions, endpoint, execution date, and observed results in the validation record.

### Optional CLI and CI

From `Chapters/Chapter 10/`, run the offline checks:

```bash
python -m pip install -r requirements-dev.txt
PYTHONPATH=. python -m pytest -q tests
python -m compileall -q cobalt_ch10.py *.py pipelines/lakeflow_supplier_apex.py
# Requires Databricks CLI authentication:
databricks bundle validate -t dev
# Optional setup-smoke job only; not Genie or paid AI provisioning:
databricks bundle deploy -t dev
```

The optional `.github/workflows/ch10-offline-checks.yml` runs syntax and deterministic tests. It does **not** execute a Databricks workspace workflow or certify Genie/Lakeflow behavior. See the included `VALIDATION.md` for a checklist of unverified integrations.

## Operational and cost warnings

**Spark/serverless compute, SQL warehouses, storage and optional inference can incur charges.** No Genie Agent, paid serving endpoint, or Lakeflow pipeline is automatically created by importing this repository. Avoid accidentally rerunning source ingestion against unrelated workloads. Independent Auto Loader checkpoints track processed files; **deleting a Bronze table without resetting its corresponding checkpoint can leave it empty on rerun**. Reset only checkpoints belonging to this lab after confirming ownership, or use a fresh chapter schema/volume.

`99_cleanup.py` defaults to `confirm=no`; with `confirm=yes`, it drops a named allowlist of Chapter 10 **tables only**. It intentionally retains the catalog, schema, volume, source CSVs, streaming checkpoints, and optional manually configured Genie/Lakeflow assets. Review and remove retained resources separately when appropriate. Do not run the cleanup notebook against a schema containing shared or production data.

## Known limitations (not hidden)

1. **The lab does not call Databricks Assistant / Genie Code automatically.** Notebook `01` validates mappings and shows an interactive prompt/review approach, rather than asserting code was generated by a private assistant API.
2. **Default telemetry is deterministic synthetic data.** It is useful for regression tests, not a physically calibrated digital twin or proof of predictive-maintenance accuracy. The optional `ai_query()` produces untrusted candidate JSON requiring validation.
3. **Notebook `03` is batch Spark, not a running Lakeflow pipeline.** The alternative source under `pipelines/` must be configured and evaluated separately. It reports expectation events, not notebook `03`'s quarantine tables.
4. **Genie configuration and its SQL-generation accuracy are not automated.** Notebook `06` checks approved candidate SQL against the seeded tables; configure the actual Genie Agent and benchmark its generated SQL in the workspace.
5. **The feeds lack a documented join key.** The two datasets are complementary parts of an onboarding exercise; their rows must not be joined to invent a shared supplier–carrier lineage.
6. **The sample does not certify safety, security, or compliance.** Human approval, actual identity-based access, data lineage, retention, and production monitoring require separate design and evidence.
7. **The optional `databricks.yml` deploys a setup-smoke job only.** It does not automatically deploy Auto Loader jobs, Lakeflow pipelines, Genie Agents, or serving endpoints.

## Troubleshooting

| **Symptom** | **What to check** |
|---|---|
| Cannot create schema/volume | Use a writable existing catalog; confirm `CREATE SCHEMA`/`CREATE VOLUME` privileges |
| `cobalt_ch10` import fails | Keep the module alongside the notebooks in the Git folder; inspect the notebook working directory and Python path |
| `02` finds no source CSVs | Run `00`; verify the volume path, permissions, and current-week files |
| `02` does not reingest after Bronze recreation | The original checkpoint may still mark files as processed; use a clean lab namespace or reset only lab-owned checkpoints |
| Supplier result differs from 60% | Check completed-delivery filtering, null `Actual_Arrival`, timestamp parsing, and sample week |
| Apex date parses to null | Interpret `TS_INIT` as `yyyy-dd-MM HH:mm` |
| `03` fails on a missing transaction ID | `expect_or_fail` behavior is intentional; inspect or fix source records rather than silently dropping them |
| Lakeflow pipeline conflicts with batch `03` | They target the same Silver table names; run only one active writer |
| Optional `04` inference fails or returns invalid JSON | Check `live_ai`, endpoint availability and permissions; reject untrusted output |
| `06` returns an unexpected heavy-load count | Confirm week boundaries, three Chicago destination codes, and **strictly > 5000** weight threshold |
| `06` SQL passes but Genie gives a wrong answer | Run Genie benchmark questions and inspect generated SQL; SQL validation is not Genie validation |
| `08` expects missing quarantine tables | The optional Lakeflow route does not create batch-route quarantine tables |
| `99` deletes nothing | Set `confirm=yes` only after reviewing the fixed deletion list |

### In-book cross-reference

The printed Chapter 10 should reference **`Chapters/Chapter 10/README.md`** and the numbered notebooks for executable examples. The chapter introduces the concepts of AI-assisted code generation, reviewed Lakeflow expectations and Genie data products; the companion provides the full input fixtures, code, controls, and verification steps. Chapter 7 focuses on building and securing compound AI applications, whereas Chapter 10 focuses on **AI-assisted data engineering, data quality and business consumption**. Keep each chapter's labs independent.

**References:** [Databricks Auto Loader with Unity Catalog](https://docs.databricks.com/aws/en/ingestion/cloud-object-storage/auto-loader/unity-catalog), [Lakeflow Python API](https://docs.databricks.com/aws/en/ldp/developer/python-ref), [Lakeflow expectations](https://docs.databricks.com/aws/en/ldp/expectations), [SQL `ai_query()`](https://docs.databricks.com/aws/en/sql/language-manual/functions/ai_query), and [Genie evaluation](https://docs.databricks.com/aws/en/genie-agents/monitor).
