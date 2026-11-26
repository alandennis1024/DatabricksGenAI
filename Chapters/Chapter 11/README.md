# Chapter 11 — Governance of AI and Multi-Modal Systems

Runnable **synthetic governance lab** and reference implementation files for
Chapter 11 of *Generative AI Solutions with Databricks*.

This chapter extends the earlier RAG, application-development, and AI-assisted
engineering examples by focusing on **governance and incident-response
boundaries**: multimodal Bronze/Silver/Gold, quarantine, document approval,
least privilege, model lifecycle, audit provenance, and operational release.

> **Important:** CobaltWorks and the XM-400 are fictional. The tests are *not*
> industrial maintenance procedures, production-ready security controls, or
> evidence of legal compliance. An image passing the PNG integrity gate remains
> **REVIEW_REQUIRED**; a simple safety keyword screen may miss dangerous advice.
> **Offline tests and a single-principal lab cannot establish enterprise
> authorization, AI Search access, or GDPR/EU AI Act compliance.**

## Notebooks

Databricks source-format (`.py`) notebooks sit directly under
`Chapters/Chapter 11/` and should be run sequentially.

| Notebook | Chapter 11 subject | Purpose | Evidence / exit condition |
|---|---|---|---|
| `00_setup_sample_data.py` | Multimodal Bronze foundations | Create a dedicated `ch11` schema, synthetic manuals, small PNG/WAV fixtures, and an isolated UC volume | `[PASS]` synthetic table and four files created |
| `01_bronze_autoloader.py` | Bronze ingestion | Use Auto Loader (`binaryFile`) to persist image bytes and source metadata to Delta | `[PASS]` two files recorded; no claim of validation |
| `02_silver_quality_redaction.py` | Silver governance | Separate corrupted PNGs from parseable but unreviewed images; **do not** publish them as PII-free | `[PASS]` quarantine and review records exist |
| `03_acoustic_features.py` | Acoustic telemetry | Validate a tiny synthetic PCM16 WAV, compute RMS, quarantine invalid WAV; full librosa MFCC is in the reference code | `[PASS]` valid/invalid audio discriminated |
| `04_document_access_governance.py` | Gold, document entitlements, AI Search | Prove fail-closed authorization fixtures and construct **PUBLIC-only** approved chunks | `[PASS]` no retired or restricted manual in public source |
| `05_model_governance_signals.py` | Model registry and safety patterns | Test deterministic LOTO screening; **optional** real MLflow PyFunc logging/reloading | `[PASS]` unsafe text blocked; remaining text requires review |
| `06_lineage_and_audit.py` | Lineage and inference logs | Compare exact request-key match versus false time-only attribution; optional system lineage query | `[PASS]` attribution-contract test; system query `[SKIP]` by default |
| `07_incident_response.py` | Incident-response walkthrough | Document synthetic retired-manual containment with a reviewable Delta record | `[PASS]` retired manual excluded from public chunks |
| `08_integration_tests.py` | Governance readiness | Check the *observed synthetic tables* for quarantine, allowlisting and fail-closed results | Explicit `[PASS]` and `[SKIP]` messages |
| `99_cleanup.py` | Cleanup and retained evidence | Opt-in removal of only seven named Chapter 11 demonstration tables | Default `[SKIP]`; requires `confirm=yes` |

**Supporting source:** `cobalt_ch11.py` contains pure-Python governance
contracts, PNG integrity checking, PCM16 RMS, deterministic manual chunking,
fail-closed document screening, incident dispositions, and request correlation.
`tests/test_governance_contracts.py` runs without Spark, Databricks APIs, MLflow,
or a cloud account.

The `reference_examples/` directory retains the **14 longer examples moved out
of the book**, covering OpenCV, librosa MFCC, SQL row filters, AI Search,
Terraform, model grants/tags, aliases, lineage, inference logging, Unity
Gateway usage, MLflow PyFunc, and multi-target DAB configuration. These
are **educational reference files, not a single ready-to-deploy infrastructure
bundle**. Some depend on resources intentionally absent from the lab.

## Run in a Databricks workspace

1. Use a **non-production Unity Catalog-enabled** Databricks workspace, open the
   repository in **Workspace → Git folders**, and keep all files under
   `Chapters/Chapter 11/`. Ensure the helper `.py` is alongside the notebooks.
2. Select a preexisting writable catalog (default `cobalt_ops`). You need
   `CREATE SCHEMA`, `CREATE VOLUME`, `CREATE TABLE`, and appropriate access to
   the new schema and volume. The lab never creates a catalog automatically.
3. Run `00`, `01`, `02`, `03`, `04`, `05`, `06`, `07`, then `08`, **in order**.
   The default path is synthetic and does not create AI Search endpoints or
   call an LLM. It does execute real Spark/Auto Loader operations, which can
   incur compute costs.
4. Only after inspecting the outputs, run `99_cleanup.py` and set
   `confirm=yes` to drop named sample Delta tables. The sample volume,
   checkpoint directories, MLflow experiments, and external services are
   deliberately retained for manual review; see cost and cleanup guidance.
5. For optional MLflow logging, set `log_mlflow=yes` in notebook `05`. For
   optional actual system lineage, set `query_system_lineage=yes` in `06`.
   Record real results and permissions separately. Do not use an unverified
   principal string or fake group membership to claim successful authorization.

### Shared widgets

| Widget | Default | Used in |
|---|---|---|
| `catalog` | `cobalt_ops` | `00`–`08`, `99` |
| `schema` | `ch11` | `00`–`08`, `99` |
| `log_mlflow` | `no` | `05` |
| `query_system_lineage` | `no` | `06` |
| `confirm` | `no` | `99` |

**Keep `catalog` and `schema` identical throughout.** The defaults are separate
from Chapters 6, 7 and 10; their existing tables/endpoints are not touched.

## Prerequisites and permissions

- Databricks compute capable of running `cloudFiles` `binaryFile` streams and
  writing files using `/Volumes/...` Python file APIs. The test fixture writes
  to a **Unity Catalog managed volume** and uses a dedicated checkpoint.
- The catalog must already exist, with appropriate schema and volume privileges.
  Permissions to create a schema do not imply permission to administer all
  assets or view system audit tables.
- The optional MLflow path requires a compatible installed MLflow version,
  configured experiment, and `mlflow.pyfunc.log_model()` support. `requirements.txt`
  contains **compatibility bounds, not a tested dependency lockfile**.
- The optional lineage path requires `system.access.table_lineage` availability,
  permission and recent captured events. It may return no rows even when a
  workflow actually ran; missing events are not evidence of no data movement.
- *Real two-principal authorization tests* require two separately authenticated
  Databricks identities, not an input widget. Use the separate
  `docs/WORKSPACE_ACCEPTANCE.md` checklist.
- *AI Search*: The lab deliberately publishes only PUBLIC-approved documents
  and does **not** provision a paid search endpoint. To study index creation,
  read `reference_examples/05_manual_chunks_ai_search.py` and verify the
  endpoint, embedding model, CDF, identity filters and billable resources first.
  Source-table RLS/column masks are **not automatically enforced on AI Search
  index queries**.
- *Full face redaction and MFCC*: The reference code needs suitable OpenCV,
  librosa and audio libraries, valid permitted datasets, detector-coverage
  evaluation, and quarantine. Neither library's presence certifies privacy.
- *Infrastructure examples*: `reference_examples/06_uc_terraform.tf` and
  `14_multi_environment_bundle.yml` are non-executed patterns requiring
  cloud-specific roles, IAM trust, existing model versions and CLI validation.

## Trust and validation boundary

| Control / artifact | What the supplied package proves locally | Needs workspace verification |
|---|---|---|
| SQL identifier validation, PUBLIC-only manual policy, retired-manual rejection | Offline unit tests | Actual UC grants, policies, and identity |
| Synthetic image PNG header/CRC checking | Offline unit tests | A production decoder and PII detector accuracy |
| PCM16 RMS and invalid WAV rejection | Offline unit tests | Approved media-processing pipeline and MFCC QA |
| LOTO screening keyword rule | Offline unit tests, **not an all-clear policy** | MLflow (if enabled), human safety controls, real adversarial eval |
| Request-ID match | Exact-key fixture tests | Actual endpoint audit correlation fields and identity provenance |
| Notebook `.py` syntax and supporting assets | Offline parsing | Real Spark notebook execution and permissions |
| AI Search, ABAC, Gateway, Terraform, DAB and EU AI Act | **Not verified by offline tests** | Separate configuration, test evidence, and expert review |

**Validation recorded while preparing this package:** offline Python tests
passed, and all 10 `.py` notebook files parsed as Python. This does not mean
any notebook has already executed on a real Databricks workspace. See the
root-level `VALIDATION.md` and `docs/WORKSPACE_ACCEPTANCE.md`.

### Optional local development and CI

From `Chapters/Chapter 11/`:

```bash
python -m pip install -r requirements-dev.txt
PYTHONPATH=. python -m pytest -q tests
python -m compileall -q cobalt_ch11.py 0*.py 99_cleanup.py
```

The optional GitHub Actions workflow `.github/workflows/ch11-offline-checks.yml`
belongs at the **repository root** and runs **only** offline tests and syntax
checks. It never grants permissions, provisions Gateway routes, or certifies
an actual environment.

## Flowcharts and manuscript assets

The manuscript references **three** images, delivered at ZIP root
`images/ch11/` so the paths are consistent with its `image::images/ch11/...`
directives. The editable `.mmd` and `.dot` flowchart source files live inside
`Chapters/Chapter 11/diagram_sources/`.

- Figure 11-1 — governed multimodal Bronze/Silver/Gold and independent AI Search
  and application access boundaries.
- Figure 11-2 — reviewed infrastructure, serving, observation, and release.
- Figure 11-3 — AI incident evidence, containment, verification and approval.

The images are illustrative; edit and re-render them to meet final publisher
standards before typesetting.

## Operational and cost warnings

**Keep this lab outside production.** Bronze binary tables and volumes can
contain personal or proprietary data in real applications. The sample code
contains only fictional fixtures. Do not relax volume permissions just to get
sample notebooks to run. The synthetic public chunks are intentionally
published separately from the restricted and retired reference manuals.

Compute and optional MLflow operations can incur charges. AI Search endpoint
creation, embedding inference, model serving, and Gateway usage would be
separate billable operations when configured. `99_cleanup` drops **only seven
dedicated synthetic Delta tables** following explicit confirmation. It does
not delete the media volume, streaming checkpoints, AI Search indexes,
serving endpoints, model versions, MLflow traces, experiments, or Git files.
Review these resources manually before incurring or retaining cost.

## Known limitations

1. The lab uses an 8×8 synthetic PNG (not a worker face). PNG header and CRC
   integrity checks are **not** redaction or privacy verification.
2. The Bronze ingestion uses a real `binaryFile` stream; the Silver inspection
   collects **two tiny synthetic images to the driver**. Never use driver
   collection as a production multimodal processing pattern.
3. The acoustic example measures PCM16 RMS; the full MFCC / librosa pattern is
   retained as separate reference code. No trained anomaly classifier is supplied.
4. The manual authorization function uses an explicit `verified_identity`
   parameter. The notebook passes a *unit-test fixture*, **not** an actual
   authenticated principal. The only searchable sample chunks are PUBLIC.
5. MLflow logging and real system table queries are disabled by default.
   The sample safety model has no permission to control equipment.
6. There is no automatic Databricks Gateway guardrail installation, endpoint
   fallback, semantic cache, data-residency proof, Terraform deployment or
   complete model-training lineage reconstruction.
7. The incident scenario documents exclusion of an obsolete manual **from the
   synthetic public table**. A real incident needs index-synchronization
   validation, audit retention, stakeholder authorization and release sign-off.

## Troubleshooting

| Symptom | Check |
|---|---|
| `CREATE SCHEMA` or `CREATE VOLUME` denied | Select an existing catalog with permission; coordinate with a UC administrator. |
| `/Volumes/...` Python file write fails | Verify volume name/permissions, compute/runtime support and path. |
| Python cannot import `cobalt_ch11` | Keep the helper in the same Git folder; verify notebook working directory/module path. |
| Auto Loader fails | Verify `01` checkpoint, source volume path, runtime/Auto Loader support and `cloudFiles.schemaLocation`. |
| Zero image rows | Run `00` and confirm the two `.png` test files exist under `image_in`. |
| All images remain REVIEW_REQUIRED | This is expected for valid PNGs; no certified PII detector is included. |
| MLflow logging skipped | Set `log_mlflow=yes`, verify libraries and experiment permissions. |
| Lineage query denied | `system.access.table_lineage` requires appropriate system schema access; default skips it. |
| AI Search results lack authorized filtering | The lab doesn't create an index. Add *independent* identity-aware index access checks before enabling search. |
| Cleanup did nothing | Set `confirm=yes`; the volume is intentionally retained. |

## Reference documentation

- [Databricks AI Search Python SDK](https://api-docs.databricks.com/python/ai-search/index.html)
- [Auto Loader format and schema options](https://docs.databricks.com/aws/en/ingestion/cloud-object-storage/auto-loader/schema)
- [Unity Catalog row filters and column masks](https://docs.databricks.com/aws/en/data-governance/unity-catalog/filters-and-masks/manually-apply)
- [MLflow model packaging](https://mlflow.org/docs/latest/ml/model/)

The book includes short code excerpts. This repository is the **learning
companion**, not a claim that a cloud-side governance configuration has been
independently audited or certified.
