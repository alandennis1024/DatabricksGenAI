# Chapter 11 — Governance of AI and Multi-Modal Systems

Code and diagram companion for Chapter 11 of *Generative AI Solutions with Databricks*.

> **Implementation and safety boundary:** CobaltWorks and its industrial-equipment examples are fictional. The repository contains **two kinds of code**: a numbered, synthetic governance lab and longer **illustrative reference examples**. The numbered lab passed local Python syntax checks and **35 offline unit tests**, but it has **not been executed end-to-end in a Databricks workspace**. Reference examples are not production security controls or an integrated deployment. Do not use these examples with production data, rely on their output for physical maintenance, or interpret them as proof of legal or regulatory compliance.

## What this chapter teaches

Chapter 11 applies governance principles to multimodal Bronze/Silver/Gold data, privacy screening, quarantine, document-level authorization, Unity Catalog permissions, model lifecycle, lineage and audit evidence, inference logs, deployment controls, incident response, and regulated AI workflows. The printed chapter keeps code excerpts short; this repository provides the corresponding exercises and longer illustrative implementations.

## How the files in this GitHub folder are organized today

The **current GitHub directory shown in the screenshot is flat**. It contains **10 filenames belonging to the numbered notebook lab**, **13 additional reference-example files**, and this `README.md`. The two code groups should **not** be executed as one numerical sequence. In particular, `02_silver_image_redaction.py` is a reference example, while `02_silver_quality_redaction.py` belongs to the numbered lab.

**One filename collides:** there are two different versions of `01_bronze_autoloader.py` in the complete companion package—a runnable lab notebook and a separate manuscript reference example. A single flat directory can hold only one. To identify the lab notebook, open `01_bronze_autoloader.py`: it should begin with `# Databricks notebook source`, import `qualified_schema` from `cobalt_ch11`, and use the `catalog` and `schema` widgets. If it instead uses the example-only `cobalt_prod.factory_bronze` paths, it is the reference version and is **not** the numbered lab step.

**Missing from the screenshot:** the supporting module `cobalt_ch11.py`, `requirements.txt`, `requirements-dev.txt`, `tests/`, `docs/`, and the flowchart assets. Those files are included in the **complete Chapter 11 GitHub companion ZIP** but must also be uploaded before the lab can be reproduced as documented below. A folder view alone cannot establish whether the files exist elsewhere in your repository.

## Numbered Databricks notebooks — the synthetic governance lab

Run these **10 notebooks**, in numerical order, after installing the supporting files. They use Databricks source-format `.py` notebooks with synthetic fixtures.

| Notebook | Chapter concept | Purpose | Expected result |
|---|---|---|---|
| `00_setup_sample_data.py` | Bronze foundations | Creates synthetic manuals, small PNG/WAV files, a Delta table, and a Unity Catalog volume | Source objects and sample files exist |
| `01_bronze_autoloader.py` | Bronze ingestion | Reads sample PNG files with Auto Loader `binaryFile` | Image bytes/metadata ingested; **not** redacted |
| `02_silver_quality_redaction.py` | Silver governance | Checks image integrity, quarantines invalid PNG files, flags valid images for review | Quarantine and `REVIEW_REQUIRED` records |
| `03_acoustic_features.py` | Audio governance | Validates synthetic PCM16 WAV files and computes RMS | Valid/invalid audio distinguished; **not** a full MFCC model |
| `04_document_access_governance.py` | Document authorization | Tests allow/deny decisions and builds approved PUBLIC-only manual chunks | Restricted and retired manuals excluded |
| `05_model_governance_signals.py` | Model safety checks | Tests conservative LOTO screening; optional MLflow artifact | Screening signal only; **not** safety approval |
| `06_lineage_and_audit.py` | Audit evidence | Tests correlation on exact request identifiers; optional system-lineage query | Honest `[PASS]`/`[SKIP]` evidence |
| `07_incident_response.py` | Incident handling | Simulates detecting, containing, and reviewing an unsafe/outdated manual | Recorded synthetic incident disposition |
| `08_integration_tests.py` | Readiness | Checks the connected sample tables and authorization/quarantine outcomes | Lab smoke-test outcomes, not security certification |
| `99_cleanup.py` | Cleanup | Opt-in deletion of named Chapter 11 test tables | Deletes nothing by default; `confirm=yes` required |

The **full lab also requires** `cobalt_ch11.py` beside the notebooks. Do not assume a `.py` notebook is self-contained because it appears in GitHub.

## Additional reference examples — illustrative code, not lab steps

These **13 distinct files appear in the same flat GitHub folder** in your screenshot. They represent code moved out of the manuscript. The **fourteenth reference example** is a *different* `01_bronze_autoloader.py`; it must be restored from the complete package under `reference_examples/` to avoid overwriting the lab's `01` notebook.

| File currently visible | Topic | Important limitation |
|---|---|---|
| `02_silver_image_redaction.py` | OpenCV face blurring | Best-effort demonstration, not validated de-identification |
| `03_silver_acoustic_mfcc.py` | MFCC feature extraction with librosa | Requires testing and invalid-audio quarantine |
| `04_region_filter.sql` | Unity Catalog region-level row filtering | Test with real identities and entitlements |
| `05_manual_chunks_ai_search.py` | Manual chunks and AI Search | Needs actual endpoint, index provisioning, CDF and separate authorization |
| `06_uc_terraform.tf` | AWS IAM role and UC external location | Incomplete illustrative cloud configuration; validate `terraform plan` |
| `07_uc_model_grants.sql` | Registered-model permissions | Requires existing models and supported account principals |
| `08_model_classification_tags.sql` | Sensitivity/export-control tags | Tags alone do not enforce model-execution ABAC |
| `09_mlflow_model_aliases.py` | Champion/challenger aliases | Aliases do not automatically redeploy serving endpoints |
| `10_lineage_review.py` | Table-lineage inspection | Does not reconstruct full training/inference provenance |
| `11_inference_logs.sql` | Inference log analysis | Log schemas and coverage depend on actual serving configuration |
| `12_gateway_usage.sql` | Gateway/service usage | Observability demonstration, not an implemented fallback policy |
| `13_loto_screen_pyfunc.py` | LOTO text screening | Not a production safety guardrail |
| `14_multi_environment_bundle.yml` | Multi-environment DABs | Requires Databricks CLI/resource-schema validation |

For clarity and to avoid collisions, the recommended final repository layout is:

```text
<repository root>/
  Chapters/
    Chapter 11/
      README.md
      00_setup_sample_data.py
      01_bronze_autoloader.py         # numbered lab version
      02_silver_quality_redaction.py
      03_acoustic_features.py
      04_document_access_governance.py
      05_model_governance_signals.py
      06_lineage_and_audit.py
      07_incident_response.py
      08_integration_tests.py
      99_cleanup.py
      cobalt_ch11.py                 # required supporting module
      requirements.txt
      requirements-dev.txt
      databricks.yml
      tests/
      docs/
      diagram_sources/
      reference_examples/
        01_bronze_autoloader.py       # different, illustrative version
        02_silver_image_redaction.py
        03_silver_acoustic_mfcc.py
        ...                          # through 14_multi_environment_bundle.yml
  images/
    ch11/                             # 3 book figures, PNG/SVG
  .github/
    workflows/
      ch11-offline-checks.yml
```

Until that reorganization is complete, the table above identifies which flat-folder files are lab steps and which are **reference-only**. Do not run reference files `09`–`14` in sequence after notebook `08`.

## Run the numbered lab in Databricks

1. Use a **non-production, Unity Catalog-enabled** Databricks workspace and compatible notebook compute. Keep the 10 lab notebooks and `cobalt_ch11.py` together under `Chapters/Chapter 11/`.
2. Select an existing writable catalog (default: `cobalt_ops`) and a dedicated schema (default: `ch11`). The lab does **not** automatically create the catalog. Confirm privileges to create schemas, volumes, tables, and checkpoints.
3. Run `00` → `01` → `02` → `03` → `04` → `05` → `06` → `07` → `08`. Start with default settings; no LLM or paid AI Search endpoint is provisioned by the lab, although Databricks compute and storage may be billed.
4. Set `log_mlflow=yes` in `05` only if you want to test optional MLflow operations. Set `query_system_lineage=yes` in `06` only if you have system-table access. Both default to `no`.
5. Review the results and the acceptance checklist in `docs/WORKSPACE_ACCEPTANCE.md`. Use `99_cleanup.py` only when finished; `confirm=yes` deletes dedicated sample Delta tables but **does not delete the sample volume or checkpoint files**.

### Shared widgets

| Widget | Default | Used in |
|---|---|---|
| `catalog` | `cobalt_ops` | `00`–`08`, `99` |
| `schema` | `ch11` | `00`–`08`, `99` |
| `log_mlflow` | `no` | `05` |
| `query_system_lineage` | `no` | `06` |
| `confirm` | `no` | `99` |

Keep catalog/schema values consistent across the entire lab. Chapter 11 uses a separate schema from earlier chapters.

## Flowcharts and manuscript references

Three illustrations accompany the shortened Chapter 11 manuscript:

- **Figure 11-1:** Governed multimodal medallion architecture.
- **Figure 11-2:** Governed deployment and observability.
- **Figure 11-3:** AI incident-response investigation and recovery.

Their editable Mermaid and Graphviz source files are supplied under `Chapters/Chapter 11/diagram_sources/`, and PNG/SVG image assets are supplied under the **repository root** `images/ch11/`, matching the manuscript's image paths. The screenshot does not show these directories; upload them from the complete package if they are not already present.

## Trust and governance boundaries

- **Identity and authorization:** A synthetic operator ID is not proof of authentication. Verify permissions with two actual authenticated principals and a least-privilege serving identity.
- **AI Search:** Unity Catalog source-table row filters and column masks do **not automatically apply to indexed search results**. Public-only sample chunks demonstrate conservative preparation, not per-user index authorization.
- **Image redaction:** A valid PNG is not necessarily PII-free. The lab tests file integrity/quarantine, whereas OpenCV face blurring is a separate illustrative reference.
- **Audio processing:** The numbered lab computes a basic WAV RMS value. MFCC extraction is a separate, unverified reference implementation.
- **Lineage and logging:** System table coverage is scoped and configuration-dependent. A matching timestamp alone cannot establish who initiated a model request or what documents a model used.
- **Model safety:** A keyword screen, a model alias, a Gateway rule, or an audit log does not prove safety or regulatory compliance. Human approval and additional tested controls are necessary.

## Local validation

The complete companion package's Python notebook sources passed syntax checks, and **35 offline tests passed**. Those results do **not** certify real Databricks workspace execution. The flat GitHub folder in the screenshot does not contain the supporting `tests/` folder, so the commands below require you to upload it first.

From `Chapters/Chapter 11/`:

```bash
python -m pip install -r requirements-dev.txt
PYTHONPATH=. python -m pytest -q tests
python -m compileall -q cobalt_ch11.py 0*.py 99_cleanup.py
```

The GitHub Actions workflow `.github/workflows/ch11-offline-checks.yml` runs **offline checks only** and must be placed under the repository root. Live Databricks validation, Terraform/IAM, AI Search, Gateway, registered-model privileges, inference logs, and legal obligations remain separate verification steps.

## Cost, troubleshooting, and cleanup

| Issue | Check |
|---|---|
| Import error: `cobalt_ch11` | Add the missing helper from the complete ZIP beside numbered notebooks |
| Wrong `01` implementation | Use the notebook version with `# Databricks notebook source`; keep the other under `reference_examples/` |
| No schema, volume, or table permissions | Choose a permitted catalog and verify Unity Catalog grants |
| Auto Loader reads zero files | Run `00` and check the synthetic input volume, checkpoint, and compute compatibility |
| Valid images remain `REVIEW_REQUIRED` | This is deliberate; integrity checks are not PII-redaction approval |
| Missing MFCC output or AI Search index | Those are reference-only patterns, not resources automatically created by the numbered lab |
| Optional lineage/MLflow checks skipped | Set the relevant widget and confirm workspace prerequisites |
| Cleanup leaves volumes or checkpoints | Expected: guarded cleanup removes named Delta tables only |

**Cost warning:** Workspace compute, storage, and any optional MLflow or separately provisioned AI resources can incur charges. Do not run the reference-only Terraform, SQL, or serving examples against production environments.

### Documentation

Consult the [Unity Catalog security documentation](https://docs.databricks.com/aws/en/data-governance/unity-catalog/filters-and-masks/), [Auto Loader](https://docs.databricks.com/aws/en/ingestion/cloud-object-storage/auto-loader/), [AI Search Python SDK](https://api-docs.databricks.com/python/ai-search/index.html), and [MLflow documentation](https://mlflow.org/docs/latest/).

**In-book reference:** `Chapters/Chapter 11/README.md` is the companion entry point. Report actual cloud, Databricks Runtime, model/SDK versions, permissions, date, and observed outcomes whenever claiming that an example was executed.
