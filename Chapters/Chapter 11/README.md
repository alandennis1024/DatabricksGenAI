# Chapter 11 — Governance of AI and Multi-Modal Systems

Code companion for Chapter 11 of *Generative AI Solutions with Databricks*.

> **Implementation and safety boundary:** CobaltWorks and its manufacturing scenarios are fictional. These notebooks use synthetic data to demonstrate governance concepts. They are **not production security controls**, proof of regulatory compliance, or equipment-maintenance instructions. Offline tests passed; live Databricks execution still needs verification.

## What this chapter teaches

Chapter 11 focuses on **governing multimodal data and AI applications**: Bronze and Silver processing, data-quality quarantine, document authorization, model safety checks, lineage, audit evidence, and incident response.

## Python notebooks

| Notebook | What it demonstrates |
|---|---|
| `00_setup_sample_data.py` | Creates synthetic manuals, images, audio, and sample tables |
| `01_bronze_autoloader.py` | Ingests image files and metadata with Auto Loader |
| `02_silver_quality_redaction.py` | Checks image integrity and quarantines invalid files; does **not** certify PII redaction |
| `03_acoustic_features.py` | Validates WAV audio and computes a basic acoustic signal metric |
| `04_document_access_governance.py` | Tests document access rules and excludes restricted or retired manuals |
| `05_model_governance_signals.py` | Demonstrates safety screening and optional MLflow logging |
| `06_lineage_and_audit.py` | Examines lineage and request-level audit evidence |
| `07_incident_response.py` | Walks through a simulated unsafe-response incident |
| `08_integration_tests.py` | Checks the connected synthetic governance workflow |
| `99_cleanup.py` | Optionally removes designated sample tables after confirmation |

## Running the notebooks

Keep the files in `Chapters/Chapter 11/`. In a **non-production, Unity Catalog-enabled Databricks workspace**, use an existing writable catalog (default `cobalt_ops`) and the `ch11` schema. Run notebooks **`00` through `08` in order**. Run `99_cleanup.py` only when finished; deletion requires `confirm=yes`.

**Required supporting file:** The notebooks import `cobalt_ch11.py`. Upload it alongside the notebooks if it is missing from your GitHub folder. Keep `requirements.txt` in the same folder.

## Validation and limitations

The complete companion package passed **35 offline tests** and Python syntax checks. This does **not** establish that Auto Loader, Unity Catalog permissions, system lineage, or optional MLflow operations have passed a live workspace test. Image integrity checks are not full face redaction, and the audio example is not the full MFCC pipeline. Verify cloud permissions and safety controls independently before adapting these patterns for production.

The chapter's three flowcharts are supplied separately under `images/ch11/`, with editable sources in `diagram_sources/` when those assets are added to the repository.
