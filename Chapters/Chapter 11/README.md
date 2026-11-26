# Chapter 11 — Governance of AI and Multi-Modal Systems

Code and diagram companion for Chapter 11 of *Generative AI Solutions with Databricks*.

> **Implementation and safety boundary:** CobaltWorks and its industrial equipment scenarios are fictional. The code in `reference_examples/` consists of **expanded, illustrative source excerpts** preserved from the manuscript. These are **not** validated end-to-end Databricks notebooks or production security controls. Do not run them on production data or treat their outputs as equipment-maintenance authorization. Any executable lab built from the examples needs cloud-specific configuration, permissions, repeatable tests, and a non-production workspace acceptance run.

## What this chapter teaches

Chapter 11 extends the data-engineering and AI-application patterns of preceding chapters to **governance boundaries**: multimodal Bronze/Silver/Gold data, redaction, authorization, registered-model privileges, lineage, model-service routing, inference logging, regulated workflows, and incident response.

The chapter intentionally includes **short code excerpts**. The longer reference examples remain separate so readers can inspect implementation patterns without interrupting the narrative. They are not a claim of full platform integration.

## Reference source files

| File | Subject | Requirements / limitation |
|---|---|---|
| `01_bronze_autoloader.py` | Governed PNG Auto Loader ingestion | Volumes, UC privileges, Spark, checkpoint; live run required |
| `02_silver_image_redaction.py` | Best-effort OpenCV face redaction | OpenCV, Pandas UDF; must test detector misses and quarantine |
| `03_silver_acoustic_mfcc.py` | MFCC audio feature extraction | librosa, soundfile, Pandas UDF; invalid audio must be quarantined |
| `04_region_filter.sql` | UC region-entitlement table and row filter | Approved account group, tables and security schema |
| `05_manual_chunks_ai_search.py` | Approved manual chunks and AI Search index | Existing AI Search endpoint, CDF, correct access controls |
| `06_uc_terraform.tf` | AWS IAM trust and UC external location | Illustrative; variables, trust policies and state not complete |
| `07_uc_model_grants.sql` | Registered-model grants | UC account group and service principal prerequisites |
| `08_model_classification_tags.sql` | Classification tags on registered models | Tags alone do not enforce ABAC or export compliance |
| `09_mlflow_model_aliases.py` | Champion/challenger aliases | Alias changes do not automatically update serving endpoints |
| `10_lineage_review.py` | Table lineage review | Does not prove complete model-training provenance |
| `11_inference_logs.sql` | Gateway model-service log review | Verify service-specific log schema and observed coverage |
| `12_gateway_usage.sql` | Model-service destination usage | Observability query, not a fallback policy or route definition |
| `13_loto_screen_pyfunc.py` | Illustrative LOTO screening model | A screening signal, not a production safety guardrail |
| `14_multi_environment_bundle.yml` | Multi-target DAB deployment illustration | Validate with Databricks CLI; model and resources must exist |

## Flowcharts

`diagram_sources/` contains editable Mermaid (`.mmd`) and Graphviz (`.dot`) sources for:

- `fig11-01-governed-multimodal-medallion` — independent data and index governance boundaries
- `fig11-02-governed-deployment-observability` — reviewed deployment and operational feedback
- `fig11-03-ai-incident-response` — unsafe answer investigation, containment, and validated release

Book-ready PNG and SVG renders are placed at the ZIP root under `images/ch11/`, matching the AsciiDoc `image::` paths. Rendered images are illustrative; the publisher may want to re-render or redesign them to the book's final visual standards.

## Before executing reference code

1. Use a **non-production Databricks workspace** and a dedicated, writable Unity Catalog catalog/schema/volume. Do not assume the fictional `cobalt_prod` resources exist.
2. Review the specific cloud and serving-feature support. Verify identity, dataset permissions, service-principal privileges, network routing, and retention policies independently.
3. Provision prerequisites and libraries deliberately; source excerpts may depend on earlier objects or notebook variables.
4. Run syntax checks and isolated tests first, then workspace integration tests using synthetic data. Record Databricks Runtime, libraries, endpoint names, cloud region, permissions, actual outcomes, and cleanup.
5. Do not use synthetic maintenance output for physical operational decisions. Real equipment work must follow approved site procedures and qualified human review.

## Local static validation

The **short code excerpts in the manuscript** were checked for Python syntax, YAML parsing, and internal figure/table cross-references. The 14 longer reference examples have not been certified as a runnable, integrated lab. Neither these static checks nor the diagram renders validate cloud infrastructure, SQL grants, Terraform, model serving, logging, or EU AI Act compliance.

## Notes for the future executable lab

A numbered Databricks notebook lab can subsequently build on these references, with setup, permission-positive/negative tests, ingestion/quarantine, approval gates, lineage evidence, Gateway usage, incident response, and guarded cleanup. Such notebooks are **not included here**; do not label this reference package as a tested executable notebook suite.
