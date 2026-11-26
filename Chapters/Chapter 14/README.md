# Chapter 14 — Monitoring and Observability for AI Systems

Code companion for Chapter 14 of *Generative AI Solutions with Databricks*.

> **Implementation boundary:** CobaltWorks and the XM-400 scenario are fictional. The files in `reference_examples/` are **teaching excerpts**, not a fully executed Databricks lab or production monitoring controls. They require workspace-specific setup, service permissions, independent privacy review, and live validation. Their outputs do not authorize physical maintenance.

## What this chapter teaches

Chapter 14 connects four complementary signals: **inference logs** capture available model-service traffic, **data profiling** tracks statistical changes, **MLflow tracing** records application spans, and **MLflow GenAI evaluation** tests response quality. The chapter then explains alerting, latency, usage costs, observational ROI analysis, and incident runbooks.

## Reference source files

| File | Subject | Requirements / limitation |
|---|---|---|
| `01_inference_table_check.sql` | Inference table configuration and coverage | Unity Gateway service, governed log table; may be incomplete |
| `02_forensic_sql.sql` | Request/response forensic SQL | Logged JSON only; retrieval IDs require MLflow traces |
| `03_time_series_data_profile.py` | Data-quality monitoring profile | Unity Catalog table and supported SDK/permissions required |
| `04_sql_alert.tf` | Databricks SQL alert Terraform | Provider, warehouse, notification destination required |
| `05_quality_review_workflow.yml` | Dispatch and review of a quality alert | Human-review GitHub environment must be configured |
| `06_mlflow_trace.py` | Instrumented retrieval and generation | Inject compatible real retriever and model adapters |
| `07_incident_outcomes.sql` | Observational downtime comparison | Requires reviewed, incident-level cohort dataset |
| `08_loto_reference_scorer.py` | Deterministic LOTO text signal | Not sufficient to approve physical maintenance |
| `09_evaluate_answer_sheet.py` | MLflow GenAI answer-sheet evaluation | Requires MLflow backend and reviewed scorer |

`loto_reference_scorer.py` is the helper imported by the answer-sheet example.

## Before using the examples

Use a **non-production, Unity Catalog-enabled** workspace. Replace fictional `cobalt_prod` names with your configured resources; inspect actual Unity Gateway inference-table fields before querying. Set up permissions and notifications deliberately and run `databricks bundle validate` or `terraform validate` when applicable. **Do not assume live inference logging is complete** or that an alert, keyword check, or LLM judge proves equipment safety.

## Validation boundary

The manuscript's Python snippets passed syntax parsing and the YAML snippet parsed as YAML. These checks do **not** validate cloud permissions, SQL results, Databricks SDK integration, Terraform, model endpoints, cost attribution, MLflow scoring or production deployment. A full numbered Databricks notebook lab is **not included** in this reference-only package.
