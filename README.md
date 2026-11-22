# Generative AI Solutions with Databricks — Companion Code and Artifacts

This repository contains the companion code, prompts, templates, and figure
sources for the O'Reilly book **Generative AI Solutions with Databricks**.

Read the book on the O'Reilly platform:
<https://learning.oreilly.com/library/view/generative-ai-solutions/9798341639881/>

Throughout the book, the fictional company **CobaltWorks** serves as the
running case study; all sample data, names, and numbers are illustrative.

## Repository layout

Code and artifacts are organized by chapter under `Chapters/Chapter N/`:

| Chapter | Contents |
|---|---|
| 1 | Genie Code image-prompt exchange (text) · figure sources |
| 2 | Figure sources |
| 3 | Genie Code prompt for building the CobaltWorks Spec Assistant · figure sources |
| 5 | Design & Architecture Blueprint template (DOCX/MD) · NFR checklist (XLSX) · example notebook · figure sources |
| 6 | RAG notebooks: sample data, AI Search index, filtered queries, RAG chain, metadata-filter access control, and the two-user access-control validation test |
| 8 | Agent notebooks: UC function tools, tool-calling agent, router agent, human-in-the-loop, guided vs. unguided |
| 9 | Agents on transactional systems: operational tools, guardrails, MCP demo, Lakebase memory, audit trail and queries |
| 12 | Evaluation notebooks: eval dataset, `mlflow.genai.evaluate`, judge debugging, custom judges, version comparison gate, production monitoring, trace audit |
| 13 | Data quality agent end to end: packaging, registry, aliases, evaluation, promotion, prompt registry, serving, gateway, AI Search, Apps, ingestion — plus the `ch13-dab` Declarative Automation Bundle and Streamlit app |
| 15 | Looking ahead: prompt registry, decision records, MCP tool contract, trust registry, operating views |

Figure sources live in `Chapters/Chapter N/figures/`; see the README in each
folder for the disclaimer and rendering instructions.

## Using the notebooks

- Notebooks are in **Databricks Python notebook source format** (files start
  with `# Databricks notebook source`). Import them into a Databricks
  workspace — as a Git folder or via workspace import — and run them there.
  They are not standalone Python scripts.
- Each chapter folder has its own `README.md` (notebook-to-book mapping,
  prerequisites, cost warnings) and `requirements.txt`.
- Run the notebooks in order, starting with `00_setup_*`. Finish with
  `99_cleanup.py`, which deletes created resources — it runs only when the
  `confirm` widget is set to `yes`.
- **These notebooks create billable resources** (for example, AI Search
  endpoints are billed while they exist). Do not skip the cleanup notebook.
- A Unity Catalog-enabled workspace with serverless compute (or a DBR 17+
  cluster) is assumed.

## Feedback and reuse

If you find an error in the code or the book, please open an issue. If you
reuse or adapt the code or diagrams, please credit the book.
