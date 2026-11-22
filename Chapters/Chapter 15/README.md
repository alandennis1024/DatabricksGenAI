# Chapter 15 notebooks: Looking to the Future

Runnable Databricks notebooks for the "CobaltWorks in 2028" examples in
Chapter 15 of *Generative AI Solutions with Databricks*. Clone the repository
into a Databricks Git folder and open the notebooks in this folder. All names,
numbers, and data are illustrative.

| Notebook | Book section | What it does |
|---|---|---|
| `00_prompt_registry` | MLflow Prompt Registry; Treat prompts as code | Registers a prompt in Unity Catalog, adds versions, sets the `staging` and `production` aliases, loads by `prompts:/name@alias`, blocks an unsafe candidate with an evaluation gate, and rolls back with one alias change. |
| `01_decision_records` | Data, model, and tool lineage | Renders the decision-record template, validates the supplier-suspension example, and shows what the checks catch. |
| `02_mcp_tool_contract` | Make protocol adoption reversible (MCP) | Runs the read-only, propose, and commit tools with an approval token, idempotency key, schema checks, and expiry. |
| `03_trust_registry` | Make protocol adoption reversible (A2A) | Validates remote-agent requests against `trust_registry.yaml`: identity, organization, capability, data classes, expiry, revocation. Includes the chapter's `validate_proposal` check. |
| `04_operating_views` | An evaluation trace; Operating after launch | Creates sample tables with synthetic data, builds the Quality, Reliability, Economics, and Control views from `operating_views.sql`, and displays each view and alert query. |
| `99_cleanup` | | Drops the views, sample tables, and prompt that notebooks 00 and 04 created, but only when `confirm` is `yes`. |

## Prerequisites

* A Databricks workspace with Unity Catalog. Serverless compute, or a cluster
  on DBR 17 or later.
* Notebooks 00 and 04 need a catalog and schema you can write to. The widgets
  default to `catalog = cobalt_ops` and `schema = future`. The notebooks try
  `CREATE CATALOG IF NOT EXISTS`. If you may not create catalogs, set the
  `catalog` widget to one you can use.
* Notebook 00 also needs `CREATE FUNCTION`, `EXECUTE`, and `MANAGE` on that
  schema, because the registry stores prompts as Unity Catalog objects.
* Notebooks 01, 02, and 03 need nothing: no tables, models, or network access.

Each notebook's first cell runs `%pip install -q -r requirements.txt`
(`mlflow[databricks]>=3.14.0`, `pyyaml`, `databricks-sdk`), and the second cell
restarts Python. Run the cells in order.

## Run order

`00`, `01`, `02`, `03`, `04`, and then `99_cleanup` when you are finished.
Notebooks 01 to 03 are independent of the others. Every notebook can be run
again; it reuses or resets what it created earlier.

## Beta notes

The MLflow Prompt Registry is a **Beta** feature (the Databricks documentation
labels it so, as does the chapter). A workspace admin may need to enable
Beta previews. If the registry is not available, or you lack the schema
privileges, notebook 00 prints a message that explains what to check and stops
without an error. Beta behavior can change between releases. Re-run the notebook
after upgrading MLflow.

Prompt names in Unity Catalog have three levels: `catalog.schema.prompt_name`.
The notebook builds the name from the widgets, so the prompt is
`<catalog>.<schema>.procurement_assistant`.

## Free Edition notes

Databricks Free Edition runs serverless compute only, which is what these
notebooks target. You may not be able to create a catalog there; set the
`catalog` widget to the catalog you have (often `workspace`). Whether the Prompt
Registry Beta is switched on for a Free Edition workspace depends on the
workspace. If it is not, notebook 00 stops with a message and the rest of the
notebooks still run.

## What the notebooks create and cleanup

| Notebook | Creates |
|---|---|
| 00 | Prompt `<catalog>.<schema>.procurement_assistant` (3 versions, aliases `staging` and `production`) and one MLflow trace |
| 04 | Tables `request_traces`, `eval_scores`, `endpoint_metrics`, `retrieval_events`, `agent_actions`, and eight views, in `<catalog>.<schema>` |

`99_cleanup` shows what it would remove. To delete, set the `confirm` widget to
`yes` and run it. It drops a table only when its comment marks it as Chapter 15
sample data, so a real table with the same name is never touched. It leaves the
catalog, the schema, and MLflow traces in place.

## Files in this folder

| File | Purpose |
|---|---|
| `cobalt_common.py` | Shared helpers: name checks, catalog and schema setup, Prompt Registry probe |
| `decision_records.py`, `decision_record_template.md`, `decision_record_supplier_suspension.md` | Decision-record validator, blank template, and filled example |
| `mcp_tool_contract.py` | The read-only, propose, and commit tools (also runs with `python mcp_tool_contract.py`) |
| `trust_registry.py`, `trust_registry.yaml` | The registry validator and data (also runs with `python trust_registry.py`) |
| `operating_views.sql` | Databricks SQL for the tables, the eight views, and the alert queries |
| `ops_sql.py`, `sample_data.py` | Load the SQL file with your catalog and schema, and generate the synthetic rows |

Compared with the `code/ch15` files in the book repository, this folder keeps
all tables and views in one schema (the book uses `monitoring` and `audit`),
adds a registry-file validator and optional issuer and organization checks to
`trust_registry.py`, and adds table comments that mark sample data. The
`v_suspicious_retrieval` view uses a join instead of an `IN` subquery inside an
`OR`. The results are the same.

## Troubleshooting

| Symptom | Likely cause and fix |
|---|---|
| Notebook 00 prints "The MLflow Prompt Registry is a Beta feature. This notebook could not use it..." and stops | The preview is off, or you lack `CREATE FUNCTION`, `EXECUTE`, and `MANAGE` on the schema. Ask an admin, or set `catalog` and `schema` to a schema where you have them. |
| `RuntimeError: Catalog 'cobalt_ops' does not exist and could not be created` | You may not create catalogs. Set the `catalog` widget to an existing catalog (for example `workspace`) and run again. |
| `RuntimeError: Schema ... does not exist and could not be created` | You need `CREATE SCHEMA` on the catalog, or set `schema` to an existing schema. |
| `RuntimeError: ... exists but was not created by this notebook` (04) | A table with that name already exists and is not Chapter 15 sample data. Choose a different `schema`. |
| `RuntimeError: Expected to create version 1 but got N` (00) | The prompt already has versions from other work. Run `99_cleanup`, or use another schema. |
| `ModuleNotFoundError` for `cobalt_common` or another local module | The notebook is not inside the cloned repository folder. Open it from the Git folder so its sibling files are available. |
| `ValueError: The catalog widget must contain only ...` | Widget values may contain only letters, digits, underscores, and hyphens. |
| `pip` errors in the first cell | Check that the cluster can reach a package index, then run the cell again. |
| Views are empty | Run notebook 04 again from the top. It refills the sample tables before it creates the views. |
