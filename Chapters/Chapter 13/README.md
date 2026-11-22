# Chapter 13 — Data Quality Agent: Packaging, Deployment, and Lifecycle

This folder contains the runnable companion code for Chapter 13
("Assembling and Deploying"). The example builds, packages, and deploys a
model-hosted agent that generates data quality rules for a table: it
analyzes schema and summary statistics, then uses a large language model
(LLM) for more complex reasoning. The notebooks cover the full MLflow
lifecycle — tracking, registration, aliases, prompt registry, evaluation
gates, and promotion — plus deployment with Declarative Automation Bundles
(DABs), Model Serving, Unity Gateway, Databricks AI Search, Databricks
Apps, and Lakeflow ingestion.

## Run order

| File | What it does |
|---|---|
| `00_create_table_and_log_model.ipynb` | Creates the sample `sales_transactions` table and logs the agent as an MLflow PyFunc model |
| `01_model_packaging.ipynb` | Validates the logged model (the first promotion gate) and registers it in Unity Catalog |
| `02_versioning_aliases.ipynb` | Points the `champion` alias at a registered version |
| `03_evaluate_agent.ipynb` | Evaluates the `@challenger` version and logs metrics and the evaluated `model_version` |
| `04_tracking_promotion.ipynb` | Queries the experiment and promotes the evaluated version if it passes the thresholds |
| `08_prompt_registry.ipynb` | Registers the system prompt, validates it, and promotes it via `staging`/`production` aliases |
| `09_model_serving.py` | Creates and checks the Model Serving endpoint (DAB and CLI) |
| `10_ai_gateway.py` | Adds Unity Gateway rate limits and usage tracking to the endpoint |
| `11_vector_search.py` | DAB job config for the AI Search index; checks index status via CLI |
| `12_databricks_apps.py` | Deploys and verifies the Streamlit app |
| `13_data_ingestion.py` | DAB configs for Lakeflow jobs, pipelines, and embedding generation |

Notebook numbering follows the chapter's walkthrough order; 05–07 were
retired in earlier drafts.

## The model package (`data_quality_agent_model/`)

```
data_quality_agent_model/
├── MLmodel
├── requirements.txt
├── code/
│   └── data_quality_agent/
│       ├── __init__.py
│       ├── agent.py
│       └── utils.py
├── artifacts/
│   ├── system_prompt.txt
│   └── example_rules.json
└── python_model.py
```

- `python_model.py` implements the PyFunc interface (`predict`)
- `code/` holds the agent's Python modules, bundled with the model
- `artifacts/` holds non-code assets (system prompt, example rules)
  available at inference time
- `requirements.txt` pins the deployment environment

## The bundle (`ch13-dab/`)

A Declarative Automation Bundle that deploys the solution as a single
unit: the inference job (`notebooks/run_agent.py`), the supporting
notebooks, the document-processing pipeline (`pipelines/`), and the
Streamlit app (`apps/data_quality_ui/`). Deploy with:

```bash
databricks bundle deploy --target dev
```

Environment-specific values (catalog, schema, node type) come from
variables in `databricks.yml`.

## Key concepts demonstrated

- **Aliases over stages.** Unity Catalog models are promoted by
  reassigning aliases (`champion`/`challenger`), not by stage transitions.
  Rollback is repointing the alias.
- **Evaluation gates.** Promotion is decided by metrics recorded on the
  evaluation run, and the version promoted is the version that run
  evaluated (via the `model_version` parameter).
- **Per-environment catalogs.** The agent is registered in the
  environment's catalog (`UC_CATALOG`); the data it analyzes lives
  elsewhere (`DATA_CATALOG`). The same code runs unchanged in every
  environment.

## Prerequisites

- A Unity Catalog-enabled workspace with serverless compute (or a DBR 17+
  cluster)
- Access to the foundation model endpoint
  `databricks-meta-llama-3-3-70b-instruct`
- Privileges to create catalogs/schemas, register models, create serving
  endpoints, and create AI Search endpoints (notebooks 09–13)

Some notebooks create billable resources; delete endpoints when you are
done experimenting.

## References

- [MLflow documentation](https://mlflow.org/docs/latest/index.html)
- [MLflow on Databricks](https://docs.databricks.com/en/mlflow/index.html)
- [Model Serving](https://docs.databricks.com/en/machine-learning/model-serving/index.html)
- [Databricks AI Search](https://docs.databricks.com/aws/en/ai-search/ai-search)
- [Databricks Apps](https://docs.databricks.com/en/dev-tools/databricks-apps/index.html)
- [Declarative Automation Bundles](https://docs.databricks.com/aws/en/dev-tools/bundles)
