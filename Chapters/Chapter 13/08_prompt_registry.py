# Databricks notebook source

# MAGIC %md
# MAGIC # Chapter 13 — Prompt Registry: Register, Validate, and Promote
# MAGIC
# MAGIC This notebook demonstrates the full prompt deployment lifecycle:
# MAGIC register a versioned prompt template, validate it in staging, and
# MAGIC promote it to production. This mirrors the model validation gate
# MAGIC pattern from `01_model_packaging`.
# MAGIC
# MAGIC **Prerequisites:**
# MAGIC - MLflow Prompt Registry enabled in your workspace
# MAGIC - A Unity Catalog schema with CREATE FUNCTION and EXECUTE privileges

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Register a Versioned Prompt Template
# MAGIC
# MAGIC Prompt templates use double-brace `{{variable}}` syntax for variables.
# MAGIC Each registration under the same name creates a new immutable version
# MAGIC automatically, similar to model versioning in Unity Catalog. The prompt
# MAGIC name must include the full Unity Catalog path (`catalog.schema.name`).

# COMMAND ----------

import mlflow

CATALOG = "dev_catalog"
SCHEMA = "finance"
PROMPT_NAME = f"{CATALOG}.{SCHEMA}.data_quality_agent_system_prompt"

prompt = mlflow.genai.register_prompt(
    name=PROMPT_NAME,
    template=(
        "You are a data quality analysis agent. Analyze the "
        "{{table_name}} table in {{catalog}}.{{schema}} "
        "and propose data quality rules based on column metadata."
    ),
    commit_message="Initial system prompt for data quality agent",
    tags={"team": "data-engineering"},
)

print(f"Registered prompt version: {prompt.version}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Validate the Prompt Before Promoting
# MAGIC
# MAGIC Just as we validate models before promoting them, we validate prompts.
# MAGIC A deployment pipeline loads the candidate from the staging alias,
# MAGIC checks that the template contains the expected variables, and promotes
# MAGIC only if the checks pass.

# COMMAND ----------

# Load the candidate prompt by alias
prompt = mlflow.genai.load_prompt(f"prompts:/{PROMPT_NAME}@staging")

# Validate the template has the variables the agent expects
required_vars = ["{{table_name}}", "{{catalog}}", "{{schema}}"]
for var in required_vars:
    assert var in prompt.template, f"Missing variable: {var}"

print(f"Validation passed for version {prompt.version}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Promote to Production
# MAGIC
# MAGIC After validation passes, we promote the prompt by assigning the
# MAGIC production alias. No code change or redeployment needed — the
# MAGIC application loads the prompt by alias at runtime.

# COMMAND ----------

mlflow.genai.set_prompt_alias(
    name=PROMPT_NAME,
    alias="production",
    version=prompt.version,
)

print(f"Promoted version {prompt.version} to 'production' alias")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Load the Production Prompt at Runtime
# MAGIC
# MAGIC In a deployed application, we load the prompt by alias rather than
# MAGIC by version number. When we promote a new version, the application
# MAGIC picks it up automatically without redeployment.

# COMMAND ----------

production_prompt = mlflow.genai.load_prompt(
    f"prompts:/{PROMPT_NAME}@production"
)

# Render the template with actual values
rendered = production_prompt.format(
    table_name="customer_transactions",
    catalog="prod_catalog",
    schema="finance",
)

print(rendered)
