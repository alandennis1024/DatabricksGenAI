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

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Register a Versioned Prompt Template
# MAGIC
# MAGIC Prompt templates use double curly brace `{{ }}` syntax for variables.
# MAGIC Each time we register an updated template under the same name, MLflow
# MAGIC creates a new immutable version automatically.

# COMMAND ----------

import mlflow

prompt = mlflow.genai.register_prompt(
    name="data-quality-agent-system-prompt",
    template=(
        "You are a data quality analysis agent. Analyze the "
        "{{ table_name }} table in {{ catalog }}.{{ schema }} "
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

import mlflow

# Load the candidate prompt from staging
prompt = mlflow.genai.load_prompt(
    "prompts:/data-quality-agent-system-prompt@staging"
)

# Validate the template has the variables the agent expects
required_vars = ["{{ table_name }}", "{{ catalog }}", "{{ schema }}"]
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

mlflow.set_prompt_alias(
    "data-quality-agent-system-prompt",
    alias="production",
    version=prompt.version
)

print(f"Promoted version {prompt.version} to 'production' alias")
