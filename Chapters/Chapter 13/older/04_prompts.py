# Databricks notebook source

# MAGIC %md
# MAGIC # Chapter 13 - Step 4: Prompt Versioning and Management
# MAGIC
# MAGIC This notebook corresponds to the **Prompts** section of Chapter 13.
# MAGIC It demonstrates:
# MAGIC
# MAGIC 1. Logging prompt versions alongside model runs
# MAGIC 2. Using prompt hashes for quick comparison in the MLflow UI
# MAGIC 3. Comparing agent behavior across different prompt versions
# MAGIC 4. Parameterized prompt templates
# MAGIC
# MAGIC **Prerequisites:**
# MAGIC - Run **01_create_table_and_log_model.py** first to create the table

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

CATALOG = "demo"
SCHEMA  = "finance"
TABLE   = "sales_transactions"

import mlflow
import json
import hashlib
import time
import pandas as pd

username = spark.conf.get("spark.databricks.notebook.userName")
mlflow.set_experiment(f"/Users/{username}/data_quality_agent_prompts")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Define two prompt versions
# MAGIC
# MAGIC We create two versions of the system prompt to demonstrate how small changes
# MAGIC can affect agent behavior. Version 1 is our original prompt. Version 2 adds
# MAGIC instructions to prioritize cross-column rules.

# COMMAND ----------

prompt_v1 = open("data_quality_agent_model/artifacts/system_prompt.txt", "r").read()

prompt_v2 = prompt_v1 + """

IMPORTANT: Prioritize cross-column consistency rules above all other rule types.
For every pair of related columns (dates, prices, quantities), propose at least
one cross-column rule. Use the column name "_cross_column" for these rules.
"""

print("Prompt V1 length:", len(prompt_v1), "characters")
print("Prompt V2 length:", len(prompt_v2), "characters")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Helper: compute a stable hash for a prompt
# MAGIC
# MAGIC We use a SHA-256 hash (truncated to 12 characters) so we can quickly
# MAGIC identify which runs used which prompt version in the MLflow UI.

# COMMAND ----------

def prompt_hash(text: str) -> str:
    """Return a short hash of the prompt text for tracking."""
    return hashlib.sha256(text.encode()).hexdigest()[:12]

print(f"V1 hash: {prompt_hash(prompt_v1)}")
print(f"V2 hash: {prompt_hash(prompt_v2)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run the agent with each prompt version
# MAGIC
# MAGIC We log the prompt hash as a parameter and the full prompt as an artifact.
# MAGIC This gives us both quick filtering (by hash) and full reproducibility
# MAGIC (by artifact).

# COMMAND ----------

from data_quality_agent.agent import DataQualityAgent

with open("data_quality_agent_model/artifacts/example_rules.json", "r") as f:
    example_rules = json.load(f)

results = {}

for version_label, prompt_text in [("v1_original", prompt_v1), ("v2_cross_column", prompt_v2)]:
    agent = DataQualityAgent(
        system_prompt=prompt_text,
        example_rules=example_rules,
    )

    with mlflow.start_run(run_name=f"prompt_{version_label}"):
        # Log prompt metadata as parameters
        mlflow.log_param("prompt_version", version_label)
        mlflow.log_param("prompt_hash", prompt_hash(prompt_text))
        mlflow.log_param("prompt_length", len(prompt_text))

        # Log the full prompt as an artifact
        prompt_path = f"/tmp/system_prompt_{version_label}.txt"
        with open(prompt_path, "w") as f:
            f.write(prompt_text)
        mlflow.log_artifact(prompt_path)

        # Run the agent
        start = time.time()
        rules = agent.run(TABLE, CATALOG, SCHEMA)
        elapsed = time.time() - start

        # Log metrics
        mlflow.log_metric("rules_generated", len(rules))
        mlflow.log_metric("latency_seconds", round(elapsed, 2))
        mlflow.log_metric("cross_column_rules",
                          len([r for r in rules if r.get("column") == "_cross_column"]))
        mlflow.log_metric("llm_rules",
                          len([r for r in rules if r.get("source") == "llm"]))

        results[version_label] = rules
        print(f"{version_label}: {len(rules)} rules ({elapsed:.1f}s)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Compare prompt versions

# COMMAND ----------

print(f"{'Metric':<25} {'V1 Original':>15} {'V2 Cross-Col':>15}")
print("-" * 57)

for label, rules in results.items():
    total = len(rules)
    cross = len([r for r in rules if r.get("column") == "_cross_column"])
    llm = len([r for r in rules if r.get("source") == "llm"])
    print(f"Total rules:              {total if label == 'v1_original' else '':>15} {total if label == 'v2_cross_column' else '':>15}")

# Cleaner comparison
for metric_name in ["Total rules", "Cross-column rules", "LLM rules"]:
    v1_rules = results["v1_original"]
    v2_rules = results["v2_cross_column"]
    if metric_name == "Total rules":
        v1_val, v2_val = len(v1_rules), len(v2_rules)
    elif metric_name == "Cross-column rules":
        v1_val = len([r for r in v1_rules if r.get("column") == "_cross_column"])
        v2_val = len([r for r in v2_rules if r.get("column") == "_cross_column"])
    else:
        v1_val = len([r for r in v1_rules if r.get("source") == "llm"])
        v2_val = len([r for r in v2_rules if r.get("source") == "llm"])
    print(f"{metric_name:<25} {v1_val:>15} {v2_val:>15}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parameterized prompt templates
# MAGIC
# MAGIC In production, we often use templates with variables that get filled in
# MAGIC at runtime. This keeps the template reusable across different contexts.

# COMMAND ----------

prompt_template = """You are a data quality expert analyzing the {table_name} table
in {catalog}.{schema}.

Given the table schema and basic validation rules, propose additional rules focusing
on {focus_area}.

Return your rules as a JSON array with keys: column, rule, source, note."""

# Fill in the template
filled_prompt = prompt_template.format(
    table_name=TABLE,
    catalog=CATALOG,
    schema=SCHEMA,
    focus_area="cross-column consistency and business logic constraints"
)

print("Template:")
print(prompt_template)
print("\nFilled:")
print(filled_prompt)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next Steps
# MAGIC
# MAGIC Open the MLflow experiment UI to compare the two prompt versions side by side.
# MAGIC Filter by `prompt_hash` to quickly find runs that used a specific prompt.
# MAGIC
# MAGIC Continue with **05_model_serving.py** to deploy the agent as a serving endpoint.
