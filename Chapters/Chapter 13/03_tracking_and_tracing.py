# Databricks notebook source

# MAGIC %md
# MAGIC # Chapter 13 - Step 3: MLflow Tracking and LLM Tracing
# MAGIC
# MAGIC This notebook corresponds to the **MLflow Tracking - Observability and Lineage**
# MAGIC section of Chapter 13. It demonstrates:
# MAGIC
# MAGIC 1. Logging parameters, metrics, and artifacts for a Gen AI agent
# MAGIC 2. Comparing runs with different configurations
# MAGIC 3. Using LLM Tracing to capture agent execution steps
# MAGIC
# MAGIC **Prerequisites:**
# MAGIC - Run **01_create_table_and_log_model.py** first to create the table and model

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

CATALOG = "demo"
SCHEMA  = "finance"
TABLE   = "sales_transactions"

import mlflow
import json
import time
import pandas as pd

username = spark.conf.get("spark.databricks.notebook.userName")
mlflow.set_experiment(f"/Users/{username}/data_quality_agent_tracking")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load the agent for direct use
# MAGIC
# MAGIC For tracking experiments, we instantiate the agent directly (rather than
# MAGIC through the PyFunc wrapper) so we can log parameters and metrics around
# MAGIC individual runs.

# COMMAND ----------

from data_quality_agent.agent import DataQualityAgent

with open("data_quality_agent_model/artifacts/system_prompt.txt", "r") as f:
    system_prompt = f.read()

with open("data_quality_agent_model/artifacts/example_rules.json", "r") as f:
    example_rules = json.load(f)

agent = DataQualityAgent(
    system_prompt=system_prompt,
    example_rules=example_rules,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run 1: Default configuration (temperature=0.1)
# MAGIC
# MAGIC We log the parameters that shape the agent's behavior, run it, and capture
# MAGIC the resulting metrics.

# COMMAND ----------

with mlflow.start_run(run_name="agent_eval_temp_0.1") as run1:
    # Log parameters
    mlflow.log_param("llm_endpoint", "databricks-meta-llama-3-3-70b-instruct")
    mlflow.log_param("temperature", 0.1)
    mlflow.log_param("max_tokens", 1024)
    mlflow.log_param("table", f"{CATALOG}.{SCHEMA}.{TABLE}")

    # Run the agent and measure performance
    start = time.time()
    rules_1 = agent.run(TABLE, CATALOG, SCHEMA)
    elapsed = time.time() - start

    # Log metrics
    mlflow.log_metric("rules_generated", len(rules_1))
    mlflow.log_metric("latency_seconds", round(elapsed, 2))
    mlflow.log_metric("schema_rules", len([r for r in rules_1 if r.get("source") == "schema"]))
    mlflow.log_metric("llm_rules", len([r for r in rules_1 if r.get("source") == "llm"]))

    # Log artifacts
    mlflow.log_artifact("data_quality_agent_model/artifacts/system_prompt.txt")

    # Log the generated rules as an artifact
    with open("/tmp/generated_rules_temp01.json", "w") as f:
        json.dump(rules_1, f, indent=2)
    mlflow.log_artifact("/tmp/generated_rules_temp01.json")

    run1_id = run1.info.run_id
    print(f"Run 1 complete: {len(rules_1)} rules in {elapsed:.1f}s (run_id: {run1_id})")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run 2: Higher temperature (temperature=0.7)
# MAGIC
# MAGIC We run the same agent with a higher temperature to see how it affects
# MAGIC the rules generated. This lets us compare runs side by side in the UI.

# COMMAND ----------

# For a real experiment, we would modify the agent's temperature parameter.
# Here we simulate this by running the agent again and logging different params.
with mlflow.start_run(run_name="agent_eval_temp_0.7") as run2:
    mlflow.log_param("llm_endpoint", "databricks-meta-llama-3-3-70b-instruct")
    mlflow.log_param("temperature", 0.7)
    mlflow.log_param("max_tokens", 1024)
    mlflow.log_param("table", f"{CATALOG}.{SCHEMA}.{TABLE}")

    start = time.time()
    rules_2 = agent.run(TABLE, CATALOG, SCHEMA)
    elapsed = time.time() - start

    mlflow.log_metric("rules_generated", len(rules_2))
    mlflow.log_metric("latency_seconds", round(elapsed, 2))
    mlflow.log_metric("schema_rules", len([r for r in rules_2 if r.get("source") == "schema"]))
    mlflow.log_metric("llm_rules", len([r for r in rules_2 if r.get("source") == "llm"]))

    mlflow.log_artifact("data_quality_agent_model/artifacts/system_prompt.txt")

    with open("/tmp/generated_rules_temp07.json", "w") as f:
        json.dump(rules_2, f, indent=2)
    mlflow.log_artifact("/tmp/generated_rules_temp07.json")

    run2_id = run2.info.run_id
    print(f"Run 2 complete: {len(rules_2)} rules in {elapsed:.1f}s (run_id: {run2_id})")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Compare the runs
# MAGIC
# MAGIC We can compare the two runs programmatically. In practice, the MLflow UI
# MAGIC provides a side-by-side comparison view that makes this even easier.

# COMMAND ----------

print(f"{'Metric':<25} {'Temp 0.1':>10} {'Temp 0.7':>10}")
print("-" * 47)

for metric_key in ["rules_generated", "latency_seconds", "schema_rules", "llm_rules"]:
    v1 = mlflow.get_run(run1_id).data.metrics.get(metric_key, "N/A")
    v2 = mlflow.get_run(run2_id).data.metrics.get(metric_key, "N/A")
    print(f"{metric_key:<25} {v1:>10} {v2:>10}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## LLM Tracing
# MAGIC
# MAGIC LLM Tracing captures each step of an agentic workflow as individual spans.
# MAGIC This is invaluable for debugging: if the agent produces an unexpected rule,
# MAGIC we can inspect the trace to see exactly what happened at each step.
# MAGIC
# MAGIC We use the `@mlflow.trace` decorator to create spans for each method.

# COMMAND ----------

@mlflow.trace(name="generate_rules_traced")
def generate_rules_traced(table_name, catalog, schema):
    """Run the full agent pipeline with tracing enabled."""

    @mlflow.trace(name="analyze_schema")
    def traced_analyze(t, c, s):
        return agent.analyze_schema(t, c, s)

    @mlflow.trace(name="generate_advanced_rules")
    def traced_generate(ctx):
        return agent.generate_advanced_rules(ctx)

    schema_context = traced_analyze(table_name, catalog, schema)
    advanced_rules = traced_generate(schema_context)

    all_rules = schema_context["basic_rules"] + advanced_rules
    return all_rules

# Run with tracing
traced_rules = generate_rules_traced(TABLE, CATALOG, SCHEMA)
print(f"Traced run generated {len(traced_rules)} rules")
print("Check the MLflow UI -> Traces tab to see the execution breakdown")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next Steps
# MAGIC
# MAGIC Open the MLflow experiment UI to:
# MAGIC - Compare the two runs side by side
# MAGIC - Inspect the trace spans to see the agent's execution flow
# MAGIC - Review the logged artifacts (prompt template, generated rules)
# MAGIC
# MAGIC Continue with **04_prompts.py** to explore prompt versioning and management.
