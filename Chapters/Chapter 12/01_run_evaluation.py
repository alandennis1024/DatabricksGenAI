# Databricks notebook source
# MAGIC %md
# MAGIC # 01 - Run an evaluation with `mlflow.genai.evaluate()`
# MAGIC
# MAGIC **Book section:** Chapter 12, "MLflow Evaluation on Databricks" and "Built-in Judges in MLflow 3".
# MAGIC
# MAGIC **What it does**
# MAGIC 1. Wraps a CobaltWorks policy assistant as a prediction function. It calls a Databricks chat endpoint with a policy system prompt and a short embedded policy text, so **no AI Search index is needed**.
# MAGIC 2. Evaluates it on the dataset from notebook 00 with built-in LLM judges (`Correctness`, `RelevanceToQuery`, `Safety`, `Guidelines`, `RetrievalGroundedness`) and a custom code scorer for policy compliance.
# MAGIC 3. Prints the aggregate metrics, then reads the per-row verdicts and rationales back from the traces.
# MAGIC
# MAGIC **Prerequisites**
# MAGIC - Notebook `00_setup_eval_dataset` has run. If not, the notebook falls back to the 20 local records.
# MAGIC - Permission to query a chat endpoint (`chat_endpoint`) and a judge endpoint (`judge_endpoint`). The preflight cell checks both and falls back to a `databricks-*` chat endpoint if a name has been retired.
# MAGIC
# MAGIC **Runtime:** serverless compute, or Databricks Runtime 17.0 or later. About 5-15 minutes for 20 rows, because each row is one agent call plus several judge calls.
# MAGIC
# MAGIC **Cost:** about 20 agent calls and 100 short judge calls. Set `max_rows` to a small number (for example 5) to try the notebook cheaply.
# MAGIC
# MAGIC **Creates:** one MLflow run in the experiment, with a trace and assessments for every row.
# MAGIC
# MAGIC **Judge model URIs:** judges take `databricks:/<endpoint-name>`. The older `endpoints:/` form belongs to MLflow 2 and `to_predict_fn`; it does not work for judges.

# COMMAND ----------

# MAGIC %pip install -q -r requirements.txt

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import os
import sys


def _notebook_dir():
    try:
        ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
        return os.path.dirname("/Workspace" + ctx.notebookPath().get())
    except Exception:
        return os.getcwd()


# Make cobalt_eval_common.py (next to this notebook) importable
for _dir in (os.getcwd(), _notebook_dir()):
    if _dir not in sys.path:
        sys.path.insert(0, _dir)

import mlflow
from databricks.sdk import WorkspaceClient

import cobalt_eval_common as cw

w = WorkspaceClient()
user_name = cw.current_user_name(w)

dbutils.widgets.text("catalog", cw.DEFAULT_CATALOG, "Unity Catalog catalog")
dbutils.widgets.text("schema", cw.DEFAULT_SCHEMA, "Schema")
dbutils.widgets.text("dataset_name", cw.DEFAULT_DATASET_NAME, "Dataset name")
dbutils.widgets.text("chat_endpoint", cw.DEFAULT_CHAT_ENDPOINT, "Agent chat endpoint")
dbutils.widgets.text(
    "judge_endpoint", cw.DEFAULT_JUDGE_ENDPOINT, "Judge endpoint (databricks:/ URI)"
)
dbutils.widgets.text(
    "experiment_name", cw.default_experiment_path(user_name), "MLflow experiment"
)
dbutils.widgets.text("max_rows", "0", "Max rows to evaluate (0 = all)")

catalog = cw.check_identifier(dbutils.widgets.get("catalog").strip(), "catalog")
schema = cw.check_identifier(dbutils.widgets.get("schema").strip(), "schema")
dataset_name = cw.check_identifier(
    dbutils.widgets.get("dataset_name").strip(), "dataset_name"
)
dataset_full_name = f"{catalog}.{schema}.{dataset_name}"
experiment_name = dbutils.widgets.get("experiment_name").strip()
max_rows = int(dbutils.widgets.get("max_rows") or 0)

# COMMAND ----------

# Preflight: list the serving endpoints you can see, check the configured
# ones exist, and fall back to a suitable databricks-* chat endpoint if not.
chat_endpoint, judge_endpoint = cw.resolve_endpoints(
    w,
    dbutils.widgets.get("chat_endpoint").strip(),
    dbutils.widgets.get("judge_endpoint").strip(),
)
judge_model = f"databricks:/{judge_endpoint}"

experiment = cw.ensure_experiment(experiment_name, user_name)
print(f"Experiment: {experiment.name} (id {experiment.experiment_id})")

# COMMAND ----------

# MAGIC %md
# MAGIC ## The application under test
# MAGIC
# MAGIC `mlflow.genai.evaluate()` calls `predict_fn(**inputs)`, so every key in a record's `inputs` must be a named parameter. Our records use the key `request`, so the function is `predict_fn(request: str)`. It is traced with `@mlflow.trace`, and its retriever step is traced as a `RETRIEVER` span so judges can see the policy text it was given.
# MAGIC
# MAGIC The model call uses the Databricks SDK's `serving_endpoints.query()`, which sends the same OpenAI-compatible chat request as `ChatDatabricks` but needs no extra packages. If you prefer LangChain, swap in `ChatDatabricks(endpoint=...)`; nothing else changes.
# MAGIC
# MAGIC The code below prints the function that `cobalt_eval_common.build_predict_fn()` builds. Read it once; it is only about 15 lines.

# COMMAND ----------

import inspect

print(inspect.getsource(cw.retrieve_policy))
print(inspect.getsource(cw.build_predict_fn))

# COMMAND ----------

predict_v2 = cw.build_predict_fn(chat_endpoint, cw.SYSTEM_PROMPT_V2)

# Smoke test: one call, so a wrong endpoint or missing permission fails here
sample = cw.EVAL_RECORDS[8]["inputs"]
print("Request :", sample["request"])
print("Response:", predict_v2(**sample))

# COMMAND ----------

# Load the evaluation data: the Unity Catalog dataset from notebook 00, or the
# local records if the dataset does not exist.
eval_data, description = cw.load_eval_data(dataset_full_name, max_rows)
print("Evaluating", description)

# COMMAND ----------

from mlflow.entities import Feedback
from mlflow.genai.scorers import (
    Correctness,
    Guidelines,
    RelevanceToQuery,
    RetrievalGroundedness,
    Safety,
    scorer,
)


@scorer
def policy_compliance(outputs, expectations) -> Feedback:
    """Code scorer: no LLM, so it is fast, free, and deterministic."""
    outcome = (expectations or {}).get("policy_outcome")
    passed, rationale = cw.check_policy_compliance(str(outputs), outcome)
    return Feedback(value=passed, rationale=rationale)


scorers = [
    Correctness(model=judge_model),           # needs expectations.expected_facts
    RelevanceToQuery(model=judge_model),      # does it address the request?
    Safety(model=judge_model),                # harmful or toxic content?
    RetrievalGroundedness(model=judge_model),  # claims supported by the context?
    Guidelines(
        name="policy_citation",
        guidelines=[
            "If the response states a discount or approval decision, it must "
            "name the policy section that governs it.",
            "The response must not grant an approval that the policy "
            "reserves for a human approver.",
        ],
        model=judge_model,
    ),
    policy_compliance,                        # our own code scorer
]
print("Scorers:", [getattr(s, "name", s) for s in scorers])

# COMMAND ----------

cw.tune_for_rate_limits()  # fewer parallel calls: friendlier to small workspaces

with mlflow.start_run(run_name="cobalt_eval_v2_prompt", tags=cw.RUN_TAGS) as run:
    mlflow.log_param("system_prompt", "v2")
    mlflow.log_param("chat_endpoint", chat_endpoint)
    results = mlflow.genai.evaluate(
        data=eval_data,
        predict_fn=predict_v2,
        scorers=scorers,
    )

print("Run id:", results.run_id)

# COMMAND ----------

# Aggregate metrics: yes/no judges and bool code scorers report a pass rate
# in <scorer name>/mean. Key names can vary a little between MLflow versions.
for key, value in sorted(results.metrics.items()):
    print(f"{key:40s} {value}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Per-row results
# MAGIC
# MAGIC Every row of the run is a trace, and every verdict is an *assessment* on that trace. Reading the traces is the most stable way to get per-row results, because the column names of the DataFrame form change between MLflow versions.

# COMMAND ----------

traces = mlflow.search_traces(run_id=results.run_id, return_type="list")
print(f"{len(traces)} traces in the run")

table = cw.assessments_table(traces)
display(table)

# COMMAND ----------

# Rows that failed a verdict: show the judge's rationale, not just the score
shown = 0
for name in ("policy_compliance", "correctness", "policy_citation"):
    for trace_id, value, rationale in cw.verdicts(traces, name):
        if not cw.feedback_passed(value) and shown < 8:
            shown += 1
            print(f"[{name}] trace {trace_id}: {value}")
            print("   ", (rationale or "")[:300])
if shown == 0:
    print("No failing verdicts in the checks above.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next
# MAGIC
# MAGIC Open the experiment in the MLflow UI and look at the **Evaluations** or **Traces** tab to see the same rows with their rationales. Then continue with `02_judge_debugging`.
