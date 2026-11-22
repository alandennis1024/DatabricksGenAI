# Databricks notebook source
# MAGIC %md
# MAGIC # 02 - Debug a judge by calling it directly
# MAGIC
# MAGIC **Book section:** Chapter 12, "Calling the Judge Directly for Debugging".
# MAGIC
# MAGIC **What it does**
# MAGIC 1. Produces one fresh trace from the CobaltWorks assistant (or loads the trace you name in the `trace_id` widget).
# MAGIC 2. Calls built-in judges directly on that trace with `judge(trace=trace)` and prints each verdict with its rationale.
# MAGIC 3. Handles the two return shapes: most judges return **one** `Feedback`; `RetrievalGroundedness` returns a **list** with one `Feedback` per `RETRIEVER` span.
# MAGIC 4. Calls a judge on a hand-written good and bad answer, to see what a failing verdict and rationale look like.
# MAGIC
# MAGIC **Prerequisites**
# MAGIC - Permission to query the `chat_endpoint` and `judge_endpoint` model endpoints (the preflight cell checks both).
# MAGIC - Notebook 01 is optional. To debug a row from its evaluation run, copy a trace ID from the MLflow UI into the `trace_id` widget.
# MAGIC
# MAGIC **Runtime:** serverless compute, or Databricks Runtime 17.0 or later. About 1-2 minutes.
# MAGIC
# MAGIC **Creates:** one trace in the experiment, plus a few judge calls. Nothing else.

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

dbutils.widgets.text("chat_endpoint", cw.DEFAULT_CHAT_ENDPOINT, "Agent chat endpoint")
dbutils.widgets.text(
    "judge_endpoint", cw.DEFAULT_JUDGE_ENDPOINT, "Judge endpoint (databricks:/ URI)"
)
dbutils.widgets.text(
    "experiment_name", cw.default_experiment_path(user_name), "MLflow experiment"
)
dbutils.widgets.text("trace_id", "", "Trace ID to debug (optional)")

experiment_name = dbutils.widgets.get("experiment_name").strip()
trace_id_widget = dbutils.widgets.get("trace_id").strip()

# COMMAND ----------

# Preflight: check the configured endpoints exist, else fall back
chat_endpoint, judge_endpoint = cw.resolve_endpoints(
    w,
    dbutils.widgets.get("chat_endpoint").strip(),
    dbutils.widgets.get("judge_endpoint").strip(),
)
judge_model = f"databricks:/{judge_endpoint}"

experiment = cw.ensure_experiment(experiment_name, user_name)
print(f"Experiment: {experiment.name} (id {experiment.experiment_id})")

# COMMAND ----------

import time

# Get the trace to debug: the one you named, or a fresh one
if trace_id_widget:
    trace = mlflow.get_trace(trace_id_widget)
else:
    predict = cw.build_predict_fn(chat_endpoint, cw.SYSTEM_PROMPT_V1)
    answer = predict(request="Can I give Midwest Steel 12% on this delayed order?")
    print("Agent answer:", answer)
    trace_id = mlflow.get_last_active_trace_id()
    trace = None
    for _ in range(5):  # traces are logged asynchronously
        try:
            mlflow.flush_trace_async_logging()
            trace = mlflow.get_trace(trace_id)
        except Exception:
            trace = None
        if trace is not None:
            break
        time.sleep(2)

assert trace is not None, "Could not load the trace; set the trace_id widget."
print("Trace:", trace.info.trace_id)
print("Spans:", [(s.name, str(s.span_type)) for s in trace.data.spans])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Call the judges on the trace
# MAGIC
# MAGIC Built-in retrieval judges do not take a `context` argument. They read the retrieved documents from the `RETRIEVER` span in the trace, which is one reason to keep tracing on. The code below prints the verdict (`value`), the judge's reasoning (`rationale`), and any `error`, for each `Feedback`.

# COMMAND ----------

from mlflow.genai.scorers import Guidelines, RelevanceToQuery, RetrievalGroundedness

policy_citation = Guidelines(
    name="policy_citation",
    guidelines=[
        "If the response states a discount or approval decision, it must name "
        "the policy section that governs it."
    ],
    model=judge_model,
)

judges = {
    "relevance_to_query": RelevanceToQuery(model=judge_model),
    "policy_citation": policy_citation,
    "retrieval_groundedness": RetrievalGroundedness(model=judge_model),
}

for name, judge in judges.items():
    result = judge(trace=trace)   # one Feedback, or a list of them
    shape = "list" if isinstance(result, list) else "single"
    print(f"== {name} ({shape})")
    for fb in cw.as_feedback_list(result):
        print("  value    :", fb.value)
        print("  rationale:", (fb.rationale or "")[:400])
        if fb.error:
            print("  error    :", fb.error)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Call a judge on text you wrote yourself
# MAGIC
# MAGIC You do not need a trace to try a judge. Pass `inputs` and `outputs` yourself. This is the quickest way to learn what makes a judge say `no`.

# COMMAND ----------

inputs = {"request": "Can I give a 25% discount to close a deal today?"}
answers = {
    "good": (
        "No. Section 4.1 does not permit discounts above 20%. A request that "
        "large must be escalated to the VP Procurement."
    ),
    "bad": "Sure, 25% is approved. Go ahead and close the deal.",
}

for label, outputs in answers.items():
    fb = policy_citation(inputs=inputs, outputs=outputs)
    print(f"[{label}] {fb.value}")
    print("   ", (fb.rationale or "")[:300])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Reading the result
# MAGIC
# MAGIC - `value` is a categorical verdict: `yes` or `no` for the built-in judges.
# MAGIC - `rationale` is the judge's reasoning. Read it before changing anything: it tells you whether the fix belongs in retrieval, in the prompt, or in the judge itself.
# MAGIC - If `error` is set, the judge could not produce a verdict (for example, a rate limit or a missing retriever span). That row has no score; it is not a `no`.
# MAGIC
# MAGIC Next: `03_custom_judge_make_judge`.
