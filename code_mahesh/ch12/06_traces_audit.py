# Databricks notebook source
# MAGIC %md
# MAGIC # 06 - Audit traces and summarize quality
# MAGIC
# MAGIC **Book section:** Chapter 12, "Interpretability: Making the Reasoning Visible".
# MAGIC
# MAGIC **What it does**
# MAGIC 1. Queries the experiment's traces with `mlflow.search_traces()`, filtered to the last N days.
# MAGIC 2. Builds a simple table with one row per trace: status, latency, span counts, and every verdict attached to it.
# MAGIC 3. Summarizes the traces by status and computes a pass rate per scorer, from the assessments left by notebooks 01-05 (including monitoring scorers, once they have run).
# MAGIC 4. Prints an audit trail for discount-related requests: which sources the assistant was given and which tools it called.
# MAGIC
# MAGIC **Prerequisites**
# MAGIC - At least one earlier notebook has produced traces in the experiment (`01_run_evaluation` is enough). With no traces, the notebook says so and ends normally.
# MAGIC
# MAGIC **Runtime:** serverless compute, or Databricks Runtime 17.0 or later. Under a minute. It makes no model calls, so it costs nothing beyond compute.
# MAGIC
# MAGIC **Creates:** nothing. It only reads.

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

import time

import mlflow
from databricks.sdk import WorkspaceClient

import cobalt_eval_common as cw

w = WorkspaceClient()
user_name = cw.current_user_name(w)

dbutils.widgets.text(
    "experiment_name", cw.default_experiment_path(user_name), "MLflow experiment"
)
dbutils.widgets.text("lookback_days", "7", "Look back (days)")
dbutils.widgets.text("max_results", "200", "Max traces to read")

experiment_name = dbutils.widgets.get("experiment_name").strip()
lookback_days = float(dbutils.widgets.get("lookback_days") or 7)
max_results = int(dbutils.widgets.get("max_results") or 200)

# COMMAND ----------

# Preflight: the experiment must exist (it is created if it does not)
experiment = cw.ensure_experiment(experiment_name, user_name)
print(f"Experiment: {experiment.name} (id {experiment.experiment_id})")

# COMMAND ----------

# Traces from the last N days, returned as Trace objects
cutoff_ms = int((time.time() - lookback_days * 86400) * 1000)
try:
    traces = mlflow.search_traces(
        locations=[experiment.experiment_id],
        filter_string=f"attributes.timestamp_ms > {cutoff_ms}",
        max_results=max_results,
        return_type="list",
    )
except Exception as exc:
    print("The time filter was not accepted here "
          f"({str(exc).splitlines()[0][:100]}); reading without a filter.")
    traces = mlflow.search_traces(
        locations=[experiment.experiment_id],
        max_results=max_results,
        return_type="list",
    )
print(f"{len(traces)} traces found")

# COMMAND ----------

if not traces:
    print("No traces yet. Run 01_run_evaluation first, then re-run this notebook.")
else:
    summary = cw.trace_summary(traces)
    display(summary)

# COMMAND ----------

if traces:
    by_status = summary.groupby("status").agg(
        traces=("trace_id", "count"), mean_latency_ms=("latency_ms", "mean")
    )
    display(by_status.reset_index())

    # Pass rate per scorer: yes / true / compliant count as a pass
    display(cw.assessment_pass_rates(traces))

# COMMAND ----------

# MAGIC %md
# MAGIC ## An audit trail for discount decisions
# MAGIC
# MAGIC Every trace records the full chain: the request, the sources retrieved, any tool calls, and the response. This assistant has no tools, so the audit shows the policy sources it was given (`RETRIEVER` spans). If you later add tools, their calls appear as `TOOL` spans and are printed too.

# COMMAND ----------

discount_traces = [t for t in traces if "discount" in (t.data.request or "").lower()]
print(f"{len(discount_traces)} discount-related traces; showing up to 3\n")

for trace in discount_traces[:3]:
    print(f"Trace {trace.info.trace_id}")
    print("  Request :", (trace.data.request or "")[:100])
    for span in trace.search_spans(span_type="RETRIEVER"):
        docs = span.outputs or []
        uris = [(d.get("metadata") or {}).get("doc_uri") for d in docs
                if isinstance(d, dict)]
        print("  Sources :", uris)
    for span in trace.search_spans(span_type="TOOL"):
        print(f"  Tool    : {span.name}  args={span.inputs}  result={span.outputs}")
    print("  Response:", (trace.info.response_preview or "")[:150])
    print()
