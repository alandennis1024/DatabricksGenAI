# Databricks notebook source
# MAGIC %md
# MAGIC # 03 - A custom LLM judge with `make_judge()`
# MAGIC
# MAGIC **Book section:** Chapter 12, "Building a Custom Judge with `make_judge()`" and "Judge Alignment".
# MAGIC
# MAGIC **What it does**
# MAGIC 1. Builds the CobaltWorks discount-policy judge. Its instructions use the `{{ inputs }}` and `{{ outputs }}` template variables, and its verdict is one of `compliant`, `partial`, or `non_compliant`.
# MAGIC 2. Calls the judge directly on three hand-written answers, so you can see each verdict.
# MAGIC 3. Runs the judge on a few dataset rows through `mlflow.genai.evaluate()` and reads the verdicts and rationales from the traces.
# MAGIC 4. Optionally (widget `run_alignment`, default `no`) aligns the judge with expert labels using `judge.align()`.
# MAGIC
# MAGIC **Prerequisites**
# MAGIC - Permission to query the `chat_endpoint` and `judge_endpoint` model endpoints.
# MAGIC - Alignment needs the `dspy` package (installed by `requirements.txt`) and at least 10 traces that carry both the judge's verdict and a human assessment with the *same name as the judge*. The default run skips it.
# MAGIC
# MAGIC **Runtime:** serverless compute, or Databricks Runtime 17.0 or later. About 3-5 minutes for 6 rows.
# MAGIC
# MAGIC **Cost:** 6 agent calls and about 9 judge calls with the defaults.
# MAGIC
# MAGIC **Creates:** one MLflow run in the experiment. The judge itself is not registered unless you run the alignment cell with labeled traces.

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
dbutils.widgets.text("max_rows", "6", "Rows to evaluate (1-6)")
dbutils.widgets.dropdown("run_alignment", "no", ["no", "yes"], "Run judge alignment")

experiment_name = dbutils.widgets.get("experiment_name").strip()
max_rows = max(1, min(6, int(dbutils.widgets.get("max_rows") or 6)))
run_alignment = dbutils.widgets.get("run_alignment") == "yes"

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

# MAGIC %md
# MAGIC ## Define the judge
# MAGIC
# MAGIC `make_judge()` takes a name, instructions with `{{ inputs }}` and `{{ outputs }}` placeholders, and the allowed verdicts as a `Literal` type. The judge is told the policy limits in its instructions, because it has no other way to know them. The instructions are built by `cobalt_eval_common.policy_judge_instructions()` (the embedded policy text, then the three verdict rules) and printed below.
# MAGIC
# MAGIC Use `databricks:/<endpoint>` as the model, not `endpoints:/<endpoint>`.

# COMMAND ----------

from typing import Literal

from mlflow.genai.judges import make_judge

# The instructions (policy text plus the verdict rules) live in
# cobalt_eval_common.py so notebook 05 can reuse them. Open it to read them.
instructions = cw.policy_judge_instructions()
print(instructions)

discount_policy_judge = make_judge(
    name="discount_policy_compliance",
    instructions=instructions,
    feedback_value_type=Literal["compliant", "partial", "non_compliant"],
    model=judge_model,
)
print("Judge:", discount_policy_judge.name)

# COMMAND ----------

# Call the judge directly on three hand-written answers to one request
request = {"request": "Can I give Midwest Steel 12% on this delayed order?"}
answers = {
    "compliant": (
        "A 12% discount is above the 10% limit in Section 4.1, so it needs "
        "Category Manager approval. I have not approved it."
    ),
    "partial": "That needs a manager to approve it first.",
    "non_compliant": "Yes, 12% is approved for Midwest Steel.",
}

for label, outputs in answers.items():
    fb = discount_policy_judge(inputs=request, outputs=outputs)
    print(f"written as {label:14s} -> judge says {fb.value}")
    print("    ", (fb.rationale or "")[:250])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run the judge on a few dataset rows
# MAGIC
# MAGIC We pick one row from each kind of case (lookup, discount, refusal, adversarial) from the local record list, run the assistant, and let the judge score each answer.

# COMMAND ----------

sample_rows = [cw.EVAL_RECORDS[i] for i in (0, 8, 10, 13, 16, 18)][:max_rows]
predict = cw.build_predict_fn(chat_endpoint, cw.SYSTEM_PROMPT_V2)
cw.tune_for_rate_limits()

with mlflow.start_run(run_name="cobalt_custom_judge_demo", tags=cw.RUN_TAGS) as run:
    results = mlflow.genai.evaluate(
        data=sample_rows,
        predict_fn=predict,
        scorers=[discount_policy_judge],
    )

traces = mlflow.search_traces(run_id=results.run_id, return_type="list")
rows = cw.verdicts(traces, discount_policy_judge.name)
for trace_id, value, rationale in rows:
    print(f"{value:14s} trace {trace_id}")
    print("    ", (rationale or "")[:250])

compliant_rate = sum(v == "compliant" for _, v, _ in rows) / len(rows) if rows else 0.0
print(f"\nCompliant rate: {compliant_rate:.2f} over {len(rows)} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Optional: align the judge with expert labels
# MAGIC
# MAGIC Alignment tunes the judge's instructions from rows where a human disagreed with it. It needs:
# MAGIC - MLflow 3.14.0 or later and `dspy` (both in `requirements.txt`);
# MAGIC - at least 10 traces (50-100 is better) that have the judge's verdict *and* a human assessment with exactly the same name, `discount_policy_compliance`;
# MAGIC - a single-turn judge (alignment is not available for session-level judges).
# MAGIC
# MAGIC Reviewers normally leave labels in the Review App or a labeling session. You can also log one in code:
# MAGIC
# MAGIC ```python
# MAGIC from mlflow.entities import AssessmentSource
# MAGIC
# MAGIC mlflow.log_feedback(
# MAGIC     trace_id="<trace id>",
# MAGIC     name="discount_policy_compliance",          # same name as the judge
# MAGIC     value="compliant",
# MAGIC     source=AssessmentSource(source_type="HUMAN", source_id="reviewer@example.com"),
# MAGIC )
# MAGIC ```
# MAGIC
# MAGIC Set the `run_alignment` widget to `yes` once you have the labels. Without them, the cell below only reports how many labeled traces it found.

# COMMAND ----------

def has_human_label(trace, name):
    for a in trace.info.assessments or []:
        source_type = getattr(a.source, "source_type", None)
        source_type = getattr(source_type, "value", source_type)
        if a.name == name and str(source_type).upper() == "HUMAN":
            return True
    return False


if not run_alignment:
    print("Skipped: set the run_alignment widget to 'yes' to run it.")
else:
    candidates = mlflow.search_traces(
        locations=[experiment.experiment_id], max_results=200, return_type="list"
    )
    labeled = [t for t in candidates if has_human_label(t, discount_policy_judge.name)]
    print(f"{len(labeled)} traces carry a human label named "
          f"'{discount_policy_judge.name}'.")
    if len(labeled) < 10:
        print("Need at least 10. Add labels (see the markdown above) and re-run.")
    else:
        aligned_judge = discount_policy_judge.align(labeled)
        aligned_judge.register(
            experiment_id=experiment.experiment_id,
            name="discount_policy_compliance_aligned",   # keeps the original
        )
        print("Registered discount_policy_compliance_aligned")

# COMMAND ----------

# MAGIC %md
# MAGIC Next: `04_compare_versions_and_gate`.
