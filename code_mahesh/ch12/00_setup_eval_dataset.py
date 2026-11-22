# Databricks notebook source
# MAGIC %md
# MAGIC # 00 - Set up the evaluation dataset
# MAGIC
# MAGIC **Book section:** Chapter 12, "Why Building Evaluation Datasets Is Really Hard" and "What a Good Evaluation Row Contains".
# MAGIC
# MAGIC **What it does**
# MAGIC 1. Sets (or creates) the MLflow experiment that holds every run, trace, and dataset for this chapter.
# MAGIC 2. Creates a Unity Catalog **MLflow evaluation dataset** and merges 20 CobaltWorks test records into it: policy lookups, discount decisions, refusals, and adversarial prompt-injection cases.
# MAGIC 3. Reads the dataset back and slices it by `tags.category`.
# MAGIC
# MAGIC **Prerequisites**
# MAGIC - Permission to use (or create) a Unity Catalog catalog and schema. If you cannot create the `cobalt_ops` catalog, set the `catalog` widget to one you can write to, such as your workspace's default catalog.
# MAGIC - No model endpoint is needed for this notebook.
# MAGIC
# MAGIC **Runtime:** serverless compute, or Databricks Runtime 17.0 or later. Runs in under a minute.
# MAGIC
# MAGIC **Creates:** the experiment `experiment_name`, and the Unity Catalog dataset `<catalog>.<schema>.<dataset_name>`. Re-running is safe: records are merged, not duplicated. `99_cleanup` removes both.
# MAGIC
# MAGIC **If the dataset cannot be created** (no Unity Catalog write access, or the feature is unavailable), the notebook says so and carries on. Every later notebook then falls back to the same 20 records, held in `cobalt_eval_common.py`, so you can still finish the chapter.

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
import mlflow.genai.datasets as ds
from databricks.sdk import WorkspaceClient

import cobalt_eval_common as cw

w = WorkspaceClient()
user_name = cw.current_user_name(w)

dbutils.widgets.text("catalog", cw.DEFAULT_CATALOG, "Unity Catalog catalog")
dbutils.widgets.text("schema", cw.DEFAULT_SCHEMA, "Schema")
dbutils.widgets.text("dataset_name", cw.DEFAULT_DATASET_NAME, "Dataset name")
dbutils.widgets.text(
    "experiment_name", cw.default_experiment_path(user_name), "MLflow experiment"
)

catalog = cw.check_identifier(dbutils.widgets.get("catalog").strip(), "catalog")
schema = cw.check_identifier(dbutils.widgets.get("schema").strip(), "schema")
dataset_name = cw.check_identifier(
    dbutils.widgets.get("dataset_name").strip(), "dataset_name"
)
experiment_name = dbutils.widgets.get("experiment_name").strip()
dataset_full_name = f"{catalog}.{schema}.{dataset_name}"
print("Dataset will be:", dataset_full_name)

# COMMAND ----------

# Preflight: make sure the catalog and schema exist (catalog creation is
# often not allowed; the message tells you what to change).
uc_ok, message = cw.ensure_uc_schema(spark, catalog, schema)
print(message)

# MLflow experiment: every run, trace, and dataset link lives here
experiment = cw.ensure_experiment(experiment_name, user_name)
print(f"Experiment: {experiment.name} (id {experiment.experiment_id})")

# COMMAND ----------

# MAGIC %md
# MAGIC ## What a record looks like
# MAGIC
# MAGIC A record is a plain dictionary with three keys that MLflow reads:
# MAGIC
# MAGIC | Key | Holds | Example |
# MAGIC |---|---|---|
# MAGIC | `inputs` | What your app receives. The keys must match the parameter names of `predict_fn`. | `{"request": "Can I give Midwest Steel 12%?"}` |
# MAGIC | `expectations` | Ground truth. `expected_facts` feeds the `Correctness` judge; `policy_outcome` is our own key for the decision the agent should reach. | `{"expected_facts": [...], "policy_outcome": "human_approval_required"}` |
# MAGIC | `tags` | Labels for slicing results. | `{"category": "discount"}` |
# MAGIC
# MAGIC **The trap:** MLflow silently drops any other top-level key. A stray `"category": "policy"` next to `inputs` disappears without an error, so `cobalt_eval_common.validate_records()` checks for it before we merge.
# MAGIC
# MAGIC The chapter's example record passes the chat `messages` list to a serving endpoint through `to_predict_fn`. Here the app is a local Python function, so `inputs` carries a single `request` string instead.

# COMMAND ----------

import json
from collections import Counter

records = cw.EVAL_RECORDS
problems = cw.validate_records(records)
assert not problems, problems
print(f"{len(records)} records, no problems found.")
print("By category:", dict(Counter(r["tags"]["category"] for r in records)))
print()
print(json.dumps(records[8], indent=2))

# Negative control: the stray-key mistake, caught before it costs you a day
bad = {"inputs": {"request": "x"}, "category": "policy"}
print()
print("Check on a record with a stray 'category' key:")
for problem in cw.validate_records([bad]):
    print("  -", problem)

# COMMAND ----------

# Create the dataset (or reuse it) and merge the records. merge_records
# matches records by their inputs, so re-running does not add duplicates.
dataset = None
if not uc_ok:
    print("Skipping dataset creation because the schema is not available.")
else:
    try:
        try:
            dataset = ds.get_dataset(name=dataset_full_name)
            print("Dataset already exists; merging records into it.")
        except Exception:
            dataset = ds.create_dataset(
                name=dataset_full_name, experiment_id=experiment.experiment_id
            )
            print("Created dataset", dataset_full_name)
        dataset = dataset.merge_records(records)
        print("Records in the dataset:", len(dataset.to_df()))
    except Exception as exc:
        dataset = None
        print(
            "Could not create the evaluation dataset:\n  "
            + str(exc).splitlines()[0][:300]
            + "\nThe later notebooks will use the 20 local records in "
            "cobalt_eval_common.py instead."
        )

# COMMAND ----------

# Read it back the way the other notebooks do, and slice by tag
if dataset is None:
    print("No Unity Catalog dataset; local records:",
          dict(Counter(r["tags"]["category"] for r in records)))
else:
    df = ds.get_dataset(name=dataset_full_name).to_df()
    print("Columns:", list(df.columns))
    show = [c for c in ("inputs", "expectations", "tags") if c in df.columns]
    display(df[show])

    categories = df["tags"].apply(lambda t: (t or {}).get("category"))
    counts = categories.value_counts().rename_axis("category")
    display(counts.reset_index(name="records"))

    adversarial = df[categories == "adversarial"]
    print(f"{len(adversarial)} adversarial rows, for example:")
    print(adversarial.iloc[0]["inputs"])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Done
# MAGIC
# MAGIC The dataset is ready. Continue with `01_run_evaluation`. If you changed `catalog`, `schema`, `dataset_name`, or `experiment_name` here, set the same values in the later notebooks.
