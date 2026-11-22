# Databricks notebook source
# MAGIC %md
# MAGIC # 99 - Clean up
# MAGIC
# MAGIC **Book section:** Chapter 12 (companion notebooks; no matching section).
# MAGIC
# MAGIC **What it does:** removes what notebooks 00-05 created, **only when the `confirm` widget is set to `yes`**. With the default (`no`) it is a dry run that lists what it would delete.
# MAGIC
# MAGIC | Item | Removed when `confirm = yes` |
# MAGIC |---|---|
# MAGIC | Monitoring scorers `cobalt_ch12_*` (stopped, then deleted) | yes |
# MAGIC | The evaluation dataset `<catalog>.<schema>.<dataset_name>` | yes |
# MAGIC | MLflow runs tagged `cobalt_ch12 = true` (the runs these notebooks created) | yes |
# MAGIC | The MLflow experiment itself, with its traces | only if `delete_experiment = yes` |
# MAGIC | The catalog and schema | never |
# MAGIC
# MAGIC **Prerequisites:** none. It is safe to run when nothing exists.
# MAGIC
# MAGIC **Runtime:** serverless compute, or Databricks Runtime 17.0 or later. Under a minute.
# MAGIC
# MAGIC **Careful:** if you pointed `experiment_name` at an experiment you use for other work, leave `delete_experiment` at `no`. Runs are matched by the `cobalt_ch12` tag, so other runs are untouched either way.

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

dbutils.widgets.dropdown("confirm", "no", ["no", "yes"], "Confirm deletion")
dbutils.widgets.dropdown(
    "delete_experiment", "no", ["no", "yes"], "Also delete the experiment"
)
dbutils.widgets.text("catalog", cw.DEFAULT_CATALOG, "Unity Catalog catalog")
dbutils.widgets.text("schema", cw.DEFAULT_SCHEMA, "Schema")
dbutils.widgets.text("dataset_name", cw.DEFAULT_DATASET_NAME, "Dataset name")
dbutils.widgets.text(
    "experiment_name", cw.default_experiment_path(user_name), "MLflow experiment"
)

confirm = dbutils.widgets.get("confirm") == "yes"
delete_experiment = dbutils.widgets.get("delete_experiment") == "yes"
catalog = cw.check_identifier(dbutils.widgets.get("catalog").strip(), "catalog")
schema = cw.check_identifier(dbutils.widgets.get("schema").strip(), "schema")
dataset_name = cw.check_identifier(
    dbutils.widgets.get("dataset_name").strip(), "dataset_name"
)
dataset_full_name = f"{catalog}.{schema}.{dataset_name}"
experiment_name = dbutils.widgets.get("experiment_name").strip()

print("Mode:", "DELETE" if confirm else "dry run (set confirm = yes to delete)")

# COMMAND ----------

# Take stock of what exists (nothing here creates anything)
experiment = mlflow.get_experiment_by_name(experiment_name)
if experiment is None:
    print(f"Experiment {experiment_name} does not exist.")
    run_ids, scorer_names = [], []
else:
    runs = mlflow.search_runs(
        experiment_ids=[experiment.experiment_id],
        filter_string=f"tags.{cw.RUN_TAG_KEY} = 'true'",
        output_format="list",
    )
    run_ids = [r.info.run_id for r in runs]
    try:
        from mlflow.genai.scorers import list_scorers

        exp_id = experiment.experiment_id
        registered = {s.name for s in list_scorers(experiment_id=exp_id)}
        scorer_names = [n for n in cw.MONITOR_SCORER_NAMES if n in registered]
    except Exception as exc:
        scorer_names = []
        print("Could not list scorers (monitoring may be unavailable):",
              str(exc).splitlines()[0][:100])

try:
    ds.get_dataset(name=dataset_full_name)
    dataset_exists = True
except Exception:
    dataset_exists = False

print("Runs created by these notebooks :", len(run_ids))
print("Monitoring scorers registered   :", scorer_names or "none")
print("Evaluation dataset exists       :", dataset_exists, f"({dataset_full_name})")
print("Experiment would be deleted     :", delete_experiment)

# COMMAND ----------

# 1. Monitoring scorers: stop (sample rate 0), then delete
for name in scorer_names:
    if not confirm:
        print("would delete scorer", name)
        continue
    try:
        from mlflow.genai.scorers import delete_scorer, get_scorer

        exp_id = experiment.experiment_id
        try:
            get_scorer(name=name, experiment_id=exp_id).stop(
                name=name, experiment_id=exp_id
            )
        except Exception as exc:
            print("  could not stop", name, "-", str(exc).splitlines()[0][:100])
        delete_scorer(
            name=name, experiment_id=exp_id, version="all"
        )
        print("deleted scorer", name)
    except Exception as exc:
        print(f"FAILED to delete scorer {name}: {str(exc).splitlines()[0][:200]}")

# COMMAND ----------

# 2. The evaluation dataset (a Unity Catalog table)
if dataset_exists and not confirm:
    print("would delete dataset", dataset_full_name)
elif dataset_exists:
    try:
        ds.delete_dataset(name=dataset_full_name)
        print("deleted dataset", dataset_full_name)
    except Exception as exc:
        print(f"FAILED to delete dataset: {str(exc).splitlines()[0][:200]}\n"
              f"If the table is still there, drop it yourself: DROP TABLE "
              f"{dataset_full_name}")

# COMMAND ----------

# 3. Runs these notebooks created (matched by the cobalt_ch12 tag)
for run_id in run_ids:
    if confirm:
        mlflow.delete_run(run_id)
print(("deleted " if confirm else "would delete ") + f"{len(run_ids)} runs")

# COMMAND ----------

# 4. The experiment itself, with its traces (only if asked)
if experiment is not None and delete_experiment:
    if confirm:
        mlflow.delete_experiment(experiment.experiment_id)
        print("deleted experiment", experiment.name)
    else:
        print("would delete experiment", experiment.name)
elif experiment is not None:
    print("Experiment kept. Its traces stay until you delete the experiment "
          "(set delete_experiment = yes).")

# COMMAND ----------

# MAGIC %md
# MAGIC The catalog and schema are never dropped. Drop them yourself if you created them only for this chapter.
