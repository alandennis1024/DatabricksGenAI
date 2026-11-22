# Databricks notebook source
# MAGIC %md
# MAGIC # 99 - Clean up (Chapter 15)
# MAGIC
# MAGIC **What it does.** Removes what notebooks 00 and 04 created in
# MAGIC `<catalog>.<schema>`: the eight operating views, the five sample tables,
# MAGIC and the `procurement_assistant` prompt (its aliases, versions, and the
# MAGIC prompt itself).
# MAGIC
# MAGIC **Safe by default.** Nothing is deleted unless the `confirm` widget is set
# MAGIC to `yes`. With `no`, the notebook only lists what it would remove. A table
# MAGIC is dropped only if its comment marks it as Chapter 15 sample data, so a
# MAGIC real table with a matching name is left alone.
# MAGIC
# MAGIC **Leaves in place.** The catalog and schema (you may have used them for
# MAGIC other work), and the MLflow traces written by notebook 00. Delete those in
# MAGIC the Catalog Explorer or the MLflow experiment UI if you want them gone.
# MAGIC
# MAGIC **Prerequisites.** The same `catalog` and `schema` widget values you used
# MAGIC before. **Runtime.** Serverless compute, or a cluster on DBR 17 or later.

# COMMAND ----------

# MAGIC %pip install -q -r requirements.txt

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

dbutils.widgets.text("catalog", "cobalt_ops", "Catalog")
dbutils.widgets.text("schema", "future", "Schema")
dbutils.widgets.dropdown("confirm", "no", ["no", "yes"], "Delete? (yes to confirm)")

# COMMAND ----------

import os
import sys


def _find_notebook_dir():
    candidates = []
    try:
        ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
        folder = os.path.dirname(ctx.notebookPath().get())
        candidates += [folder, "/Workspace" + folder]
    except Exception:
        pass
    candidates.append(os.getcwd())
    for folder in candidates:
        if os.path.exists(os.path.join(folder, "cobalt_common.py")):
            return folder
    raise RuntimeError(
        "cobalt_common.py was not found next to this notebook. Clone the whole "
        "repository into a Databricks Git folder and open the notebook there."
    )


NOTEBOOK_DIR = _find_notebook_dir()
if NOTEBOOK_DIR not in sys.path:
    sys.path.insert(0, NOTEBOOK_DIR)

# COMMAND ----------

import cobalt_common as cc

CATALOG = cc.check_identifier(dbutils.widgets.get("catalog"), "catalog")
SCHEMA = cc.check_identifier(dbutils.widgets.get("schema"), "schema")
CONFIRMED = dbutils.widgets.get("confirm").strip().lower() == "yes"
print(f"Target: {CATALOG}.{SCHEMA}")
print("Mode:", "DELETE" if CONFIRMED else "dry run (set confirm = yes to delete)")


def schema_exists():
    try:
        rows = spark.sql(f"SHOW SCHEMAS IN {cc.quote(CATALOG)}").collect()
    except Exception:
        return False
    return SCHEMA in {str(row[0]) for row in rows}


HAS_SCHEMA = schema_exists()
if not HAS_SCHEMA:
    print(f"Schema {CATALOG}.{SCHEMA} does not exist: no views or tables to remove.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Views and tables

# COMMAND ----------

if HAS_SCHEMA:
    for name in cc.VIEWS:
        fqn = cc.fq(CATALOG, SCHEMA, name)
        if CONFIRMED:
            spark.sql(f"DROP VIEW IF EXISTS {fqn}")
            print(f"dropped view  {name}")
        else:
            print(f"would drop view  {name}")

    for name in cc.SAMPLE_TABLES:
        fqn = cc.fq(CATALOG, SCHEMA, name)
        marked = cc.is_sample_table(spark, fqn)
        if marked is None:
            print(f"no table       {name}")
        elif not marked:
            print(f"SKIPPED table  {name}: not marked as Chapter 15 sample data")
        elif CONFIRMED:
            spark.sql(f"DROP TABLE IF EXISTS {fqn}")
            print(f"dropped table {name}")
        else:
            print(f"would drop table {name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Prompt

# COMMAND ----------

import mlflow
from mlflow import MlflowClient

PROMPT = cc.prompt_name(CATALOG, SCHEMA)
ready, message = cc.prompt_registry_ready(mlflow, CATALOG, SCHEMA)
if not ready:
    print("Prompt Registry is not available, so no prompt was removed.")
    print(message)
else:
    client = MlflowClient()
    try:
        versions = [v.version for v in client.search_prompt_versions(PROMPT)]
    except Exception as exc:
        versions = []
        print(f"Could not list versions of {PROMPT} ({type(exc).__name__}).")
    if not versions:
        print(f"No prompt named {PROMPT} was found.")
    elif not CONFIRMED:
        print(f"would delete prompt {PROMPT} (versions {sorted(versions)})")
    else:
        for alias in ("staging", "production"):
            try:
                mlflow.genai.delete_prompt_alias(name=PROMPT, alias=alias)
            except Exception:
                pass  # the alias may never have been set
        for version in versions:
            client.delete_prompt_version(PROMPT, str(version))
        client.delete_prompt(PROMPT)
        print(f"deleted prompt {PROMPT} (versions {sorted(versions)})")

# COMMAND ----------

print("Done." if CONFIRMED else "Dry run only. Nothing was deleted.")
