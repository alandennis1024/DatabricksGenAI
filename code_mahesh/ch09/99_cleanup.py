# Databricks notebook source
# MAGIC %md
# MAGIC # 99 - Clean up
# MAGIC
# MAGIC **Book section:** none (housekeeping for the companion code).
# MAGIC
# MAGIC **What it does:** drops what this chapter's notebooks created, **only
# MAGIC when the `confirm` widget is `yes`**:
# MAGIC * tables `tool_actions` and `human_approvals` in `<catalog>.<audit_schema>`
# MAGIC * the schemas `<audit_schema>` and `agent_resources`, but only if they are
# MAGIC   empty afterwards (a plain `DROP SCHEMA`, never `CASCADE`, so anything
# MAGIC   else you stored there is safe)
# MAGIC * optionally the HTTP connection named in `drop_connection`
# MAGIC
# MAGIC It never drops the catalog. If you created `cobalt_ops` just for this
# MAGIC chapter, drop it yourself with `DROP CATALOG cobalt_ops`.
# MAGIC
# MAGIC The audit table is append-only, which blocks `DELETE` and `UPDATE` but
# MAGIC not `DROP TABLE`. In a real system, restrict who can drop it.
# MAGIC
# MAGIC Lakebase checkpoint tables (notebook 04) and MLflow traces are not
# MAGIC touched. Delete the Lakebase instance or project in the Lakebase UI if you
# MAGIC no longer need it.
# MAGIC
# MAGIC **Prerequisites:** none. **Runtime:** serverless or DBR 17+, seconds.
# MAGIC **Creates:** nothing.

# COMMAND ----------

# MAGIC %pip install -q -r requirements.txt

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

dbutils.widgets.text("catalog", "cobalt_ops", "Catalog")
dbutils.widgets.text("audit_schema", "agent_audit", "Audit schema")
dbutils.widgets.dropdown("confirm", "no", ["no", "yes"], "Confirm deletion")
dbutils.widgets.text("drop_connection", "", "HTTP connection to drop (optional)")

CATALOG = dbutils.widgets.get("catalog").strip()
AUDIT_SCHEMA = dbutils.widgets.get("audit_schema").strip()
CONFIRM = dbutils.widgets.get("confirm").strip() == "yes"
DROP_CONNECTION = dbutils.widgets.get("drop_connection").strip()

# COMMAND ----------

import os
import sys


def _find_notebook_dir():
    candidates = [os.getcwd()]
    try:
        ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
        candidates.insert(0, os.path.dirname("/Workspace" + ctx.notebookPath().get()))
    except Exception:
        pass  # cwd is the notebook folder in Git folders
    for d in candidates:
        if os.path.exists(os.path.join(d, "cobaltworks_tools.py")):
            return d
    raise FileNotFoundError(
        "cobaltworks_tools.py not found. Run this notebook from the cloned "
        "'Chapter 9' Git folder so the helper module sits next to it.")


NB_DIR = _find_notebook_dir()
if NB_DIR not in sys.path:
    sys.path.insert(0, NB_DIR)

import cobaltworks_tools as cw

TABLES = [cw.qualified(CATALOG, AUDIT_SCHEMA, t)
          for t in ("tool_actions", "human_approvals")]
SCHEMAS = [cw.qualified(CATALOG, s) for s in (AUDIT_SCHEMA, "agent_resources")]

# COMMAND ----------

if not CONFIRM:
    print("confirm is 'no', so nothing was dropped. Set the widget to 'yes' "
          "to run:")
    for t in TABLES:
        print(f"  DROP TABLE IF EXISTS {t}")
    for s in SCHEMAS:
        print(f"  DROP SCHEMA IF EXISTS {s}   (only if empty)")
    if DROP_CONNECTION:
        print(f"  DROP CONNECTION IF EXISTS `{DROP_CONNECTION}`")
else:
    for t in TABLES:
        try:
            spark.sql(f"DROP TABLE IF EXISTS {t}")
            print(f"dropped table {t}")
        except Exception as exc:
            print(f"could not drop {t}: {str(exc)[:200]}")
    for s in SCHEMAS:
        try:
            remaining = spark.sql(f"SHOW TABLES IN {s}").collect()
        except Exception as exc:
            print(f"skipping schema {s}: {str(exc)[:120]}")
            continue
        if remaining:
            print(f"kept schema {s}: it still holds {len(remaining)} table(s) "
                  "that this chapter did not create")
        else:
            try:
                spark.sql(f"DROP SCHEMA IF EXISTS {s}")
                print(f"dropped schema {s}")
            except Exception as exc:
                print(f"could not drop {s}: {str(exc)[:200]}")
    if DROP_CONNECTION:
        assert cw.qualified("x", "y", DROP_CONNECTION)  # validates the name
        try:
            spark.sql(f"DROP CONNECTION IF EXISTS `{DROP_CONNECTION}`")
            print(f"dropped connection {DROP_CONNECTION}")
        except Exception as exc:
            print(f"could not drop connection: {str(exc)[:200]}")
