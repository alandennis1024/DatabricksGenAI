# Databricks notebook source
# MAGIC %md
# MAGIC # 05 - Query the audit trail
# MAGIC
# MAGIC **Book section:** *Logs, Compliance, and Inference Tables* (the anomaly
# MAGIC query) and *Governance, Auditing, and Visibility > Data Profiling and
# MAGIC Genie Agents*.
# MAGIC
# MAGIC **What it does:** runs the behavioural anomaly query from the book,
# MAGIC `input_params:discount_pct::double`, over `agent_audit.tool_actions`,
# MAGIC plus a few more questions an auditor would ask (rejections by guardrail
# MAGIC ring, latency, error rate). It also proves the table is append-only by
# MAGIC trying, and failing, to delete from it.
# MAGIC
# MAGIC **Prerequisites:** run `00_setup_audit_trail` and
# MAGIC `02_agent_with_guardrails` (same `catalog` and `audit_schema`) so the
# MAGIC table has rows. Read-only notebook, except for the blocked `DELETE`
# MAGIC demonstration.
# MAGIC
# MAGIC **Runtime:** serverless compute or DBR 17+. Seconds. No model endpoint.
# MAGIC
# MAGIC **Creates:** nothing.

# COMMAND ----------

# MAGIC %pip install -q -r requirements.txt

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

dbutils.widgets.text("catalog", "cobalt_ops", "Catalog")
dbutils.widgets.text("audit_schema", "agent_audit", "Audit schema")

CATALOG = dbutils.widgets.get("catalog").strip()
AUDIT_SCHEMA = dbutils.widgets.get("audit_schema").strip()

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

T_AUDIT = cw.qualified(CATALOG, AUDIT_SCHEMA, "tool_actions")

try:
    n_rows = spark.sql(f"SELECT COUNT(*) AS n FROM {T_AUDIT}").collect()[0]["n"]
except Exception as exc:
    raise RuntimeError(
        f"Cannot read {T_AUDIT} ({str(exc)[:120]}). Run 00_setup_audit_trail "
        "and 02_agent_with_guardrails first, with the same widgets.") from exc
print(f"{T_AUDIT} has {n_rows} rows")
if n_rows == 0:
    print("The table is empty, so the queries below return no rows. Run "
          "02_agent_with_guardrails to generate audit events.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Anomaly signal: large discounts in the last 7 days
# MAGIC
# MAGIC `input_params` is a JSON string. The `:` operator extracts a field from
# MAGIC it, and `::double` casts the value, so no schema or `from_json` is needed.
# MAGIC Notebook 02 produces at least one row above 8% on purpose: the scripted
# MAGIC 15% request on a STANDARD customer, which the approval gate rejected. The
# MAGIC agent's own `apply_discount` calls appear here too.

# COMMAND ----------

anomalies = spark.sql(f"""
SELECT session_id,
       DATE(event_timestamp)              AS call_date,
       input_params:discount_pct::double  AS discount_pct,
       status,
       policy_basis
FROM {T_AUDIT}
WHERE tool_name = 'apply_discount'
  AND event_timestamp >= current_timestamp() - INTERVAL 7 DAYS
  AND input_params:discount_pct::double > 8.0
ORDER BY event_timestamp DESC
""")
display(anomalies)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Which guardrail ring made the decision?

# COMMAND ----------

display(spark.sql(f"""
SELECT tool_name, status, policy_basis, COUNT(*) AS calls
FROM {T_AUDIT}
GROUP BY tool_name, status, policy_basis
ORDER BY calls DESC
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Latency and error rate per tool
# MAGIC
# MAGIC `latency_ms` is `BIGINT`. These are the numbers a data profile would
# MAGIC track over time (see the note below).

# COMMAND ----------

display(spark.sql(f"""
SELECT tool_name,
       COUNT(*)                                        AS calls,
       ROUND(AVG(latency_ms), 1)                       AS avg_latency_ms,
       percentile_approx(latency_ms, 0.95)             AS p95_latency_ms,
       ROUND(100 * AVG(CASE WHEN status = 'error' THEN 1 ELSE 0 END), 1)
                                                       AS error_pct,
       ROUND(100 * AVG(CASE WHEN status = 'policy_rejected' THEN 1 ELSE 0 END), 1)
                                                       AS policy_rejected_pct
FROM {T_AUDIT}
GROUP BY tool_name
ORDER BY calls DESC
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. The log cannot be rewritten
# MAGIC
# MAGIC `delta.appendOnly` blocks deletes and updates of existing rows. The cell
# MAGIC first confirms the property is set and refuses to run otherwise, then
# MAGIC tries to delete every row and checks that the row count is unchanged.

# COMMAND ----------

props = {r["key"]: r["value"]
         for r in spark.sql(f"SHOW TBLPROPERTIES {T_AUDIT}").collect()}
if props.get("delta.appendOnly") != "true":
    print("delta.appendOnly is not set on this table, so the delete test is "
          "skipped. Re-run 00_setup_audit_trail to set it.")
else:
    before = spark.sql(f"SELECT COUNT(*) AS n FROM {T_AUDIT}").collect()[0]["n"]
    try:
        spark.sql(f"DELETE FROM {T_AUDIT} WHERE session_id IS NOT NULL")
        print("DELETE finished without error. That is expected only if the "
              "table was empty.")
    except Exception as exc:
        print("DELETE blocked, as intended:", str(exc).strip()[:200])
    after = spark.sql(f"SELECT COUNT(*) AS n FROM {T_AUDIT}").collect()[0]["n"]
    assert after == before, f"audit rows changed: {before} -> {after}"
    print(f"PASS row count unchanged ({after})")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data profiling and Genie Agents
# MAGIC
# MAGIC These two features are set up in the UI, so this notebook does not
# MAGIC automate them.
# MAGIC
# MAGIC **Data profiling** (formerly Lakehouse Monitoring) tracks statistics and
# MAGIC drift on the audit table over time. In Catalog Explorer, open
# MAGIC `agent_audit.tool_actions` and look for the **Quality** tab to create a
# MAGIC profile. Useful custom metrics for this table: error rate, the share of
# MAGIC discounts near the 10% policy limit, and calls outside business hours.
# MAGIC Put a Databricks SQL alert on the profile's metrics tables to catch
# MAGIC spikes.
# MAGIC
# MAGIC **Genie Agent** (formerly a Genie space): create one over
# MAGIC `tool_actions` and the profile metrics tables so operations managers can
# MAGIC ask "How many discount requests were flagged for human approval last
# MAGIC week?" without writing SQL. Add the queries from this notebook as
# MAGIC example SQL and define terms such as "flagged" (`status =
# MAGIC 'policy_rejected'` with `policy_basis = 'ring5_human_approval_gate'`)
# MAGIC in the instructions so it answers consistently.
