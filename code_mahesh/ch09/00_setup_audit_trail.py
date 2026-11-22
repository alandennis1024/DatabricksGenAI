# Databricks notebook source
# MAGIC %md
# MAGIC # 00 - Set up the audit trail
# MAGIC
# MAGIC **Book section:** *Governance, Auditing, and Visibility > Unity Catalog
# MAGIC Governance* (and the Ring 5 human approval gate in *Importance of
# MAGIC Guardrails*).
# MAGIC
# MAGIC **What it does:** creates the Unity Catalog objects that every other
# MAGIC notebook in this chapter writes to.
# MAGIC
# MAGIC **Prerequisites:** a Unity Catalog workspace. If you cannot create
# MAGIC catalogs (Free Edition and many company workspaces restrict this), set the
# MAGIC `catalog` widget to a catalog you can already use.
# MAGIC
# MAGIC **Runtime:** serverless compute, or a DBR 17+ cluster. Takes under a
# MAGIC minute. Needs no model endpoint and no external system.
# MAGIC
# MAGIC **Creates (idempotent, safe to re-run):**
# MAGIC * catalog `cobalt_ops` (if you may create it)
# MAGIC * schemas `agent_resources` and `agent_audit`
# MAGIC * `agent_audit.tool_actions`: append-only audit log
# MAGIC   (`delta.appendOnly` = true, `latency_ms` is `BIGINT`)
# MAGIC * `agent_audit.human_approvals`: the approval table behind the Ring 5
# MAGIC   human gate

# COMMAND ----------

# MAGIC %pip install -q -r requirements.txt

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

dbutils.widgets.text("catalog", "cobalt_ops", "Catalog")
dbutils.widgets.text("audit_schema", "agent_audit", "Audit schema")
dbutils.widgets.text(
    "agent_principal", "", "Agent principal to grant access (optional)")

CATALOG = dbutils.widgets.get("catalog").strip()
AUDIT_SCHEMA = dbutils.widgets.get("audit_schema").strip()
AGENT_PRINCIPAL = dbutils.widgets.get("agent_principal").strip()

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

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Catalog and schemas
# MAGIC
# MAGIC `CREATE CATALOG` may be blocked in your workspace. If so, the helper
# MAGIC prints what to do and carries on with the catalog you named.

# COMMAND ----------

cw.ensure_catalog_schema(spark, CATALOG, ["agent_resources", AUDIT_SCHEMA])
print(f"Using {CATALOG}.{AUDIT_SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. The audit table
# MAGIC
# MAGIC `delta.appendOnly` makes Delta reject `UPDATE`, `DELETE` and `MERGE`
# MAGIC changes to existing rows, so "append-only" is enforced rather than just
# MAGIC promised. `latency_ms` is `BIGINT` so large latencies never overflow.

# COMMAND ----------

T_AUDIT = cw.qualified(CATALOG, AUDIT_SCHEMA, "tool_actions")
T_APPR = cw.qualified(CATALOG, AUDIT_SCHEMA, "human_approvals")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {T_AUDIT} (
  event_timestamp TIMESTAMP NOT NULL,
  session_id      STRING    NOT NULL,
  agent_id        STRING,
  tool_name       STRING    NOT NULL,
  input_params    STRING    COMMENT 'JSON, sensitive fields masked',
  output_summary  STRING,
  status          STRING    NOT NULL COMMENT 'success|error|policy_rejected',
  latency_ms      BIGINT,
  policy_basis    STRING
) COMMENT 'Append-only audit log of every agent tool invocation'
TBLPROPERTIES ('delta.appendOnly' = 'true')
""")


def table_props(table):
    rows = spark.sql(f"SHOW TBLPROPERTIES {table}").collect()
    return {r["key"]: r["value"] for r in rows}


if table_props(T_AUDIT).get("delta.appendOnly") != "true":
    # The table existed from an earlier run without the property.
    spark.sql(f"ALTER TABLE {T_AUDIT} "
              "SET TBLPROPERTIES ('delta.appendOnly' = 'true')")
assert table_props(T_AUDIT).get("delta.appendOnly") == "true", (
    "delta.appendOnly is not set on the audit table")
print("tool_actions is append-only")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. The approvals table (Ring 5)
# MAGIC
# MAGIC The agent can only *propose* a high-risk action. A human approver
# MAGIC writes an `approved_action_id` into this table, and only then can the
# MAGIC *execute* step run (notebook 02). Rows are updated as the request moves
# MAGIC from `PENDING` to `APPROVED` and `EXECUTED`, so this table is **not**
# MAGIC append-only; the audit table keeps the permanent record. The book
# MAGIC suggests Lakebase for this table in production. A Delta table keeps the
# MAGIC demo runnable everywhere.

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {T_APPR} (
  proposal_id        STRING    NOT NULL,
  proposed_at        TIMESTAMP NOT NULL,
  session_id         STRING,
  order_id           STRING    NOT NULL,
  discount_pct       DOUBLE    NOT NULL,
  reason             STRING,
  impact_json        STRING    COMMENT 'Propose-step impact analysis',
  status             STRING    NOT NULL COMMENT 'PENDING|APPROVED|REJECTED|EXECUTED',
  approved_action_id STRING    COMMENT 'Written by the human approver',
  approver           STRING,
  decided_at         TIMESTAMP,
  executed_at        TIMESTAMP,
  confirmation_id    STRING
) COMMENT 'Human approval requests for high-risk agent actions'
""")
print("human_approvals is ready")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Grants (optional example)
# MAGIC
# MAGIC Unity Catalog has no separate `INSERT` privilege. `MODIFY` allows insert,
# MAGIC update and delete, and it needs `SELECT`, `USE SCHEMA` and `USE CATALOG`
# MAGIC alongside it. On the append-only audit table, `MODIFY` is safe: Delta
# MAGIC still blocks updates and deletes, so the agent identity can only add rows.
# MAGIC
# MAGIC The approvals table is different. The agent identity gets `SELECT`
# MAGIC only, and approvers get `MODIFY`, so the agent can never approve its own
# MAGIC request.
# MAGIC
# MAGIC Set the `agent_principal` widget to a service principal or group name to
# MAGIC run the grants. Left empty, the cell only prints them.

# COMMAND ----------

grants = [
    f"GRANT USE CATALOG ON CATALOG `{CATALOG}` TO `{{p}}`",
    f"GRANT USE SCHEMA ON SCHEMA {cw.qualified(CATALOG, AUDIT_SCHEMA)} TO `{{p}}`",
    f"GRANT SELECT, MODIFY ON TABLE {T_AUDIT} TO `{{p}}`",
    f"GRANT SELECT ON TABLE {T_APPR} TO `{{p}}`",
]
principal = AGENT_PRINCIPAL or "procurement-agent-sp"

if not AGENT_PRINCIPAL:
    print("agent_principal is empty, so nothing was granted. Example grants:")
    for g in grants:
        print("  " + g.format(p=principal))
else:
    for g in grants:
        stmt = g.format(p=principal)
        try:
            spark.sql(stmt)
            print("OK   " + stmt)
        except Exception as exc:
            print(f"FAIL {stmt}\n     {str(exc)[:200]}")
            print("     Check the principal name and that you own the objects.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next
# MAGIC
# MAGIC Run `01_operational_tools` to exercise the tools. Run
# MAGIC `99_cleanup` when you are finished to drop what this notebook created.
