# Databricks notebook source
# MAGIC %md
# MAGIC # 04 - Operating views: Quality, Reliability, Economics, Control (Chapter 15)
# MAGIC
# MAGIC **Book section:** Looking to the Future > CobaltWorks in 2028 >
# MAGIC *An evaluation trace* and *Operating after launch* (Figure 15-5).
# MAGIC
# MAGIC **What it does.** Creates five tables in Unity Catalog, fills them with
# MAGIC **synthetic sample data**, creates the SQL views from `operating_views.sql`,
# MAGIC and shows each view and alert query:
# MAGIC
# MAGIC The four views, each with the question it answers:
# MAGIC * **Quality** (groundedness, citation correctness, task success): are
# MAGIC   shipment answers still citing the right carrier record?
# MAGIC * **Reliability** (endpoint errors, tool timeouts, queue depth, latency):
# MAGIC   is the shipment tool timing out during the morning rush?
# MAGIC * **Economics** (tokens, cache hit rate, cost per successful task): is a
# MAGIC   rebooking request costing more than it did last month?
# MAGIC * **Control** (approval holds, policy denials, suspicious retrieval): how
# MAGIC   often is a rebooking held for approval?
# MAGIC
# MAGIC The `agent_actions` table is the audit table from the chapter. The
# MAGIC sample data has planted patterns (a groundedness dip in the last three days,
# MAGIC timeouts at 08:00-09:59 UTC, a cross-tenant read, a retrieval burst) so each
# MAGIC view has something to show. Real monitoring tables are filled by your trace
# MAGIC export and evaluation jobs instead.
# MAGIC
# MAGIC **Prerequisites.** A catalog and schema where you can create tables and
# MAGIC views (set the `catalog` and `schema` widgets). The notebook tries
# MAGIC `CREATE CATALOG IF NOT EXISTS` and tells you to change the widget if that is
# MAGIC not allowed. It never overwrites a table that it did not create.
# MAGIC
# MAGIC **Runtime.** Serverless compute, or a cluster on DBR 17 or later.
# MAGIC
# MAGIC **Creates.** Tables `request_traces`, `eval_scores`, `endpoint_metrics`,
# MAGIC `retrieval_events`, `agent_actions`, and eight views, all in
# MAGIC `<catalog>.<schema>`. Run `99_cleanup` to remove them. Running this notebook
# MAGIC again resets the sample tables.

# COMMAND ----------

# MAGIC %pip install -q -r requirements.txt

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

dbutils.widgets.text("catalog", "cobalt_ops", "Catalog")
dbutils.widgets.text("schema", "future", "Schema")

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
        if os.path.exists(os.path.join(folder, "operating_views.sql")):
            return folder
    raise RuntimeError(
        "operating_views.sql was not found next to this notebook. Clone the "
        "whole repository into a Databricks Git folder and open the notebook "
        "there."
    )


NOTEBOOK_DIR = _find_notebook_dir()
if NOTEBOOK_DIR not in sys.path:
    sys.path.insert(0, NOTEBOOK_DIR)

# COMMAND ----------

from pathlib import Path

import cobalt_common as cc
import ops_sql

CATALOG = cc.check_identifier(dbutils.widgets.get("catalog"), "catalog")
SCHEMA = cc.check_identifier(dbutils.widgets.get("schema"), "schema")

cc.ensure_catalog_schema(spark, CATALOG, SCHEMA)
statements = ops_sql.load_statements(
    CATALOG, SCHEMA, Path(NOTEBOOK_DIR) / "operating_views.sql"
)
print(f"Target: {CATALOG}.{SCHEMA}")
print(f"{len(statements)} statements loaded from operating_views.sql")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Create the tables
# MAGIC
# MAGIC The DDL comes straight from `operating_views.sql`. Each table is marked as
# MAGIC sample data in its comment. If a table with the same name already exists
# MAGIC without that mark, the notebook stops rather than overwrite it.

# COMMAND ----------

for statement in statements:
    if ops_sql.kind(statement) in ("schema", "table"):
        spark.sql(statement)

for table in cc.SAMPLE_TABLES:
    fqn = cc.fq(CATALOG, SCHEMA, table)
    if not cc.is_sample_table(spark, fqn):
        raise RuntimeError(
            f"{fqn} exists but was not created by this notebook, so it will "
            "not be overwritten. Choose a different `schema` widget value."
        )
print("Tables ready:", ", ".join(cc.SAMPLE_TABLES))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Fill them with synthetic sample data
# MAGIC
# MAGIC About 45 days of requests ending today. The generator uses a fixed random
# MAGIC seed, so the shape is the same each time. Everything is made up.

# COMMAND ----------

from datetime import datetime, timezone

import sample_data

rows = sample_data.build_sample(datetime.now(timezone.utc))
for table, sql in sample_data.insert_statements(CATALOG, SCHEMA, rows):
    spark.sql(sql)

for table in cc.SAMPLE_TABLES:
    count = spark.sql(
        f"SELECT count(*) AS n FROM {cc.fq(CATALOG, SCHEMA, table)}"
    ).collect()[0][0]
    print(f"{table:<18} {count:>6} rows")
    assert count == len(rows[table]), f"unexpected row count for {table}"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Create the views
# MAGIC
# MAGIC `CREATE OR REPLACE VIEW`, so this step can be repeated.

# COMMAND ----------

created = []
for statement in statements:
    if ops_sql.kind(statement) == "view":
        spark.sql(statement)
        first_line = statement.splitlines()[0]
        created.append(first_line[len("CREATE OR REPLACE VIEW "):-len(" AS")])
print("Views created:")
for name in created:
    print(" ", name.replace("`", ""))

# COMMAND ----------


def view(name):
    return cc.fq(CATALOG, SCHEMA, name)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Quality
# MAGIC
# MAGIC *Are shipment answers still citing the right carrier record?* Daily scores
# MAGIC per scorer, and the task success rate. The alert query computes a
# MAGIC three-day rolling groundedness average. Look for the dip in the last three
# MAGIC days.

# COMMAND ----------

display(spark.sql(
    f"SELECT * FROM {view('v_quality_daily')} ORDER BY score_date DESC, scorer_name"
))

# COMMAND ----------

display(spark.sql(
    f"SELECT * FROM {view('v_task_success_daily')} ORDER BY request_date DESC"
))

# COMMAND ----------

display(spark.sql(ops_sql.alert_query(statements, "v_quality_daily")))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Reliability
# MAGIC
# MAGIC *Is the shipment MCP tool timing out during the morning rush?* Errors and
# MAGIC p95 latency per endpoint, tool timeouts per hour, and queue depth. The
# MAGIC sample data times out more at 08:00-09:59 UTC.

# COMMAND ----------

display(spark.sql(
    f"SELECT * FROM {view('v_endpoint_reliability_daily')} "
    "ORDER BY request_date DESC, endpoint_name"
))

# COMMAND ----------

display(spark.sql(
    f"""
    SELECT hour(event_hour) AS hour_of_day,
           sum(tool_calls) AS tool_calls,
           sum(timeouts) AS timeouts,
           sum(timeouts) / sum(tool_calls) AS timeout_rate
    FROM {view('v_tool_timeouts_hourly')}
    WHERE tool_name = 'get_shipment_status'
    GROUP BY hour(event_hour)
    ORDER BY hour_of_day
    """
))

# COMMAND ----------

display(spark.sql(
    f"SELECT * FROM {view('v_queue_depth_hourly')} "
    "ORDER BY sample_hour DESC, endpoint_name LIMIT 48"
))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Economics
# MAGIC
# MAGIC *Is a rebooking request costing more than it did last month?* Tokens, cache
# MAGIC hit rate, and cost per successful task by route. The small model handles
# MAGIC lookups, so that route is cheap.

# COMMAND ----------

display(spark.sql(
    f"SELECT * FROM {view('v_economics_daily')} ORDER BY request_date DESC, route"
))

# COMMAND ----------

display(spark.sql(ops_sql.alert_query(statements, "v_economics_daily")))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Control
# MAGIC
# MAGIC *How often does the policy service hold a rebooking for approval?*
# MAGIC Proposals, approval holds, denials, and permission failures per day, and the
# MAGIC retrievals that cross tenants or arrive in a burst.

# COMMAND ----------

display(spark.sql(
    f"SELECT * FROM {view('v_control_daily')} "
    "WHERE proposals > 0 OR policy_denials > 0 OR permission_failures > 0 "
    "ORDER BY event_date DESC, tool_name"
))

# COMMAND ----------

display(spark.sql(ops_sql.alert_query(statements, "v_control_daily")))

# COMMAND ----------

display(spark.sql(
    f"SELECT * FROM {view('v_suspicious_retrieval')} "
    "ORDER BY event_time DESC LIMIT 100"
))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next
# MAGIC
# MAGIC Alerts should open an investigation with an owner and a runbook. A red
# MAGIC number with no owner is decoration. Run `99_cleanup` when you are done.
