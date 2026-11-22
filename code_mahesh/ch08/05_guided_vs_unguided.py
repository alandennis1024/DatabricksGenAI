# Databricks notebook source
# MAGIC %md
# MAGIC # 05 - Guided vs. unguided: the data deletion request
# MAGIC
# MAGIC **Book section:** Chapter 8, *Guided vs. Unguided Agents* > *Example: Data Deletion
# MAGIC Request*.
# MAGIC
# MAGIC **What it does:** handles the request *"Delete all test records older than 6 months"* two
# MAGIC ways on a scratch copy of the data.
# MAGIC
# MAGIC * **Unguided:** runs `DELETE` right away. Nobody reviews what it removes.
# MAGIC * **Guided:** first does a **dry run** that only counts and previews the matching rows, then
# MAGIC   asks *"I found N records to delete. Should I proceed?"*, and deletes **only** when the
# MAGIC   `approve` widget says `approve`.
# MAGIC
# MAGIC Both versions use the same deletion function. The difference is the approval point in
# MAGIC front of it. The code is plain Python so you can see the pattern without a model in the
# MAGIC way. Notebook 04 shows how a real agent pauses and resumes.
# MAGIC
# MAGIC **Prerequisites:** run `00_setup_sample_data` first.
# MAGIC
# MAGIC **Compute:** serverless or DBR 17+. **Expected runtime:** under a minute.
# MAGIC
# MAGIC **Creates:** the scratch table `deletion_demo_records` (rebuilt each time a section
# MAGIC runs). The real `customer_records` table is never changed.
# MAGIC
# MAGIC **How to use the approval widget:** run the notebook once with `approve` empty to see the
# MAGIC dry run and the waiting message. Then type `approve` in the widget and run the guided
# MAGIC section again to see the delete.

# COMMAND ----------

dbutils.widgets.text("catalog", "cobalt_ops", "Catalog")
dbutils.widgets.text("schema", "sales", "Schema")
dbutils.widgets.text("approve", "", "Type 'approve' to allow the guided delete")
dbutils.widgets.text("months", "6", "Delete test records older than N months")

import re

catalog = dbutils.widgets.get("catalog").strip()
schema = dbutils.widgets.get("schema").strip()
for _label, _value in (("catalog", catalog), ("schema", schema)):
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", _value):
        raise ValueError(
            f"Widget '{_label}' must use letters, digits and underscores only "
            f"(got '{_value}')."
        )
try:
    months = int(dbutils.widgets.get("months"))
except ValueError as exc:
    raise ValueError("Widget 'months' must be a whole number, e.g. 6") from exc
if not 1 <= months <= 120:
    raise ValueError("Widget 'months' must be between 1 and 120.")

FQ = f"{catalog}.{schema}"
SOURCE = f"{FQ}.customer_records"
SCRATCH = f"{FQ}.deletion_demo_records"
print(f"Source: {SOURCE}\nScratch copy: {SCRATCH}")

# COMMAND ----------

# Fail early with a clear message when notebook 00 has not been run.
try:
    spark.sql(f"SELECT 1 FROM {SOURCE} LIMIT 1").collect()
except Exception as exc:
    raise RuntimeError(
        f"Table {SOURCE} was not found. Run 00_setup_sample_data first (with the "
        f"same catalog and schema widgets). Original error: {str(exc)[:200]}"
    ) from exc
print("Source table found.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## The tools
# MAGIC
# MAGIC `delete_old_test_records` is the dangerous one. `find_old_test_records` is the safe,
# MAGIC read-only one that the guided agent uses for its dry run.

# COMMAND ----------

from datetime import date, timedelta


def cutoff_date(n_months):
    """Approximate N months as N * 30 days and return an ISO date string."""
    return (date.today() - timedelta(days=30 * n_months)).isoformat()


def reset_scratch():
    """Rebuild the scratch table from the source so every demo starts the same."""
    spark.sql(f"CREATE OR REPLACE TABLE {SCRATCH} AS SELECT * FROM {SOURCE}")


def counts():
    rows = spark.sql(
        f"""
        SELECT record_type, COUNT(*) AS n,
               SUM(CASE WHEN created_date < CAST(:cutoff AS DATE) THEN 1 ELSE 0 END) AS old
        FROM {SCRATCH} GROUP BY record_type ORDER BY record_type
        """,
        args={"cutoff": cutoff_date(months)},
    ).collect()
    return {r["record_type"]: (r["n"], int(r["old"])) for r in rows}


def find_old_test_records(preview=5):
    """Read only: count the matching rows and fetch a few to show."""
    args = {"cutoff": cutoff_date(months)}
    where = (
        f"FROM {SCRATCH} "
        "WHERE record_type = 'test' AND created_date < CAST(:cutoff AS DATE)"
    )
    n = spark.sql(f"SELECT COUNT(*) AS n {where}", args=args).collect()[0]["n"]
    sample = spark.sql(
        f"SELECT record_id, record_type, created_date, owner {where} "
        f"ORDER BY created_date LIMIT {int(preview)}",
        args=args,
    )
    return n, sample


def delete_old_test_records():
    """Delete the matching rows. This cannot be undone without table history."""
    spark.sql(
        f"DELETE FROM {SCRATCH} "
        "WHERE record_type = 'test' AND created_date < CAST(:cutoff AS DATE)",
        args={"cutoff": cutoff_date(months)},
    )


def show_counts(label):
    print(label)
    for record_type, (n, old) in counts().items():
        print(f"  {record_type:<11} total={n:<3} older than {months} months={old}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Unguided agent
# MAGIC
# MAGIC It reads the request and acts. If its idea of a "test record" were wrong, real data would
# MAGIC be gone before anyone saw it.

# COMMAND ----------

def unguided_agent(request):
    print(f"REQUEST: {request}")
    delete_old_test_records()
    print("DONE: deleted the matching records. Nobody reviewed them first.")


reset_scratch()
show_counts("Before:")
unguided_agent(f"Delete all test records older than {months} months.")
show_counts("After:")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Guided agent
# MAGIC
# MAGIC The guided agent follows a fixed path: **find, report, ask, then delete**. Without an
# MAGIC explicit `approve`, it stops after the dry run and changes nothing.
# MAGIC
# MAGIC Run this section with the widget empty first, then type `approve` and run it again.

# COMMAND ----------

def guided_agent(request, approval):
    print(f"REQUEST: {request}")
    n, sample = find_old_test_records()
    print(f"DRY RUN: I found {n} records to delete. Preview of the oldest rows:")
    display(sample)
    if n == 0:
        print("Nothing to delete.")
        return "nothing_to_delete"
    print(f"QUESTION: Should I proceed with deleting {n} records? [approve / no]")
    if approval.strip().lower() != "approve":
        print(
            "WAITING: nothing was deleted. Type 'approve' in the 'approve' widget and "
            "run this section again to continue."
        )
        return "waiting_for_approval"
    delete_old_test_records()
    print(f"APPROVED: deleted {n} records.")
    return "deleted"


reset_scratch()
show_counts("Before:")
outcome = guided_agent(
    f"Delete all test records older than {months} months.",
    dbutils.widgets.get("approve"),
)
show_counts(f"After ({outcome}):")

# COMMAND ----------

# MAGIC %md
# MAGIC ## What to notice
# MAGIC
# MAGIC * With `approve` empty, the guided agent deleted **nothing**. With `approve`, it deleted
# MAGIC   exactly the rows it had previewed, and only `test` rows.
# MAGIC * The `production` rows, old and new, and the recent `test` rows are untouched in both
# MAGIC   runs.
# MAGIC * In a real agent, the approval comes from a person through Slack, email, or an app. The
# MAGIC   agent saves its state and waits, as in notebook 04. Here the widget plays that role.
# MAGIC
# MAGIC Run `99_cleanup` when you are finished with the whole folder.

# COMMAND ----------

after = counts()
production_total = after.get("production", (0, 0))[0]
expected_production = spark.sql(
    f"SELECT COUNT(*) AS n FROM {SOURCE} WHERE record_type = 'production'"
).collect()[0]["n"]
assert production_total == expected_production, "production rows were changed!"
print("OK: all production rows are still in the scratch table.")
print("Done. Next: run 99_cleanup when you are finished.")
