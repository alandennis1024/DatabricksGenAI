# Databricks notebook source
# MAGIC %md
# MAGIC # 01 - A Unity Catalog function as an agent tool
# MAGIC
# MAGIC **Book section:** Chapter 8, *Tools* > *Defining a Tool in Python* (the Sales Summary tool).
# MAGIC
# MAGIC **What it does:** registers the SQL function `region_revenue(region_name STRING)` in Unity
# MAGIC Catalog, tests it with plain SQL, and shows how `EXECUTE` grants control who may call it.
# MAGIC The `COMMENT` text on the function and on its parameter is what the LLM reads to decide
# MAGIC when and how to use the tool.
# MAGIC
# MAGIC **Prerequisites:** run `00_setup_sample_data` first. You need `CREATE FUNCTION` on the
# MAGIC schema.
# MAGIC
# MAGIC **Compute:** serverless or DBR 17+. **Expected runtime:** under a minute.
# MAGIC
# MAGIC **Creates:** the function `<catalog>.<schema>.region_revenue`. No model endpoint is needed.

# COMMAND ----------

dbutils.widgets.text("catalog", "cobalt_ops", "Catalog")
dbutils.widgets.text("schema", "sales", "Schema")
dbutils.widgets.text("grant_to", "", "Optional: group to grant EXECUTE to")

import re

catalog = dbutils.widgets.get("catalog").strip()
schema = dbutils.widgets.get("schema").strip()
grant_to = dbutils.widgets.get("grant_to").strip()
for _label, _value in (("catalog", catalog), ("schema", schema)):
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", _value):
        raise ValueError(
            f"Widget '{_label}' must use letters, digits and underscores only "
            f"(got '{_value}')."
        )

FQ = f"{catalog}.{schema}"
FUNCTION = f"{FQ}.region_revenue"
print(f"Function will be created as {FUNCTION}")

# COMMAND ----------

# Fail early with a clear message when notebook 00 has not been run.
try:
    spark.sql(f"SELECT 1 FROM {FQ}.sales LIMIT 1").collect()
except Exception as exc:
    raise RuntimeError(
        f"Table {FQ}.sales was not found. Run 00_setup_sample_data first (with the "
        f"same catalog and schema widgets). Original error: {str(exc)[:200]}"
    ) from exc
print(f"{FQ}.sales found.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create the function
# MAGIC
# MAGIC `region_name` is a typed parameter. The SQL never joins strings together, so a value from
# MAGIC the LLM cannot change the shape of the query.

# COMMAND ----------

spark.sql(
    f"""
    CREATE OR REPLACE FUNCTION {FUNCTION}(
      region_name STRING COMMENT 'Sales region, for example EMEA')
    RETURNS DOUBLE
    COMMENT 'Total revenue for one sales region.'
    RETURN (SELECT SUM(revenue) FROM {FQ}.sales
            WHERE region = region_name)
    """
)
print(f"Created {FUNCTION}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test it with SQL
# MAGIC
# MAGIC `EMEA` should return `40100.5`. An unknown region returns `NULL`, and so does a value that
# MAGIC tries to smuggle in extra SQL, because it is only ever compared as a string.

# COMMAND ----------

display(
    spark.sql(
        f"""
        SELECT
          {FUNCTION}('EMEA')      AS emea,
          {FUNCTION}('AMER')      AS amer,
          {FUNCTION}('Atlantis')  AS unknown_region,
          {FUNCTION}(:attempt)    AS injection_attempt
        """,
        args={"attempt": "x' OR '1'='1"},
    )
)

emea = spark.sql(f"SELECT {FUNCTION}('EMEA') AS v").collect()[0]["v"]
expected = spark.sql(
    f"SELECT SUM(revenue) AS v FROM {FQ}.sales WHERE region = 'EMEA'"
).collect()[0]["v"]
assert abs(emea - expected) < 0.01, f"Function returned {emea}, table says {expected}"
print(f"OK: region_revenue('EMEA') = {emea}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## What the LLM sees
# MAGIC
# MAGIC The description of the function and its parameter come from the `COMMENT` strings.

# COMMAND ----------

display(spark.sql(f"DESCRIBE FUNCTION EXTENDED {FUNCTION}"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Control who can run the tool
# MAGIC
# MAGIC Because the tool is a Unity Catalog function, `EXECUTE` decides who may call it. The next
# MAGIC cell does nothing unless you type a group or user name into the `grant_to` widget (for
# MAGIC example `account users`). You must be the function owner or have `MANAGE` on it.

# COMMAND ----------

if not grant_to:
    print(
        "Skipped: the 'grant_to' widget is empty. Example of what would run:\n"
        f"  GRANT EXECUTE ON FUNCTION {FUNCTION} TO `account users`"
    )
elif not re.fullmatch(r"[A-Za-z0-9_@. -]+", grant_to):
    print("Skipped: 'grant_to' contains characters that are not allowed in a name.")
else:
    try:
        spark.sql(f"GRANT EXECUTE ON FUNCTION {FUNCTION} TO `{grant_to}`")
        print(f"Granted EXECUTE on {FUNCTION} to `{grant_to}`.")
    except Exception as exc:
        print(f"Grant failed (you may not own the function): {str(exc)[:300]}")

print("Done. Next: run 02_tool_calling_agent.")
