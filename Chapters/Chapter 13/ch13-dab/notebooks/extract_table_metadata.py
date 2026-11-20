# Databricks notebook source
# MAGIC %md
# MAGIC # Extract table metadata for the agent
# MAGIC
# MAGIC Scheduled job that extracts table metadata and basic statistics for
# MAGIC the tables in the configured schema, writing the context the agent
# MAGIC uses when generating rules. Runs on the schedule defined in the DAB
# MAGIC (`prepare_agent_context` job); job parameters target the correct
# MAGIC catalog per environment.

# COMMAND ----------

dbutils.widgets.text("catalog", "demo", "Catalog to profile")
dbutils.widgets.text("schema", "finance", "Schema to profile")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

# COMMAND ----------

import json

tables = [
    row["tableName"]
    for row in spark.sql(f"SHOW TABLES IN {catalog}.{schema}").collect()
]

rows = []
for name in tables:
    df = spark.table(f"{catalog}.{schema}.{name}")
    columns = [(f.name, f.dataType.simpleString()) for f in df.schema.fields]
    rows.append((name, df.count(), json.dumps(columns)))

context = spark.createDataFrame(
    rows, "table_name string, row_count long, columns_json string"
)

output_table = f"{catalog}.{schema}.dq_agent_context"
context.write.mode("overwrite").saveAsTable(output_table)

print(f"Metadata for {len(rows)} tables written to {output_table}")
display(context)
