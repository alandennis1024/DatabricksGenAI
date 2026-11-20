# Databricks notebook source
# MAGIC %md
# MAGIC # Chapter 6 - Sample data: CobaltWorks policy chunks
# MAGIC
# MAGIC Creates `cobalt_ops.serving.policy_chunks` with synthetic procurement
# MAGIC policy content. One document (`POL-100`) is restricted to the
# MAGIC `leadership` group so the access-control filter and the two-user test
# MAGIC in `05_access_control_test` have something real to allow and deny.
# MAGIC
# MAGIC Design note: each chunk carries exactly one `access_group` value. A
# MAGIC document readable by several groups would be stored as one row per
# MAGIC group, with the group name suffixed to `chunk_id` to keep the primary
# MAGIC key unique (dict filters on scalar columns are documented; filtering
# MAGIC on array columns is not).

# COMMAND ----------

catalog = "cobalt_ops"
schema = "serving"
table = f"{catalog}.{schema}.policy_chunks"

spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")

# COMMAND ----------

documents = [
    # document_id, section_title, effective_date, plant_id, access_group, content
    ("POL-001", "Discount approval limits", "2026-01-01", "CHI",
     "procurement-chi",
     "Chicago procurement managers may approve supplier discounts up to 10 "
     "percent. Discounts above 10 percent and up to 15 percent require "
     "plant-director approval. Discounts above 15 percent require the vice "
     "president of procurement."),
    ("POL-001", "Effective scope", "2026-01-01", "CHI",
     "procurement-chi",
     "This policy applies to the Chicago plant only. It supersedes the 2025 "
     "discount policy. Archived versions remain available for audit."),
    ("POL-002", "Discount approval limits", "2026-01-01", "DET",
     "procurement-det",
     "Detroit procurement managers may approve supplier discounts up to 8 "
     "percent. Higher discounts require plant-director approval."),
    ("POL-100", "Q3 supplier rebate memo", "2026-02-15", "CHI",
     "leadership",
     "Confidential rebate terms: CobaltWorks receives a 4 percent quarterly "
     "rebate from Apex Fasteners contingent on volume targets. Do not "
     "distribute outside leadership."),
]

rows = []
for doc_id, section, effective, plant, group, content in documents:
    chunk_id = f"{doc_id}-{section.replace(' ', '_')}-{group}"
    rows.append((chunk_id, content, doc_id, section,
                 effective, plant, group))

columns = ["chunk_id", "content", "document_id", "section_title",
           "effective_date", "plant_id", "access_group"]

df = spark.createDataFrame(rows, columns)
df.write.mode("overwrite").saveAsTable(table)

# Change Data Feed is required for a Delta Sync index source
spark.sql(
    f"ALTER TABLE {table} "
    "SET TBLPROPERTIES (delta.enableChangeDataFeed = true)"
)

print(f"Wrote {len(rows)} chunks to {table}")
display(spark.table(table))
