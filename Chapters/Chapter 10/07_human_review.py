# Databricks notebook source
# MAGIC %md
# MAGIC # 07 — Human-in-the-loop review and traceable release evidence
# MAGIC
# MAGIC Corresponds to the chapter's human-in-the-loop section. Code and metadata
# MAGIC derived from a generative assistant need pull-request review, quality
# MAGIC verification, and business-owner sign-off before deployment.
# MAGIC
# MAGIC This notebook records **PENDING_REVIEW**, not a fabricated approval.
# MAGIC The publication workflow must separately capture human acceptance in
# MAGIC the actual GitHub PR and Databricks Genie Agent benchmark results.

# COMMAND ----------
from datetime import datetime, timezone
from cobalt_ch10 import name, notebook_settings
from pyspark.sql import types as T

s = notebook_settings(dbutils)
review_table = name(s, "ch10_review_evidence")
actor = spark.sql("SELECT current_user() AS username").first()["username"]

checklist = [
    ("code", "Reviewed actual input columns, timestamp formats and checkpoints"),
    ("quality", "Validated Bronze-to-Silver acceptance and quarantine records"),
    ("metrics", "Compared SQL metrics with independent expected sample answers"),
    ("genie", "Ran Genie UI benchmark queries and documented mismatches"),
    ("security", "Checked Unity Catalog data privileges and intended analyst audience"),
]

# COMMAND ----------
when = datetime.now(timezone.utc)
rows = [(when, actor, "PENDING_REVIEW", artifact, evidence)
        for artifact, evidence in checklist]
review_schema = T.StructType([
    T.StructField("recorded_at_utc", T.TimestampType(), False),
    T.StructField("recorded_by", T.StringType(), False),
    T.StructField("review_state", T.StringType(), False),
    T.StructField("artifact", T.StringType(), False),
    T.StructField("evidence_required", T.StringType(), False),
])
(spark.createDataFrame(rows, review_schema)
    .write.format("delta").mode("append").saveAsTable(review_table))

print(f"[PASS] Five PENDING_REVIEW items recorded in {review_table}")
print("[REVIEW REQUIRED] This is evidence of an open checklist, not sign-off")
for artifact, evidence in checklist:
    print(f"- {artifact}: {evidence}")
print("Do not deploy generated code or promote Genie without independent approval")
