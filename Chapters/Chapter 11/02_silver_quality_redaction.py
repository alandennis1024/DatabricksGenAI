# Databricks notebook source
# MAGIC %md
# MAGIC # 02 — Silver review and image quarantine
# MAGIC Header/CRC validation **does not prove PII redaction**. Valid PNGs remain
# MAGIC **REVIEW_REQUIRED** until an independently tested detector and approval gate exist.
# MAGIC The tiny synthetic lab inspects records on the driver; do not collect large real images.

# COMMAND ----------
from cobalt_ch11 import png_integrity_gate, qualified_schema

dbutils.widgets.text("catalog", "cobalt_ops")
dbutils.widgets.text("schema", "ch11")
ns = qualified_schema(dbutils.widgets.get("catalog"),
                      dbutils.widgets.get("schema"))
source_table = f"{ns}.ch11_bronze_images"

# COMMAND ----------
rows = spark.table(source_table).select("source_file", "content", "length").collect()
review_records = []
for row in rows:
    verdict = png_integrity_gate(bytes(row.content) if row.content else None)
    review_records.append((str(row.source_file), int(row.length), verdict))

review_schema = "source_file STRING, file_size LONG, review_status STRING"
review_table = f"{ns}.ch11_image_review"
(spark.createDataFrame(review_records, review_schema).write
 .format("delta").mode("overwrite").saveAsTable(review_table))
quarantine = f"{ns}.ch11_image_quarantine"
(spark.table(review_table).where("review_status = 'QUARANTINE'").write
 .format("delta").mode("overwrite").saveAsTable(quarantine))
assert spark.table(quarantine).count() == 1
assert spark.table(review_table).where("review_status = 'REVIEW_REQUIRED'").count() >= 1
print("[PASS] Invalid PNG quarantined; parseable PNG still requires PII review")
print("[REVIEW] No images are certified 'PII-free' or published as approved Silver")
print("[NOTE] Full OpenCV best-effort example: reference_examples/02_silver_image_redaction.py")
