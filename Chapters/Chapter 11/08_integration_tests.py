# Databricks notebook source
# MAGIC %md
# MAGIC # 08 — Chapter 11 workspace smoke and governance boundary tests
# MAGIC These tests exercise the synthetic Delta tables created by 00–07.
# MAGIC They **do not** certify actual Unity Catalog policies, search or gateways.

# COMMAND ----------
from cobalt_ch11 import (MANUALS, authorized_manuals,
                         qualified_schema, safety_screen_signal)

dbutils.widgets.text("catalog", "cobalt_ops")
dbutils.widgets.text("schema", "ch11")
ns = qualified_schema(dbutils.widgets.get("catalog"),
                      dbutils.widgets.get("schema"))

expected = [
    "ch11_source_manuals", "ch11_bronze_images", "ch11_image_review",
    "ch11_image_quarantine", "ch11_audio_review",
    "ch11_public_manual_chunks", "ch11_incident_reviews",
]
for name in expected:
    assert spark.catalog.tableExists(f"{ns}.{name}"), f"Missing {name}: run prior notebooks"
print(f"[PASS] {len(expected)} synthetic lab tables exist")

# COMMAND ----------
review = spark.table(f"{ns}.ch11_image_review")
assert review.where("review_status = 'QUARANTINE'").count() == 1
assert review.where("review_status = 'REVIEW_REQUIRED'").count() >= 1
print("[PASS] Both quarantine and not-yet-approved image categories present")

audio = spark.table(f"{ns}.ch11_audio_review")
assert audio.where("review_status = 'QUARANTINE'").count() == 1
print("[PASS] Corrupt WAV quarantined")

chunks = spark.table(f"{ns}.ch11_public_manual_chunks")
assert chunks.where("approval_status <> 'APPROVED'").count() == 0
assert chunks.where("classification <> 'PUBLIC'").count() == 0
assert chunks.where("manual_id = 'XM400-RETIRED'").count() == 0
print("[PASS] No retired or restricted material in public chunk table")

assert not authorized_manuals(MANUALS,verified_identity=False,
     allowed_classifications={"PUBLIC"},family="XM-400")
assert safety_screen_signal("bypass the interlock") == "BLOCK_SAFETY_VIOLATION"
assert spark.table(f"{ns}.ch11_incident_reviews").count() >= 1
print("[PASS] Offline identity and incident invariants")
print("[SKIP] Real multi-principal UC policy, live AI Search, Gateway, audit linkage")
