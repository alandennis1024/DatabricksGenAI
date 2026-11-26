# Databricks notebook source
# MAGIC %md
# MAGIC # 99 — Guarded cleanup of synthetic Chapter 11 tables
# MAGIC **Default: no deletion.** Never drop the catalog or schema.
# MAGIC Media volume and optional external endpoints are **not** deleted here.

# COMMAND ----------
from cobalt_ch11 import qualified_schema

dbutils.widgets.text("catalog", "cobalt_ops")
dbutils.widgets.text("schema", "ch11")
dbutils.widgets.dropdown("confirm", "no", ["no", "yes"])
ns = qualified_schema(dbutils.widgets.get("catalog"),
                      dbutils.widgets.get("schema"))

names = [
    "ch11_incident_reviews", "ch11_public_manual_chunks",
    "ch11_audio_review", "ch11_image_quarantine", "ch11_image_review",
    "ch11_bronze_images", "ch11_source_manuals",
]
if dbutils.widgets.get("confirm") != "yes":
    print("[SKIP] No deletion. Set confirm=yes to drop only seven ch11 tables")
else:
    for name in names:
        spark.sql(f"DROP TABLE IF EXISTS {ns}.{name}")
        print(f"[PASS] Dropped dedicated table {ns}.{name} if present")
    print("[REVIEW] ch11_media volume and its checkpoints deliberately remain")
    print("[NOTE] MLflow experiments, UC policies, AI Search resources are NOT touched")
