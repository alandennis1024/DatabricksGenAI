# Databricks notebook source
# MAGIC %md
# MAGIC # 03 — Governed acoustic feature extraction
# MAGIC Default uses PCM16 RMS for a synthetic test tone. The companion also contains
# MAGIC an **optional MFCC / librosa** reference example, not a trained anomaly classifier.

# COMMAND ----------
from cobalt_ch11 import qualified_schema, wav_rms
from pathlib import Path

dbutils.widgets.text("catalog", "cobalt_ops")
dbutils.widgets.text("schema", "ch11")
ns = qualified_schema(dbutils.widgets.get("catalog"),
                      dbutils.widgets.get("schema"))
root = f"/Volumes/{dbutils.widgets.get('catalog')}/{dbutils.widgets.get('schema')}/ch11_media"

# COMMAND ----------
rows = []
for filename in ("acoustic_01.wav", "acoustic_corrupt.wav"):
    path = Path(root) / "audio_in" / filename
    with open(path, "rb") as handle:
        rms = wav_rms(handle.read())
    rows.append((str(path), rms, "REVIEW_REQUIRED" if rms is not None else "QUARANTINE"))

features_table = f"{ns}.ch11_audio_review"
(spark.createDataFrame(rows, "source_file STRING, rms DOUBLE, review_status STRING")
 .write.format("delta").mode("overwrite").saveAsTable(features_table))
assert sum(r[2] == "QUARANTINE" for r in rows) == 1
print("[PASS] Invalid audio quarantined; synthetic valid audio assigned a review signal")
print("[SKIP] MFCC training feature extraction: reference_examples/03_silver_acoustic_mfcc.py")
