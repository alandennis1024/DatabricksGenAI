# Databricks notebook source
# MAGIC %md
# MAGIC # 00 — Set up synthetic CobaltWorks governance data
# MAGIC **This lab contains invented records, flat-color PNGs, and audio test tones.**
# MAGIC Never use these fixtures or their outputs as industrial procedures.

# COMMAND ----------
from cobalt_ch11 import (MANUALS, qualified_schema, png_synthetic_bytes,
                         synthetic_wav_bytes)
from pathlib import Path

dbutils.widgets.text("catalog", "cobalt_ops")
dbutils.widgets.text("schema", "ch11")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
namespace = qualified_schema(catalog, schema)

# The catalog must already exist and permit CREATE SCHEMA.
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {namespace}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {namespace}.ch11_media")
volume_root = f"/Volumes/{catalog}/{schema}/ch11_media"
for folder in ("image_in", "audio_in", "_checkpoint_images"):
    dbutils.fs.mkdirs(f"{volume_root}/{folder}")

# COMMAND ----------
# Python file writes to UC Volumes require compatible workspace support.
fixtures = {
    "image_in/qa_image_01.png": png_synthetic_bytes(),
    "image_in/qa_image_corrupt.png": b"broken\nnot a PNG",
    "audio_in/acoustic_01.wav": synthetic_wav_bytes(),
    "audio_in/acoustic_corrupt.wav": b"broken\nnot a WAV",
}
for relative_path, data in fixtures.items():
    path = Path(volume_root) / relative_path
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "wb") as handle:
        handle.write(data)

records = [vars(manual) for manual in MANUALS]
manuals_table = f"{namespace}.ch11_source_manuals"
(spark.createDataFrame(records).write.format("delta")
 .mode("overwrite").saveAsTable(manuals_table))
print(f"[PASS] Created {manuals_table} and {len(fixtures)} synthetic files")
print("[REVIEW] All manual text, including the retired unsafe draft, is synthetic")
