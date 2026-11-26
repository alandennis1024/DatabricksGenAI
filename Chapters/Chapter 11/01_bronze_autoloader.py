# Databricks notebook source
# MAGIC %md
# MAGIC # 01 — Bronze governed image ingestion
# MAGIC Binary ingestion records files; it **does not validate or redact** their contents.

# COMMAND ----------
from cobalt_ch11 import qualified_schema
from pyspark.sql import functions as F

dbutils.widgets.text("catalog", "cobalt_ops")
dbutils.widgets.text("schema", "ch11")
catalog, schema = dbutils.widgets.get("catalog"), dbutils.widgets.get("schema")
ns = qualified_schema(catalog, schema)
root = f"/Volumes/{catalog}/{schema}/ch11_media"
table_name = f"{ns}.ch11_bronze_images"

# COMMAND ----------
raw = (spark.readStream.format("cloudFiles")
       .option("cloudFiles.format", "binaryFile")
       .option("cloudFiles.schemaLocation", f"{root}/_schema_images")
       .option("pathGlobFilter", "*.png")
       .load(f"{root}/image_in")
       .select(F.col("path").alias("source_file"),
               F.col("content"), F.col("length"),
               F.current_timestamp().alias("ingested_at")))
query = (raw.writeStream.format("delta")
         .option("checkpointLocation", f"{root}/_checkpoint_images")
         .trigger(availableNow=True)
         .toTable(table_name))
query.awaitTermination()
count = spark.table(table_name).count()
assert count >= 2, "Run 00 first; expected a valid and an invalid PNG"
print(f"[PASS] Read {count} raw image records into {table_name}")
print("[REVIEW] The broken .png is deliberately ingested for later quarantine")
