# Databricks notebook source
# MAGIC %md
# MAGIC # 02 — Pattern 1: Auto Loader ingestion for both CSV feeds
# MAGIC
# MAGIC Reads files seeded by `00`. Creates **two Bronze Delta tables**, matching
# MAGIC the chapter's supplier-onboarding and Apex Haulers listings.
# MAGIC
# MAGIC Spark schemas are explicit; schema/checkpoint directories reside in the
# MAGIC same managed volume but **outside source folders**. `availableNow` processes
# MAGIC only currently discovered files and stops. A checkpoint preserves progress.
# MAGIC Source timestamps and weights are parsed, not silently guessed.

# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType
from cobalt_ch10 import name, notebook_settings, volume_path

s = notebook_settings(dbutils)
root = volume_path(s)
supplier_bronze = name(s, "supplier_shipments_bronze")
apex_bronze = name(s, "apex_shipping_logs")

supplier_schema = StructType([
    StructField(k, StringType(), True)
    for k in ("Delivery_TX", "Est_Arrival", "Actual_Arrival", "part_type", "quantity")
])
apex_schema = StructType([
    StructField(k, StringType(), True)
    for k in ("TX_ID", "TS_INIT", "UNIT_VAL", "LOC_CODE", "fuel_surcharge")
])

# COMMAND ----------
supplier_raw = (
    spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("header", "true")
    .option("cloudFiles.schemaLocation", f"{root}/_schema_supplier")
    .schema(supplier_schema)
    .load(f"{root}/supplier_in")
)
supplier_mapped = supplier_raw.select(
    F.trim(F.col("Delivery_TX")).alias("transaction_id"),
    F.try_to_timestamp("Est_Arrival", F.lit("yyyy-MM-dd HH:mm:ss")).alias("eta_timestamp"),
    F.try_to_timestamp("Actual_Arrival", F.lit("yyyy-MM-dd HH:mm:ss")).alias("actual_arrival_timestamp"),
    F.trim(F.col("part_type")).alias("part_type"),
    F.expr("try_cast(`quantity` AS INT)").alias("quantity"),
)
supplier_query = (
    supplier_mapped.writeStream
    .option("checkpointLocation", f"{root}/_checkpoint_supplier")
    .trigger(availableNow=True)
    .toTable(supplier_bronze)
)
supplier_query.awaitTermination()
print(f"[PASS] Supplier ingestion complete: {supplier_bronze} (rows={spark.table(supplier_bronze).count()})")

# COMMAND ----------
apex_raw = (
    spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("header", "true")
    .option("cloudFiles.schemaLocation", f"{root}/_schema_apex")
    .schema(apex_schema)
    .load(f"{root}/apex_in")
)
apex_mapped = apex_raw.select(
    F.trim(F.col("TX_ID")).alias("transaction_id"),
    # IMPORTANT: source format is YEAR-DAY-MONTH, unlike ISO yyyy-MM-dd.
    F.try_to_timestamp("TS_INIT", F.lit("yyyy-dd-MM HH:mm")).alias("start_timestamp"),
    F.expr("try_cast(`UNIT_VAL` AS DOUBLE)").alias("cargo_weight_kg"),
    F.trim(F.col("LOC_CODE")).alias("destination_code"),
    F.expr("try_cast(`fuel_surcharge` AS DOUBLE)").alias("fuel_surcharge"),
)
apex_query = (
    apex_mapped.writeStream
    .option("checkpointLocation", f"{root}/_checkpoint_apex")
    .trigger(availableNow=True)
    .toTable(apex_bronze)
)
apex_query.awaitTermination()
print(f"[PASS] Apex ingestion complete: {apex_bronze} (rows={spark.table(apex_bronze).count()})")
print("Next: 03 reviews the records and materializes checked Silver tables.")
