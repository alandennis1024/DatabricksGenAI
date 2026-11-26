from pyspark.sql.functions import col, current_timestamp, lit

# 1. Define the secure volume path in Unity Catalog
# Volumes provide a managed way to handle files without raw S3/ADLS IAM access
raw_images_path = "/Volumes/cobalt_prod/factory_bronze/raw_qa_images/"
checkpoint_path = "/Volumes/cobalt_prod/factory_bronze/_checkpoints/qa_images/"

# 2. Use Databricks Auto Loader to ingest binary files (images)
# Retain the file path and basic metadata for source attribution.
# Binary-file ingestion does NOT validate image contents or guarantee
# detection of malformed images. Validate and quarantine separately.
df_bronze_images = (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "binaryFile")
    .option("pathGlobFilter", "*.png")
    .load(raw_images_path)
    .withColumn("source_file_path", col("path"))
    .withColumn("ingestion_timestamp", current_timestamp())
    .withColumn("processing_status", lit("RAW"))
)

# 3. Store raw content and metadata in a governed Delta table.
# This table is not an AI Search index and may contain sensitive data.
(df_bronze_images.writeStream
    .format("delta")
    .option("checkpointLocation", checkpoint_path)
    .trigger(availableNow=True)
    .table("cobalt_prod.factory_bronze.qa_image_metadata")
)
