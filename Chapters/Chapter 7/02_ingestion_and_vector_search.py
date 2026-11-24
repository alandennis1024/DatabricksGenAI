# Databricks notebook source
# MAGIC %md
# MAGIC # 02 — Auto Loader PDF ingestion and AI Search Delta Sync
# MAGIC Requires the synthetic PDF generated in notebook 00 and permissions to
# MAGIC create a STANDARD AI Search endpoint and index. Endpoint creation incurs cost.

# COMMAND ----------
# MAGIC %pip install -q -r requirements.txt

# COMMAND ----------
dbutils.library.restartPython()

# COMMAND ----------
import hashlib
from datetime import timedelta
from pyspark.sql.functions import col,current_timestamp,udf
from pyspark.sql.types import StringType
from databricks.ai_search.client import AISearchClient
from cobalt_ch07 import notebook_settings, table, index_resource, extract_pdf_text

s=notebook_settings(dbutils)
parsed_table=table(s,"parsed_manuals")
chunks_table=table(s,"manual_chunks")
manual_dir=f"/Volumes/{s['catalog']}/{s['schema']}/ch07_manuals/"
checkpoint=f"/Volumes/{s['catalog']}/{s['schema']}/ch07_manuals/_checkpoint_pdf"
spark.sql(f"""CREATE TABLE IF NOT EXISTS {parsed_table}
(source_file STRING,parsed_text STRING,ingested_at TIMESTAMP) USING DELTA""")

# COMMAND ----------
def safe_pdf_extract(data):
    try: return extract_pdf_text(data)
    except Exception: return None  # Invalid PDF never enters indexed knowledge.

pdf_udf=udf(safe_pdf_extract,StringType())
stream=(spark.readStream.format("cloudFiles")
    .option("cloudFiles.format","binaryFile")
    .option("pathGlobFilter","*.pdf")
    .load(manual_dir)
    .select(col("_metadata.file_path").alias("source_file"),
            pdf_udf(col("content")).alias("parsed_text"),
            current_timestamp().alias("ingested_at")))
query=(stream.writeStream.format("delta")
    .option("checkpointLocation",checkpoint)
    .trigger(availableNow=True).toTable(parsed_table))
query.awaitTermination()
assert spark.table(parsed_table).filter(col("parsed_text").isNotNull()).limit(1).count()>0, "No text PDF was parsed; check 00 and workspace Auto Loader support"
print("[PASS] Auto Loader ingested the synthetic PDF")

# COMMAND ----------
# Build one miniature, auditable knowledge chunk from each synthetic PDF.
# Production ingestion would chunk and version each section individually.
rows=[]
for record in spark.table(parsed_table).where(col("parsed_text").isNotNull()).collect():
    if "XM-400-SYNTHETIC-MANUAL.pdf" not in record.source_file: continue
    content=record.parsed_text.strip()
    if not content: continue
    rows.append((hashlib.sha256((record.source_file+content).encode()).hexdigest()[:24],
                 "XM-400","DOC-CH07-001",content,record.source_file))
assert rows,"No synthetic PDF chunks. Re-run notebook 00."
spark.sql(f"""CREATE TABLE IF NOT EXISTS {chunks_table}
(chunk_id STRING,machine_family STRING,doc_reference STRING,chunk_text STRING,
source_file STRING) USING DELTA TBLPROPERTIES (delta.enableChangeDataFeed = true)""")
spark.createDataFrame(rows,["chunk_id","machine_family","doc_reference","chunk_text","source_file"]).createOrReplaceTempView("ch07_new_chunks")
spark.sql(f"""MERGE INTO {chunks_table} t USING ch07_new_chunks s
ON t.chunk_id=s.chunk_id WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *""")
print("[PASS] Synthetic source table ready with Change Data Feed")

# COMMAND ----------
client=AISearchClient()
if not client.endpoint_exists(s["search_endpoint"]):
    print("[INFO] Creating billable STANDARD AI Search endpoint",s["search_endpoint"])
    client.create_endpoint_and_wait(name=s["search_endpoint"],endpoint_type="STANDARD",verbose=True)
else:
    client.wait_for_endpoint(s["search_endpoint"],verbose=True)
index_name=index_resource(s)
if not client.index_exists(endpoint_name=s["search_endpoint"],index_name=index_name):
    index=client.create_delta_sync_index_and_wait(
        endpoint_name=s["search_endpoint"],index_name=index_name,
        source_table_name=chunks_table,primary_key="chunk_id",
        pipeline_type="TRIGGERED",embedding_source_column="chunk_text",
        embedding_model_endpoint_name=s["embedding_endpoint"],verbose=True,
        timeout=timedelta(minutes=30))
else:
    index=client.get_index(endpoint_name=s["search_endpoint"],index_name=index_name)
    index.sync()
    index.wait_until_ready(verbose=True,wait_for_updates=True,timeout=timedelta(minutes=30))
print("[PASS] AI Search index ready:",index_name)

# COMMAND ----------
from cobalt_ch07 import rows_to_documents
raw=index.similarity_search(query_text="XM-400 lockout tagout safety requirements",
    columns=["chunk_id","machine_family","doc_reference","chunk_text"],
    filters={"machine_family":"XM-400"},num_results=3)
docs=rows_to_documents(raw,"XM-400-01")
assert docs,"No indexed manual rows retrieved; check sync and embedding endpoint."
assert all(d["machine_family"]=="XM-400" for d in docs)
print("[PASS] Machine-scoped retrieval. Document refs:",[x["doc_reference"] for x in docs])
