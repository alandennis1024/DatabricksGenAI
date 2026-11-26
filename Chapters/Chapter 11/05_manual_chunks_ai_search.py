import pandas as pd
from langchain_text_splitters import RecursiveCharacterTextSplitter
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, StringType
from databricks.ai_search.client import AISearchClient

@F.pandas_udf(ArrayType(StringType()))
def chunk_text(text_series: pd.Series) -> pd.Series:
    splitter = RecursiveCharacterTextSplitter(
        chunk_size=1000, chunk_overlap=150
    )
    return pd.Series([
        splitter.split_text(text) if text else []
        for text in text_series
    ])

# Assume manual_id + manual_revision identifies an approved source.
df_chunked = (
    spark.table("cobalt_prod.factory_silver.maintenance_manuals")
    .withColumn("parts", chunk_text(F.col("full_text")))
    .select("manual_id", "manual_revision", "security_classification",
            F.posexplode("parts").alias("chunk_number", "text_chunk"))
    .withColumn("chunk_id", F.sha2(F.concat_ws(":",
                "manual_id", "manual_revision", "chunk_number"), 256))
)
source_table = "cobalt_prod.factory_gold.manual_chunks"
df_chunked.write.mode("overwrite").saveAsTable(source_table)
spark.sql(f"ALTER TABLE {source_table} SET TBLPROPERTIES "
          "('delta.enableChangeDataFeed' = 'true')")

# This creates an index on a pre-existing permitted search endpoint.
client = AISearchClient()
index = client.create_delta_sync_index(
    endpoint_name="cobalt-search-ch11",
    source_table_name=source_table,
    index_name="cobalt_prod.factory_gold.manuals_vector_index",
    primary_key="chunk_id",
    pipeline_type="TRIGGERED",
    embedding_source_column="text_chunk",
    embedding_model_endpoint_name="REPLACE_WITH_PERMITTED_ENDPOINT",
)
# Trigger index.sync() once the created index is ready.
# Apply independent document-authorization filters at query time.
