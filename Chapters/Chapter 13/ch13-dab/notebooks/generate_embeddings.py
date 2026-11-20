# Databricks notebook source
# MAGIC %md
# MAGIC # Generate embeddings for the rules knowledge base
# MAGIC
# MAGIC Chunks raw rule documentation and computes embeddings with the
# MAGIC configured embedding endpoint, writing the knowledge base that feeds
# MAGIC the vector index.
# MAGIC
# MAGIC Note: a Delta Sync index with *managed* embeddings computes vectors
# MAGIC automatically from a source text column. This notebook covers the
# MAGIC case where you generate embeddings yourself — for example a Direct
# MAGIC Vector Access index, or a custom chunking strategy.

# COMMAND ----------

dbutils.widgets.text("catalog", "dev_catalog", "Catalog")
dbutils.widgets.text("schema", "finance", "Schema")
dbutils.widgets.text("embedding_endpoint", "databricks-gte-large-en",
                     "Embedding endpoint")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
embedding_endpoint = dbutils.widgets.get("embedding_endpoint")

raw_table = f"{catalog}.{schema}.dq_documents_gold"
output_table = f"{catalog}.{schema}.dq_rules_knowledge_base"

# COMMAND ----------

import uuid

from databricks.sdk import WorkspaceClient
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, FloatType, StringType, StructType, StructField

workspace = WorkspaceClient()

CHUNK_SIZE = 1000
CHUNK_OVERLAP = 150


def chunk_text(text):
    """Split text into overlapping chunks on a simple sliding window."""
    chunks = []
    start = 0
    while start < len(text):
        chunks.append(text[start:start + CHUNK_SIZE])
        start += CHUNK_SIZE - CHUNK_OVERLAP
    return [c for c in chunks if c.strip()]


def embed(texts):
    response = workspace.serving_endpoints.query(
        name=embedding_endpoint,
        input=texts,
    )
    return [list(map(float, e)) for e in response.embeddings]

# COMMAND ----------

documents = (
    spark.table(raw_table)
    .select("document_id", "section_title", "content")
    .collect()
)

rows = []
for doc in documents:
    for i, chunk in enumerate(chunk_text(doc["content"])):
        rows.append((str(uuid.uuid4()), doc["document_id"],
                     doc["section_title"], i, chunk))

chunks = spark.createDataFrame(
    rows,
    "rule_id string, document_id string, section_title string, "
    "chunk_index int, content string",
)

print(f"Generated {chunks.count()} chunks")
display(chunks.limit(10))

# COMMAND ----------

# Embed in batches and write the knowledge base table
batch = [r["content"] for r in chunks.limit(1000).collect()]
embeddings = embed(batch) if batch else []

embedded = (
    chunks.limit(len(embeddings))
    .rdd.zipWithIndex()
    .map(lambda t: tuple(list(t[0]) + [embeddings[t[1]]]))
    .toDF(chunks.columns + ["embedding"])
)

embedded.write.mode("overwrite").saveAsTable(output_table)
print(f"Knowledge base written to {output_table}")
