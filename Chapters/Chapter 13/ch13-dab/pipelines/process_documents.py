"""Document processing pipeline for the data quality knowledge base.

Lakeflow Spark Declarative Pipeline that processes raw rule documentation
through the medallion architecture: raw documents in bronze, cleaned and
parsed rules in silver, and structured, embedding-ready content in gold.

Catalog and schema come from the pipeline configuration in the DAB, so the
same pipeline definition runs in every environment.
"""

from pyspark import pipelines as dp
from pyspark.sql import functions as F

catalog = spark.conf.get("catalog")
schema = spark.conf.get("schema")


@dp.table(name="dq_documents_bronze", comment="Raw rule documentation")
def dq_documents_bronze():
    return spark.read.table(f"{catalog}.{schema}.dq_documents_raw")


@dp.table(name="dq_documents_silver", comment="Cleaned and parsed rules")
@dp.expect("content_present", "content IS NOT NULL")
@dp.expect("document_identified", "document_id IS NOT NULL")
def dq_documents_silver():
    return (
        spark.read.table(f"{catalog}.{schema}.dq_documents_bronze")
        .filter(F.col("content").isNotNull())
        .withColumn("content", F.trim(F.col("content")))
        .withColumn("effective_date", F.to_date("effective_date"))
        .dropDuplicates(["document_id", "section_title"])
    )


@dp.table(name="dq_documents_gold",
          comment="Structured, embedding-ready content")
@dp.expect("current_docs_only", "status = 'current'")
def dq_documents_gold():
    return (
        spark.read.table(f"{catalog}.{schema}.dq_documents_silver")
        .filter(F.col("status") == "current")
        .select("document_id", "section_title", "effective_date",
                "plant_id", "content")
    )
