"""
This module provides a function to generate a sample Spark DataFrame
representing a CobaltWorks delivery manifest with data quality issues.
"""
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    BooleanType,
)

def get_delivery_manifest_data() -> DataFrame:
    """
    Creates a sample Spark DataFrame representing a delivery manifest.

    The DataFrame contains common data quality issues, such as null values,
    incorrect formats, and invalid entries.

    Returns:
        A Spark DataFrame with sample delivery manifest data.
    """
    spark = SparkSession.builder.getOrCreate()
    schema = StructType(
        [
            StructField("product_id", StringType(), True),
            StructField("quantity", IntegerType(), True),
            StructField("estimated_arrival_date", StringType(), True),
            StructField("supplier_id", StringType(), True),
            StructField("is_inspected", BooleanType(), True),
        ]
    )
    data = [
        ("CW-P-001", 10000, "2026-03-15T14:00:00Z", "SUP-001", True),
        ("CW-P-002", 5000, "2026-03-16", "SUP-001", True),
        ("CW-T-001", 250, "2026-03-17T18:30:00Z", "SUP-002", True),
        ("CW-P-001", 12000, "2025-01-10T10:00:00Z", "SUP-001", False), # Date in the past
        ("CW-X-999", 500, "2026-03-18T09:00:00Z", "SUP-003", False),   # Invalid product_id
        ("CW-P-002", 0, "2026-03-19T11:00:00Z", "SUP-001", True),      # Zero quantity
        ("CW-T-001", None, "2026-03-20T12:00:00Z", "SUP-002", False),  # Null quantity
        ("CW-P-001", 8000, "2026/03/21", "SUP-001", True),         # Malformed date
        ("CW-P-003", 20000, "2026-03-22T16:00:00Z", "SUP-004", False),
        (None, 1500, "2026-03-23T13:00:00Z", "SUP-003", False),       # Null product_id
    ]
    return spark.createDataFrame(data, schema)
