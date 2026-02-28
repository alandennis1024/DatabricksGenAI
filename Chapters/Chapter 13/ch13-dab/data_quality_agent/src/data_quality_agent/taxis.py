"""
This module provides a function to load the NYC taxi sample dataset.
"""
from databricks.sdk.runtime import spark
from pyspark.sql import DataFrame


def find_all_taxis() -> DataFrame:
    """
    Reads the 'samples.nyctaxi.trips' table and returns it as a Spark DataFrame.

    Returns:
        A Spark DataFrame containing the NYC taxi trips data.
    """
    return spark.read.table("samples.nyctaxi.trips")
