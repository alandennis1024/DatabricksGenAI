from databricks.sdk.runtime import spark

def find_all_taxis():
    """
    Reads the 'samples.nyctaxi.trips' table and returns it as a Spark DataFrame.
    """
    return spark.read.table("samples.nyctaxi.trips")
