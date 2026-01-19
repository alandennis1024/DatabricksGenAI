"""
This script is the main entry point for the data quality agent job.

It parses command-line arguments for catalog, schema, and table,
loads the specified table as a Spark DataFrame, and then uses the
DataQualityAgent to propose and print data quality rules.
"""
import argparse
from databricks.sdk.runtime import spark
from data_quality_agent.data_quality_agent import DataQualityAgent, DummyLLMClient


def main() -> None:
    """
    Main function to run the data quality agent job.
    """
    # Process command-line arguments
    parser = argparse.ArgumentParser(
        description="Databricks job with catalog, schema, and table parameters",
    )
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--schema", required=True)
    parser.add_argument("--table", required=True)
    args = parser.parse_args()

    # Set the default catalog and schema
    spark.sql(f"USE CATALOG {args.catalog}")
    spark.sql(f"USE SCHEMA {args.schema}")

    # Load data from the specified table
    df = spark.sql(f"SELECT * FROM {args.catalog}.{args.schema}.{args.table}")

    # Create and run the agent
    agent = DataQualityAgent(DummyLLMClient())
    rules = agent.propose_rules(df)

    # Print the rules
    print("--- Proposed Data Quality Rules ---")
    print(rules)
    print("------------------------------------")


if __name__ == "__main__":
    main()
