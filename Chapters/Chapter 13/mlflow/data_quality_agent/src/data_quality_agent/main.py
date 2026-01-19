import argparse
from databricks.sdk.runtime import spark
from data_quality_agent import taxis
from data_quality_agent.data_quality_agent import DataQualityAgent, DummyLLMClient


def main():
    # Process command-line arguments
    parser = argparse.ArgumentParser(
        description="Databricks job with catalog and schema parameters",
    )
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--schema", required=True)
    args = parser.parse_args()

    # Set the default catalog and schema
    spark.sql(f"USE CATALOG {args.catalog}")
    spark.sql(f"USE SCHEMA {args.schema}")

    # Load data
    df = taxis.find_all_taxis().toPandas()

    # Create and run the agent
    agent = DataQualityAgent(DummyLLMClient())
    rules = agent.propose_rules(df)

    # Print the rules
    print("--- Proposed Data Quality Rules ---")
    print(rules)
    print("------------------------------------")


if __name__ == "__main__":
    main()
