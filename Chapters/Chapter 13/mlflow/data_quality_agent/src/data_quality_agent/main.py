"""
This script is the main entry point for the data quality agent job.

It loads a sample CobaltWorks delivery manifest as a Spark DataFrame,
and then uses the DataQualityAgent to propose and print data quality rules.
"""
import argparse
import mlflow
from databricks.sdk.runtime import spark
from data_quality_agent.data_quality_agent import DataQualityAgent, DummyLLMClient, log_model # Import log_model
from data_quality_agent.cobaltworks_data import get_delivery_manifest_data


def main() -> None:
    """
    Main function to run the data quality agent job.
    """
    with mlflow.start_run(run_name="CobaltWorks_Data_Quality_Agent_Run"):
        # Process command-line arguments (currently none, but parser kept for --conf)
        parser = argparse.ArgumentParser(
            description="Databricks job for CobaltWorks data quality agent",
        )
        args = parser.parse_args()

        # Load data from the internal CobaltWorks data generator
        df = get_delivery_manifest_data()

        # Create and run the agent
        agent = DataQualityAgent(DummyLLMClient())
        rules = agent.propose_rules(df)

        # Print the rules
        print("--- Proposed Data Quality Rules ---")
        print(rules)
        print("------------------------------------")

        # Log the model to MLflow Model Registry
        log_model(
            agent=agent,
            sample_df=df,
            model_name="CobaltWorks_Data_Quality_Agent" # Register the model
        )


if __name__ == "__main__":
    main()
