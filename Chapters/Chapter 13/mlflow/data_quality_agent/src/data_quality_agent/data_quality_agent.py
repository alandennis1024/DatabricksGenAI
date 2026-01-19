"""
This module defines a DataQualityAgent that uses a Large Language Model (LLM)
to propose data quality rules for a Spark DataFrame.
"""

import mlflow
import pandas as pd
from pyspark.sql import DataFrame, SparkSession
from typing import Any, Dict, List
from mlflow.models import infer_signature
from mlflow.pyfunc import PythonModel
import yaml
import os


class DummyVectorSearchClient:
    """A dummy client that simulates Databricks Vector Search for historical rules."""

    def search_historical_rules(self, query: str) -> List[str]:
        """Simulates searching for historical data quality rules.

        Args:
            query: A query string derived from the current DataFrame's characteristics.

            Returns:
                A list of simulated historical data quality rules.
        """
        # In a real scenario, this would query a Vector Search endpoint
        # based on an embedding of the query.
        if "product_id" in query and "CW-X-999" in query:
            return [
                "product_id must match known SKUs in the 'products' table",
                "product_id format validation (e.g., 'CW-P-XXX', 'CW-T-XXX')",
            ]
        elif "quantity" in query and ("0" in query or "None" in query):
            return [
                "quantity must be a positive integer",
                "quantity cannot be zero or negative",
                "quantity cannot be null",
            ]
        elif "estimated_arrival_date" in query and "past" in query:
            return [
                "estimated_arrival_date must be in the future",
                "estimated_arrival_date must be in ISO 8601 format",
            ]
        return ["general data type consistency checks"]


class DataQualityAgent:
    """An agent that proposes data quality rules for a Spark DataFrame using an LLM.

    This class takes a Spark DataFrame, extracts its schema and a sample of data,
    and then uses a provided LLM client to generate data quality rules in YAML format.
    It can also leverage a Vector Search client to incorporate historical rules into the prompt.

    Attributes:
        llm_client: A client object for interacting with a Large Language Model.
                    The client must have a `generate_rules` method that accepts a
                    prompt string and returns a YAML string.
        vector_search_client: An optional client object for interacting with a
                              Vector Search service to retrieve historical rules.
    """

    def __init__(self, llm_client: Any, vector_search_client: Any = None):
        """Initializes the DataQualityAgent with an LLM client.

        Args:
            llm_client: A client for a Large Language Model.
            vector_search_client: An optional client for a Vector Search service.
        """
        self.llm_client = llm_client
        self.vector_search_client = vector_search_client or DummyVectorSearchClient()

    def propose_rules(self, df: DataFrame) -> str:
        """Proposes data quality rules for a given Spark DataFrame.

        This method analyzes the DataFrame's schema and data to generate a prompt
        for the LLM, which then returns a set of proposed data quality rules.
        It also logs the prompt, generated rules, and number of rules to MLflow.

        Args:
            df: The input Spark DataFrame to analyze.

        Returns:
            A string containing data quality rules in YAML format.
        """
        with mlflow.start_run(nested=True):  # Use nested=True for runs within a main run
            schema = dict(df.dtypes)
            columns = df.columns
            sample = [row.asDict() for row in df.limit(5).collect()]
            summary = df.summary().toPandas().to_dict(orient="records")

            # Retrieve historical rules from Vector Search
            vector_search_query = f"schema: {schema}, summary: {summary}, sample: {sample}"
            historical_rules = self.vector_search_client.search_historical_rules(
                vector_search_query
            )

            final_prompt = self._build_prompt(
                schema, columns, sample, summary, historical_rules
            )

            # Log the prompt template as an artifact
            prompt_file_path = "prompt_template.txt"
            with open(prompt_file_path, "w") as f:
                f.write(final_prompt)
            mlflow.log_artifact(prompt_file_path)
            os.remove(prompt_file_path)

            rules_yaml = self.llm_client.generate_rules(final_prompt)

            # Log generated rules as an artifact
            with open("rules.yaml", "w") as f:
                f.write(rules_yaml)
            mlflow.log_artifact("rules.yaml")

            # Count number of rules and log as a metric
            rules_data = yaml.safe_load(rules_yaml)
            num_rules = len(rules_data.get("rules", []))
            mlflow.log_metric("num_proposed_rules", num_rules)

            return rules_yaml

    def _build_prompt(
        self,
        schema: Dict[str, str],
        columns: List[str],
        sample: List[Dict[str, Any]],
        summary: List[Dict[str, Any]],
        historical_rules: List[str] = None,
    ) -> str:
        """Builds a prompt for the LLM based on the DataFrame's characteristics.

        Args:
            schema: A dictionary representing the DataFrame's schema (column_name: data_type).
            columns: A list of column names in the DataFrame.
            sample: A list of dictionaries, where each dictionary is a row of sample data.
            summary: A list of dictionaries containing summary statistics for the DataFrame.
            historical_rules: An optional list of historical data quality rules from Vector Search.

        Returns:
            A formatted string to be used as a prompt for the LLM.
        """
        prompt_str = (
            "You are a data quality agent for CobaltWorks, a manufacturing company.\n"
            "Your task is to propose a set of data quality rules in YAML format for a new dataset.\n"
            "The dataset is a delivery manifest from a supplier.\n\n"
            "Here is the information about the dataset:\n"
            f"Schema: {schema}\n\n"
            f"Columns: {columns}\n\n"
            f"Summary Statistics: {summary}\n\n"
            f"Sample Data: {sample}\n\n"
        )
        if historical_rules:
            prompt_str += (
                "Consider these historical data quality rules for similar datasets, "
                "which might be relevant:\n"
                f"{historical_rules}\n\n"
            )
        prompt_str += (
            "Based on this information, propose a set of data quality rules. "
            "Known CobaltWorks SKUs start with 'CW-'. 'quantity' should be a positive integer. "
            "'estimated_arrival_date' should be a valid ISO 8601 timestamp and not in the past."
        )
        return prompt_str


class DummyLLMClient:
    """A dummy LLM client that returns a fixed set of data quality rules.

    This class is a placeholder and should be replaced with a real LLM client
    for actual use cases.
    """

    def generate_rules(self, prompt: str) -> str:
        """Generates a hardcoded YAML string of data quality rules.

        Args:
            prompt: The input prompt string (ignored by this dummy implementation).

        Returns:
            A string containing a sample set of data quality rules in YAML format.
        """
        # Replace with actual LLM API call based on the rich prompt
        print("--- LLM Prompt ---")
        print(prompt)
        print("--------------------")
        return (
            "rules:\n"
            "  - column: product_id\n"
            "    rules:\n"
            "      - 'is not null'\n"
            "      - 'starts with CW-'\n"
            "  - column: quantity\n"
            "    rules:\n"
            "      - 'is not null'\n"
            "      - 'must be > 0'\n"
            "  - column: estimated_arrival_date\n"
            "    rules:\n"
            "      - 'is not null'\n"
            "      - 'is a valid ISO 8601 timestamp'\n"
            "      - 'is not in the past'\n"
        )


def log_model(
    agent: DataQualityAgent,
    sample_df: DataFrame,
    artifact_path: str = "data_quality_agent",
    conda_env: Dict = None,
    model_name: str = None,
) -> None:
    """Logs the DataQualityAgent as an MLflow pyfunc model.

    This function wraps the DataQualityAgent in an MLflow-compatible format
    and logs it to the MLflow Model Registry.

    Args:
        agent: An instance of the DataQualityAgent.
        sample_df: A sample Spark DataFrame to infer the model signature.
        artifact_path: The name of the artifact path to log the model to.
        conda_env: A dictionary representing the conda environment for the model.
        model_name: If provided, the model will be registered to the MLflow Model Registry
                    under this name.
    """
    signature = infer_signature(sample_df.toPandas(), agent.propose_rules(sample_df))
    mlflow.pyfunc.log_model(
        artifact_path=artifact_path,
        python_model=MLflowDataQualityAgent(agent),
        signature=signature,
        conda_env=conda_env,
        registered_model_name=model_name,
    )


class MLflowDataQualityAgent(PythonModel):
    """An MLflow pyfunc wrapper for the DataQualityAgent.

    This class allows the DataQualityAgent to be used with MLflow's standard
    `predict` interface, making it easy to deploy and serve.
    """

    def __init__(self, agent: DataQualityAgent = None, vector_search_client: Any = None):
        """Initializes the MLflow wrapper.

        Args:
            agent: An instance of the DataQualityAgent.
            vector_search_client: An optional client for a Vector Search service.
        """
        self.agent = agent
        self.spark: SparkSession = None
        self.vector_search_client = vector_search_client

    def load_context(self, context: Any) -> None:
        """Loads the model context, including the SparkSession.

        This method is called by MLflow when the model is loaded. It initializes
        the SparkSession, which is required to convert the pandas DataFrame input
        back to a Spark DataFrame.

        Args:
            context: The MLflow context for the model.
        """
        if self.agent is None:
            self.agent = DataQualityAgent(
                llm_client=DummyLLMClient(),
                vector_search_client=self.vector_search_client,
            )
        self.spark = SparkSession.builder.getOrCreate()

    def predict(self, context: Any, model_input: pd.DataFrame) -> str:
        """Generates data quality rules from the input DataFrame.

        This is the main prediction method called by MLflow. It receives a pandas
        DataFrame, converts it to a Spark DataFrame, and then invokes the
        DataQualityAgent to propose rules.

        Args:
            context: The MLflow context for the model.
            model_input: The input data, which MLflow provides as a pandas DataFrame.

        Returns:
            A string containing the proposed data quality rules in YAML format.
        """
        df = self.spark.createDataFrame(model_input)
        return self.agent.propose_rules(df)


# Example usage for Databricks Model Serving:
# from databricks.sdk.runtime import spark
#
# # Create a sample Spark DataFrame
# sample_df = spark.createDataFrame([
#     {"passenger_count": 1, "fare_amount": 10.5},
#     {"passenger_count": 2, "fare_amount": 20.0}
# ])
#
# # Initialize the agent and log it as an MLflow model
# agent = DataQualityAgent(DummyLLMClient())
# log_model(agent, sample_df)
#
# # The model can then be deployed from the MLflow UI in Databricks.
