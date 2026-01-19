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


class DataQualityAgent:
    """An agent that proposes data quality rules for a Spark DataFrame using an LLM.

    This class takes a Spark DataFrame, extracts its schema and a sample of data,
    and then uses a provided LLM client to generate data quality rules in YAML format.

    Attributes:
        llm_client: A client object for interacting with a Large Language Model.
                    The client must have a `generate_rules` method that accepts a
                    prompt string and returns a YAML string.
    """

    def __init__(self, llm_client: Any):
        """Initializes the DataQualityAgent with an LLM client.

        Args:
            llm_client: A client for a Large Language Model.
        """
        self.llm_client = llm_client

    def propose_rules(self, df: DataFrame) -> str:
        """Proposes data quality rules for a given Spark DataFrame.

        This method analyzes the DataFrame's schema and data to generate a prompt
        for the LLM, which then returns a set of proposed data quality rules.

        Args:
            df: The input Spark DataFrame to analyze.

        Returns:
            A string containing data quality rules in YAML format.
        """
        schema = dict(df.dtypes)
        columns = df.columns
        sample = [row.asDict() for row in df.limit(10).collect()]
        prompt = self._build_prompt(schema, columns, sample)
        rules_yaml = self.llm_client.generate_rules(prompt)
        return rules_yaml

    def _build_prompt(
        self, schema: Dict[str, str], columns: List[str], sample: List[Dict[str, Any]]
    ) -> str:
        """Builds a prompt for the LLM based on the DataFrame's characteristics.

        Args:
            schema: A dictionary representing the DataFrame's schema (column_name: data_type).
            columns: A list of column names in the DataFrame.
            sample: A list of dictionaries, where each dictionary is a row of sample data.

        Returns:
            A formatted string to be used as a prompt for the LLM.
        """
        return (
            "Given the following dataframe schema, columns, and sample data, "
            "propose data quality rules in YAML format.\n"
            f"Schema: {schema}\nColumns: {columns}\nSample: {sample}\n"
        )


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
        # Replace with actual LLM API call
        return (
            "rules:\n"
            "  - column: passenger_count\n"
            "    rule: must be >= 1\n"
            "  - column: fare_amount\n"
            "    rule: must be > 0\n"
        )


def log_model(
    agent: DataQualityAgent,
    sample_df: DataFrame,
    artifact_path: str = "data_quality_agent",
    conda_env: Dict = None,
) -> None:
    """Logs the DataQualityAgent as an MLflow pyfunc model.

    This function wraps the DataQualityAgent in an MLflow-compatible format
    and logs it to the MLflow Model Registry.

    Args:
        agent: An instance of the DataQualityAgent.
        sample_df: A sample Spark DataFrame to infer the model signature.
        artifact_path: The name of the artifact path to log the model to.
        conda_env: A dictionary representing the conda environment for the model.
    """
    signature = infer_signature(sample_df.toPandas(), agent.propose_rules(sample_df))
    mlflow.pyfunc.log_model(
        artifact_path=artifact_path,
        python_model=MLflowDataQualityAgent(agent),
        signature=signature,
        conda_env=conda_env,
    )


class MLflowDataQualityAgent(PythonModel):
    """An MLflow pyfunc wrapper for the DataQualityAgent.

    This class allows the DataQualityAgent to be used with MLflow's standard
    `predict` interface, making it easy to deploy and serve.
    """

    def __init__(self, agent: DataQualityAgent = None):
        """Initializes the MLflow wrapper.

        Args:
            agent: An instance of the DataQualityAgent.
        """
        self.agent = agent
        self.spark: SparkSession = None

    def load_context(self, context: Any) -> None:
        """Loads the model context, including the SparkSession.

        This method is called by MLflow when the model is loaded. It initializes
        the SparkSession, which is required to convert the pandas DataFrame input
        back to a Spark DataFrame.

        Args:
            context: The MLflow context for the model.
        """
        if self.agent is None:
            # In a real scenario, you might load a serialized agent here
            self.agent = DataQualityAgent(DummyLLMClient())
        self.spark = SparkSession.builder.getOrCreate()

    def predict(
        self, context: Any, model_input: pd.DataFrame
    ) -> str:
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
