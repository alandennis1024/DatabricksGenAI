"""
PyFunc entry point for the Data Quality Agent.

This module is the file referenced by the MLmodel manifest.  MLflow loads it
when the model is served or called via mlflow.pyfunc.load_model.  The
DataQualityAgentModel class wires together the agent logic from the code/
directory and the artifacts/ directory into a single predict() interface.
"""

import json
import pandas as pd
import mlflow


class DataQualityAgentModel(mlflow.pyfunc.PythonModel):
    """MLflow PyFunc wrapper around the Data Quality Agent."""

    def load_context(self, context):
        """
        Called once when the model is loaded.  We read the artifacts
        (system prompt and example rules) and instantiate the agent.
        """
        from data_quality_agent.agent import DataQualityAgent

        with open(context.artifacts["system_prompt"], "r") as f:
            system_prompt = f.read()

        with open(context.artifacts["example_rules"], "r") as f:
            example_rules = json.load(f)

        self.agent = DataQualityAgent(
            system_prompt=system_prompt,
            example_rules=example_rules,
        )

    def predict(self, context, model_input: pd.DataFrame) -> pd.DataFrame:
        """
        Generate data quality rules for each table in the input DataFrame.

        Expected input columns:
            - table_name (str): the table to analyze
            - catalog (str):    the Unity Catalog catalog name
            - schema (str):     the Unity Catalog schema name

        Returns a DataFrame with a single column:
            - rules (str): a JSON array of rule objects
        """
        from data_quality_agent.utils import validate_rule_structure, format_rules_as_json

        results = []
        for _, row in model_input.iterrows():
            raw_rules = self.agent.run(
                table_name=row["table_name"],
                catalog=row["catalog"],
                schema=row["schema"],
            )
            valid_rules = validate_rule_structure(raw_rules)
            results.append(format_rules_as_json(valid_rules))

        return pd.DataFrame({"rules": results})
