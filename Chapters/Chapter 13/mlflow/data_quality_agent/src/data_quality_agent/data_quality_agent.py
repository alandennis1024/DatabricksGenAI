import mlflow
import pandas as pd
from typing import Any, Dict
from mlflow.models import infer_signature

class DataQualityAgent:
    def __init__(self, llm_client):
        self.llm_client = llm_client

    def propose_rules(self, df: pd.DataFrame) -> str:
        schema = df.dtypes.apply(lambda x: x.name).to_dict()
        columns = list(df.columns)
        sample = df.head(10).to_dict(orient="records")
        prompt = self._build_prompt(schema, columns, sample)
        rules_yaml = self.llm_client.generate_rules(prompt)
        return rules_yaml

    def _build_prompt(self, schema: Dict[str, str], columns: list, sample: list) -> str:
        return (
            f"Given the following dataframe schema, columns, and sample data, propose data quality rules in YAML format.\n"
            f"Schema: {schema}\nColumns: {columns}\nSample: {sample}\n"
        )

class DummyLLMClient:
    def generate_rules(self, prompt: str) -> str:
        # Replace with actual LLM API call
        return "rules:\n  - column: passenger_count\n    rule: must be >= 1\n  - column: fare_amount\n    rule: must be > 0\n"

def log_model(agent: DataQualityAgent, sample_df: pd.DataFrame, artifact_path: str = "data_quality_agent"):
    signature = infer_signature(sample_df, agent.propose_rules(sample_df))
    mlflow.pyfunc.log_model(
        artifact_path=artifact_path,
        python_model=MLflowDataQualityAgent(agent),
        signature=signature
    )

class MLflowDataQualityAgent(mlflow.pyfunc.PythonModel):
    def __init__(self, agent=None):
        self.agent = agent

    def load_context(self, context):
        if self.agent is None:
            self.agent = load_model(None)

    def predict(self, context, model_input: Any) -> str:
        df = pd.DataFrame(model_input)
        return self.agent.propose_rules(df)

# Databricks Model Serving Example
# To deploy, use mlflow.pyfunc.log_model and Databricks Model Serving UI or API
# Example usage:
# import pandas as pd
# sample_df = pd.DataFrame({"passenger_count": [1,2], "fare_amount": [10.5, 20.0]})
# agent = DataQualityAgent(DummyLLMClient())
# log_model(agent, sample_df)
# Then deploy the model using Databricks Model Serving
