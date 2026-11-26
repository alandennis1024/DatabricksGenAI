import mlflow
import pandas as pd

# Define a custom MLflow PyFunc model that acts as our safety guardrail
class SafetyGuardrailModel(mlflow.pyfunc.PythonModel):
    def predict(self, context, model_input: pd.DataFrame):
        results = []
        for text in model_input["prompt"]:
            text_lower = str(text).lower()
            # Domain-specific adversarial check for manufacturing safety
            # This keyword demo is not a robust NLP safety classifier.
            if "bypass" in text_lower and "loto" in text_lower:
                results.append("BLOCK_SAFETY_VIOLATION")
            else:
                results.append("PASS")
        return pd.Series(results)

# Register a demonstration MLflow model, NOT a Gateway policy.
# Set the correct UC registry and permissions before running.
mlflow.set_registry_uri("databricks-uc")
with mlflow.start_run():
    mlflow.pyfunc.log_model(
        name="guardrail_model",
        python_model=SafetyGuardrailModel(),
        registered_model_name="cobalt_prod.safety.loto_guardrail"
    )
