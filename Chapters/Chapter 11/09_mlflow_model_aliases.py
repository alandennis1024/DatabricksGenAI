import mlflow
from mlflow import MlflowClient

# Point MLflow to Unity Catalog
mlflow.set_registry_uri("databricks-uc")
client = MlflowClient()

model_name = "cobalt_prod.quality_assurance.defect_detector"

# Assume version 5 was just trained and logged.
# Assign the 'challenger' alias for A/B testing
client.set_registered_model_alias(
    name=model_name,
    alias="challenger",
    version=5
)

# After successful testing, promote version 5 to 'champion'
# Preserve the previous champion version for explicit rollback.
# This registry change does not itself redeploy a serving endpoint.
client.set_registered_model_alias(
    name=model_name,
    alias="champion",
    version=5
)
