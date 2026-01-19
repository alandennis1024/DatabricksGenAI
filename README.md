= Data Quality Agent Example: MLflow Packaging, Deployment, and Lifecycle

This example project demonstrates how to build, package, and deploy a model-hosted agent for data quality rule generation using MLflow and Databricks. The agent analyzes dataframe schemas and statistics, then leverages a large language model (LLM) for advanced reasoning. The project covers the full MLflow lifecycle, including tracking, projects, prompt registry, deployment pipelines, and integration with Databricks Model Serving, vector search indexes, and DB Apps.

== Project Structure

[source]
/data_quality_agent_model/
├── MLmodel
├── requirements.txt
├── code/
│ └── data_quality_agent/
│ ├── init.py
│ ├── agent.py
│ └── utils.py
├── artifacts/
│ ├── system_prompt.txt
│ └── example_rules.json
└── python_model.py
== MLflow Packaging

The agent is packaged using MLflow’s Python model format.
All dependencies are listed in requirements.txt.
Custom logic is organized under code/data_quality_agent/.
Artifacts such as prompt templates and example rules are stored in the artifacts/ directory.
== MLflow Tracking

Training runs, parameters, metrics, and artifacts are logged using MLflow Tracking.
Example:
[source,python]
import mlflow

with mlflow.start_run():
mlflow.log_param("llm_model", "gpt-4")
mlflow.log_metric("rule_coverage", 0.92)
mlflow.log_artifact("artifacts/example_rules.json")

== MLflow Projects

The project can be run as an MLflow Project for reproducibility.
Example MLproject file:
[source,yaml]
name: data-quality-agent
conda_env: requirements.txt
entry_points:
main:
parameters:
data_path: {type: str}
command: "python code/data_quality_agent/agent.py --data_path {data_path}"

== MLflow Model Registry

Models are registered, versioned, and promoted through the MLflow Model Registry.
Use stage transitions (Staging, Production, Archived) for lifecycle management.
== Prompt Registry

Prompt templates are versioned and tracked as MLflow artifacts.
Example:
[source,python]
mlflow.log_artifact("artifacts/system_prompt.txt")
== Deployment Pipelines

CI/CD pipelines (GitHub Actions, Azure DevOps) automate packaging, validation, and deployment.
Example workflow:
Checkout code from Git.
Build and validate MLflow model.
Deploy to Databricks Model Serving using Databricks CLI or Asset Bundles (DABs).
== Databricks Model Serving

The packaged agent is deployed as a REST endpoint using Databricks Model Serving.
Supports scalable, secure inference for production workloads.
== Vector Search Indexes & DB Apps

Integrate with Databricks Vector Search for semantic retrieval.
Expose agentic logic via DB Apps for interactive user experiences.
== Getting Started

Clone the repository.
Install dependencies: pip install -r requirements.txt
Run training and log results: python code/data_quality_agent/agent.py --data_path <your_data.csv>
Package and register the model with MLflow.
Deploy using Databricks CLI or Asset Bundles.
== References

https://mlflow.org/docs/latest/index.html
https://docs.databricks.com/en/mlflow/index.html
https://docs.databricks.com/en/machine-learning/model-serving/index.html
https://docs.databricks.com/en/generative-ai/vector-search/index.html
https://docs.databricks.com/en/db-apps/index.html
