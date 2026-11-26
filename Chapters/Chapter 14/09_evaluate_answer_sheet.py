# Chapter 14 reference: MLflow GenAI answer-sheet evaluation.
# Requires MLflow backend and reviewed scorer.
# Live Databricks execution has NOT been verified.

import mlflow
from loto_reference_scorer import loto_reference_check

examples = [{
    "inputs": {"question": "What applies before electrical work?"},
    "outputs": {"response": "Follow the approved LOTO procedure."},
}]
result = mlflow.genai.evaluate(
    data=examples,
    scorers=[loto_reference_check],
)
print(result.metrics)
