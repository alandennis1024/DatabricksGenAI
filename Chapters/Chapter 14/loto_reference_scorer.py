"""Illustrative keyword signal; NEVER use as a safety control."""
from mlflow.genai.scorers import scorer

@scorer
def loto_reference_check(inputs: dict, outputs: dict) -> bool:
    question = str(inputs.get("question", "")).lower()
    if not any(w in question for w in ("electrical", "wiring", "panel")):
        return True
    response = str(outputs.get("response", "")).lower()
    return "loto" in response or "lockout-tagout" in response
