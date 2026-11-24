# Databricks notebook source
# MAGIC %md
# MAGIC # 01 — Pydantic contracts and MLflow model dependency
# MAGIC Proves that a guarded PythonModel can be logged and loaded; it **does not**
# MAGIC simulate a successful turbine diagnosis. UC registration is opt-in.

# COMMAND ----------
# MAGIC %pip install -q -r requirements.txt

# COMMAND ----------
dbutils.library.restartPython()

# COMMAND ----------
import sys
from pathlib import Path
import pandas as pd
import mlflow
from mlflow.models import infer_signature
from cobalt_ch07 import notebook_settings, model_resource
from cobalt_model import CobaltGuardrailModel

s=notebook_settings(dbutils)
example=pd.DataFrame([{
    "operator_id":"TECH-104", "machine_id":"XM-400-01",
    "query_text":"Unexpected vibration around the bearing housing",
    "acoustic_anomaly_score":0.88,"thermal_risk":False,
}])
model=CobaltGuardrailModel()
expected=model.predict(None,example)
assert expected.loc[0,"status"] == "INSUFFICIENT_CONTEXT"
print("[PASS] Safety gate returns non-actionable fallback")

# COMMAND ----------
# Use the same notebook working directory as Chapter 6 Git-folder notebooks.
root=Path.cwd()
assert (root / "cobalt_ch07.py").exists(), f"Open this as a Git-folder notebook in {root}"
signature=infer_signature(example,expected)
mlflow.set_registry_uri("databricks-uc")
# To register, create UC model privileges and set register_model=yes.
registered = model_resource(s) if s["register_model"].lower()=="yes" else None
with mlflow.start_run(run_name="chapter7_guardrail_pyfunc"):
    info=mlflow.pyfunc.log_model(
        name="ch07_guardrail_model",
        python_model=model,
        code_paths=[str(root/"cobalt_ch07.py"),str(root/"cobalt_model.py")],
        signature=signature,input_example=example,
        pip_requirements=["pydantic>=2.10,<3", "pandas>=2.2,<3", "mlflow>=3.4,<4"],
        registered_model_name=registered,
    )
loaded=mlflow.pyfunc.load_model(info.model_uri)
actual=loaded.predict(example)
assert actual.loc[0,"status"]=="INSUFFICIENT_CONTEXT"
print("[PASS] Logged and reloaded MLflow model:",info.model_uri)
print("[INFO] Registered in UC:", registered or "No — set register_model=yes to opt in")
