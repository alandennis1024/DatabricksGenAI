# Databricks notebook source
# MAGIC %md
# MAGIC # 05 — Model governance and a fail-closed LOTO screening signal
# MAGIC This keyword rule is deliberately **not** an AI Gateway guardrail and
# MAGIC cannot authorize real equipment work. MLflow logging is opt-in.

# COMMAND ----------
from cobalt_ch11 import safety_screen_signal, qualified_schema

dbutils.widgets.text("catalog", "cobalt_ops")
dbutils.widgets.text("schema", "ch11")
dbutils.widgets.dropdown("log_mlflow", "no", ["no", "yes"])
qualified_schema(dbutils.widgets.get("catalog"), dbutils.widgets.get("schema"))

assert safety_screen_signal("Please bypass LOTO") == "BLOCK_SAFETY_VIOLATION"
assert safety_screen_signal("Follow approved procedure") == "REVIEW_REQUIRED"
print("[PASS] Explicit unsafe phrase blocked; other text stays manual-review only")

# COMMAND ----------
if dbutils.widgets.get("log_mlflow") == "yes":
    import mlflow
    import pandas as pd
    import cobalt_ch11

    class LotoScreen(mlflow.pyfunc.PythonModel):
        def predict(self, context, model_input: pd.DataFrame):
            return pd.Series([safety_screen_signal(str(x))
                              for x in model_input["prompt"]])

    with mlflow.start_run(run_name="ch11_governance_signal_demo"):
        info = mlflow.pyfunc.log_model(
            name="ch11_loto_review_signal", python_model=LotoScreen(),
            code_paths=[cobalt_ch11.__file__],
            input_example=pd.DataFrame({"prompt": ["Follow manual"]}),
        )
    loaded = mlflow.pyfunc.load_model(info.model_uri)
    result = loaded.predict(pd.DataFrame({"prompt": ["bypass LOTO"]}))
    assert result.iloc[0] == "BLOCK_SAFETY_VIOLATION"
    print("[PASS] MLflow PyFunc logged, loaded and returned observed signal")
else:
    print("[SKIP] MLflow log/reload: set log_mlflow=yes with a configured experiment")
print("[NOTE] Alias changes do not automatically update serving endpoints")
