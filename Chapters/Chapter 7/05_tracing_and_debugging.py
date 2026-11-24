# Databricks notebook source
# MAGIC %md
# MAGIC # 05 — MLflow tracing of real SDK calls
# MAGIC No hardcoded call latencies, token counts or fictional successful traces.
# MAGIC Traces contain synthetic inputs; never log real sensitive plant information.

# COMMAND ----------
# MAGIC %pip install -q -r requirements.txt

# COMMAND ----------
dbutils.library.restartPython()

# COMMAND ----------
import json,mlflow
from cobalt_ch07 import notebook_settings, process_request
from cobalt_runtime import check_clearance, retrieve_manuals, generate_draft
s=notebook_settings(dbutils)
mlflow.set_experiment("/Shared/cobalt_ch07_tracing")

@mlflow.trace(name="ch07_clearance_lookup")
def clearance(operator_id,machine_id):
    return check_clearance(spark,s,operator_id,machine_id)

@mlflow.trace(name="ch07_ai_search")
def retrieve(query,machine_id):
    return retrieve_manuals(s,query,machine_id)

@mlflow.trace(name="ch07_chat_synthesis")
def synthesize(request,documents):
    return generate_draft(s,request,documents)

@mlflow.trace(name="ch07_compound_pipeline")
def run_traced(request):
    return process_request(request,clearance,retrieve,synthesize)

# COMMAND ----------
if s["live_inference"].lower()!="yes":
    print("[SKIP] No synthetic fake tracing was created. Set live_inference=yes to execute SDK calls.")
else:
    payload={"operator_id":"TECH-104","machine_id":"XM-400-01",
       "query_text":"Which synthetic manual requires lockout tagout and expert review?",
       "acoustic_anomaly_score":0.88,"thermal_risk":False}
    with mlflow.start_run(run_name="ch07_real_sdk_trace"):
        output=run_traced(payload)
    print("[RESULT]",json.dumps(output,indent=2))
    print("Open MLflow experiment '/Shared/cobalt_ch07_tracing' to inspect actual spans.")
    # A successful trace proves observability, not physical diagnostic accuracy.
