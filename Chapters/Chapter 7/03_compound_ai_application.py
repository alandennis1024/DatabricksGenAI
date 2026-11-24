# Databricks notebook source
# MAGIC %md
# MAGIC # 03 — Compound AI application, real retrieval and deterministic safety checks
# MAGIC The acoustic score and thermal-risk flag are precomputed *synthetic inputs*.
# MAGIC No trained acoustic classifier or VLM is deployed by this chapter.
# MAGIC This is an educational draft assistant; qualified engineers approve all work.

# COMMAND ----------
# MAGIC %pip install -q -r requirements.txt

# COMMAND ----------
dbutils.library.restartPython()

# COMMAND ----------
import json
from cobalt_ch07 import notebook_settings, OperatorRequest
from cobalt_runtime import check_clearance, retrieve_manuals, execute_live

s=notebook_settings(dbutils)
dbutils.widgets.text("machine_id","XM-400-01")
dbutils.widgets.text("query_text","What does the synthetic manual say about lockout tagout for unexpected vibration?")
dbutils.widgets.text("acoustic_anomaly_score","0.88")
dbutils.widgets.dropdown("thermal_risk","no",["no","yes"])
request={"operator_id":s["operator_id"], "machine_id":dbutils.widgets.get("machine_id"),
         "query_text":dbutils.widgets.get("query_text"),
         "acoustic_anomaly_score":float(dbutils.widgets.get("acoustic_anomaly_score")),
         "thermal_risk":dbutils.widgets.get("thermal_risk")=="yes"}
request=OperatorRequest.model_validate(request).model_dump()

# COMMAND ----------
# IMPORTANT: clearance is a lookup against fictional staff data, not login
# authentication. A deployed application must bind user identity to SSO/SCIM.
clearance=check_clearance(spark,s,request["operator_id"],request["machine_id"])
print("[PASS] Demo clearance lookup" if clearance else "[DENY] No matching clearance")
assert clearance, "Operator is not cleared for this fictional machine family"
manuals=retrieve_manuals(s,request["query_text"],request["machine_id"])
assert manuals, "No approved teaching manual was retrieved"
print("[PASS] Retrieved synthetic manual references",[m["doc_reference"] for m in manuals])

# COMMAND ----------
if s["live_inference"].lower()!="yes":
    print("[SKIP] Live LLM call disabled. Set live_inference=yes to run real inference (billable).")
else:
    result=execute_live(spark,s,request)
    print(json.dumps(result,indent=2))
    if not result["success"]:
        print("[REVIEW] Generated output not accepted for automated use.")
    else:
        print("[PASS] Schema and deterministic invariants met; STILL REQUIRES human approval.")
