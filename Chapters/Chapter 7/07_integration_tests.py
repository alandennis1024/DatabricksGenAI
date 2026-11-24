# Databricks notebook source
# MAGIC %md
# MAGIC # 07 — Integration acceptance tests
# MAGIC Distinguishes verified *platform connections* from *diagnostic quality*.
# MAGIC A safe rejection is not counted as a successful diagnostic response.

# COMMAND ----------
# MAGIC %pip install -q -r requirements.txt

# COMMAND ----------
dbutils.library.restartPython()

# COMMAND ----------
from cobalt_ch07 import notebook_settings, process_request, parse_diagnostic
from cobalt_runtime import check_clearance, retrieve_manuals, execute_live
s=notebook_settings(dbutils)
req={"operator_id":"TECH-104","machine_id":"XM-400-01",
     "query_text":"What do the fictional documents say about lockout tagout precautions?",
     "acoustic_anomaly_score":0.88,"thermal_risk":False}

# COMMAND ----------
assert check_clearance(spark,s,"TECH-104","XM-400-01")
assert not check_clearance(spark,s,"TECH-999","XM-400-01")
print("[PASS] Demo clearance lookup positive + negative")

manuals=retrieve_manuals(s,req["query_text"],req["machine_id"])
assert manuals and all(d["machine_family"]=="XM-400" for d in manuals)
print("[PASS] Real index responds with machine-family-filtered results")

# COMMAND ----------
if s["live_inference"].lower()!="yes":
    print("[SKIP] Real LLM evaluation disabled; platform retrieval only verified.")
else:
    denied=execute_live(spark,s,{**req,"operator_id":"TECH-999"})
    assert denied["status_code"]==403 and denied["success"] is False
    print("[PASS] Denied request stops at the demo clearance boundary")
    authorized=execute_live(spark,s,req)
    status=authorized["response"]["status"]
    assert status in ("DIAGNOSTIC_COMPLETE","INSUFFICIENT_CONTEXT","SAFETY_ABORT")
    if authorized["success"]:
        assert status=="DIAGNOSTIC_COMPLETE"
        assert authorized["response"]["severity"]=="CRITICAL"
        assert all(p["requires_loto"] for p in authorized["response"]["prescribed_protocols"])
        print("[PASS] Real model response passed structural and deterministic safety checks")
    else:
        print("[REVIEW] Model did not return an accepted complete diagnostic:",status)
        print("[INFO] Fail-closed path was used. This is NOT a successful diagnostic result.")
