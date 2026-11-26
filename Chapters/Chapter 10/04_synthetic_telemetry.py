# Databricks notebook source
# MAGIC %md
# MAGIC # 04 — Pattern 2: synthetic telemetry and optional `ai_query`
# MAGIC
# MAGIC Chapter 10 describes synthetic motor-overheat records for evaluating
# MAGIC data quality and machine-learning pipelines. The default path creates
# MAGIC **deterministic, fictional test cases** that can be inspected and verified.
# MAGIC They are *not* a calibrated digital twin, actual sensor measurements,
# MAGIC a model trained on machinery, or a safety certification.
# MAGIC
# MAGIC To try one **real** model call, set `live_ai=yes` and choose a compatible,
# MAGIC permitted `ai_endpoint`. This can incur cost and returns *untrusted text*.

# COMMAND ----------
from datetime import date
from cobalt_ch10 import (
    name, notebook_settings, sql_literal, telemetry_cases,
    validate_telemetry_json,
)

s = notebook_settings(dbutils)
scenarios = telemetry_cases(100)
assert all(x["is_synthetic"] for x in scenarios)
assert all(x["motor_temps"][0] < x["motor_temps"][-1] for x in scenarios)
print(f"[PASS] Constructed {len(scenarios)} rising-temperature test fixtures")

# COMMAND ----------
from pyspark.sql import types as T

schema = T.StructType([
    T.StructField("scenario_id", T.StringType(), False),
    T.StructField("motor_temps", T.ArrayType(T.DoubleType()), False),
    T.StructField("status", T.StringType(), False),
    T.StructField("is_synthetic", T.BooleanType(), False),
])
rows = [
    (x["scenario_id"], x["motor_temps"], x["status"], x["is_synthetic"])
    for x in scenarios
]
telemetry_table = name(s, "ch10_synthetic_telemetry")
spark.createDataFrame(rows, schema).write.format("delta").mode("overwrite").saveAsTable(telemetry_table)
assert spark.table(telemetry_table).count() == 100
print(f"[PASS] Synthetic telemetry fixture saved: {telemetry_table}")

# COMMAND ----------
if s["live_ai"].lower() == "yes":
    # One inference call only. No AI output is approved or written to any table.
    prompt = (
        "Return ONLY one JSON object with exactly these keys: id (string), "
        "timestamp (ISO 8601 string), motor_temp (array of three strictly "
        "increasing numeric readings), status (FAILING or WARNING). "
        "Use entirely fictional robotic-arm measurements; no real procedures."
    )
    endpoint = sql_literal(s["ai_endpoint"])
    print("Calling permitted AI endpoint once; verify costs and permissions")
    raw = spark.sql(
        f"SELECT ai_query({endpoint}, {sql_literal(prompt)}) AS candidate"
    ).first()["candidate"]
    try:
        accepted = validate_telemetry_json(raw)
        print("[PASS] Optional generated JSON passed STRICT structure checks:", accepted)
        print("[REVIEW REQUIRED] Plausibility, realism, safety, and test value remain unverified")
    except Exception as exc:
        print(f"[FAIL] Optional generated response rejected: {exc}")
        raise
else:
    print("[SKIP] No model call. Set live_ai=yes only if you intend to use and pay for inference")
