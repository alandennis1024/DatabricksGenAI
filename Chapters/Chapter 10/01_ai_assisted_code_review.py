# Databricks notebook source
# MAGIC %md
# MAGIC # 01 — AI-assisted code generation: review before execution
# MAGIC
# MAGIC This notebook corresponds to **Pattern 1**. The code examples were drafted
# MAGIC as if with Databricks Assistant / Genie Code, then manually reviewed.
# MAGIC The notebook **does not claim to call an Assistant API**.
# MAGIC
# MAGIC Use the prompts in `prompts/assistant_prompts.md` interactively in the
# MAGIC Databricks editor. Never accept hallucinated source columns or an inferred
# MAGIC timestamp format without checking the real input files.

# COMMAND ----------
from datetime import date
from cobalt_ch10 import (
    APEX_COLUMNS, SUPPLIER_COLUMNS, apex_sample_rows, monday_of_week,
    notebook_settings, normalize_apex, normalize_supplier,
    supplier_sample_rows, sql_identifier,
)

s = notebook_settings(dbutils)
week = monday_of_week(date.today())
print("Supplier CSV fields:", SUPPLIER_COLUMNS)
print("Apex CSV fields:", APEX_COLUMNS)
print("Required Apex timestamp pattern: yyyy-dd-MM HH:mm")
print("Supplier timestamp pattern: yyyy-MM-dd HH:mm:ss")

# COMMAND ----------
# Verify the reviewer's mappings against controlled source examples.
supplier_example = normalize_supplier(supplier_sample_rows(week)[0])
apex_example = normalize_apex(apex_sample_rows(week)[0])
assert supplier_example["transaction_id"] == "S-001"
assert supplier_example["quantity"] == 10
assert apex_example["transaction_id"] == "A-001"
assert apex_example["start_timestamp"].strftime("%Y-%m-%d %H:%M") == f"{week} 08:30"
assert apex_example["cargo_weight_kg"] == 5200
print("[PASS] Reviewed schema mappings and nonstandard Apex timestamp parsing")

# COMMAND ----------
# What to check in a code review before accepting an AI-suggested notebook:
review_gates = [
    "1. Confirm the actual input column names; do not invent columns.",
    "2. Check Apex TS_INIT is YYYY-DD-MM, not YYYY-MM-DD.",
    "3. Preserve nullable supplier Actual_Arrival; an open delivery is not late.",
    "4. Use a separate checkpoint for each Auto Loader source.",
    "5. Check Unity Catalog permissions and read only approved paths.",
    "6. Run 03 quality tests before publishing tables to business users.",
    "7. Review any AI-generated SQL and compare it with fixed benchmarks.",
]
for item in review_gates:
    print(item)
print("[PASS] Code-review checklist displayed; human approval is still required")
