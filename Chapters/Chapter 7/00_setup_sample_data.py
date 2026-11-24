# Databricks notebook source
# MAGIC %md
# MAGIC # 00 — CobaltWorks synthetic lab setup
# MAGIC Creates *fictional* turbine reference data and a sample, text-based PDF.
# MAGIC Does not create or modify Chapter 6 assets. No actual turbine safety claims.

# COMMAND ----------
# MAGIC %pip install -q -r requirements.txt

# COMMAND ----------
dbutils.library.restartPython()

# COMMAND ----------
from pathlib import Path
from reportlab.pdfgen import canvas
from reportlab.lib.pagesizes import letter
from cobalt_ch07 import notebook_settings, table

s = notebook_settings(dbutils)
print("Catalog/schema:",s['catalog'],s['schema'])
# Create the schema only; catalog creation is intentionally left to the user/admin.
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {s['catalog']}.{s['schema']}")

# COMMAND ----------
clearance_table = table(s,"operator_clearances")
telemetry_table = table(s,"sample_telemetry")
spark.sql(f"""CREATE TABLE IF NOT EXISTS {clearance_table}
(operator_id STRING, authorized_machine STRING) USING DELTA""")
spark.sql(f"""CREATE TABLE IF NOT EXISTS {telemetry_table}
(machine_id STRING, acoustic_anomaly_score DOUBLE, thermal_risk BOOLEAN,
 measured_at TIMESTAMP) USING DELTA""")
# Explicitly fictional identities and telemetry. Re-runs update the demo rows.
spark.sql(f"""MERGE INTO {clearance_table} t USING (
 SELECT 'TECH-104' AS operator_id, 'XM-400' AS authorized_machine
 UNION ALL SELECT 'TECH-210', 'XM-500'
) s ON t.operator_id=s.operator_id
WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *""")
spark.sql(f"""MERGE INTO {telemetry_table} t USING (
 SELECT 'XM-400-01' AS machine_id, CAST(0.88 AS DOUBLE) AS acoustic_anomaly_score,
 FALSE AS thermal_risk, current_timestamp() AS measured_at
) s ON t.machine_id=s.machine_id
WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *""")
print("[PASS] Fictional clearance and telemetry tables are present")

# COMMAND ----------
volume_name = f"{s['catalog']}.{s['schema']}.ch07_manuals"
spark.sql(f"CREATE VOLUME IF NOT EXISTS {volume_name}")
manual_dir = Path(f"/Volumes/{s['catalog']}/{s['schema']}/ch07_manuals")
manual_dir.mkdir(parents=True,exist_ok=True)
manual = manual_dir / "XM-400-SYNTHETIC-MANUAL.pdf"
if not manual.exists():
    pdf = canvas.Canvas(str(manual), pagesize=letter)
    lines=[
      "SYNTHETIC TRAINING DOCUMENT - NOT AN APPROVED MAINTENANCE MANUAL",
      "CobaltWorks XM-400 educational reference (DOC-CH07-001)",
      "Example policy: identify the asset and consult locally approved maintenance procedures.",
      "Electrical and rotating equipment work requires verified lockout-tagout procedures.",
      "A software assistant must not authorize maintenance or diagnose faults autonomously.",
      "Anomaly scores in this lab are invented and not calibrated to physical equipment.",
      "For high-risk observations, isolate the recommendation for human review.",
    ]
    y=750
    for line in lines:
        pdf.drawString(40,y,line[:108]); y-=25
    pdf.save()
print(f"[PASS] Synthetic PDF exists: {manual}")
print("Next: 01 package the safety gate; 02 ingest this PDF and build the index.")
