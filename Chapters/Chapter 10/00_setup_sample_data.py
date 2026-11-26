# Databricks notebook source
# MAGIC %md
# MAGIC # 00 — Set up the CobaltWorks supplier and Apex Haulers lab
# MAGIC
# MAGIC The company and all data are fictional. This notebook creates **only** the
# MAGIC configured chapter schema, a `ch10_demo_files` Unity Catalog volume, and
# MAGIC two small CSV files. It does **not** create production datasets or call AI.
# MAGIC
# MAGIC Prerequisites: a writable Unity Catalog catalog and permission to create
# MAGIC a schema and managed volume. Match widgets in subsequent notebooks.

# COMMAND ----------
from datetime import date
from cobalt_ch10 import (
    APEX_COLUMNS, SUPPLIER_COLUMNS, apex_sample_rows, monday_of_week,
    notebook_settings, render_csv, supplier_sample_rows, volume_path,
)

s = notebook_settings(dbutils)
week = monday_of_week(date.today())
source_root = volume_path(s)
print(f"Configured namespace: {s['catalog']}.{s['schema']}")
print(f"Synthetic reference week (Monday): {week}")

# COMMAND ----------
# Never create or drop the catalog automatically. Ask an administrator if needed.
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {s['catalog']}.{s['schema']}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {s['catalog']}.{s['schema']}.{s['volume']}")

supplier_csv = render_csv(supplier_sample_rows(week), SUPPLIER_COLUMNS)
apex_csv = render_csv(apex_sample_rows(week), APEX_COLUMNS)

supplier_input = f"{source_root}/supplier_in"
apex_input = f"{source_root}/apex_in"
dbutils.fs.mkdirs(supplier_input)
dbutils.fs.mkdirs(apex_input)

# Filenames are week-qualified; a repeat within the week reuses the same file.
# Auto Loader's existing checkpoint means re-running 02 does NOT append duplicates.
supplier_file = f"{supplier_input}/ch10_supplier_{week.isoformat()}.csv"
apex_file = f"{apex_input}/ch10_apex_{week.isoformat()}.csv"
dbutils.fs.put(supplier_file, supplier_csv, overwrite=True)
dbutils.fs.put(apex_file, apex_csv, overwrite=True)

print(f"[PASS] Seeded {len(supplier_sample_rows(week))} supplier rows -> {supplier_file}")
print(f"[PASS] Seeded {len(apex_sample_rows(week))} carrier rows -> {apex_file}")
print("Expected approved sample metrics: 3/5 completed steel deliveries on time (60%);")
print("four Chicago-hub shipments weigh strictly more than 5,000 kg.")
print("Next: 01 review generated mappings; 02 run the two Auto Loader streams.")
