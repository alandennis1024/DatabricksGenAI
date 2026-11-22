# Databricks notebook source
# MAGIC %md
# MAGIC # Chapter 6 - Sample data: CobaltWorks policy chunks
# MAGIC
# MAGIC **Book section:** "Preparing Data for Retrieval" (the Gold, or serving,
# MAGIC table `cobalt_ops.serving.policy_chunks`). This notebook is new; the
# MAGIC book only describes the table.
# MAGIC
# MAGIC **What it does:** creates the schema (and the catalog if you are allowed
# MAGIC to) and a Delta table of synthetic procurement-policy chunks with
# MAGIC **Change Data Feed enabled**, which a standard AI Search endpoint needs
# MAGIC to sync incrementally. One document, `POL-100`, is restricted to the
# MAGIC leadership group so the access-control filter and the two-user test in
# MAGIC `05_access_control_test` have something real to allow and deny.
# MAGIC
# MAGIC **Prerequisites:** a Unity Catalog workspace, and `USE CATALOG`,
# MAGIC `CREATE SCHEMA` and `CREATE TABLE` on a catalog you can write to. If you
# MAGIC cannot create catalogs, set the `catalog` widget to one you own.
# MAGIC
# MAGIC **Runtime:** under a minute on serverless compute.
# MAGIC
# MAGIC **Creates:** `<catalog>.<schema>.policy_chunks` (7 rows). Safe to re-run:
# MAGIC the rows are overwritten in place and the table keeps its Delta history.
# MAGIC
# MAGIC **Design note:** each chunk carries exactly one `access_group` value. A
# MAGIC document readable by several groups is stored as one row per group, with
# MAGIC the group suffixed to `chunk_id` to keep the primary key unique. The
# MAGIC chapter documents filters on scalar columns, not on array columns.
# MAGIC
# MAGIC **Group names:** the access groups are widgets. The defaults
# MAGIC (`leadership`, `procurement-chi`, `procurement-det`) do not have to exist
# MAGIC as workspace groups for notebooks `00` to `04` to run. They must exist
# MAGIC for the two-user test in `05`; reuse any existing groups by changing the
# MAGIC widgets (use the same values in `04` and `05`).

# COMMAND ----------

# MAGIC %pip install -q -r requirements.txt

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import re

dbutils.widgets.text("catalog", "cobalt_ops", "Catalog")
dbutils.widgets.text("schema", "serving", "Schema")
dbutils.widgets.text("leadership_group", "leadership", "Group for POL-100")
dbutils.widgets.text("chi_group", "procurement-chi", "Chicago procurement group")
dbutils.widgets.text("det_group", "procurement-det", "Detroit procurement group")

CATALOG = dbutils.widgets.get("catalog").strip()
SCHEMA = dbutils.widgets.get("schema").strip()
LEADERSHIP_GROUP = dbutils.widgets.get("leadership_group").strip()
CHI_GROUP = dbutils.widgets.get("chi_group").strip()
DET_GROUP = dbutils.widgets.get("det_group").strip()

for label, value in (("catalog", CATALOG), ("schema", SCHEMA)):
    if not re.fullmatch(r"[A-Za-z0-9_]+", value):
        raise ValueError(
            f"Widget '{label}' must contain only letters, digits and "
            f"underscores, got {value!r}."
        )
for label, value in (("leadership_group", LEADERSHIP_GROUP),
                     ("chi_group", CHI_GROUP), ("det_group", DET_GROUP)):
    if not value:
        raise ValueError(f"Widget '{label}' must not be empty.")

TABLE = f"{CATALOG}.{SCHEMA}.policy_chunks"
print(f"Target table: {TABLE}")

# COMMAND ----------

def ensure_catalog(catalog):
    """Use the catalog if it is visible to you; otherwise try to create it."""
    visible = {row[0] for row in spark.sql("SHOW CATALOGS").collect()}
    if catalog in visible:
        print(f"Catalog {catalog} already exists.")
        return
    try:
        spark.sql(f"CREATE CATALOG IF NOT EXISTS `{catalog}`")
    except Exception as err:
        raise RuntimeError(
            f"Could not create catalog '{catalog}': {err}\n"
            "You may not have CREATE CATALOG, or your metastore has no "
            "default storage location. Set the 'catalog' widget at the top "
            "of this notebook to a catalog you own (for example your "
            "workspace's default catalog) and run again."
        ) from err
    print(f"Created catalog {catalog}.")


def ensure_schema(catalog, schema):
    try:
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{schema}`")
    except Exception as err:
        raise RuntimeError(
            f"Could not create schema {catalog}.{schema}: {err}\n"
            f"You need USE CATALOG and CREATE SCHEMA on '{catalog}'. Ask "
            "an admin, or point the 'catalog' widget at a catalog you own."
        ) from err
    print(f"Schema {catalog}.{schema} is ready.")


ensure_catalog(CATALOG)
ensure_schema(CATALOG, SCHEMA)

# COMMAND ----------

# document_id, section_title, effective_date, plant_id, access_group, content
DOCUMENTS = [
    ("POL-001", "Discount approval limits", "2026-01-01", "CHI", CHI_GROUP,
     "Chicago procurement managers may approve supplier discounts up to 10 "
     "percent. Discounts above 10 percent and up to 15 percent require "
     "plant-director approval. Discounts above 15 percent require the vice "
     "president of procurement."),
    ("POL-001", "Platinum suppliers", "2026-01-01", "CHI", CHI_GROUP,
     "Platinum suppliers follow the same approval limits as other "
     "suppliers. A Platinum supplier tier does not raise the discount a "
     "Chicago procurement manager may approve."),
    ("POL-001", "Effective scope", "2026-01-01", "CHI", CHI_GROUP,
     "This policy applies to the Chicago plant only. It supersedes the 2025 "
     "discount policy. Archived versions remain available for audit."),
    ("POL-002", "Discount approval limits", "2026-01-01", "DET", DET_GROUP,
     "Detroit procurement managers may approve supplier discounts up to 8 "
     "percent. Higher discounts require plant-director approval."),
    ("POL-002", "Effective scope", "2026-01-01", "DET", DET_GROUP,
     "This policy applies to the Detroit plant only and supersedes the 2025 "
     "Detroit discount guidance."),
    ("POL-100", "Q3 supplier rebate memo", "2026-02-15", "CHI",
     LEADERSHIP_GROUP,
     "Confidential rebate terms: CobaltWorks receives a 4 percent quarterly "
     "rebate from Apex Fasteners contingent on volume targets. Do not "
     "distribute outside leadership."),
    ("POL-100", "Q3 rebate approval", "2026-02-15", "DET", LEADERSHIP_GROUP,
     "Confidential: rebate payouts to the Detroit plant above 25,000 dollars "
     "need chief financial officer approval. Do not distribute outside "
     "leadership."),
]

SCHEMA_DDL = (
    "chunk_id STRING, content STRING, document_id STRING, "
    "section_title STRING, effective_date STRING, plant_id STRING, "
    "access_group STRING"
)

rows = []
for number, (doc_id, section, effective, plant, group, content) in enumerate(
        DOCUMENTS, start=1):
    chunk_id = f"{doc_id}-{number:02d}-{group}"
    rows.append((chunk_id, content, doc_id, section, effective, plant, group))

# COMMAND ----------

# Create the table with Change Data Feed first, so the index source is valid
# from the first write and an existing table keeps its history on re-runs.
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {TABLE} ({SCHEMA_DDL})
    TBLPROPERTIES (delta.enableChangeDataFeed = true)
""")
spark.sql(
    f"ALTER TABLE {TABLE} "
    "SET TBLPROPERTIES (delta.enableChangeDataFeed = true)"
)

df = spark.createDataFrame(rows, SCHEMA_DDL)
df.write.mode("overwrite").insertInto(TABLE)

props = {r[0]: r[1] for r in spark.sql(f"SHOW TBLPROPERTIES {TABLE}").collect()}
cdf = props.get("delta.enableChangeDataFeed")
if str(cdf).lower() != "true":
    raise RuntimeError(f"Change Data Feed is not enabled on {TABLE}: {cdf}")

print(f"Wrote {len(rows)} chunks to {TABLE} (Change Data Feed: {cdf}).")

# COMMAND ----------

display(spark.table(TABLE))
