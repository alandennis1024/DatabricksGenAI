# Databricks notebook source
# MAGIC %md
# MAGIC # 04 — Manual approval, document entitlements, and retrieval boundaries
# MAGIC Search-index access does **not** automatically inherit source-table row filters.
# MAGIC This exercise creates a PUBLIC-only sample chunk table. A real application
# MAGIC must derive the caller's entitlements from a **verified identity**, not a widget.

# COMMAND ----------
from cobalt_ch11 import (MANUALS, authorized_manuals, chunk_manual,
                         qualified_schema)

dbutils.widgets.text("catalog", "cobalt_ops")
dbutils.widgets.text("schema", "ch11")
ns = qualified_schema(dbutils.widgets.get("catalog"),
                      dbutils.widgets.get("schema"))

# COMMAND ----------
assert not authorized_manuals(MANUALS, verified_identity=False,
    allowed_classifications={"PUBLIC", "INTERNAL"}, family="XM-400")
# True here is a UNIT-TEST FIXTURE, not a claim that a real user authenticated.
public_only = authorized_manuals(MANUALS, verified_identity=True,
    allowed_classifications={"PUBLIC"}, family="XM-400")
assert {x.manual_id for x in public_only} == {"XM400-PUBLIC"}
assert "XM400-RETIRED" not in {x.manual_id for x in public_only}

chunks = [c for manual in public_only for c in chunk_manual(manual)]
chunk_table = f"{ns}.ch11_public_manual_chunks"
(spark.createDataFrame(chunks).write.format("delta")
 .option("delta.enableChangeDataFeed", "true")
 .mode("overwrite").saveAsTable(chunk_table))
assert spark.table(chunk_table).count() > 0
assert spark.table(chunk_table).where("classification <> 'PUBLIC'").count() == 0
print("[PASS] Published only approved PUBLIC synthetic chunks")
print("[SKIP] Live AI Search index provisioning requires separate privileges and cost review")
print("[NOTE] Index creation pattern: reference_examples/05_manual_chunks_ai_search.py")
print("[NOTE] Table row-filter SQL example: reference_examples/04_region_filter.sql")
