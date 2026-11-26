feature_table = "cobalt_prod.factory_gold.qa_image_features"

lineage_query = """
SELECT source_table_full_name, source_path, target_table_full_name,
       target_path, entity_type, event_time
FROM system.access.table_lineage
WHERE target_table_full_name = 'cobalt_prod.factory_gold.qa_image_features'
  AND event_time >= current_timestamp() - INTERVAL 90 DAYS
ORDER BY event_time DESC
"""

results = spark.sql(lineage_query)
# Govern audit output: choose an authorized, existing catalog/schema.
results.write.mode("overwrite").saveAsTable(
    "cobalt_prod.compliance.qa_feature_lineage_review"
)
