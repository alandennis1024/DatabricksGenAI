# Databricks notebook source

# MAGIC %md
# MAGIC # Chapter 13 — Vector Search Index Deployment
# MAGIC
# MAGIC This notebook demonstrates how to deploy Vector Search infrastructure
# MAGIC as part of the solution. We define a job in the DAB that creates or
# MAGIC updates the index, and use the CLI to check index status.
# MAGIC
# MAGIC **Use case:** Extend the data quality agent with a vector index that
# MAGIC stores rules from across the organization. The agent retrieves the
# MAGIC most relevant examples for the specific table it is analyzing.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. DAB Configuration — Vector Search Setup Job
# MAGIC
# MAGIC Vector Search endpoints are workspace-level resources shared across
# MAGIC multiple indexes.
# MAGIC
# MAGIC ```yaml
# MAGIC resources:
# MAGIC   jobs:
# MAGIC     setup_vector_index:
# MAGIC       name: "setup-dq-rules-index"
# MAGIC       tasks:
# MAGIC         - task_key: create_or_update_index
# MAGIC           notebook_task:
# MAGIC             notebook_path: ./notebooks/setup_vector_index.py
# MAGIC             base_parameters:
# MAGIC               catalog: "${var.catalog}"
# MAGIC               schema: "${var.schema}"
# MAGIC               source_table: "dq_rules_knowledge_base"
# MAGIC               index_name: "dq_rules_index"
# MAGIC               endpoint_name: "dq-vector-endpoint"
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Check Index Status
# MAGIC
# MAGIC Index creation and synchronization are asynchronous. The CI/CD pipeline
# MAGIC may need to poll for completion before deploying the agent that queries
# MAGIC the index.

# COMMAND ----------

CATALOG = "demo"
SCHEMA  = "finance"

# COMMAND ----------

# MAGIC %sh
# MAGIC databricks vector-search indexes get \
# MAGIC   "${CATALOG}.${SCHEMA}.dq_rules_index" \
# MAGIC   --endpoint-name dq-vector-endpoint

# COMMAND ----------

# MAGIC %md
# MAGIC ## Notes
# MAGIC
# MAGIC - **Delta Sync Index** automatically updates when the source table changes
# MAGIC   (ideal for production)
# MAGIC - **Direct Vector Access Index** requires manual updates (more control)
# MAGIC - Cross-environment deployment follows the same pattern as models: same
# MAGIC   index name in different catalogs
# MAGIC - Environment-specific embedding endpoints let you use a smaller model in
# MAGIC   dev and a higher-quality model in production
