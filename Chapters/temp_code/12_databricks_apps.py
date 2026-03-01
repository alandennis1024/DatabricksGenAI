# Databricks notebook source

# MAGIC %md
# MAGIC # Chapter 13 — Databricks Apps Deployment and Verification
# MAGIC
# MAGIC This notebook demonstrates how to deploy a Databricks App as part of the
# MAGIC solution and verify that it is running. The app gives end users a way to
# MAGIC interact with the agent without needing access to notebooks or API endpoints.
# MAGIC
# MAGIC **Continues from:** `09_model_serving` (serving endpoint available)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. DAB Configuration — App Definition
# MAGIC
# MAGIC The app is included in the DAB alongside the serving endpoint, so both
# MAGIC deploy as a single unit.
# MAGIC
# MAGIC ```yaml
# MAGIC resources:
# MAGIC   apps:
# MAGIC     data_quality_ui:
# MAGIC       name: "data-quality-ui"
# MAGIC       description: "UI for generating data quality rules"
# MAGIC       source_code_path: ./apps/data_quality_ui
# MAGIC       config:
# MAGIC         command:
# MAGIC           - "streamlit"
# MAGIC           - "run"
# MAGIC           - "app.py"
# MAGIC         env:
# MAGIC           - name: "MODEL_ENDPOINT"
# MAGIC             value: "data-quality-agent-endpoint"
# MAGIC           - name: "CATALOG"
# MAGIC             value: "${var.catalog}"
# MAGIC           - name: "SCHEMA"
# MAGIC             value: "${var.schema}"
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Deploy the Full Solution
# MAGIC
# MAGIC This is the final step in the deployment pipeline. The DAB deploys the
# MAGIC serving endpoint and the app as a single unit.

# COMMAND ----------

# MAGIC %sh
# MAGIC # Deploy the full solution (endpoint + app) as a single unit
# MAGIC databricks bundle deploy --target production

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Verify the App is Running

# COMMAND ----------

# MAGIC %sh
# MAGIC # Returns the app's status, URL, and configuration
# MAGIC databricks apps get data-quality-ui

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Check Logs if Needed

# COMMAND ----------

# MAGIC %sh
# MAGIC # Shows container startup logs — useful for diagnosing missing
# MAGIC # dependencies or configuration errors
# MAGIC databricks apps get-logs data-quality-ui

# COMMAND ----------

# MAGIC %md
# MAGIC ## Notes
# MAGIC
# MAGIC - The app references the serving endpoint by **name**, not by model version
# MAGIC - When we promote a new model version or update a prompt alias, the app
# MAGIC   continues to work without modification
# MAGIC - The Databricks SDK handles authentication automatically inside a
# MAGIC   Databricks App — no credentials needed in code
# MAGIC - Secrets should be managed through Databricks secret scopes, not
# MAGIC   hardcoded in configuration
