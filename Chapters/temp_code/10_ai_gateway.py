# Databricks notebook source

# MAGIC %md
# MAGIC # Chapter 13 — AI Gateway Configuration
# MAGIC
# MAGIC This notebook demonstrates how to configure AI Gateway on an existing
# MAGIC serving endpoint. AI Gateway adds rate limiting, usage tracking,
# MAGIC guardrails, and fallback routing without changing application code.
# MAGIC
# MAGIC **Continues from:** `09_model_serving` (endpoint already created)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Configure Rate Limiting and Usage Tracking
# MAGIC
# MAGIC We add AI Gateway configuration to our existing serving endpoint.
# MAGIC The endpoint name stays the same — consuming applications do not need
# MAGIC to change.

# COMMAND ----------

# MAGIC %sh
# MAGIC databricks serving-endpoints put-ai-gateway data-quality-agent-endpoint \
# MAGIC   --json '{
# MAGIC     "rate_limits": [
# MAGIC       {
# MAGIC         "calls": 100,
# MAGIC         "key": "endpoint",
# MAGIC         "renewal_period": "minute"
# MAGIC       }
# MAGIC     ],
# MAGIC     "usage_tracking_config": {
# MAGIC       "enabled": true
# MAGIC     }
# MAGIC   }'

# COMMAND ----------

# MAGIC %md
# MAGIC ## Notes
# MAGIC
# MAGIC - **Rate limits** prevent runaway costs and protect shared endpoints
# MAGIC - **Usage tracking** logs all traffic for monitoring and chargeback
# MAGIC - **Guardrails** (not shown) can block or flag inappropriate inputs
# MAGIC - **Fallback routing** (not shown) directs traffic to a backup endpoint
# MAGIC   if the primary one fails
# MAGIC - When deploying across environments, the gateway configuration changes
# MAGIC   per environment (different rate limits, different fallback endpoints),
# MAGIC   but the endpoint name stays the same
