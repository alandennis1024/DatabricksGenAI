# Databricks notebook source

# MAGIC %md
# MAGIC # Chapter 13 - Step 5: Databricks Model Serving
# MAGIC
# MAGIC This notebook corresponds to the **Databricks Model Serving** section
# MAGIC of Chapter 13. It demonstrates:
# MAGIC
# MAGIC 1. Creating a serving endpoint for the data quality agent
# MAGIC 2. Querying the endpoint via the Python SDK
# MAGIC 3. Checking endpoint status and configuration
# MAGIC 4. Enabling inference tables for request/response logging
# MAGIC
# MAGIC **Prerequisites:**
# MAGIC - Run **01_create_table_and_log_model.py** to create the model
# MAGIC - Run **02_register_and_version.py** to register the model in Unity Catalog

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

CATALOG = "demo"
SCHEMA  = "finance"
TABLE   = "sales_transactions"

MODEL_NAME = f"{CATALOG}.{SCHEMA}.data_quality_agent"
ENDPOINT_NAME = "data-quality-agent-endpoint"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create a serving endpoint
# MAGIC
# MAGIC We use the Databricks SDK to create a serverless serving endpoint.
# MAGIC The endpoint references our registered model in Unity Catalog.

# COMMAND ----------

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.serving import (
    EndpointCoreConfigInput,
    ServedEntityInput,
)
import time

w = WorkspaceClient()

# Check if endpoint already exists
existing = [e for e in w.serving_endpoints.list() if e.name == ENDPOINT_NAME]

if existing:
    print(f"Endpoint '{ENDPOINT_NAME}' already exists. Updating configuration...")
    w.serving_endpoints.update_config(
        name=ENDPOINT_NAME,
        served_entities=[
            ServedEntityInput(
                entity_name=MODEL_NAME,
                entity_version="1",
                workload_size="Small",
                scale_to_zero_enabled=True,
            )
        ],
    )
else:
    print(f"Creating endpoint '{ENDPOINT_NAME}'...")
    w.serving_endpoints.create(
        name=ENDPOINT_NAME,
        config=EndpointCoreConfigInput(
            served_entities=[
                ServedEntityInput(
                    entity_name=MODEL_NAME,
                    entity_version="1",
                    workload_size="Small",
                    scale_to_zero_enabled=True,
                )
            ]
        ),
    )

print("Endpoint creation/update initiated.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Wait for the endpoint to be ready
# MAGIC
# MAGIC Serving endpoints take a few minutes to provision. We poll the status
# MAGIC until it reaches the READY state.

# COMMAND ----------

def wait_for_endpoint(endpoint_name, timeout_minutes=15):
    """Poll until the endpoint is ready or timeout is reached."""
    start = time.time()
    timeout = timeout_minutes * 60

    while time.time() - start < timeout:
        endpoint = w.serving_endpoints.get(endpoint_name)
        state = endpoint.state

        if state.ready == "READY":
            print(f"Endpoint '{endpoint_name}' is READY")
            return endpoint
        elif state.config_update == "UPDATE_FAILED":
            raise RuntimeError(f"Endpoint update failed: {state}")
        else:
            elapsed = int(time.time() - start)
            print(f"  [{elapsed}s] Status: {state.ready} / {state.config_update}")
            time.sleep(30)

    raise TimeoutError(f"Endpoint not ready after {timeout_minutes} minutes")

endpoint = wait_for_endpoint(ENDPOINT_NAME)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Query the endpoint
# MAGIC
# MAGIC Once the endpoint is ready, we can send requests to it using the SDK.
# MAGIC The input format matches the signature we defined when logging the model.

# COMMAND ----------

import json

response = w.serving_endpoints.query(
    name=ENDPOINT_NAME,
    dataframe_records=[
        {
            "table_name": TABLE,
            "catalog": CATALOG,
            "schema": SCHEMA,
        }
    ],
)

# Parse the response
predictions = response.predictions
if isinstance(predictions, list) and len(predictions) > 0:
    rules_json = predictions[0].get("rules", "[]")
    rules = json.loads(rules_json)
    print(f"Endpoint returned {len(rules)} rules:\n")
    for r in rules:
        source = r.get("source", "?")
        print(f"  [{source:6s}]  {r['column']:20s}  ->  {r['rule']}")
else:
    print("Response:", predictions)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Inspect endpoint configuration
# MAGIC
# MAGIC We can review the endpoint's current configuration, including which model
# MAGIC version is being served, the workload size, and scaling settings.

# COMMAND ----------

endpoint = w.serving_endpoints.get(ENDPOINT_NAME)

print(f"Endpoint: {endpoint.name}")
print(f"State: {endpoint.state.ready}")

for entity in endpoint.config.served_entities:
    print(f"\nServed Entity:")
    print(f"  Model: {entity.entity_name}")
    print(f"  Version: {entity.entity_version}")
    print(f"  Workload Size: {entity.workload_size}")
    print(f"  Scale to Zero: {entity.scale_to_zero_enabled}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Enable inference tables (optional)
# MAGIC
# MAGIC Inference tables automatically log every request and response to a Delta
# MAGIC table. This provides a complete audit trail without any additional code.
# MAGIC
# MAGIC Note: Inference tables require the endpoint to be configured with an
# MAGIC auto_capture_config. This is typically set at creation time.

# COMMAND ----------

# To enable inference tables, the endpoint needs to be recreated or updated
# with auto_capture_config. Here is what the configuration looks like:

print("""
To enable inference tables, include this in your endpoint configuration:

  "auto_capture_config": {
      "catalog_name": "%s",
      "schema_name": "%s",
      "table_name_prefix": "data_quality_agent_inference"
  }

This will create a table at:
  %s.%s.data_quality_agent_inference_payload

Every request and response will be logged automatically.
""" % (CATALOG, SCHEMA, CATALOG, SCHEMA))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Query via REST API (alternative)
# MAGIC
# MAGIC In addition to the SDK, we can query the endpoint via its REST API.
# MAGIC This is how external applications would typically call the endpoint.

# COMMAND ----------

import requests

# Get the workspace URL and token
workspace_url = spark.conf.get("spark.databricks.workspaceUrl")
token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()

endpoint_url = f"https://{workspace_url}/serving-endpoints/{ENDPOINT_NAME}/invocations"

payload = {
    "dataframe_records": [
        {
            "table_name": TABLE,
            "catalog": CATALOG,
            "schema": SCHEMA,
        }
    ]
}

response = requests.post(
    endpoint_url,
    headers={
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    },
    json=payload,
)

print(f"Status: {response.status_code}")
if response.status_code == 200:
    result = response.json()
    print(f"Predictions: {json.dumps(result, indent=2)[:500]}...")
else:
    print(f"Error: {response.text}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Clean up (optional)
# MAGIC
# MAGIC Uncomment the line below to delete the endpoint when you are done.
# MAGIC Endpoints with scale-to-zero enabled will not incur costs when idle,
# MAGIC but deleting removes the resource entirely.

# COMMAND ----------

# w.serving_endpoints.delete(ENDPOINT_NAME)
# print(f"Deleted endpoint '{ENDPOINT_NAME}'")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC In this notebook we:
# MAGIC - Created a serverless serving endpoint for our data quality agent
# MAGIC - Queried it using both the Python SDK and REST API
# MAGIC - Inspected the endpoint configuration
# MAGIC - Discussed inference tables for automatic request/response logging
# MAGIC
# MAGIC This completes the core deployment workflow for Chapter 13:
# MAGIC 1. **01** - Create table and log model
# MAGIC 2. **02** - Register in Unity Catalog, set aliases
# MAGIC 3. **03** - Track experiments and enable tracing
# MAGIC 4. **04** - Version and compare prompts
# MAGIC 5. **05** - Deploy as a serving endpoint
