# Databricks notebook source
# MAGIC %md
# MAGIC # 06 - Unity Catalog HTTP connections (optional, guarded)
# MAGIC
# MAGIC **Book section:** *Integration Patterns in Practice > Direct Tool
# MAGIC Integration via REST APIs* (the NOTE on Unity Catalog connections) and
# MAGIC *Salesforce REST API Integration*.
# MAGIC
# MAGIC **What it does:** shows how to create the Unity Catalog HTTP connection
# MAGIC that `tool_mode = connection` uses, and how to grant `USE CONNECTION` to
# MAGIC the agent's identity. The tools call the connection through the workspace
# MAGIC proxy, so Databricks injects the credential and your code never sees the
# MAGIC token. The proxy also forwards your `Idempotency-Key` header.
# MAGIC
# MAGIC **Runs nothing by default.** Everything that changes your workspace is
# MAGIC behind the `run_example` widget (default `no`). With `no`, the notebook
# MAGIC only prints the SQL it would run.
# MAGIC
# MAGIC **Prerequisites to run it for real (`run_example = yes`):**
# MAGIC * you own a real REST system (CobaltWorks' SAP host in the book is
# MAGIC   fictional) and know its host, base path and a bearer token;
# MAGIC * the token is stored in a secret scope you can read;
# MAGIC * you have the `CREATE CONNECTION` privilege on the metastore;
# MAGIC * Standard or Dedicated access mode compute, or serverless.
# MAGIC
# MAGIC **Runtime:** serverless compute or DBR 17+. Seconds.
# MAGIC
# MAGIC **Creates (only with `run_example = yes`):** one UC HTTP connection,
# MAGIC named by the `connection_name` widget. `99_cleanup` can drop it.
# MAGIC
# MAGIC **Using the connection from the tools:** the tools expect the connection
# MAGIC names `sap_s4`, `salesforce` and `rpa_platform` (constants at the top of
# MAGIC `cobaltworks_tools.py`). Create each one the same way, then set the
# MAGIC `tool_mode` widget to `connection` in notebooks 01, 02 and 04.

# COMMAND ----------

# MAGIC %pip install -q -r requirements.txt

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

dbutils.widgets.dropdown("run_example", "no", ["no", "yes"], "Run the example")
dbutils.widgets.text("connection_name", "sap_s4", "Connection name")
dbutils.widgets.text("host", "https://sap.example.com", "Host (https://...)")
dbutils.widgets.text("base_path", "/odata/v4", "Base path")
dbutils.widgets.text("secret_scope", "cobalt-ops-secrets", "Secret scope")
dbutils.widgets.text("secret_key", "sap-api-token", "Secret key (bearer token)")
dbutils.widgets.text("grantee", "", "Principal to grant USE CONNECTION (optional)")
dbutils.widgets.text("smoke_test_path", "", "Proxy path to GET (optional)")

RUN = dbutils.widgets.get("run_example").strip() == "yes"
CONN = dbutils.widgets.get("connection_name").strip()
HOST = dbutils.widgets.get("host").strip()
BASE_PATH = dbutils.widgets.get("base_path").strip()
SCOPE = dbutils.widgets.get("secret_scope").strip()
KEY = dbutils.widgets.get("secret_key").strip()
GRANTEE = dbutils.widgets.get("grantee").strip()
SMOKE_PATH = dbutils.widgets.get("smoke_test_path").strip()

# COMMAND ----------

import os
import re
import sys


def _find_notebook_dir():
    candidates = [os.getcwd()]
    try:
        ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
        candidates.insert(0, os.path.dirname("/Workspace" + ctx.notebookPath().get()))
    except Exception:
        pass  # cwd is the notebook folder in Git folders
    for d in candidates:
        if os.path.exists(os.path.join(d, "cobaltworks_tools.py")):
            return d
    raise FileNotFoundError(
        "cobaltworks_tools.py not found. Run this notebook from the cloned "
        "'Chapter 9' Git folder so the helper module sits next to it.")


NB_DIR = _find_notebook_dir()
if NB_DIR not in sys.path:
    sys.path.insert(0, NB_DIR)

import cobaltworks_tools as cw

# The values are pasted into SQL, so only accept plain-looking values.
assert re.fullmatch(r"[A-Za-z0-9_]+", CONN), "connection_name: letters, digits, _"
assert re.fullmatch(r"https://[A-Za-z0-9.:-]+", HOST), "host must be https://hostname"
assert re.fullmatch(r"/[A-Za-z0-9._/-]*", BASE_PATH), "base_path must start with /"
assert re.fullmatch(r"[A-Za-z0-9_.-]+", SCOPE), "secret_scope has invalid characters"
assert re.fullmatch(r"[A-Za-z0-9_.-]+", KEY), "secret_key has invalid characters"
assert not GRANTEE or re.fullmatch(r"[A-Za-z0-9_.@ -]+", GRANTEE), "invalid grantee"

CREATE_SQL = f"""CREATE CONNECTION IF NOT EXISTS `{CONN}` TYPE HTTP OPTIONS (
  host '{HOST}',
  port '443',
  base_path '{BASE_PATH}',
  bearer_token secret('{SCOPE}', '{KEY}')
)"""
GRANT_SQL = (f"GRANT USE CONNECTION ON CONNECTION `{CONN}` "
             f"TO `{GRANTEE or 'procurement-agent-sp'}`")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. The SQL
# MAGIC
# MAGIC The bearer token comes from a secret, so it never appears in the notebook
# MAGIC or in the connection's visible options. For OAuth (recommended for
# MAGIC Salesforce, so tokens refresh automatically) use the
# MAGIC `client_id`, `client_secret`, `oauth_scope` and `token_endpoint` options
# MAGIC or the Catalog Explorer UI instead.

# COMMAND ----------

print(CREATE_SQL, end="\n\n")
print(GRANT_SQL)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Create and grant (only when `run_example = yes`)

# COMMAND ----------

if not RUN:
    print("run_example is 'no', so nothing was created. Set the widget to "
          "'yes' (and fill in host, base_path and the secret) to run the SQL "
          "above.")
else:
    try:
        dbutils.secrets.get(SCOPE, KEY)
    except Exception as exc:
        raise RuntimeError(
            f"Cannot read secret {SCOPE}/{KEY} ({str(exc)[:120]}). Create the "
            "scope and key first, for example with the Databricks CLI: "
            "`databricks secrets put-secret <scope> <key>`.") from exc
    try:
        spark.sql(CREATE_SQL)
        print(f"Connection {CONN} is ready.")
    except Exception as exc:
        raise RuntimeError(
            f"CREATE CONNECTION failed: {str(exc)[:300]}\n"
            "You need the CREATE CONNECTION privilege on the metastore.") from exc
    if GRANTEE:
        spark.sql(GRANT_SQL)
        print(f"Granted USE CONNECTION to {GRANTEE}.")
    else:
        print("grantee is empty: only you (the owner) can use the connection.")
    display(spark.sql(f"DESCRIBE CONNECTION `{CONN}`"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Optional smoke test through the proxy
# MAGIC
# MAGIC Set `smoke_test_path` to a GET path your system serves, for example
# MAGIC `/PurchaseOrderSet('PO-7723')`. The call goes to
# MAGIC `/api/2.0/unity-catalog/connections/<name>/proxy<path>`, which Databricks
# MAGIC forwards to `{host}{base_path}{path}` with the credential injected. This
# MAGIC is exactly what `cobaltworks_tools` does in connection mode.
# MAGIC
# MAGIC The older `http_request` SQL function still exists but is deprecated;
# MAGIC prefer the proxy.

# COMMAND ----------

if RUN and SMOKE_PATH:
    cw.set_tool_mode("connection")
    try:
        print(cw._connection_call(CONN, "GET", SMOKE_PATH))
    except Exception as exc:
        print(f"Smoke test failed: {str(exc)[:300]}\n"
              "Check the path, that the host is reachable from Databricks "
              "(private hosts need Private Link or an IP allowlist), and "
              "that you have USE CONNECTION.")
else:
    print("Smoke test skipped (needs run_example = yes and smoke_test_path).")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next
# MAGIC
# MAGIC Set `tool_mode = connection` in notebooks 01, 02 and 04 to use the real
# MAGIC system. The self-checks that assume the mock data are then informational.
