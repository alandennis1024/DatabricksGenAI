# Databricks notebook source
# MAGIC %md
# MAGIC # 03 - A local MCP server over stdio
# MAGIC
# MAGIC **Book section:** *Integration Patterns in Practice > The Model Context
# MAGIC Protocol (MCP)*.
# MAGIC
# MAGIC **What it does:** starts `cobaltworks_sap_mcp_server.py` (a FastMCP
# MAGIC server that exposes `check_order_status` and `apply_discount`) as a child
# MAGIC process, talks to it over stdio with the MCP Python client, loads its
# MAGIC tools into LangChain with `langchain-mcp-adapters`, and lets an agent
# MAGIC use them. The agent code is the same one you would point at a hosted
# MAGIC server.
# MAGIC
# MAGIC **Stdio is for demos only.** A deployed agent should not depend on a
# MAGIC process running next to it. Use a server hosted on Databricks Apps with
# MAGIC `DatabricksMCPServer`, shown at the end of this notebook.
# MAGIC
# MAGIC **Prerequisites:** a tool-calling chat endpoint for the agent step. The
# MAGIC protocol steps in section 2 need no model and no workspace features.
# MAGIC The server always runs in `mock` mode: the child process does not
# MAGIC inherit your notebook's workspace credentials, so it cannot reach real
# MAGIC Unity Catalog connections.
# MAGIC
# MAGIC **Runtime:** serverless compute or DBR 17+. Under a minute.
# MAGIC
# MAGIC **Creates:** nothing persistent. The child process exits when each
# MAGIC `async with` block ends.

# COMMAND ----------

# MAGIC %pip install -q -r requirements.txt

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

dbutils.widgets.text(
    "chat_endpoint", "databricks-claude-sonnet-4-5", "Chat endpoint")
CHAT_ENDPOINT_WIDGET = dbutils.widgets.get("chat_endpoint").strip()

# COMMAND ----------

import os
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

import asyncio
import json
import tempfile
import uuid
from concurrent.futures import ThreadPoolExecutor

import mlflow
import mlflow.langchain
from databricks.sdk import WorkspaceClient
from databricks_langchain import ChatDatabricks
from langchain.agents import create_agent
from langchain_mcp_adapters.tools import load_mcp_tools
from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client

import cobaltworks_tools as cw

SERVER_FILE = os.path.join(NB_DIR, "cobaltworks_sap_mcp_server.py")
assert os.path.exists(SERVER_FILE), f"missing {SERVER_FILE}"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Preflight: model endpoint
# MAGIC
# MAGIC Endpoint names change over time. This picks a working chat endpoint.

# COMMAND ----------

w = WorkspaceClient()
try:
    endpoints = list(w.serving_endpoints.list())
    print(f"{len(endpoints)} serving endpoints visible. Chat endpoints "
          "starting with databricks-:")
    for e in endpoints:
        if e.name.startswith("databricks-") and "chat" in str(e.task):
            print("  ", e.name)
    CHAT_ENDPOINT, msg = cw.resolve_chat_endpoint(endpoints, CHAT_ENDPOINT_WIDGET)
    print(msg)
    if CHAT_ENDPOINT is None:
        raise RuntimeError(msg)
except RuntimeError:
    raise
except Exception as exc:
    print(f"Could not list serving endpoints ({str(exc)[:120]}). "
          f"Trying {CHAT_ENDPOINT_WIDGET!r} as configured.")
    CHAT_ENDPOINT = CHAT_ENDPOINT_WIDGET

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. The server
# MAGIC
# MAGIC Two tools, each a thin wrapper over the same validated functions you
# MAGIC used in notebook 01. Only these two are advertised, which is Ring 2
# MAGIC (tool exposure control): the Salesforce and RPA tools do not exist as far
# MAGIC as an MCP client can tell.

# COMMAND ----------

with open(SERVER_FILE, encoding="utf-8") as f:
    print(f.read())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Running async code in a notebook
# MAGIC
# MAGIC MCP clients are async. Calling `asyncio.run` inside a notebook fails when
# MAGIC the kernel already runs an event loop, so `run_sync` runs the coroutine in
# MAGIC a helper thread that has its own loop when needed. No `nest_asyncio`.
# MAGIC
# MAGIC The child process's stderr is captured in a temporary file, because a
# MAGIC notebook's `sys.stderr` has no file descriptor to hand to a subprocess.

# COMMAND ----------

def run_sync(coro):
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return asyncio.run(coro)
    with ThreadPoolExecutor(max_workers=1) as pool:
        return pool.submit(asyncio.run, coro).result()


SERVER = StdioServerParameters(
    command=sys.executable,           # the notebook's own Python
    args=[SERVER_FILE],
    env={"COBALT_TOOL_MODE": "mock"},
    cwd=NB_DIR,
)


def with_server_log(fn):
    """Run an async function that opens the server; show its stderr on error."""
    async def runner(*args):
        errlog = tempfile.TemporaryFile(mode="w+")
        try:
            return await fn(errlog, *args)
        except BaseException:
            errlog.seek(0)
            log = errlog.read().strip()
            if log:
                print("--- server stderr ---\n" + log[-2000:])
            raise
        finally:
            errlog.close()
    return runner

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Talk to the server directly (no LLM)
# MAGIC
# MAGIC `initialize`, `list_tools`, then two `call_tool` requests: a valid one and
# MAGIC an injection-style order ID that the tool's own validation must refuse.

# COMMAND ----------

async def protocol_demo(errlog):
    async with stdio_client(SERVER, errlog=errlog) as (read, write):
        async with ClientSession(read, write) as session:
            init = await session.initialize()
            print("server:", init.serverInfo.name, init.serverInfo.version)
            listed = await session.list_tools()
            print("tools:", [t.name for t in listed.tools])
            good = await session.call_tool(
                "check_order_status", {"order_id": "PO-7723"})
            bad = await session.call_tool(
                "check_order_status", {"order_id": "PO-7723'; DROP TABLE x;--"})
            disc = await session.call_tool(
                "apply_discount",
                {"order_id": "PO-7723", "discount_pct": 15.0,
                 "reason": "Apex Haulers missed the delivery window"})
            return ([t.name for t in listed.tools],
                    [json.loads(r.content[0].text) for r in (good, bad, disc)])


tool_names, (good, bad, disc) = run_sync(with_server_log(protocol_demo)())
print("valid call     ->", good)
print("injection call ->", bad)
print(">10% STANDARD  ->", disc)

assert sorted(tool_names) == ["apply_discount", "check_order_status"]
assert good["status"] == "PENDING"
assert bad["code"] == "VALIDATION_ERROR"
assert disc["status"] == "human_approval_required"
print("PASS MCP round trip works and policy is enforced inside the server")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. An agent that uses the MCP tools
# MAGIC
# MAGIC `load_mcp_tools` turns each MCP tool into a LangChain tool. The agent runs
# MAGIC inside the `async with` blocks, because the tools call back into the
# MAGIC open session.

# COMMAND ----------

try:
    mlflow.langchain.autolog()
except Exception as exc:
    print(f"MLflow autolog could not be enabled: {str(exc)[:150]}")

llm = ChatDatabricks(endpoint=CHAT_ENDPOINT)
PROMPT = ("You are the CobaltWorks procurement assistant. Check an order "
          "before changing it. Only report what the tools returned.")


async def agent_demo(errlog, question):
    async with stdio_client(SERVER, errlog=errlog) as (read, write):
        async with ClientSession(read, write) as session:
            await session.initialize()
            tools = await load_mcp_tools(session)
            print("loaded tools:", [t.name for t in tools])
            agent = create_agent(llm, tools, system_prompt=PROMPT)
            return await agent.ainvoke(
                {"messages": [{"role": "user", "content": question}]})


QUESTION = ("Apex Haulers is late on PO-7723. Check its status, then apply a "
            "5% goodwill discount because of the delay.")
try:
    result = run_sync(with_server_log(agent_demo)(QUESTION))
except Exception as exc:
    raise RuntimeError(
        f"The MCP agent call failed on endpoint {CHAT_ENDPOINT!r}: "
        f"{str(exc)[:300]}") from exc

print("Q:", QUESTION)
for line in cw.tool_call_summary(result["messages"]):
    print("  " + line)
print("A:", cw.message_text(result["messages"][-1]))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. For production: a hosted MCP server
# MAGIC
# MAGIC Deploy the same server on Databricks Apps, then connect with the
# MAGIC Databricks client. The agent code does not change; only the transport
# MAGIC does. Replace `<app-host>` with your app's host. This cell is shown but
# MAGIC not executed.
# MAGIC
# MAGIC ```python
# MAGIC from databricks_langchain import (
# MAGIC     DatabricksMCPServer, DatabricksMultiServerMCPClient)
# MAGIC
# MAGIC client = DatabricksMultiServerMCPClient([
# MAGIC     DatabricksMCPServer(
# MAGIC         name="cobaltworks-sap",
# MAGIC         url="https://<app-host>/mcp",
# MAGIC         workspace_client=WorkspaceClient(),
# MAGIC     ),
# MAGIC ])
# MAGIC tools = await client.get_tools()
# MAGIC agent = create_agent(model=llm, tools=tools)
# MAGIC ```
# MAGIC
# MAGIC Databricks-managed and registered MCP servers use Unity Catalog
# MAGIC permissions, and Unity Gateway controls who can call them.
