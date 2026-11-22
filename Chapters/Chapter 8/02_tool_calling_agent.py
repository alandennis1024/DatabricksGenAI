# Databricks notebook source
# MAGIC %md
# MAGIC # 02 - A tool calling (ReAct) agent
# MAGIC
# MAGIC **Book section:** Chapter 8, *Tools* > *Defining a Tool in Python*, and *Types of Agents* >
# MAGIC *Tool Calling Agents*.
# MAGIC
# MAGIC **What it does:** hands the `region_revenue` Unity Catalog function to an LLM as a tool,
# MAGIC asks *"What was revenue in EMEA?"*, and shows the steps of the loop: the model asks for a
# MAGIC tool call, the agent runs the function, and the model writes the answer from the result.
# MAGIC MLflow tracing records each step.
# MAGIC
# MAGIC **Prerequisites:** run `00_setup_sample_data` and `01_uc_function_tool`. Your workspace
# MAGIC needs a Foundation Model API chat endpoint that supports tool calling (the default is
# MAGIC `databricks-claude-sonnet-4-5`). If that name does not exist in your workspace, the
# MAGIC preflight cell picks another `databricks-*` chat endpoint and tells you which one.
# MAGIC
# MAGIC **Compute:** serverless or DBR 17+. **Expected runtime:** 2 to 4 minutes the first time
# MAGIC (package install), about a minute after that.
# MAGIC
# MAGIC **Creates:** nothing in Unity Catalog. It writes MLflow traces to this notebook's
# MAGIC experiment.
# MAGIC
# MAGIC **Cost:** a few model calls on a pay-per-token endpoint. That is a few cents at most.

# COMMAND ----------

# MAGIC %pip install -q -r requirements.txt

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

dbutils.widgets.text("catalog", "cobalt_ops", "Catalog")
dbutils.widgets.text("schema", "sales", "Schema")
dbutils.widgets.text("chat_endpoint", "databricks-claude-sonnet-4-5", "Chat endpoint")

import re

catalog = dbutils.widgets.get("catalog").strip()
schema = dbutils.widgets.get("schema").strip()
requested_endpoint = dbutils.widgets.get("chat_endpoint").strip()
for _label, _value in (("catalog", catalog), ("schema", schema)):
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", _value):
        raise ValueError(
            f"Widget '{_label}' must use letters, digits and underscores only "
            f"(got '{_value}')."
        )

FQ = f"{catalog}.{schema}"
FUNCTION = f"{FQ}.region_revenue"  # Unity Catalog function names have three levels

# COMMAND ----------

# MAGIC %md
# MAGIC ## Preflight: find a chat endpoint
# MAGIC
# MAGIC Model endpoint names change over time. This cell lists the endpoints in your workspace,
# MAGIC checks that the configured one exists and is a chat model, and otherwise falls back to
# MAGIC another `databricks-*` chat endpoint. The endpoint list says whether an endpoint is a chat
# MAGIC model, not whether it supports tool calling. The fallback order prefers model families
# MAGIC that do.

# COMMAND ----------

from databricks.sdk import WorkspaceClient


def resolve_chat_endpoint(requested):
    """Return the name of a chat endpoint that exists in this workspace."""
    try:
        endpoints = list(WorkspaceClient().serving_endpoints.list())
    except Exception as exc:
        print(
            f"WARNING: could not list serving endpoints ({str(exc)[:200]}). "
            f"Using '{requested}' without checking it."
        )
        return requested

    chat = {e.name for e in endpoints if (e.task or "") == "llm/v1/chat"}
    if requested in chat:
        print(f"Chat endpoint '{requested}' found.")
        return requested

    if any(e.name == requested for e in endpoints):
        print(f"WARNING: '{requested}' exists but is not a chat endpoint.")
    else:
        print(f"WARNING: no endpoint named '{requested}' in this workspace.")

    families = ("claude", "gpt-5", "gpt-oss", "llama-4", "llama-3-3", "qwen", "gemini")
    candidates = sorted((n for n in chat if n.startswith("databricks-")), reverse=True)
    for family in families:
        for name in candidates:
            if family in name:
                print(
                    f"Falling back to '{name}'. To choose a different model, set the "
                    "'chat_endpoint' widget (Serving > Foundation models)."
                )
                return name
    if candidates:
        print(f"Falling back to '{candidates[0]}' (not checked for tool calling).")
        return candidates[0]
    raise RuntimeError(
        "No databricks-* chat endpoint is available. Open Serving in the sidebar and "
        "check that Foundation Model APIs (pay-per-token) are enabled in your region, "
        "then set the 'chat_endpoint' widget to a chat endpoint that supports tools."
    )


chat_endpoint = resolve_chat_endpoint(requested_endpoint)
print("Using chat endpoint:", chat_endpoint)

# COMMAND ----------

# Fail early with a clear message when notebook 01 has not been run.
try:
    spark.sql(f"DESCRIBE FUNCTION {FUNCTION}").collect()
except Exception as exc:
    raise RuntimeError(
        f"Function {FUNCTION} was not found. Run 00_setup_sample_data and "
        "01_uc_function_tool first, with the same catalog and schema widgets. "
        f"Original error: {str(exc)[:200]}"
    ) from exc
print(f"{FUNCTION} found.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Turn on tracing
# MAGIC
# MAGIC `autolog()` makes MLflow record every model call and tool call. On Databricks the traces
# MAGIC appear under the notebook's cells and in the notebook's experiment.

# COMMAND ----------

import mlflow
import mlflow.langchain

mlflow.langchain.autolog()
print("MLflow tracing is on.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Build the agent
# MAGIC
# MAGIC `UCFunctionToolkit` reads the function's signature and comments from Unity Catalog and
# MAGIC turns it into a tool the model can see. `create_agent` wires the model and the tools into
# MAGIC a loop: call the model, run any tools it asks for, call the model again.

# COMMAND ----------

from databricks_langchain import ChatDatabricks, UCFunctionToolkit
from langchain.agents import create_agent
from unitycatalog.ai.core.databricks import DatabricksFunctionClient

toolkit = UCFunctionToolkit(
    function_names=[FUNCTION],
    client=DatabricksFunctionClient(),
)
print("Tools the model can see:")
for t in toolkit.tools:
    print(f"  {t.name}: {t.description}")

agent = create_agent(
    model=ChatDatabricks(endpoint=chat_endpoint),
    tools=toolkit.tools,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ask the question

# COMMAND ----------

result = agent.invoke(
    {"messages": [{"role": "user", "content": "What was revenue in EMEA?"}]}
)


def text_of(message):
    """Return the text of a message, whether content is a string or a list of blocks."""
    content = message.content
    if isinstance(content, str):
        return content
    return "".join(
        block.get("text", "") if isinstance(block, dict) else str(block)
        for block in content
    )


print("FINAL ANSWER:")
print(text_of(result["messages"][-1]))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read the loop
# MAGIC
# MAGIC The result holds every message. An `ai` message with `tool_calls` is the model asking for a
# MAGIC tool. The `tool` message that follows is what came back. The last `ai` message is the
# MAGIC answer.

# COMMAND ----------

for i, m in enumerate(result["messages"], start=1):
    if m.type == "human":
        print(f"{i}. USER: {text_of(m)}")
    elif m.type == "ai" and m.tool_calls:
        for call in m.tool_calls:
            print(f"{i}. MODEL ASKS FOR TOOL: {call['name']}({call['args']})")
    elif m.type == "tool":
        print(f"{i}. TOOL RESULT from {m.name}: {text_of(m)[:300]}")
    else:
        print(f"{i}. MODEL ANSWER: {text_of(m)}")

tool_names = [c["name"] for m in result["messages"] if m.type == "ai" for c in m.tool_calls]
if not any("region_revenue" in name for name in tool_names):
    print(
        "\nNOTE: the model did not call region_revenue. If the chosen endpoint does not "
        "support tool calling, set the 'chat_endpoint' widget to one that does."
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Look at the trace
# MAGIC
# MAGIC Open the trace to see the model call, the tool call, its input and its output. Run the
# MAGIC cell below for the trace id. In a notebook, the trace UI also shows under the cell that
# MAGIC ran the agent.

# COMMAND ----------

get_trace_id = getattr(mlflow, "get_last_active_trace_id", None)
trace_id = get_trace_id() if get_trace_id else None
if trace_id:
    print("Last trace id:", trace_id)
    print("Find it in the Experiments page of this notebook, on the Traces tab.")
else:
    print("No trace was recorded. Check that the cell above ran without errors.")

print("Done. Next: run 03_router_agent.")
