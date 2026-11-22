# Databricks notebook source
# MAGIC %md
# MAGIC # 04 - Agent memory with Lakebase checkpoints
# MAGIC
# MAGIC **Book section:** *Integration Patterns in Practice > Lakebase for Agent
# MAGIC Memory*.
# MAGIC
# MAGIC **What it does:** builds an agent with a LangGraph checkpointer and has a
# MAGIC two-turn conversation on one `thread_id`. The second turn only makes
# MAGIC sense if the agent remembers the first ("that order"). A different
# MAGIC `thread_id` shows that sessions are isolated.
# MAGIC
# MAGIC **Two modes, same code:**
# MAGIC * `lakebase_instance` set: memory is stored in Lakebase with
# MAGIC   `databricks_langchain.CheckpointSaver`, so it survives restarts and can
# MAGIC   be shared by several agent replicas.
# MAGIC * `lakebase_instance` empty (default): the notebook says so clearly and
# MAGIC   uses LangGraph's in-process `MemorySaver`. Memory lasts only while
# MAGIC   this notebook's Python process lives. **Lakebase is skipped.**
# MAGIC
# MAGIC **To enable Lakebase:** create a Lakebase database instance (or, on the
# MAGIC newer autoscaling Lakebase, a project) in your workspace and paste its
# MAGIC name into `lakebase_instance`. For an autoscaling project fill
# MAGIC `lakebase_project` and `lakebase_branch` instead. Your user needs
# MAGIC permission to connect. Free Edition allows one Lakebase project.
# MAGIC
# MAGIC **Prerequisites:** a tool-calling chat endpoint.
# MAGIC
# MAGIC **Runtime:** serverless compute or DBR 17+. About a minute.
# MAGIC
# MAGIC **Creates:** in Lakebase mode, the LangGraph checkpoint tables in the
# MAGIC database you named (`CheckpointSaver.setup()` is idempotent). Otherwise
# MAGIC nothing.

# COMMAND ----------

# MAGIC %pip install -q -r requirements.txt

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

dbutils.widgets.text(
    "chat_endpoint", "databricks-claude-sonnet-4-5", "Chat endpoint")
dbutils.widgets.dropdown("tool_mode", "mock", ["mock", "connection"], "Tool mode")
dbutils.widgets.text("lakebase_instance", "", "Lakebase instance (empty = skip)")
dbutils.widgets.text("lakebase_project", "", "Lakebase project (autoscaling, optional)")
dbutils.widgets.text("lakebase_branch", "", "Lakebase branch (autoscaling, optional)")

CHAT_ENDPOINT_WIDGET = dbutils.widgets.get("chat_endpoint").strip()
TOOL_MODE = dbutils.widgets.get("tool_mode").strip()
LAKEBASE_INSTANCE = dbutils.widgets.get("lakebase_instance").strip()
LAKEBASE_PROJECT = dbutils.widgets.get("lakebase_project").strip()
LAKEBASE_BRANCH = dbutils.widgets.get("lakebase_branch").strip()

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

import uuid
from contextlib import ExitStack

import mlflow
import mlflow.langchain
from databricks.sdk import WorkspaceClient
from databricks_langchain import ChatDatabricks, CheckpointSaver
from langchain.agents import create_agent
from langgraph.checkpoint.memory import MemorySaver

import cobaltworks_tools as cw

cw.set_tool_mode(TOOL_MODE)
cw.reset_mock_state()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Preflight: model endpoint

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
# MAGIC ## The conversation
# MAGIC
# MAGIC `run_conversation(saver)` builds the agent with the given checkpointer and
# MAGIC runs everything inside the caller's `with` block. With Lakebase, the
# MAGIC connection pool closes when the `with` block ends, so the agent must not
# MAGIC be used after it.

# COMMAND ----------

try:
    mlflow.langchain.autolog()
except Exception as exc:
    print(f"MLflow autolog could not be enabled: {str(exc)[:150]}")

PROMPT = ("You are the CobaltWorks procurement assistant. Use the tools for "
          "facts, never guess. Keep answers to two sentences.")
TOOLS = [cw.check_order_status, cw.get_customer_account]


def run_conversation(saver):
    llm = ChatDatabricks(endpoint=CHAT_ENDPOINT)
    agent = create_agent(model=llm, tools=TOOLS, system_prompt=PROMPT,
                         checkpointer=saver)
    run_id = uuid.uuid4().hex[:8]
    thread_a = {"configurable": {"thread_id": f"plant-mgr-chicago-{run_id}"}}
    thread_b = {"configurable": {"thread_id": f"plant-mgr-detroit-{run_id}"}}
    turns = [
        (thread_a, "Status of order PO-7723?"),
        (thread_a, "Why is that order late, and who is the customer's tier?"),
        (thread_b, "Which order were we just talking about?"),
    ]
    answers = []
    for config, question in turns:
        result = agent.invoke(
            {"messages": [{"role": "user", "content": question}]},
            config=config)
        answer = cw.message_text(result["messages"][-1])
        print(f"[{config['configurable']['thread_id']}]\nQ: {question}\nA: {answer}\n")
        answers.append((result, answer))
    return thread_a, answers


with ExitStack() as stack:
    saver = None
    if LAKEBASE_INSTANCE or (LAKEBASE_PROJECT and LAKEBASE_BRANCH):
        kwargs = ({"instance_name": LAKEBASE_INSTANCE} if LAKEBASE_INSTANCE
                  else {"project": LAKEBASE_PROJECT, "branch": LAKEBASE_BRANCH})
        try:
            saver = stack.enter_context(CheckpointSaver(**kwargs))
            saver.setup()  # creates checkpoint tables (idempotent)
            print(f"MEMORY: Lakebase checkpoints via {kwargs}")
        except Exception as exc:
            print(f"Lakebase connection failed: {str(exc)[:300]}\n"
                  "-> Check the instance or project name and that you can "
                  "connect to it. Falling back to in-process memory.")
            saver = None
    if saver is None:
        saver = MemorySaver()
        print("MEMORY: Lakebase SKIPPED. Using in-process MemorySaver, which "
              "forgets everything when this Python process stops.\n"
              "To enable Lakebase, set the `lakebase_instance` widget "
              "(see the top of this notebook) and re-run.\n")
    thread_a, answers = run_conversation(saver)

# COMMAND ----------

# MAGIC %md
# MAGIC ## What to look for
# MAGIC
# MAGIC * Turn 2 on the Chicago thread answers about **PO-7723** without being
# MAGIC   told the order number again. That is the checkpointer at work.
# MAGIC * The Detroit thread has no history, so the agent should say it does not
# MAGIC   know which order you mean.
# MAGIC
# MAGIC In Lakebase mode, you can open the database's SQL editor and look at the
# MAGIC `checkpoints` tables to see the saved state per `thread_id`.

# COMMAND ----------

turn2 = answers[1][0]["messages"]
history_kept = any("PO-7723" in str(getattr(m, "content", "")) or
                   "PO-7723" in str(getattr(m, "tool_calls", ""))
                   for m in turn2[:-1]) and len(turn2) > 4
print("PASS" if history_kept else "CHECK",
      "turn 2 carried turn 1's messages (PO-7723) through the checkpointer"
      if history_kept else
      "turn 2's state does not contain the earlier turn; memory is not working")
