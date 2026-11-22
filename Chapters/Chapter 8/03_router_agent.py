# Databricks notebook source
# MAGIC %md
# MAGIC # 03 - A router agent with three specialists
# MAGIC
# MAGIC **Book section:** Chapter 8, *Types of Agents* > *Router Agents*.
# MAGIC
# MAGIC **What it does:** builds three small specialist agents and a router that sends each request
# MAGIC to the right one. Each specialist is exposed to the router as a tool with a clear
# MAGIC description, and the router makes one decision and steps aside.
# MAGIC
# MAGIC | Specialist | Tools |
# MAGIC |---|---|
# MAGIC | Customer support | A keyword search over the `help_articles` Delta table |
# MAGIC | Analytics | The `region_revenue` Unity Catalog function and a top products query |
# MAGIC | API | A mock order status API (no network call) |
# MAGIC
# MAGIC The book uses AI Search for the help articles. To keep this notebook runnable anywhere, it
# MAGIC searches a small Delta table by keyword. Swap in an AI Search index when you have one.
# MAGIC
# MAGIC **Prerequisites:** run `00_setup_sample_data` and `01_uc_function_tool`. You need a
# MAGIC Foundation Model API chat endpoint that supports tool calling.
# MAGIC
# MAGIC **Compute:** serverless or DBR 17+. **Expected runtime:** 3 to 5 minutes the first time
# MAGIC (package install), then a couple of minutes for the four requests.
# MAGIC
# MAGIC **Creates:** nothing in Unity Catalog. It writes MLflow traces to this notebook's
# MAGIC experiment.
# MAGIC
# MAGIC **Cost:** each request makes two to four model calls on a pay-per-token endpoint.

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
# MAGIC Model endpoint names change over time. This cell checks that the configured endpoint
# MAGIC exists and is a chat model, and otherwise falls back to another `databricks-*` chat
# MAGIC endpoint. The endpoint list says whether an endpoint is a chat model, not whether it
# MAGIC supports tool calling. The fallback order prefers model families that do.

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

# Fail early with a clear message when notebooks 00 and 01 have not been run.
try:
    spark.sql(f"SELECT 1 FROM {FQ}.help_articles LIMIT 1").collect()
    spark.sql(f"DESCRIBE FUNCTION {FUNCTION}").collect()
except Exception as exc:
    raise RuntimeError(
        f"Could not find {FQ}.help_articles or the function {FUNCTION}. Run "
        "00_setup_sample_data and 01_uc_function_tool first, with the same catalog "
        f"and schema widgets. Original error: {str(exc)[:200]}"
    ) from exc
print("Sample data and function found.")

# COMMAND ----------

import mlflow
import mlflow.langchain

mlflow.langchain.autolog()
print("MLflow tracing is on.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## The specialists' tools
# MAGIC
# MAGIC The docstring of each tool is what the model reads, so each one says what the tool does
# MAGIC and when to use it.

# COMMAND ----------

import json

from databricks_langchain import ChatDatabricks, UCFunctionToolkit
from langchain.agents import create_agent
from langchain.tools import tool
from unitycatalog.ai.core.databricks import DatabricksFunctionClient

STOP_WORDS = {"how", "the", "and", "for", "you", "your", "can", "what", "with", "does"}


def _stem(word):
    return word[:-1] if len(word) > 3 and word.endswith("s") else word


def _tokens(text):
    return {_stem(w) for w in re.findall(r"[a-z0-9]+", text.lower())}


@tool
def search_help_articles(query: str) -> str:
    """Search the help articles for steps that answer a how-to question.

    Pass a few keywords, for example 'reset password'. Returns the best matching
    articles, or says that nothing matched."""
    wanted = {w for w in _tokens(query) if len(w) > 2 and w not in STOP_WORDS}
    rows = spark.sql(f"SELECT title, body, keywords FROM {FQ}.help_articles").collect()
    scored = []
    for row in rows:
        score = len(wanted & _tokens(f"{row['title']} {row['keywords']} {row['body']}"))
        if score:
            scored.append((score, row["title"], row["body"]))
    scored.sort(reverse=True)
    if not scored:
        return "No matching help article."
    return "\n\n".join(f"{title}: {body}" for _, title, body in scored[:2])


@tool
def top_products_by_revenue(limit: int = 3) -> str:
    """Return the top selling products by total revenue, highest first."""
    n = max(1, min(int(limit), 10))  # clamp whatever the model passes
    rows = spark.sql(
        f"""
        SELECT product, ROUND(SUM(revenue), 2) AS revenue
        FROM {FQ}.sales GROUP BY product ORDER BY revenue DESC LIMIT {n}
        """
    ).collect()
    return "\n".join(f"{r['product']}: {r['revenue']}" for r in rows)


ORDERS_API = {  # a stand-in for a real HTTP API
    "10482": {"status": "delivered", "carrier": "FastShip", "delivered": "2026-03-02"},
    "10490": {"status": "in transit", "carrier": "FastShip", "eta": "2026-03-20"},
    "10517": {"status": "processing", "carrier": None, "eta": None},
}


@tool
def get_order_status(order_id: str) -> str:
    """Look up the status of one order by its order number, for example '10482'."""
    order_id = order_id.strip()
    if not order_id.isdigit():
        return "Invalid order number: use digits only."
    info = ORDERS_API.get(order_id)
    return json.dumps(info) if info else f"Order {order_id} was not found."


uc_tools = UCFunctionToolkit(
    function_names=[FUNCTION], client=DatabricksFunctionClient()
).tools
print("Tools:", [t.name for t in [search_help_articles, *uc_tools, get_order_status]])

# COMMAND ----------

# MAGIC %md
# MAGIC ## The three specialist agents
# MAGIC
# MAGIC Each specialist is a small tool calling agent with its own tools and a short prompt.

# COMMAND ----------

model = ChatDatabricks(endpoint=chat_endpoint)

support_agent = create_agent(
    model=model,
    tools=[search_help_articles],
    system_prompt=(
        "You are a customer support agent. Use search_help_articles to find the "
        "answer, then give the steps in a short reply. If nothing matches, say so."
    ),
)
analytics_agent = create_agent(
    model=model,
    tools=[*uc_tools, top_products_by_revenue],
    system_prompt=(
        "You are a sales analytics agent. Use your tools to answer with real numbers "
        "from the sales data. Never guess a number."
    ),
)
api_agent = create_agent(
    model=model,
    tools=[get_order_status],
    system_prompt=(
        "You are an API agent. Use get_order_status to look up orders and report the "
        "result in one or two sentences."
    ),
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


def run_specialist(agent, question):
    out = agent.invoke({"messages": [{"role": "user", "content": question}]})
    return text_of(out["messages"][-1])


# COMMAND ----------

# MAGIC %md
# MAGIC ## The router
# MAGIC
# MAGIC The router never queries data or searches articles itself. Its three tools simply hand the
# MAGIC question to a specialist.

# COMMAND ----------

@tool
def customer_support_agent(question: str) -> str:
    """Use for how-to and account questions, such as resetting a password, billing
    addresses, returns, or turning on security features. Pass the question unchanged."""
    return run_specialist(support_agent, question)


@tool
def analytics_agent_tool(question: str) -> str:
    """Use for questions about sales numbers: revenue by region, best selling
    products, and other figures from the sales data. Pass the question unchanged."""
    return run_specialist(analytics_agent, question)


@tool
def api_agent_tool(question: str) -> str:
    """Use for questions about a specific order, such as where it is or whether it was
    delivered. The question must contain an order number. Pass it unchanged."""
    return run_specialist(api_agent, question)


router = create_agent(
    model=model,
    tools=[customer_support_agent, analytics_agent_tool, api_agent_tool],
    system_prompt=(
        "You are a router. Pick exactly one specialist tool for the request and "
        "return its answer. If the request fits none of them, do not call a tool: "
        "ask one short clarifying question instead."
    ),
)
print("Router ready.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Try four requests
# MAGIC
# MAGIC For each one we print which specialist the router picked. Models are not fully
# MAGIC deterministic, so a route can occasionally differ from the one we expect. The last request
# MAGIC matches no specialist, so the router should ask a clarifying question.

# COMMAND ----------

requests = [
    ("How do I reset my device password?", "customer_support_agent"),
    ("Which products have earned us the most revenue?", "analytics_agent_tool"),
    ("Where is order 10482?", "api_agent_tool"),
    ("Can you help me with something?", None),
]

for question, expected in requests:
    out = router.invoke({"messages": [{"role": "user", "content": question}]})
    routes = [c["name"] for m in out["messages"] if m.type == "ai" for c in m.tool_calls]
    print("=" * 70)
    print("QUESTION:", question)
    print("ROUTED TO:", routes or "(no specialist - asked a question)")
    if (routes[0] if routes else None) != expected:
        print(f"NOTE: expected {expected or 'no specialist'}.")
    print("ANSWER:", text_of(out["messages"][-1]))

print("=" * 70)
print("Done. Next: run 04_human_in_the_loop.")
