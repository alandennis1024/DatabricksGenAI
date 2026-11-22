# Databricks notebook source
# MAGIC %md
# MAGIC # 02 - An agent with guardrails and a full audit trail
# MAGIC
# MAGIC **Book section:** *Importance of Guardrails* (rings 1 to 5), *Logs,
# MAGIC Compliance, and Inference Tables*, and the CobaltWorks case study.
# MAGIC
# MAGIC **What it does:**
# MAGIC 1. wraps every tool so each call is written to
# MAGIC    `agent_audit.tool_actions` (timestamp, session, tool, masked
# MAGIC    parameters, status, latency, policy basis);
# MAGIC 2. runs scripted tool calls without an LLM, so the audit table always
# MAGIC    has rows to look at;
# MAGIC 3. builds a `create_agent` agent with a system prompt and MLflow
# MAGIC    tracing, and asks it Apex Haulers style questions;
# MAGIC 4. runs the **propose, approve, execute** human-approval flow and shows
# MAGIC    the `approved_action_id` written to `agent_audit.human_approvals`.
# MAGIC
# MAGIC **Prerequisites:** run `00_setup_audit_trail` (same `catalog` and
# MAGIC `audit_schema`). Needs a tool-calling chat endpoint. The notebook checks
# MAGIC that `chat_endpoint` exists and falls back to another `databricks-*` chat
# MAGIC endpoint if it does not.
# MAGIC
# MAGIC **Runtime:** serverless compute or DBR 17+. A few minutes, mostly LLM
# MAGIC latency. Uses pay-per-token foundation model calls (a handful of short
# MAGIC requests).
# MAGIC
# MAGIC **Creates:** rows in `tool_actions` and `human_approvals`, and MLflow
# MAGIC traces in this notebook's experiment. Re-running adds more rows;
# MAGIC `99_cleanup` removes them.

# COMMAND ----------

# MAGIC %pip install -q -r requirements.txt

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

dbutils.widgets.text("catalog", "cobalt_ops", "Catalog")
dbutils.widgets.text("audit_schema", "agent_audit", "Audit schema")
dbutils.widgets.text(
    "chat_endpoint", "databricks-claude-sonnet-4-5", "Chat endpoint")
dbutils.widgets.dropdown("tool_mode", "mock", ["mock", "connection"], "Tool mode")

CATALOG = dbutils.widgets.get("catalog").strip()
AUDIT_SCHEMA = dbutils.widgets.get("audit_schema").strip()
CHAT_ENDPOINT_WIDGET = dbutils.widgets.get("chat_endpoint").strip()
TOOL_MODE = dbutils.widgets.get("tool_mode").strip()

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

import json
import time
import uuid

import mlflow
import mlflow.langchain
from databricks.sdk import WorkspaceClient
from databricks_langchain import ChatDatabricks
from langchain.agents import create_agent
from langchain_core.tools import StructuredTool, tool

import cobaltworks_tools as cw

cw.set_tool_mode(TOOL_MODE)
cw.reset_mock_state()  # same starting data on every run
MOCK = TOOL_MODE == "mock"
T_AUDIT = cw.qualified(CATALOG, AUDIT_SCHEMA, "tool_actions")
T_APPR = cw.qualified(CATALOG, AUDIT_SCHEMA, "human_approvals")
print(f"tool_mode = {cw.TOOL_MODE}; audit table = {T_AUDIT}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Preflight: tables and model endpoint
# MAGIC
# MAGIC Endpoint names change over time. This cell lists what your workspace
# MAGIC serves and picks a working chat endpoint, so a renamed model does not stop
# MAGIC the notebook.

# COMMAND ----------

for t in (T_AUDIT, T_APPR):
    try:
        spark.sql(f"SELECT 1 FROM {t} LIMIT 1").collect()
    except Exception as exc:
        raise RuntimeError(
            f"Table {t} is not available ({str(exc)[:120]}). Run "
            "00_setup_audit_trail with the same catalog and audit_schema "
            "widgets first.") from exc

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
# MAGIC ## 1. Audit every tool call
# MAGIC
# MAGIC `audited()` wraps a tool so that every invocation, including rejected
# MAGIC and failed ones, writes one row to the append-only table. Sensitive
# MAGIC parameters are masked first (`cw.mask_params`); the discount percentage
# MAGIC stays readable because the anomaly query in notebook 05 needs it.
# MAGIC `policy_basis` records which guardrail ring made the decision.
# MAGIC
# MAGIC The audit write happens after the tool returns. If it fails, the error
# MAGIC propagates instead of being swallowed. For production write tools, also
# MAGIC log an *intent* row before the call.

# COMMAND ----------

AGENT_ID = "cobalt-procurement-agent"
SESSION = {"id": "unset"}


def write_audit(tool_name, params, result, status, latency_ms, policy_basis):
    spark.sql(
        f"""
        INSERT INTO {T_AUDIT}
          (event_timestamp, session_id, agent_id, tool_name, input_params,
           output_summary, status, latency_ms, policy_basis)
        VALUES (current_timestamp(), :session_id, :agent_id, :tool_name,
                :input_params, :output_summary, :status, :latency_ms,
                :policy_basis)
        """,
        args={
            "session_id": SESSION["id"],
            "agent_id": AGENT_ID,
            "tool_name": tool_name,
            "input_params": json.dumps(cw.mask_params(params)),
            "output_summary": json.dumps(result, default=str)[:500],
            "status": status,
            "latency_ms": int(latency_ms),
            "policy_basis": policy_basis,
        },
    )


def audited(base_tool):
    """Return a copy of `base_tool` that logs every call to the audit table."""

    def run(**kwargs):
        t0 = time.perf_counter()
        try:
            result = base_tool.invoke(kwargs)
        except Exception as exc:  # upstream failure: log it, tell the agent
            result = {"error": f"{type(exc).__name__}: {exc}"[:200],
                      "code": "UPSTREAM_ERROR"}
        status, policy = cw.classify_result(base_tool.name, result)
        write_audit(base_tool.name, kwargs, result, status,
                    (time.perf_counter() - t0) * 1000, policy)
        return result

    return StructuredTool.from_function(
        func=run,
        name=base_tool.name,
        description=base_tool.description,
        args_schema=base_tool.args_schema,
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Ring 5: propose and execute
# MAGIC
# MAGIC * `propose_discount_for_approval` is an **agent tool**. It computes the
# MAGIC   impact, changes nothing in SAP and records a `PENDING` request.
# MAGIC * `execute_approved_discount` is **not given to the agent** (Ring 2: tool
# MAGIC   exposure control). The approval workflow calls it, and it refuses
# MAGIC   unless a human has written a matching `approved_action_id` into the
# MAGIC   approvals table.

# COMMAND ----------

@tool
def propose_discount_for_approval(order_id: str, discount_pct: float,
                                  reason: str) -> dict:
    """Request human approval for a discount that apply_discount refused.

    Use this when apply_discount returns human_approval_required. It records
    an approval request and changes nothing in SAP. Tell the user that an
    approver must approve it; never say the discount was applied.
    """
    plan = cw.build_discount_proposal(order_id, discount_pct, reason)
    if "error" in plan:
        return plan
    if not plan["requires_human_approval"]:
        return {"status": "no_approval_needed",
                "message": "Within autonomous authority; use apply_discount."}
    proposal_id = "PRP-" + uuid.uuid4().hex[:8].upper()
    spark.sql(
        f"""
        INSERT INTO {T_APPR}
          (proposal_id, proposed_at, session_id, order_id, discount_pct,
           reason, impact_json, status)
        VALUES (:pid, current_timestamp(), :sid, :oid, :pct, :reason,
                :impact, 'PENDING')
        """,
        args={"pid": proposal_id, "sid": SESSION["id"], "oid": order_id,
              "pct": float(discount_pct), "reason": reason,
              "impact": json.dumps(plan)},
    )
    return {"status": "pending_human_approval", "proposal_id": proposal_id,
            "impact": plan,
            "message": "Recorded. A human approver must approve it."}


@tool
def execute_approved_discount(proposal_id: str, approved_action_id: str) -> dict:
    """Execute a discount that a human approved. Called by the workflow."""
    rows = spark.sql(
        f"""
        SELECT status, approved_action_id, order_id, discount_pct, reason
        FROM {T_APPR} WHERE proposal_id = :pid
        """,
        args={"pid": proposal_id},
    ).collect()
    if not rows:
        return {"error": "unknown proposal_id", "code": "VALIDATION_ERROR"}
    row = rows[0]
    if (row["status"] != "APPROVED" or not approved_action_id
            or row["approved_action_id"] != approved_action_id):
        return {"error": f"proposal is {row['status']}; a matching "
                         "approved_action_id from a human is required",
                "code": "APPROVAL_REQUIRED"}
    res = cw.apply_discount_after_approval(
        row["order_id"], float(row["discount_pct"]), row["reason"],
        approved_action_id)
    if res.get("status") == "success":
        spark.sql(
            f"""
            UPDATE {T_APPR}
            SET status = 'EXECUTED', executed_at = current_timestamp(),
                confirmation_id = :cid
            WHERE proposal_id = :pid AND status = 'APPROVED'
            """,
            args={"cid": res["confirmation_id"], "pid": proposal_id},
        )
    return res


TOOLS = {t.name: audited(t) for t in [
    cw.check_order_status, cw.get_customer_account, cw.apply_discount,
    propose_discount_for_approval, execute_approved_discount]}
AGENT_TOOLS = [TOOLS[n] for n in (
    "check_order_status", "get_customer_account", "apply_discount",
    "propose_discount_for_approval")]
print("tools the agent can see:", [t.name for t in AGENT_TOOLS])


def check(label, condition):
    """Self-check. Enforced against the mock data, informational otherwise."""
    print(("PASS " if condition else "FAIL ") + label)
    if MOCK:
        assert condition, label

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Scripted tool calls (no LLM)
# MAGIC
# MAGIC These calls always run, so the audit table has rows even if your model
# MAGIC endpoint is busy. Each row shows which ring stopped the request.

# COMMAND ----------

SESSION["id"] = "scripted-" + uuid.uuid4().hex[:8]
ok_reason = "Late delivery by Apex Haulers, goodwill discount"

calls = [
    ("check_order_status", {"order_id": "PO-7701"}, "success"),
    ("apply_discount", {"order_id": "PO-7701", "discount_pct": 5.0,
                        "reason": ok_reason}, "success"),
    ("apply_discount", {"order_id": "PO-7701", "discount_pct": 85.0,
                        "reason": ok_reason}, "policy_rejected"),
    ("apply_discount", {"order_id": "PO-7650", "discount_pct": 5.0,
                        "reason": ok_reason}, "policy_rejected"),
    ("apply_discount", {"order_id": "PO-7723", "discount_pct": 15.0,
                        "reason": ok_reason}, "policy_rejected"),
    ("check_order_status", {"order_id": "PO-7723'; DROP TABLE x;--"},
     "policy_rejected"),
    ("check_order_status", {"order_id": "PO-0000001"}, "error"),
]
for name, args, expected in calls:
    out = TOOLS[name].invoke(args)
    status, basis = cw.classify_result(name, out)
    print(f"{name:20s} {json.dumps(args)[:60]:60s} -> {status} ({basis})")
    check(f"{name} logged as {expected}", status == expected)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. The agent
# MAGIC
# MAGIC Ring 1 is the system prompt: soft policy in plain language. The rings
# MAGIC behind it (tool exposure, schema validation, business rules in code, the
# MAGIC human gate) still hold if the model ignores the prompt.
# MAGIC `mlflow.langchain.autolog()` records every LLM call and tool call as an
# MAGIC MLflow trace. Open the **Traces** tab of this notebook's experiment to see
# MAGIC the span tree.

# COMMAND ----------

SYSTEM_PROMPT = """You are the CobaltWorks procurement assistant.
- Always call check_order_status before changing an order.
- Never modify orders in SHIPPED or CLOSED status.
- Discounts must be between 0.1% and 20% and need a clear business reason.
- If apply_discount returns human_approval_required, call
  propose_discount_for_approval and tell the user an approver must approve it.
  Never claim a discount was applied unless a tool returned status success.
- Never invent order IDs, prices or confirmation numbers. Use tool results.
- Keep answers short and state what you did and did not do."""

try:
    mlflow.langchain.autolog()
    print("MLflow tracing is on.")
except Exception as exc:
    print(f"MLflow autolog could not be enabled: {str(exc)[:150]}")

llm = ChatDatabricks(endpoint=CHAT_ENDPOINT)
agent = create_agent(model=llm, tools=AGENT_TOOLS, system_prompt=SYSTEM_PROMPT)


def ask(question):
    SESSION["id"] = "agent-" + uuid.uuid4().hex[:8]
    print("=" * 80, f"\nQ [{SESSION['id']}]: {question}")
    try:
        result = agent.invoke({"messages": [{"role": "user", "content": question}]})
    except Exception as exc:
        raise RuntimeError(
            f"The agent call failed on endpoint {CHAT_ENDPOINT!r}: "
            f"{str(exc)[:300]}. Check the chat_endpoint widget and that the "
            "endpoint supports tool calling.") from exc
    for line in cw.tool_call_summary(result["messages"]):
        print("  " + line)
    print("A:", cw.message_text(result["messages"][-1]))
    return result

# COMMAND ----------

QUESTIONS = [
    "Apex Haulers is late on order PO-7723. What is the live status, and why "
    "is it delayed?",
    "Pinnacle Rail's order PO-7701 was hit by earlier carrier delays. Apply a "
    "12% goodwill discount to PO-7701.",
    "The customer on PO-7723 is upset about the Apex Haulers delay. Give them "
    "15% off.",
    "Apply a 5% discount to PO-7650 because the shipment was late.",
    "Check order PO-7723'; DROP TABLE orders;-- and tell me its status.",
]
for q in QUESTIONS:
    ask(q)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Propose, approve, execute (scripted)
# MAGIC
# MAGIC The agent may or may not have reached the gate in the last section,
# MAGIC depending on how the model phrased its reasons. This cell walks the whole
# MAGIC flow deterministically:
# MAGIC
# MAGIC 1. the agent-side tool records a `PENDING` proposal with an impact
# MAGIC    analysis;
# MAGIC 2. executing before approval is refused;
# MAGIC 3. **a human approves** by writing an `approved_action_id` (here we play
# MAGIC    the approver with SQL; in production an approver app does this under
# MAGIC    an identity the agent does not have);
# MAGIC 4. executing with a wrong id is refused;
# MAGIC 5. executing with the right id applies the discount once;
# MAGIC 6. executing again is refused because the proposal is `EXECUTED`.

# COMMAND ----------

SESSION["id"] = "approval-flow-" + uuid.uuid4().hex[:8]

proposal = TOOLS["propose_discount_for_approval"].invoke({
    "order_id": "PO-7723", "discount_pct": 14.0,
    "reason": "Apex Haulers missed the delivery window by six days"})
print(json.dumps(proposal, indent=2))
check("proposal recorded as pending",
      proposal.get("status") == "pending_human_approval")
proposal_id = proposal["proposal_id"]

r = TOOLS["execute_approved_discount"].invoke(
    {"proposal_id": proposal_id, "approved_action_id": "APR-GUESSED"})
check("execute before approval refused", r.get("code") == "APPROVAL_REQUIRED")

approved_action_id = "APR-" + uuid.uuid4().hex[:8].upper()
spark.sql(
    f"""
    UPDATE {T_APPR}
    SET status = 'APPROVED', approved_action_id = :aid,
        approver = current_user(), decided_at = current_timestamp()
    WHERE proposal_id = :pid AND status = 'PENDING'
    """,
    args={"aid": approved_action_id, "pid": proposal_id},
)
print("human approval written:", approved_action_id)

r = TOOLS["execute_approved_discount"].invoke(
    {"proposal_id": proposal_id, "approved_action_id": "APR-WRONG"})
check("wrong approved_action_id refused", r.get("code") == "APPROVAL_REQUIRED")

r = TOOLS["execute_approved_discount"].invoke(
    {"proposal_id": proposal_id, "approved_action_id": approved_action_id})
print(json.dumps(r))
check("approved discount executed", r.get("status") == "success")

r = TOOLS["execute_approved_discount"].invoke(
    {"proposal_id": proposal_id, "approved_action_id": approved_action_id})
check("second execution refused", r.get("code") == "APPROVAL_REQUIRED")

# COMMAND ----------

display(spark.sql(f"""
    SELECT proposal_id, order_id, discount_pct, status, approved_action_id,
           approver, confirmation_id
    FROM {T_APPR} ORDER BY proposed_at DESC LIMIT 10
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. The audit trail
# MAGIC
# MAGIC One row per tool call, from the scripted calls, the agent and the
# MAGIC approval flow. Notebook 05 queries this table.

# COMMAND ----------

display(spark.sql(f"""
    SELECT event_timestamp, session_id, tool_name, input_params, status,
           latency_ms, policy_basis
    FROM {T_AUDIT} ORDER BY event_timestamp DESC LIMIT 40
"""))
