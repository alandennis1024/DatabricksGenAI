# Databricks notebook source
# MAGIC %md
# MAGIC # 04 - Human-in-the-loop: pause a refund for approval
# MAGIC
# MAGIC **Book section:** Chapter 8, *Types of Agents* > *Human-in-the-Loop Agents*, and the
# MAGIC *Guided Agents* note about checkpointing and approval steps.
# MAGIC
# MAGIC **What it does:** builds a small LangGraph workflow for refund requests.
# MAGIC
# MAGIC * A **$20 refund** passes the risk check and runs on its own.
# MAGIC * A **$12,000 refund** is high risk. The workflow writes a `pending` row to the approval
# MAGIC   table, saves its state, and **pauses**. A person then approves, edits, or rejects it, and
# MAGIC   the workflow resumes from the saved checkpoint.
# MAGIC * Every decision is recorded in the `refund_approvals` Delta table, and MLflow tracing
# MAGIC   records each run, including the resumed one.
# MAGIC
# MAGIC The workflow uses no model endpoint and no external service. The "people" in this notebook
# MAGIC are simulated by cells that send a decision. In production, a Slack message, an email, or an
# MAGIC app screen would send the same decision.
# MAGIC
# MAGIC **Prerequisites:** run `00_setup_sample_data` first.
# MAGIC
# MAGIC **Compute:** serverless or DBR 17+. **Expected runtime:** 2 to 4 minutes the first time
# MAGIC (package install), then under a minute.
# MAGIC
# MAGIC **Creates:** rows in `refund_approvals` and `refund_ledger` (both created by notebook 00).
# MAGIC The refund is pretend: it only writes a ledger row.
# MAGIC
# MAGIC **Where the saved state lives:** in this notebook's memory (`InMemorySaver`), so a paused
# MAGIC run is lost if you detach the notebook or restart the cluster. That keeps the demo
# MAGIC runnable anywhere. For a durable store, use a database-backed checkpointer such as the
# MAGIC Lakebase-backed one described in the chapter.

# COMMAND ----------

# MAGIC %pip install -q -r requirements.txt

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

dbutils.widgets.text("catalog", "cobalt_ops", "Catalog")
dbutils.widgets.text("schema", "sales", "Schema")
dbutils.widgets.text("approval_threshold", "1000", "Approval needed at or above")

import re

catalog = dbutils.widgets.get("catalog").strip()
schema = dbutils.widgets.get("schema").strip()
for _label, _value in (("catalog", catalog), ("schema", schema)):
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", _value):
        raise ValueError(
            f"Widget '{_label}' must use letters, digits and underscores only "
            f"(got '{_value}')."
        )
try:
    APPROVAL_THRESHOLD = float(dbutils.widgets.get("approval_threshold"))
except ValueError as exc:
    raise ValueError("Widget 'approval_threshold' must be a number, e.g. 1000") from exc
# The demo refunds are $20 (must run alone) and $7,500 to $12,000 (must pause).
if not 20 < APPROVAL_THRESHOLD <= 7500:
    raise ValueError("Widget 'approval_threshold' must be above 20 and at most 7500.")

FQ = f"{catalog}.{schema}"
T_ORDERS = f"{FQ}.orders"
T_APPROVALS = f"{FQ}.refund_approvals"
T_LEDGER = f"{FQ}.refund_ledger"
print(f"Refunds of {APPROVAL_THRESHOLD:,.2f} or more need a person's approval.")

# COMMAND ----------

# Fail early with a clear message when notebook 00 has not been run.
for _table in (T_ORDERS, T_APPROVALS, T_LEDGER):
    try:
        spark.sql(f"SELECT 1 FROM {_table} LIMIT 1").collect()
    except Exception as exc:
        raise RuntimeError(
            f"Table {_table} was not found. Run 00_setup_sample_data first (with the "
            f"same catalog and schema widgets). Original error: {str(exc)[:200]}"
        ) from exc
print("Tables found.")

# COMMAND ----------

import mlflow
import mlflow.langchain

mlflow.langchain.autolog()
print("MLflow tracing is on.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## The workflow
# MAGIC
# MAGIC ```
# MAGIC validate -> check risk -+-> low  -> record auto approval ---------------> issue refund
# MAGIC                         |
# MAGIC                         +-> high -> record pending -> WAIT (interrupt) -> record decision
# MAGIC                                                                             |
# MAGIC                                                         approve / edit ----> issue refund
# MAGIC                                                         reject ------------> end
# MAGIC ```
# MAGIC
# MAGIC Two rules keep this safe:
# MAGIC
# MAGIC * **Do side effects in their own node.** When a paused run resumes, LangGraph runs the
# MAGIC   interrupted node again from its start. So the node that calls `interrupt()` only asks
# MAGIC   the question, and the database writes happen in nodes before and after it.
# MAGIC * **Fail closed.** An unknown answer, or an edit that raises the amount, counts as a
# MAGIC   rejection.

# COMMAND ----------

import uuid
from typing import TypedDict

from langgraph.checkpoint.memory import InMemorySaver
from langgraph.graph import END, START, StateGraph
from langgraph.types import Command, interrupt


class RefundState(TypedDict, total=False):
    request_id: str
    order_id: str
    amount: float
    reason: str
    risk: str
    decision: str
    approver: str
    note: str
    approved_amount: float
    outcome: str


def validate_request(state: RefundState) -> dict:
    rows = spark.sql(
        f"SELECT order_total FROM {T_ORDERS} WHERE order_id = :order_id",
        args={"order_id": state["order_id"]},
    ).collect()
    if not rows:
        return {"outcome": f"invalid: order {state['order_id']} not found"}
    if not 0 < state["amount"] <= rows[0]["order_total"]:
        return {"outcome": "invalid: refund is not between 0 and the order total"}
    return {}


def check_risk(state: RefundState) -> dict:
    return {"risk": "high" if state["amount"] >= APPROVAL_THRESHOLD else "low"}


def record_auto(state: RefundState) -> dict:
    spark.sql(
        f"""
        INSERT INTO {T_APPROVALS} VALUES (
          :request_id, :order_id, CAST(:amount AS DOUBLE), :reason, 'auto_approved',
          current_timestamp(), 'risk-check', current_timestamp(),
          'Below the approval threshold', CAST(:amount AS DOUBLE))
        """,
        args={k: state[k] for k in ("request_id", "order_id", "amount", "reason")},
    )
    return {"decision": "auto", "approved_amount": state["amount"]}


def record_pending(state: RefundState) -> dict:
    spark.sql(
        f"""
        INSERT INTO {T_APPROVALS} VALUES (
          :request_id, :order_id, CAST(:amount AS DOUBLE), :reason, 'pending',
          current_timestamp(), NULL, NULL, NULL, NULL)
        """,
        args={k: state[k] for k in ("request_id", "order_id", "amount", "reason")},
    )
    return {}


def wait_for_approval(state: RefundState) -> dict:
    # Pauses here. The value passed to Command(resume=...) becomes `answer`.
    answer = interrupt(
        {
            "request_id": state["request_id"],
            "order_id": state["order_id"],
            "amount": state["amount"],
            "reason": state["reason"],
            "question": "Approve this refund?",
            "allowed_decisions": ["approve", "edit", "reject"],
        }
    )
    answer = answer if isinstance(answer, dict) else {}
    decision = str(answer.get("decision", "reject")).lower()
    approved = state["amount"]
    if decision == "edit":
        try:
            approved = float(answer.get("amount", 0))
        except (TypeError, ValueError):
            approved = 0.0
        if not 0 < approved <= state["amount"]:
            decision = "reject"  # an edit may lower the amount, never raise it
    elif decision != "approve":
        decision = "reject"
    if decision == "reject":
        approved = 0.0
    return {
        "decision": decision,
        "approver": str(answer.get("approver", "unknown")),
        "note": str(answer.get("note", "")),
        "approved_amount": approved,
    }


STATUS_FOR = {"approve": "approved", "edit": "approved_edited", "reject": "rejected"}


def record_decision(state: RefundState) -> dict:
    spark.sql(
        f"""
        UPDATE {T_APPROVALS}
        SET status = :status, decided_by = :approver, decided_at = current_timestamp(),
            decision_note = :note, approved_amount = CAST(:approved AS DOUBLE)
        WHERE request_id = :request_id
        """,
        args={
            "status": STATUS_FOR[state["decision"]],
            "approver": state["approver"],
            "note": state["note"],
            "approved": state["approved_amount"],
            "request_id": state["request_id"],
        },
    )
    if state["decision"] == "reject":
        return {"outcome": "cancelled: refund rejected"}
    return {}


def issue_refund(state: RefundState) -> dict:
    # NOT EXISTS makes the insert safe to repeat for the same request.
    spark.sql(
        f"""
        INSERT INTO {T_LEDGER}
        SELECT :request_id, :order_id, CAST(:amount AS DOUBLE), current_timestamp()
        WHERE NOT EXISTS (SELECT 1 FROM {T_LEDGER} WHERE request_id = :request_id)
        """,
        args={
            "request_id": state["request_id"],
            "order_id": state["order_id"],
            "amount": state["approved_amount"],
        },
    )
    return {"outcome": f"refund issued: {state['approved_amount']:,.2f}"}


builder = StateGraph(RefundState)
for _name, _fn in [
    ("validate_request", validate_request),
    ("check_risk", check_risk),
    ("record_auto", record_auto),
    ("record_pending", record_pending),
    ("wait_for_approval", wait_for_approval),
    ("record_decision", record_decision),
    ("issue_refund", issue_refund),
]:
    builder.add_node(_name, _fn)

builder.add_edge(START, "validate_request")
builder.add_conditional_edges(
    "validate_request", lambda s: END if s.get("outcome") else "check_risk"
)
builder.add_conditional_edges(
    "check_risk", lambda s: "record_auto" if s["risk"] == "low" else "record_pending"
)
builder.add_edge("record_auto", "issue_refund")
builder.add_edge("record_pending", "wait_for_approval")
builder.add_edge("wait_for_approval", "record_decision")
builder.add_conditional_edges(
    "record_decision", lambda s: END if s["decision"] == "reject" else "issue_refund"
)
builder.add_edge("issue_refund", END)

graph = builder.compile(checkpointer=InMemorySaver())
print("Workflow compiled.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Two helpers: submit a request, and send a decision
# MAGIC
# MAGIC Each request gets its own `thread_id`. The checkpointer saves the paused state under
# MAGIC that id, so the decision can arrive later, as a separate call.

# COMMAND ----------

run_ids = []


def submit_refund(order_id, amount, reason):
    """Start a refund request. Returns the request id."""
    request_id = "REQ-" + uuid.uuid4().hex[:8]
    run_ids.append(request_id)
    config = {"configurable": {"thread_id": request_id}}
    result = graph.invoke(
        {
            "request_id": request_id,
            "order_id": order_id,
            "amount": amount,
            "reason": reason,
        },
        config,
    )
    if result.get("__interrupt__"):
        ask = result["__interrupt__"][0].value
        print(f"{request_id}: PAUSED, waiting for a person.")
        print(f"  {ask['question']} Order {ask['order_id']}, {ask['amount']:,.2f}")
        print(f"  Saved state is waiting at: {graph.get_state(config).next}")
    else:
        print(f"{request_id}: finished without a pause -> {result['outcome']}")
    return request_id


def decide(request_id, decision, approver, note="", amount=None):
    """Send a person's decision (approve, edit or reject) to a paused request."""
    config = {"configurable": {"thread_id": request_id}}
    if "wait_for_approval" not in graph.get_state(config).next:
        print(f"{request_id}: nothing is waiting for a decision.")
        return None
    answer = {"decision": decision, "approver": approver, "note": note}
    if amount is not None:
        answer["amount"] = amount
    result = graph.invoke(Command(resume=answer), config)
    print(f"{request_id}: {decision.upper()} by {approver} -> {result['outcome']}")
    return result


def status_of(request_id):
    rows = spark.sql(
        f"SELECT status FROM {T_APPROVALS} WHERE request_id = :request_id",
        args={"request_id": request_id},
    ).collect()
    return rows[0]["status"] if rows else None


def show_run():
    """Show the approval rows and ledger rows created by this notebook run."""
    marks = ", ".join(f":r{i}" for i in range(len(run_ids)))
    args = {f"r{i}": rid for i, rid in enumerate(run_ids)}
    print("refund_approvals")
    display(
        spark.sql(
            f"""
            SELECT request_id, order_id, requested_amount, status, decided_by,
                   approved_amount, decision_note
            FROM {T_APPROVALS} WHERE request_id IN ({marks}) ORDER BY requested_at
            """,
            args=args,
        )
    )
    print("refund_ledger")
    display(
        spark.sql(
            f"SELECT request_id, order_id, amount FROM {T_LEDGER} "
            f"WHERE request_id IN ({marks}) ORDER BY issued_at",
            args=args,
        )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Path 1: a $20 refund runs automatically

# COMMAND ----------

small = submit_refund("10517", 20.00, "Item arrived with a scratch")
assert status_of(small) == "auto_approved"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Path 2: a $12,000 refund pauses, then a manager approves
# MAGIC
# MAGIC The first cell pauses. Nothing has been refunded yet, and the approval table shows
# MAGIC `pending`.

# COMMAND ----------

big = submit_refund("10482", 12000.00, "Customer returned the full shipment")
assert status_of(big) == "pending"
display(
    spark.sql(
        f"SELECT request_id, order_id, requested_amount, status FROM {T_APPROVALS} "
        "WHERE request_id = :request_id",
        args={"request_id": big},
    )
)

# COMMAND ----------

# The finance manager approves. The workflow resumes from its saved checkpoint.
decide(big, "approve", approver="finance.manager@cobaltworks.example", note="Verified")
assert status_of(big) == "approved"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Path 3: pending, then rejected

# COMMAND ----------

rejected = submit_refund("10490", 12000.00, "Customer says it never arrived")
decide(rejected, "reject", approver="finance.manager@cobaltworks.example",
       note="Tracking shows it was delivered")
assert status_of(rejected) == "rejected"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Path 4: pending, then edited to a lower amount

# COMMAND ----------

edited = submit_refund("10503", 7500.00, "Two of three items were damaged")
decide(edited, "edit", approver="finance.manager@cobaltworks.example",
       note="Refund the two damaged items only", amount=5000.00)
assert status_of(edited) == "approved_edited"

# COMMAND ----------

# MAGIC %md
# MAGIC ## A request that fails validation
# MAGIC
# MAGIC A refund larger than the order total stops before the risk check and writes nothing.

# COMMAND ----------

bad = submit_refund("10517", 500.00, "Refund more than the order total")
assert status_of(bad) is None

# COMMAND ----------

# MAGIC %md
# MAGIC ## The audit trail
# MAGIC
# MAGIC The approval table shows who decided what, and when. The ledger shows only the refunds
# MAGIC that were actually issued: the rejected request has no ledger row, and the edited one
# MAGIC shows the lower amount.

# COMMAND ----------

show_run()

# COMMAND ----------

# MAGIC %md
# MAGIC ## The trace
# MAGIC
# MAGIC Tracing recorded each `invoke`, including the resumed runs. Open the trace to see every
# MAGIC node that ran, its inputs and its outputs. Look at the traces of the approved request and
# MAGIC compare them with the one for the $20 refund.

# COMMAND ----------

get_trace_id = getattr(mlflow, "get_last_active_trace_id", None)
trace_id = get_trace_id() if get_trace_id else None
if trace_id:
    print("Most recent trace id:", trace_id)
    print("Find all traces on the Traces tab of this notebook's experiment.")
else:
    print("No trace was recorded. Check that tracing is on and the cells ran.")

print("Done. Next: run 05_guided_vs_unguided.")
