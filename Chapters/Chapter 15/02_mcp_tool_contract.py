# Databricks notebook source
# MAGIC %md
# MAGIC # 02 - MCP tool contract: read first, propose, then commit (Chapter 15)
# MAGIC
# MAGIC **Book section:** Looking to the Future > CobaltWorks in 2028 > *Make
# MAGIC protocol adoption reversible* (MCP rules: read-only tools first, separate
# MAGIC proposal from commit).
# MAGIC
# MAGIC **What it does.** Runs the CobaltWorks shipment tool contract from
# MAGIC `mcp_tool_contract.py` as plain Python:
# MAGIC 1. `get_shipment_status` (read-only) is the first tool to ship.
# MAGIC 2. `propose_rebooking` checks the quote against the spending limit, the
# MAGIC    deadline, and the quote's expiry, but books nothing.
# MAGIC 3. `commit_rebooking` runs only with a signed, short-lived approval token
# MAGIC    tied to one shipment and quote, plus an idempotency key, so a retry
# MAGIC    cannot book twice.
# MAGIC
# MAGIC Each rule is shown working, and then shown refusing a bad call. It is a
# MAGIC sketch of the contract, not an MCP server: there is no network call, and
# MAGIC the signing secret is a demo value. In production the secret lives in a
# MAGIC Databricks secret scope and the approval service is a separate system.
# MAGIC
# MAGIC **Prerequisites.** None. No tables, models, or permissions are needed.
# MAGIC
# MAGIC **Runtime.** Serverless compute, or a cluster on DBR 17 or later.
# MAGIC
# MAGIC **Creates.** Nothing. All state is in memory.

# COMMAND ----------

# MAGIC %pip install -q -r requirements.txt

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import os
import sys


def _find_notebook_dir():
    candidates = []
    try:
        ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
        folder = os.path.dirname(ctx.notebookPath().get())
        candidates += [folder, "/Workspace" + folder]
    except Exception:
        pass
    candidates.append(os.getcwd())
    for folder in candidates:
        if os.path.exists(os.path.join(folder, "mcp_tool_contract.py")):
            return folder
    raise RuntimeError(
        "mcp_tool_contract.py was not found next to this notebook. Clone the "
        "whole repository into a Databricks Git folder and open the notebook "
        "there."
    )


NOTEBOOK_DIR = _find_notebook_dir()
if NOTEBOOK_DIR not in sys.path:
    sys.path.insert(0, NOTEBOOK_DIR)

# COMMAND ----------

import json

import mcp_tool_contract as mcp

print(json.dumps(mcp.SERVER_MANIFEST, indent=2))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. A read-only tool, with a validated input
# MAGIC
# MAGIC The request type checks the shipment id (`SHP-` and six digits) before the
# MAGIC tool runs. A malformed id never reaches the tool.

# COMMAND ----------

NOW = 1_800_000_000  # a fixed clock, so the demo gives the same result each time
clock = {"now": NOW}
tools = mcp.ShipmentTools(clock=lambda: clock["now"])
tools.load_status(
    mcp.ShipmentStatus("SHP-000123", "delayed", "2028-03-06", "sap:order/9981")
)

print(tools.get_shipment_status(mcp.ShipmentStatusRequest("SHP-000123")))

try:
    mcp.ShipmentStatusRequest("123; DROP TABLE shipments")
except ValueError as exc:
    print("Rejected input:", exc)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Propose: check the rules, book nothing
# MAGIC
# MAGIC A proposal is a record that a person or an evaluator can inspect. The
# MAGIC three denials below are the business rules, which sit outside the model.

# COMMAND ----------

request = mcp.RebookingRequest("SHP-000123", "2028-03-10", 450_000)
quote = mcp.CarrierQuote("apex-haulers", "2028-03-09", 420_000, NOW + 600)
proposal = tools.propose_rebooking(request, quote)
print("Proposal:", proposal)

bad_quotes = {
    "above the spending limit": mcp.CarrierQuote(
        "apex-haulers", "2028-03-09", 900_000, NOW + 600
    ),
    "arrives after the deadline": mcp.CarrierQuote(
        "apex-haulers", "2028-03-12", 420_000, NOW + 600
    ),
    "already expired": mcp.CarrierQuote("apex-haulers", "2028-03-09", 420_000, NOW - 1),
}
for label, bad in bad_quotes.items():
    try:
        tools.propose_rebooking(request, bad)
        raise AssertionError(f"quote {label} should have been denied")
    except mcp.PolicyDenied as exc:
        print(f"Denied ({label}): {exc}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Commit needs an approval token and an idempotency key
# MAGIC
# MAGIC Without a valid token the write is refused. With one, the first call
# MAGIC commits and a retry with the same key returns the first confirmation, so
# MAGIC the booking happens once.

# COMMAND ----------

try:
    tools.commit_rebooking(mcp.CommitRequest(proposal.proposal_id, "x.y", "key-0001"))
except mcp.ApprovalError as exc:
    print("Blocked without approval:", exc)

token = mcp.issue_approval_token(proposal, "manager-17", NOW)
commit = mcp.CommitRequest(proposal.proposal_id, token, "key-0001")
first = tools.commit_rebooking(commit)
second = tools.commit_rebooking(commit)
assert first == second, "a retry must not book twice"
print("Committed once:", first)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. More ways the commit is refused
# MAGIC
# MAGIC A tampered token, a token for a different quote, an expired token, a
# MAGIC reused idempotency key, and a too-short key are all refused. Each case
# MAGIC uses a fresh tool instance so the earlier commit does not hide the check.

# COMMAND ----------


def fresh_tools():
    t = mcp.ShipmentTools(clock=lambda: clock["now"])
    t.load_status(
        mcp.ShipmentStatus("SHP-000123", "delayed", "2028-03-06", "sap:order/9981")
    )
    return t


def expect(error_type, label, action):
    try:
        action()
    except error_type as exc:
        print(f"Refused ({label}): {exc}")
    else:
        raise AssertionError(f"{label} should have been refused")


t = fresh_tools()
p = t.propose_rebooking(request, quote)
good_token = mcp.issue_approval_token(p, "manager-17", NOW)

forged = good_token[:-4] + ("0000" if not good_token.endswith("0000") else "1111")
expect(
    mcp.ApprovalError, "forged signature",
    lambda: t.commit_rebooking(mcp.CommitRequest(p.proposal_id, forged, "key-0002")),
)

other_quote = mcp.CarrierQuote("apex-haulers", "2028-03-09", 410_000, NOW + 600)
other = t.propose_rebooking(request, other_quote)
other_token = mcp.issue_approval_token(other, "manager-17", NOW)
expect(
    mcp.ApprovalError, "token for another quote",
    lambda: t.commit_rebooking(
        mcp.CommitRequest(p.proposal_id, other_token, "key-0003")
    ),
)

expect(
    mcp.PolicyDenied, "unknown proposal",
    lambda: t.commit_rebooking(
        mcp.CommitRequest("PRP-0000000000", good_token, "key-0004")
    ),
)

t.commit_rebooking(mcp.CommitRequest(p.proposal_id, good_token, "key-0005"))
expect(
    mcp.PolicyDenied, "idempotency key reused for another task",
    lambda: t.commit_rebooking(
        mcp.CommitRequest(other.proposal_id, other_token, "key-0005")
    ),
)

expect(
    ValueError, "idempotency key too short",
    lambda: mcp.CommitRequest(p.proposal_id, good_token, "k"),
)

# An approval is valid for 15 minutes. After that the token is refused, even
# though the proposal itself has not expired yet.
t2 = fresh_tools()
p2 = t2.propose_rebooking(
    request, mcp.CarrierQuote("apex-haulers", "2028-03-09", 420_000, NOW + 3600)
)
old_token = mcp.issue_approval_token(p2, "manager-17", NOW)
clock["now"] = NOW + mcp.APPROVAL_TTL_SECONDS + 1
expect(
    mcp.ApprovalError, "expired approval token",
    lambda: t2.commit_rebooking(
        mcp.CommitRequest(p2.proposal_id, old_token, "key-0006")
    ),
)
clock["now"] = NOW

# COMMAND ----------

# MAGIC %md
# MAGIC ## What to take from this
# MAGIC
# MAGIC * The read-only tool can ship, and prove itself, before any write tool
# MAGIC   exists.
# MAGIC * Proposal and commit are separate tools, so an evaluator and a human
# MAGIC   reviewer have a safe point to inspect the intended side effect.
# MAGIC * The approval token is tied to one shipment and one quote and expires in
# MAGIC   15 minutes. The idempotency key makes retries safe.
# MAGIC * The rules are plain code outside the model. A model cannot talk its way
# MAGIC   past them.
