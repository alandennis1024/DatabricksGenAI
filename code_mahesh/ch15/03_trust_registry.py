# Databricks notebook source
# MAGIC %md
# MAGIC # 03 - Trust registry for remote agents (Chapter 15)
# MAGIC
# MAGIC **Book section:** Looking to the Future > CobaltWorks in 2028 > *Make
# MAGIC protocol adoption reversible* (A2A: keep a trust registry, authenticate and
# MAGIC limit every task, treat results as untrusted).
# MAGIC
# MAGIC **What it does.** Loads `trust_registry.yaml` (Apex Haulers is active;
# MAGIC Northfield Steel is revoked), checks the registry file itself, and then
# MAGIC validates task requests for remote agents:
# MAGIC identity (issuer), organization, capability, allowed data classes, expiry,
# MAGIC and revocation. A request that passes gets a grant tied to one tenant and
# MAGIC a short expiry. A late or misdirected result is refused. The last step
# MAGIC runs the chapter's `validate_proposal` check on a remote rebooking
# MAGIC proposal.
# MAGIC
# MAGIC Comparing the issuer and organization strings is a stand-in. A real
# MAGIC deployment verifies the caller's token or signed Agent Card.
# MAGIC
# MAGIC **Prerequisites.** None. No tables, models, or permissions are needed.
# MAGIC
# MAGIC **Runtime.** Serverless compute, or a cluster on DBR 17 or later. The first
# MAGIC cell installs `pyyaml` from `requirements.txt`.
# MAGIC
# MAGIC **Creates.** Nothing. The registry file is read and checked in memory.

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
        if os.path.exists(os.path.join(folder, "trust_registry.yaml")):
            return folder
    raise RuntimeError(
        "trust_registry.yaml was not found next to this notebook. Clone the "
        "whole repository into a Databricks Git folder and open the notebook "
        "there."
    )


NOTEBOOK_DIR = _find_notebook_dir()
if NOTEBOOK_DIR not in sys.path:
    sys.path.insert(0, NOTEBOOK_DIR)

# COMMAND ----------

import copy
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path

import trust_registry as tr

registry = tr.load_registry(Path(os.path.join(NOTEBOOK_DIR, "trust_registry.yaml")))
problems = tr.validate_registry(registry)
print("Registry problems:", problems or "none")
assert not problems, problems

for agent in registry["agents"]:
    print(f"{agent['agent_id']:<26} {agent['organization']:<16} "
          f"{agent['status']:<8} expires {agent['expires_on']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. A request that passes
# MAGIC
# MAGIC The grant names the agent, the tenant, the capability, and the data classes,
# MAGIC and it expires after at most 15 minutes. `check_result` accepts an answer
# MAGIC only for the same tenant and before the grant expires.

# COMMAND ----------

NOW = datetime(2028, 3, 6, 9, 0, tzinfo=timezone.utc)  # inside Apex's validity

grant = tr.authorize_task(
    registry,
    "apex-haulers-logistics",
    "rebooking_quote",
    {"shipment_reference", "delivery_window"},
    tenant_id="plant-3",
    now=NOW,
    issuer="https://agents.apex.example",
    organization="Apex Haulers",
)
print("Granted:", grant)
tr.check_result(grant, "plant-3", NOW + timedelta(minutes=5))
print("Result at +5 minutes for plant-3: accepted")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Requests that are refused
# MAGIC
# MAGIC Each case breaks one rule. Every one must raise `TrustError`.

# COMMAND ----------

expired_registry = copy.deepcopy(registry)
expired_registry["agents"][0]["expires_on"] = "2028-03-01"


def refused(label, registry_used=registry, **overrides):
    request = dict(
        agent_id="apex-haulers-logistics",
        capability="rebooking_quote",
        data_classes={"shipment_reference"},
        tenant_id="plant-3",
        now=NOW,
    )
    request.update(overrides)
    try:
        tr.authorize_task(registry_used, **request)
    except tr.TrustError as exc:
        print(f"Refused ({label}): {exc}")
    else:
        raise AssertionError(f"{label} should have been refused")


refused("unknown agent", agent_id="unknown-agent")
refused("revoked agent", agent_id="steel-supplier-pricing",
        capability="spot_price_quote", data_classes={"public"})
refused("wrong issuer", issuer="https://agents.evil.example")
refused("wrong organization", organization="Apex Haulers Ltd")
refused("capability not offered", capability="spot_price_quote")
refused("data class not allowed", data_classes={"supplier_master"})
refused("data class not in the registry", data_classes={"payroll"})
refused("registry entry expired", registry_used=expired_registry)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. A late or misdirected result is refused

# COMMAND ----------

for label, tenant, when in [
    ("another tenant", "plant-9", NOW + timedelta(minutes=1)),
    ("after the grant expired", "plant-3", NOW + timedelta(minutes=16)),
]:
    try:
        tr.check_result(grant, tenant, when)
    except tr.TrustError as exc:
        print(f"Refused ({label}): {exc}")
    else:
        raise AssertionError(f"{label} should have been refused")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. A broken registry is caught before it is used
# MAGIC
# MAGIC `validate_registry` reports an agent with no issuer URL, a revoked status
# MAGIC without a revocation time, and an unknown data class.

# COMMAND ----------

broken = copy.deepcopy(registry)
broken["agents"][0]["identity"]["issuer"] = "http://agents.apex.example"
broken["agents"][0]["allowed_data_classes"].append("payroll")
broken["agents"][1]["revoked_at"] = None
found = tr.validate_registry(broken)
for line in found:
    print("Problem:", line)
assert len(found) == 3, found

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Treat the remote result as input until it is validated
# MAGIC
# MAGIC This is the chapter's check for a remote rebooking proposal. It refers to
# MAGIC the right shipment and must not have expired. Only a proposal that passes
# MAGIC moves on to CobaltWorks' own approval and commit steps (notebook 02).

# COMMAND ----------

from dataclasses import dataclass
from datetime import datetime, timezone


@dataclass(frozen=True)
class RebookingProposal:
    shipment_id: str
    carrier_id: str
    arrival_date: str
    quoted_cost: str
    expires_at: str


def validate_proposal(proposal: RebookingProposal, shipment_id: str) -> None:
    """Keep a remote proposal separate from the local commit operation."""
    if proposal.shipment_id != shipment_id:
        raise ValueError("Remote proposal refers to a different shipment")
    expires = datetime.fromisoformat(proposal.expires_at)
    if expires <= datetime.now(timezone.utc):
        raise ValueError("Remote proposal has expired")


soon = (datetime.now(timezone.utc) + timedelta(minutes=10)).isoformat()
past = (datetime.now(timezone.utc) - timedelta(minutes=10)).isoformat()

good = RebookingProposal("SHP-000123", "apex-haulers", "2028-03-09", "4200.00", soon)
validate_proposal(good, "SHP-000123")
print("Valid proposal accepted.")

for label, proposal, shipment in [
    ("different shipment", good, "SHP-000999"),
    ("expired", RebookingProposal("SHP-000123", "apex-haulers", "2028-03-09",
                                  "4200.00", past), "SHP-000123"),
]:
    try:
        validate_proposal(proposal, shipment)
    except ValueError as exc:
        print(f"Refused ({label}): {exc}")
    else:
        raise AssertionError(f"{label} should have been refused")
