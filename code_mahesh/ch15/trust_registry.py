"""Validate a task for a remote agent against the CobaltWorks trust registry.

Run it from this folder: python trust_registry.py
It needs PyYAML (pip install pyyaml) and makes no network calls.

Notebook 03 adds two things to the chapter version: ``validate_registry``
checks the registry file itself, and ``authorize_task`` can also compare the
issuer and organization that a request presents with the registry entry.
Comparing strings is a stand-in. A real deployment verifies the caller's
token or signed Agent Card.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path

import yaml

REGISTRY_PATH = Path(__file__).with_name("trust_registry.yaml")
MAX_TASK_SECONDS = 15 * 60
ALLOWED_STATUSES = {"active", "revoked"}
REQUIRED_AGENT_FIELDS = (
    "agent_id",
    "organization",
    "identity",
    "capabilities",
    "allowed_data_classes",
    "status",
    "expires_on",
    "revoked_at",
)


class TrustError(Exception):
    """Raised when a remote agent may not receive, or answer, a task."""


@dataclass(frozen=True)
class TaskGrant:
    """What one task is allowed to do, tied to a tenant and an expiry."""

    agent_id: str
    tenant_id: str
    capability: str
    data_classes: frozenset[str]
    expires_at: datetime


def load_registry(path: Path = REGISTRY_PATH) -> dict:
    with path.open(encoding="utf-8") as handle:
        return yaml.safe_load(handle)


def validate_registry(registry: dict) -> list[str]:
    """Return a list of problems with the registry file (empty if clean)."""
    problems: list[str] = []
    known = set(registry.get("data_classes") or [])
    if not known:
        problems.append("data_classes is missing or empty")
    seen: set[str] = set()
    for index, agent in enumerate(registry.get("agents") or []):
        label = agent.get("agent_id", f"agents[{index}]")
        for field in REQUIRED_AGENT_FIELDS:
            if field not in agent:
                problems.append(f"{label}: missing field {field}")
        if label in seen:
            problems.append(f"{label}: duplicate agent_id")
        seen.add(label)
        identity = agent.get("identity") or {}
        for key in ("issuer", "agent_card_url"):
            value = str(identity.get(key, ""))
            if not value.startswith("https://"):
                problems.append(f"{label}: identity.{key} must be an https URL")
        if not agent.get("capabilities"):
            problems.append(f"{label}: no capabilities listed")
        extra = set(agent.get("allowed_data_classes") or []) - known
        if extra:
            problems.append(f"{label}: unknown data classes {sorted(extra)}")
        status = agent.get("status")
        if status not in ALLOWED_STATUSES:
            allowed = sorted(ALLOWED_STATUSES)
            problems.append(f"{label}: status must be one of {allowed}")
        if (status == "revoked") != (agent.get("revoked_at") is not None):
            problems.append(f"{label}: status and revoked_at disagree")
        try:
            date.fromisoformat(str(agent.get("expires_on")))
        except ValueError:
            problems.append(f"{label}: expires_on must be an ISO date")
    return problems


def _find_agent(registry: dict, agent_id: str) -> dict:
    for agent in registry["agents"]:
        if agent["agent_id"] == agent_id:
            return agent
    raise TrustError(f"{agent_id} is not in the trust registry")


def authorize_task(
    registry: dict,
    agent_id: str,
    capability: str,
    data_classes: set[str],
    tenant_id: str,
    now: datetime,
    issuer: str | None = None,
    organization: str | None = None,
) -> TaskGrant:
    """Return a grant for one task, or raise TrustError."""
    agent = _find_agent(registry, agent_id)
    if issuer is not None and issuer != agent["identity"]["issuer"]:
        raise TrustError(f"{agent_id} presented an unexpected issuer")
    if organization is not None and organization != agent["organization"]:
        raise TrustError(f"{agent_id} presented an unexpected organization")
    if agent["status"] != "active" or agent["revoked_at"] is not None:
        raise TrustError(f"{agent_id} is revoked or inactive")
    registry_expiry = datetime.combine(
        date.fromisoformat(agent["expires_on"]),
        datetime.max.time(),
        tzinfo=timezone.utc,
    )
    if registry_expiry <= now:
        raise TrustError(f"{agent_id} registry entry has expired")
    if capability not in agent["capabilities"]:
        raise TrustError(f"{agent_id} does not offer {capability}")
    known = set(registry["data_classes"])
    unknown = data_classes - known
    if unknown:
        raise TrustError(f"Unknown data classes: {sorted(unknown)}")
    blocked = data_classes - set(agent["allowed_data_classes"])
    if blocked:
        raise TrustError(f"Data classes not allowed: {sorted(blocked)}")
    ttl = min(MAX_TASK_SECONDS, int((registry_expiry - now).total_seconds()))
    return TaskGrant(
        agent_id=agent_id,
        tenant_id=tenant_id,
        capability=capability,
        data_classes=frozenset(data_classes),
        expires_at=datetime.fromtimestamp(now.timestamp() + ttl, timezone.utc),
    )


def check_result(grant: TaskGrant, tenant_id: str, now: datetime) -> None:
    """Reject a late or misdirected result before it is treated as input."""
    if tenant_id != grant.tenant_id:
        raise TrustError("Result belongs to a different tenant")
    if now >= grant.expires_at:
        raise TrustError("Task grant has expired")


def _demo() -> None:
    registry = load_registry()
    now = datetime(2028, 3, 6, 9, 0, tzinfo=timezone.utc)

    grant = authorize_task(
        registry,
        "apex-haulers-logistics",
        "rebooking_quote",
        {"shipment_reference", "delivery_window"},
        tenant_id="plant-3",
        now=now,
    )
    print("Granted:", grant)
    check_result(grant, "plant-3", now)

    attempts = [
        ("steel-supplier-pricing", "spot_price_quote", {"public"}),
        ("apex-haulers-logistics", "rebooking_quote", {"supplier_master"}),
        ("unknown-agent", "rebooking_quote", {"public"}),
    ]
    for agent_id, capability, classes in attempts:
        try:
            authorize_task(
                registry, agent_id, capability, classes, "plant-3", now
            )
        except TrustError as exc:
            print("Refused:", exc)


if __name__ == "__main__":
    _demo()
