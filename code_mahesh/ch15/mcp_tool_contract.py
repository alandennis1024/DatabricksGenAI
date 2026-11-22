"""Sketch of an MCP tool contract: read-only, propose, and commit tools.

The tools are plain functions over in-memory data, so the contract
(schemas, approval token, idempotency key) can be read and tested without
any external call. In a real server, each method becomes an MCP tool and
the approval service, secret, and audit writer live outside this file.
"""
from __future__ import annotations

import base64
import hashlib
import hmac
import json
import re
from dataclasses import dataclass
from typing import Callable

# Demo-only secret. In production, read it from a Databricks secret scope.
APPROVAL_SECRET = b"demo-only-secret"
APPROVAL_TTL_SECONDS = 900

# The "define the basics" checklist for an MCP server, kept next to the code.
SERVER_MANIFEST = {
    "owner": "logistics-platform@cobaltworks.example",
    "protocol_version": "2026-07-28",
    "authentication": "OAuth 2.0, on behalf of the calling user",
    "rate_limit_per_minute": 60,
    "deprecation_policy": "Announce 90 days before removing a tool version",
    "read_only_tools": ["get_shipment_status"],
    "write_tools": ["propose_rebooking", "commit_rebooking"],
}

SHIPMENT_ID = re.compile(r"^SHP-\d{6}$")


class PolicyDenied(Exception):
    """Raised when a proposal breaks a business rule."""


class ApprovalError(Exception):
    """Raised when an approval token is missing, invalid, or expired."""


def _check_shipment_id(value: str) -> None:
    if not SHIPMENT_ID.match(value):
        raise ValueError(f"Invalid shipment id: {value!r}")


@dataclass(frozen=True)
class ShipmentStatusRequest:
    shipment_id: str

    def __post_init__(self) -> None:
        _check_shipment_id(self.shipment_id)


@dataclass(frozen=True)
class ShipmentStatus:
    shipment_id: str
    carrier_status: str
    last_update: str
    source_ref: str


@dataclass(frozen=True)
class RebookingRequest:
    shipment_id: str
    must_arrive_by: str  # ISO date, for example 2028-03-10
    max_cost_cents: int

    def __post_init__(self) -> None:
        _check_shipment_id(self.shipment_id)
        if self.max_cost_cents <= 0:
            raise ValueError("max_cost_cents must be positive")


@dataclass(frozen=True)
class CarrierQuote:
    """A quote from a remote agent, already schema-validated upstream."""

    carrier_id: str
    arrival_date: str  # ISO date
    cost_cents: int
    expires_at: int  # epoch seconds


@dataclass(frozen=True)
class RebookingProposal:
    proposal_id: str
    shipment_id: str
    carrier_id: str
    arrival_date: str
    quoted_cost_cents: int
    expires_at: int
    state: str = "pending_approval"


@dataclass(frozen=True)
class CommitRequest:
    proposal_id: str
    approval_token: str
    idempotency_key: str

    def __post_init__(self) -> None:
        if not 8 <= len(self.idempotency_key) <= 64:
            raise ValueError("idempotency_key must be 8-64 characters")


@dataclass(frozen=True)
class CommitConfirmation:
    confirmation_id: str
    proposal_id: str
    idempotency_key: str


def _sign(payload: bytes) -> str:
    return hmac.new(APPROVAL_SECRET, payload, hashlib.sha256).hexdigest()


def issue_approval_token(
    proposal: RebookingProposal, approver_id: str, now: int
) -> str:
    """Stand-in for the approval service: a short-lived, signed token."""
    claims = {
        "proposal_id": proposal.proposal_id,
        "shipment_id": proposal.shipment_id,
        "quoted_cost_cents": proposal.quoted_cost_cents,
        "approver_id": approver_id,
        "exp": now + APPROVAL_TTL_SECONDS,
    }
    payload = base64.urlsafe_b64encode(json.dumps(claims).encode())
    return payload.decode() + "." + _sign(payload)


class ShipmentTools:
    """The three tools, in the order a team should ship them."""

    def __init__(self, clock: Callable[[], int]) -> None:
        self._clock = clock
        self._statuses: dict[str, ShipmentStatus] = {}
        self._proposals: dict[str, RebookingProposal] = {}
        self._commits: dict[str, CommitConfirmation] = {}

    def load_status(self, status: ShipmentStatus) -> None:
        self._statuses[status.shipment_id] = status

    def get_shipment_status(
        self, req: ShipmentStatusRequest
    ) -> ShipmentStatus:
        """Read-only: ship this first and let it prove itself."""
        return self._statuses[req.shipment_id]

    def propose_rebooking(
        self, req: RebookingRequest, quote: CarrierQuote
    ) -> RebookingProposal:
        """Records an intent for review. Nothing is booked."""
        now = self._clock()
        if quote.expires_at <= now:
            raise PolicyDenied("Carrier quote has expired")
        if quote.cost_cents > req.max_cost_cents:
            raise PolicyDenied("Quote is above the spending limit")
        if quote.arrival_date > req.must_arrive_by:
            raise PolicyDenied("Quote arrives after the deadline")
        seed = f"{req.shipment_id}|{quote.carrier_id}|{quote.cost_cents}"
        digest = hashlib.sha256(seed.encode()).hexdigest()[:10]
        proposal = RebookingProposal(
            proposal_id="PRP-" + digest,
            shipment_id=req.shipment_id,
            carrier_id=quote.carrier_id,
            arrival_date=quote.arrival_date,
            quoted_cost_cents=quote.cost_cents,
            expires_at=quote.expires_at,
        )
        self._proposals[proposal.proposal_id] = proposal
        return proposal

    def _check_token(self, token: str, proposal: RebookingProposal) -> None:
        try:
            payload_text, signature = token.rsplit(".", 1)
            payload = payload_text.encode()
            claims = json.loads(base64.urlsafe_b64decode(payload))
        except ValueError as exc:
            raise ApprovalError("Malformed approval token") from exc
        if not hmac.compare_digest(signature, _sign(payload)):
            raise ApprovalError("Bad approval signature")
        if claims["exp"] <= self._clock():
            raise ApprovalError("Approval token has expired")
        bound = (
            claims["proposal_id"] == proposal.proposal_id
            and claims["shipment_id"] == proposal.shipment_id
            and claims["quoted_cost_cents"] == proposal.quoted_cost_cents
        )
        if not bound:
            raise ApprovalError("Token is not tied to this shipment and quote")

    def commit_rebooking(self, req: CommitRequest) -> CommitConfirmation:
        """Write: needs an approval token and an idempotency key."""
        proposal = self._proposals.get(req.proposal_id)
        if proposal is None:
            raise PolicyDenied("Unknown proposal")
        earlier = self._commits.get(req.idempotency_key)
        if earlier is not None:
            if earlier.proposal_id != req.proposal_id:
                raise PolicyDenied("Idempotency key was used for another task")
            return earlier  # a retry returns the first result, once
        if proposal.expires_at <= self._clock():
            raise PolicyDenied("Proposal has expired")
        self._check_token(req.approval_token, proposal)
        confirmation = CommitConfirmation(
            confirmation_id="CNF-" + req.proposal_id[4:],
            proposal_id=req.proposal_id,
            idempotency_key=req.idempotency_key,
        )
        self._commits[req.idempotency_key] = confirmation
        return confirmation


def _demo() -> None:
    now = 1_800_000_000
    tools = ShipmentTools(clock=lambda: now)
    tools.load_status(
        ShipmentStatus("SHP-000123", "delayed", "2028-03-06", "sap:order/9981")
    )
    print(tools.get_shipment_status(ShipmentStatusRequest("SHP-000123")))

    request = RebookingRequest("SHP-000123", "2028-03-10", 450_000)
    quote = CarrierQuote("apex-haulers", "2028-03-09", 420_000, now + 600)
    proposal = tools.propose_rebooking(request, quote)
    print(proposal)

    no_approval = CommitRequest(proposal.proposal_id, "x.y", "key-0001")
    try:
        tools.commit_rebooking(no_approval)
    except ApprovalError as exc:
        print("Blocked without approval:", exc)

    token = issue_approval_token(proposal, "manager-17", now)
    commit = CommitRequest(proposal.proposal_id, token, "key-0001")
    first = tools.commit_rebooking(commit)
    second = tools.commit_rebooking(commit)
    assert first == second, "a retry must not book twice"
    print("Committed once:", first)

    expensive = CarrierQuote("apex-haulers", "2028-03-09", 900_000, now + 600)
    try:
        tools.propose_rebooking(request, expensive)
    except PolicyDenied as exc:
        print("Denied:", exc)


if __name__ == "__main__":
    _demo()
