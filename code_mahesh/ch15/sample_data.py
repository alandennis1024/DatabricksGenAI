"""Synthetic sample rows for the Chapter 15 operating views.

Everything is generated from a fixed random seed and is illustrative. The data
has a few planted patterns so that each view has something to show:

* groundedness drops during the last three days (Quality);
* the shipment status tool times out more often at 08:00-09:59 UTC (Reliability);
* small-model lookups are cheap and large-model analysis is not (Economics);
* high-risk requests are held for approval, and a few retrievals cross tenants
  or arrive in a burst (Control).
"""
from __future__ import annotations

import random
from datetime import datetime, timedelta, timezone
from decimal import Decimal

from cobalt_common import SAMPLE_TABLES, fq

COLUMNS = {
    "request_traces": (
        "trace_id", "request_time", "endpoint_name", "route", "model_name",
        "status", "latency_ms", "input_tokens", "cached_input_tokens",
        "output_tokens", "cost_usd", "task_success",
    ),
    "eval_scores": ("trace_id", "scored_at", "scorer_name", "score"),
    "endpoint_metrics": ("sample_time", "endpoint_name", "queue_depth"),
    "retrieval_events": (
        "trace_id", "event_time", "requester_tenant", "source_tenant",
        "chunks_returned", "request_json",
    ),
    "agent_actions": (
        "event_id", "trace_id", "event_time", "actor_type", "actor_id",
        "tool_name", "action_state", "policy_version", "source_refs",
        "idempotency_key", "redaction_status",
    ),
}
# (endpoint, model, price per million input tokens, price per million output)
ROUTES = {
    "lookup": ("status-lookup-small", "small-fast-model", 0.15, 0.60),
    "analysis": ("exception-agent", "large-reasoning-model", 3.00, 12.00),
    "high_risk_action": ("exception-agent", "large-reasoning-model", 3.00, 12.00),
}
ROUTE_WEIGHTS = (("lookup", 0.82), ("analysis", 0.12), ("high_risk_action", 0.06))
TENANTS = ("plant-1", "plant-2", "plant-3", "plant-4")
QUERY_TYPES = ("shipment_status", "carrier_rules", "supplier_contract")


def sql_literal(value) -> str:
    """Render a Python value as a Databricks SQL literal."""
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, datetime):
        return "TIMESTAMP'" + value.strftime("%Y-%m-%d %H:%M:%S") + "'"
    if isinstance(value, (int, Decimal)):
        return str(value)
    if isinstance(value, float):
        return repr(round(value, 4))
    if isinstance(value, (list, tuple)):
        return "array(" + ", ".join(sql_literal(v) for v in value) + ")"
    return "'" + str(value).replace("'", "''") + "'"


def _pick_route(rng: random.Random) -> str:
    roll, total = rng.random(), 0.0
    for route, weight in ROUTE_WEIGHTS:
        total += weight
        if roll < total:
            return route
    return ROUTE_WEIGHTS[-1][0]


def _request_time(rng: random.Random, day: datetime) -> datetime:
    # Morning rush: half of the requests arrive between 08:00 and 09:59.
    hour = rng.choice((8, 9)) if rng.random() < 0.5 else rng.randint(6, 17)
    return day.replace(
        hour=hour, minute=rng.randint(0, 59), second=rng.randint(0, 59),
        microsecond=0,
    )


def build_sample(now: datetime, days: int = 45, per_day: int = 16, seed: int = 15):
    """Return ``{table_name: [row, ...]}`` for the five sample tables."""
    rng = random.Random(seed)
    today = now.astimezone(timezone.utc).replace(
        hour=0, minute=0, second=0, microsecond=0
    )
    rows = {name: [] for name in SAMPLE_TABLES}
    event_no = 0

    def add_action(trace, when, actor, actor_id, tool, state, **extra):
        nonlocal event_no
        event_no += 1
        rows["agent_actions"].append((
            f"evt-{event_no:06d}", trace, when, actor, actor_id, tool, state,
            "policy-v12", extra.get("refs", []), extra.get("idem"),
            "redacted" if actor == "agent" else "not_required",
        ))

    trace_no = 0
    for offset in range(days - 1, -1, -1):
        day = today - timedelta(days=offset)
        recent = offset < 3
        for _ in range(per_day):
            trace_no += 1
            trace = f"trc-{trace_no:06d}"
            when = _request_time(rng, day)
            route = _pick_route(rng)
            endpoint, model, price_in, price_out = ROUTES[route]
            rush = when.hour in (8, 9)
            draw = rng.random()
            if draw < 0.015 + (0.02 if rush else 0.0):
                status = "timeout"
            elif draw < 0.04 + (0.02 if rush else 0.0):
                status = "error"
            else:
                status = "ok"
            base_in, base_out, base_ms = {
                "lookup": (800, 120, 900),
                "analysis": (6000, 700, 4500),
                "high_risk_action": (9000, 900, 6500),
            }[route]
            tokens_in = int(base_in * rng.uniform(0.7, 1.4))
            tokens_out = int(base_out * rng.uniform(0.7, 1.4))
            cached = int(tokens_in * rng.uniform(0.4, 0.7))
            latency = int(base_ms * rng.uniform(0.6, 1.8) * (1.5 if rush else 1.0))
            cost = Decimal(str(round(
                tokens_in * price_in / 1e6 + tokens_out * price_out / 1e6, 6
            ))).quantize(Decimal("0.000001"))
            success_rate = 0.95 if route == "lookup" else 0.88
            success = status == "ok" and rng.random() < success_rate
            rows["request_traces"].append((
                trace, when, endpoint, route, model, status, latency, tokens_in,
                cached, tokens_out, cost, success,
            ))

            drop = 0.12 if recent else 0.0
            scores = [
                ("groundedness", 0.93 - drop),
                ("citation_correctness", 0.95),
            ]
            if rng.random() < 0.15:
                scores.append(("refusal_accuracy", 0.9))
            for scorer, mean in scores:
                score = min(1.0, max(0.0, rng.gauss(mean, 0.05)))
                rows["eval_scores"].append((
                    trace, when + timedelta(minutes=5), scorer, score,
                ))

            tenant = rng.choice(TENANTS)
            source = tenant
            if rng.random() < 0.01:
                source = rng.choice([t for t in TENANTS if t != tenant])
            rows["retrieval_events"].append((
                trace, when, tenant, source, rng.randint(3, 8),
                '{"query_type": "%s"}' % rng.choice(QUERY_TYPES),
            ))

            refs = [f"sap:order/{rng.randint(1000, 9999)}"]
            if route == "lookup":
                timeout_rate = 0.30 if rush else 0.03
                state = "tool_timeout" if rng.random() < timeout_rate else "completed"
                add_action(trace, when, "agent", "shipment-exception-agent",
                           "get_shipment_status", state, refs=refs)
                continue
            add_action(trace, when, "agent", "shipment-exception-agent",
                       "get_shipment_status", "completed", refs=refs)
            if route == "analysis":
                continue
            step = when + timedelta(seconds=20)
            add_action(trace, step, "agent", "shipment-exception-agent",
                       "propose_rebooking", "proposed", refs=refs)
            roll = rng.random()
            if roll < 0.10:
                add_action(trace, step, "policy_service", "policy-engine",
                           "propose_rebooking", "policy_denied", refs=refs)
            elif roll < 0.15:
                add_action(trace, step, "policy_service", "policy-engine",
                           "propose_rebooking", "permission_denied", refs=refs)
            else:
                add_action(trace, step, "policy_service", "policy-engine",
                           "propose_rebooking", "approval_requested", refs=refs)
                if rng.random() < 0.85:
                    approved = step + timedelta(minutes=rng.randint(2, 30))
                    add_action(trace, approved, "human", f"manager-{rng.randint(1, 9)}",
                               "propose_rebooking", "approved", refs=refs)
                    add_action(trace, approved + timedelta(seconds=5), "agent",
                               "shipment-exception-agent", "commit_rebooking",
                               "committed", refs=refs, idem=f"idem-{trace}")

    # One endpoint sample per hour for two weeks; queues peak in the morning.
    start = today - timedelta(days=13)
    for hours in range(14 * 24):
        sample = start + timedelta(hours=hours)
        for endpoint in ("status-lookup-small", "exception-agent"):
            peak = 6 if sample.hour in (8, 9) else 1
            depth = max(0, int(rng.gauss(2 * peak, 1 + peak / 2)))
            rows["endpoint_metrics"].append((sample, endpoint, depth))

    # One trace that retrieves far more often than normal.
    burst_time = today - timedelta(days=1) + timedelta(hours=13)
    for i in range(55):
        rows["retrieval_events"].append((
            "trc-burst-0001", burst_time + timedelta(seconds=i), "plant-2",
            "plant-2", 5, '{"query_type": "supplier_contract"}',
        ))
    return rows


def insert_statements(catalog: str, schema: str, rows, chunk: int = 200):
    """Build INSERT statements: OVERWRITE for a table's first chunk, then INTO.

    Values follow the column order of the table DDL (see COLUMNS).
    Returns ``[(table_name, sql), ...]`` in a safe order.
    """
    statements = []
    for table in SAMPLE_TABLES:
        data = rows[table]
        for start in range(0, len(data), chunk):
            values = ",\n".join(
                "(" + ", ".join(sql_literal(v) for v in row) + ")"
                for row in data[start:start + chunk]
            )
            verb = "INSERT OVERWRITE" if start == 0 else "INSERT INTO"
            statements.append(
                (table, f"{verb} {fq(catalog, schema, table)} VALUES\n{values}")
            )
    return statements
