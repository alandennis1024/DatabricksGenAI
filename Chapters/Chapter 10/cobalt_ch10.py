"""Chapter 10: offline-testable CobaltWorks sample data and business contracts.

No network calls, Databricks secrets, or LLM calls occur in this module.
These examples are synthetic and are not real manufacturing or shipping records.
"""
from __future__ import annotations

import csv
import io
import json
import re
from datetime import date, datetime, timedelta
from typing import Any, Mapping

ID_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
ENDPOINT_RE = re.compile(r"^[A-Za-z0-9_.-]{3,140}$")
HUB_CODES = frozenset({"ORD", "MDW", "GYY"})
SUPPLIER_COLUMNS = ("Delivery_TX", "Est_Arrival", "Actual_Arrival", "part_type", "quantity")
APEX_COLUMNS = ("TX_ID", "TS_INIT", "UNIT_VAL", "LOC_CODE", "fuel_surcharge")


def sql_identifier(value: str) -> str:
    """Validate catalog, schema, and volume names before formatting SQL."""
    if not isinstance(value, str) or not ID_RE.fullmatch(value):
        raise ValueError(f"Unsafe SQL identifier: {value!r}")
    return value


def serving_endpoint_name(value: str) -> str:
    if not isinstance(value, str) or not ENDPOINT_RE.fullmatch(value):
        raise ValueError("Endpoint must contain only letters, digits, '_', '-', '.'")
    return value


def monday_of_week(day: date) -> date:
    return day - timedelta(days=day.weekday())


def week_bounds(day: date) -> tuple[date, date]:
    start = monday_of_week(day)
    return start, start + timedelta(days=7)


def dt_of(week_start: date, day_offset: int, hh: int, mm: int = 0) -> datetime:
    if week_start.weekday() != 0:
        raise ValueError("The demo week_start must be a Monday")
    return datetime.combine(week_start + timedelta(days=day_offset),
                            datetime.min.time()).replace(hour=hh, minute=mm)


def supplier_sample_rows(week_start: date) -> list[dict[str, str]]:
    """Seven rows: six valid and one invalid quantity.

    Valid rows include five completed deliveries, three within two hours of ETA.
    The open delivery has no actual arrival and must be excluded from the rate.
    """
    cases = [
        ("S-001", 0, 8, 0, 1, "steel_coil", 10),
        ("S-002", 0, 10, 0, 3, "steel_coil", 5),
        ("S-003", 1, 11, 0, 1, "steel_plate", 2),
        ("S-004", 1, 9, 0, None, "steel_plate", 4),
        ("S-005", 2, 10, 0, 1, "steel_coil", 7),
        ("S-006", 2, 10, 0, 3, "steel_plate", 6),
        ("S-BAD-QTY", 3, 9, 0, 0, "steel_coil", 0),
    ]
    result = []
    for ident, day_no, hh, mm, arrival_delta_hr, material, quantity in cases:
        eta = dt_of(week_start, day_no, hh, mm)
        arrival = (eta + timedelta(hours=arrival_delta_hr)) if arrival_delta_hr is not None else None
        result.append({
            "Delivery_TX": ident,
            "Est_Arrival": eta.strftime("%Y-%m-%d %H:%M:%S"),
            "Actual_Arrival": arrival.strftime("%Y-%m-%d %H:%M:%S") if arrival else "",
            "part_type": material,
            "quantity": str(quantity),
        })
    return result


def apex_sample_rows(week_start: date) -> list[dict[str, str]]:
    """Ten rows: eight valid, two rejected; four valid Chicago heavy loads.

    TS_INIT intentionally uses YYYY-DD-MM HH:mm, as in the chapter example.
    A-003 weighs exactly 5,000 kg and must NOT count as a heavy load (>5,000).
    """
    cases = [
        ("A-001", 0, 8, 30, 5200, "ORD", "120.00"),
        ("A-002", 0, 9, 0, 7000, "MDW", ""),
        ("A-003", 1, 12, 0, 5000, "GYY", "89.00"),
        ("A-004", 1, 9, 0, 6500, "LHR", ""),
        ("A-005", 2, 10, 0, 8000, "GYY", "115.00"),
        ("A-006", 3, 11, 0, 4000, "ORD", ""),
        ("A-007", 4, 12, 0, 5100, "ORD", "104.00"),
        ("A-008", 5, 8, 0, 5300, "YYZ", "93.00"),
        ("A-BAD-NEG", 2, 8, 0, -50, "ORD", ""),
        ("A-BAD-LOC", 2, 8, 0, 8000, "XX1", ""),
    ]
    rows = []
    for ident, day_no, hh, mm, kg, loc, fuel in cases:
        rows.append({
            "TX_ID": ident,
            "TS_INIT": dt_of(week_start, day_no, hh, mm).strftime("%Y-%d-%m %H:%M"),
            "UNIT_VAL": str(kg),
            "LOC_CODE": loc,
            "fuel_surcharge": fuel,
        })
    return rows


def render_csv(rows: list[Mapping[str, str]], fieldnames: tuple[str, ...]) -> str:
    out = io.StringIO(newline="")
    writer = csv.DictWriter(out, fieldnames=fieldnames, lineterminator="\n")
    writer.writeheader()
    for row in rows:
        writer.writerow(row)
    return out.getvalue()


def parse_csv(content: str) -> list[dict[str, str]]:
    return list(csv.DictReader(io.StringIO(content)))


def normalize_supplier(row: Mapping[str, str]) -> dict[str, Any]:
    """Map original supplier fields into typed Bronze record (no quality filtering)."""
    eta = datetime.strptime(row["Est_Arrival"], "%Y-%m-%d %H:%M:%S")
    actual = (datetime.strptime(row["Actual_Arrival"], "%Y-%m-%d %H:%M:%S")
              if row.get("Actual_Arrival") else None)
    return {
        "transaction_id": row["Delivery_TX"], "eta_timestamp": eta,
        "actual_arrival_timestamp": actual, "part_type": row["part_type"],
        "quantity": int(row["quantity"]),
    }


def normalize_apex(row: Mapping[str, str]) -> dict[str, Any]:
    return {
        "transaction_id": row["TX_ID"],
        "start_timestamp": datetime.strptime(row["TS_INIT"], "%Y-%d-%m %H:%M"),
        "cargo_weight_kg": float(row["UNIT_VAL"]),
        "destination_code": row["LOC_CODE"],
        "fuel_surcharge": float(row["fuel_surcharge"]) if row.get("fuel_surcharge") else None,
    }


def supplier_rejection_reason(r: Mapping[str, Any]) -> str | None:
    if not r.get("transaction_id") or not str(r["transaction_id"]).strip():
        return "missing_id_fail"
    if (isinstance(r.get("quantity"), bool)
            or not isinstance(r.get("quantity"), (float, int))
            or r["quantity"] <= 0):
        return "invalid_quantity_drop"
    if not isinstance(r.get("eta_timestamp"), datetime):
        return "missing_eta_drop"
    return None


def apex_rejection_reason(r: Mapping[str, Any]) -> str | None:
    if not r.get("transaction_id") or not str(r["transaction_id"]).strip():
        return "missing_id_fail"
    weight = r.get("cargo_weight_kg")
    if isinstance(weight, bool) or not isinstance(weight, (int, float)) or weight <= 0:
        return "invalid_weight_drop"
    if not isinstance(r.get("start_timestamp"), datetime):
        return "invalid_start_drop"
    if not re.fullmatch(r"[A-Z]{3}", str(r.get("destination_code", ""))):
        return "invalid_location_drop"
    return None


def supplier_on_time_metrics(rows: list[Mapping[str, Any]], grace_hours: float = 2.0) -> dict[str, Any]:
    if grace_hours < 0:
        raise ValueError("grace_hours must not be negative")
    clean = [r for r in rows if supplier_rejection_reason(r) is None]
    done = [r for r in clean if isinstance(r.get("actual_arrival_timestamp"), datetime)]
    on_time = sum(int(r["actual_arrival_timestamp"] <= r["eta_timestamp"] + timedelta(hours=grace_hours))
                  for r in done)
    return {"valid_rows": len(clean), "completed_deliveries": len(done),
            "on_time_deliveries": on_time,
            "on_time_rate": on_time / len(done) if done else None}


def chicago_heavy_loads(rows: list[Mapping[str, Any]], week_start: date,
                        threshold_kg: float = 5000.0) -> int:
    week_end = week_start + timedelta(days=7)
    return sum(1 for r in rows
               if apex_rejection_reason(r) is None
               and week_start <= r["start_timestamp"].date() < week_end
               and r["destination_code"] in HUB_CODES
               and r["cargo_weight_kg"] > threshold_kg)


def telemetry_cases(count: int = 20) -> list[dict[str, Any]]:
    """Deterministic, obviously synthetic rising-temperature scenarios.

    Readings aren't physical predictions; they demonstrate test-fixture generation.
    """
    if not 1 <= count <= 1000:
        raise ValueError("Count must be in [1, 1000]")
    out = []
    for i in range(count):
        first = 45.0 + (i % 10)
        readings = [first, first + 12.0, first + 27.0]
        out.append({"scenario_id": f"SYN-ARM-{i+1:03d}",
                    "motor_temps": readings,
                    "status": "FAILING" if readings[-1] >= 80.0 else "WARNING",
                    "is_synthetic": True})
    return out


def validate_telemetry_json(raw: str) -> dict[str, Any]:
    """Strict validator for optional model-generated text; rejects untrusted output."""
    value = json.loads(raw)
    if not isinstance(value, dict) or set(value) != {"id", "timestamp", "motor_temp", "status"}:
        raise ValueError("Expected id, timestamp, motor_temp, status and no extra fields")
    if not isinstance(value["id"], str) or not value["id"].strip():
        raise ValueError("id must be a non-empty string")
    if (not isinstance(value["timestamp"], str)
            or "T" not in value["timestamp"]):
        raise ValueError("timestamp must be an ISO 8601 datetime string")
    datetime.fromisoformat(value["timestamp"].replace("Z", "+00:00"))
    temps = value["motor_temp"]
    if (not isinstance(temps, list) or len(temps) < 2
            or not all(isinstance(x, (float, int)) and not isinstance(x, bool) for x in temps)
            or any(b <= a for a, b in zip(temps, temps[1:]))):
        raise ValueError("motor_temp must be a strictly increasing numeric list")
    if value["status"] not in ("FAILING", "WARNING"):
        raise ValueError("status must be FAILING or WARNING")
    return value


DEFAULT_WIDGETS = {
    "catalog": "cobalt_ops", "schema": "ch10", "volume": "ch10_demo_files",
    "live_ai": "no", "ai_endpoint": "system.ai.meta-llama-3-3-70b-instruct",
}


def notebook_settings(dbutils: Any) -> dict[str, str]:
    """Same widgets in every notebook; no global notebook state assumption."""
    for key, default in DEFAULT_WIDGETS.items():
        try:
            dbutils.widgets.get(key)
        except Exception:
            dbutils.widgets.text(key, default)
    config = {key: dbutils.widgets.get(key).strip() for key in DEFAULT_WIDGETS}
    for key in ("catalog", "schema", "volume"):
        sql_identifier(config[key])
    serving_endpoint_name(config["ai_endpoint"])
    if config["live_ai"].lower() not in ("yes", "no"):
        raise ValueError("live_ai must be 'yes' or 'no'")
    return config


def name(config: Mapping[str, str], short_table: str) -> str:
    return ".".join(sql_identifier(x) for x in (config["catalog"], config["schema"], short_table))


def volume_path(config: Mapping[str, str]) -> str:
    return "/Volumes/" + "/".join(sql_identifier(config[key]) for key in ("catalog", "schema", "volume"))


def sql_literal(raw: str) -> str:
    """Quote illustrative SQL string values; not an identifier or authorization system."""
    return "'" + raw.replace("'", "''") + "'"


def verified_supplier_sql(config: Mapping[str, str]) -> str:
    """Benchmark SQL for the sample's completed steel-delivery measure."""
    return f"""SELECT
  COUNT(*) AS completed_deliveries,
  SUM(CASE WHEN actual_arrival_timestamp <= eta_timestamp + INTERVAL 2 HOURS
           THEN 1 ELSE 0 END) AS on_time_deliveries,
  ROUND(AVG(CASE WHEN actual_arrival_timestamp <= eta_timestamp + INTERVAL 2 HOURS
             THEN 1.0 ELSE 0.0 END), 4) AS on_time_rate
FROM {name(config, 'supplier_shipments_silver')}
WHERE actual_arrival_timestamp IS NOT NULL"""


def verified_apex_sql(config: Mapping[str, str]) -> str:
    """Benchmark SQL for strictly >5000 kg Chicago-hub loads this week."""
    return f"""SELECT COUNT(*) AS chicago_heavy_loads
FROM {name(config, 'apex_shipments_silver')}
WHERE start_timestamp >= DATE_TRUNC('WEEK', CURRENT_TIMESTAMP())
  AND start_timestamp < DATE_TRUNC('WEEK', CURRENT_TIMESTAMP()) + INTERVAL 7 DAYS
  AND cargo_weight_kg > 5000
  AND destination_code IN ('ORD', 'MDW', 'GYY')"""
