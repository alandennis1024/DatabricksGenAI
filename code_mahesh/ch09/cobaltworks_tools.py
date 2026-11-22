"""CobaltWorks operational tools (Chapter 9), runnable with or without SAP.

The tools are the ones from the book (see code/ch09/rest_tools.py): SAP
order status, SAP discount, Salesforce account lookup and the legacy
shop-floor RPA bot. The tool bodies are identical in both modes. Only the
transport function ``_call`` changes:

* ``mock`` (default): deterministic, in-process fake SAP, Salesforce and RPA
  systems. Needs no workspace, no network and no credentials.
* ``connection``: calls Unity Catalog HTTP connections through the
  workspace proxy. Use it when you own real systems and have created the
  connections (see notebook 06_uc_http_connection_example).

Select the mode with ``set_tool_mode("mock" | "connection")`` or with the
``COBALT_TOOL_MODE`` environment variable (used by the MCP server process).

The module has no notebook header on purpose: notebooks import it. It never
prints on import, because the stdio MCP server uses stdout for protocol
traffic.
"""
import copy
import json
import os
import re
import uuid

from langchain_core.tools import tool

TOOL_MODES = ("mock", "connection")
TOOL_MODE = os.environ.get("COBALT_TOOL_MODE", "mock")
if TOOL_MODE not in TOOL_MODES:
    TOOL_MODE = "mock"

# Names of the Unity Catalog HTTP connections used in "connection" mode.
SAP_CONN = "sap_s4"
SALESFORCE_CONN = "salesforce"
RPA_CONN = "rpa_platform"


def set_tool_mode(mode):
    """Switch between mock systems and real Unity Catalog connections."""
    global TOOL_MODE
    if mode not in TOOL_MODES:
        raise ValueError(f"tool_mode must be one of {TOOL_MODES}, got {mode!r}")
    TOOL_MODE = mode
    return TOOL_MODE


# Validate every identifier the LLM supplies before it reaches a URL.
# fullmatch with explicit ASCII classes also rejects a trailing newline,
# which `re.match(r"...$")` would let through.
PO = re.compile(r"PO-[0-9]{1,10}")
CUST = re.compile(r"[A-Z]{3}-[0-9]{3,6}")
BOT = re.compile(r"CHI_MES_[A-Z_]+_[0-9]{3}")

MIN_DISCOUNT, MAX_DISCOUNT, AUTONOMOUS_LIMIT = 0.1, 20.0, 10.0


# ---------------------------------------------------------------------
# Mock systems: SAP S/4HANA, Salesforce and an RPA platform
# ---------------------------------------------------------------------
_SEED = {
    "orders": {
        "PO-7723": {"Status": "PENDING", "CustomerID": "NRD-4410",
                    "ExpectedShipDate": "2026-11-18", "DelayDays": 6,
                    "DelayReason": "Carrier capacity shortage (Apex Haulers)",
                    "TotalAmount": 48250.00, "Currency": "USD"},
        "PO-7701": {"Status": "OPEN", "CustomerID": "PLT-1001",
                    "ExpectedShipDate": "2026-11-25", "DelayDays": 0,
                    "DelayReason": None,
                    "TotalAmount": 120000.00, "Currency": "USD"},
        "PO-7650": {"Status": "SHIPPED", "CustomerID": "NRD-4410",
                    "ExpectedShipDate": "2026-11-10", "DelayDays": 2,
                    "DelayReason": "Port congestion",
                    "TotalAmount": 15800.00, "Currency": "USD"},
        "PO-7599": {"Status": "CLOSED", "CustomerID": "GLD-2002",
                    "ExpectedShipDate": "2026-10-30", "DelayDays": 0,
                    "DelayReason": None,
                    "TotalAmount": 9400.00, "Currency": "USD"},
    },
    "customers": {
        "NRD-4410": {"Name": "Nordwind Logistics", "Tier": "STANDARD",
                     "Volume": False, "ContractEnd": "2027-03-31"},
        "PLT-1001": {"Name": "Pinnacle Rail", "Tier": "PLATINUM",
                     "Volume": True, "ContractEnd": "2028-06-30"},
        "GLD-2002": {"Name": "Granite Freight", "Tier": "GOLD",
                     "Volume": False, "ContractEnd": "2026-12-31"},
    },
    "idempotency": {},          # Idempotency-Key -> stored response
    "applications": {},         # order_id -> number of discounts applied
    "lose_next_response": False,
}
_STATE = copy.deepcopy(_SEED)


def reset_mock_state():
    """Restore the seed data so a notebook re-run starts from the same state."""
    global _STATE
    _STATE = copy.deepcopy(_SEED)


def mock_state():
    """Read-only copy of the mock systems (for demos and tests)."""
    return copy.deepcopy(_STATE)


def mock_lose_next_response():
    """Make the next ApplyDiscount apply, then fail with a gateway timeout.

    Simulates the classic "the write happened but the answer was lost" case
    that idempotency keys exist for.
    """
    _STATE["lose_next_response"] = True


def _http_error(conn, status, text):
    return RuntimeError(f"{conn} {status}: {text}")


def _mock_sap(method, path, headers, body):
    m = re.fullmatch(r"/PurchaseOrderSet\('([^']*)'\)(/ApplyDiscount)?", path)
    if m:
        order_id, action = m.group(1), m.group(2)
        order = _STATE["orders"].get(order_id)
        if order is None:
            raise _http_error(SAP_CONN, 404, f"PurchaseOrder {order_id} not found")
        if action is None and method == "GET":
            return dict(order)
        if action and method == "POST":
            return _mock_apply_discount(order_id, order, headers, body)
    m = re.fullmatch(r"/CustomerSet\('([^']*)'\)", path)
    if m and method == "GET":
        cust = _STATE["customers"].get(m.group(1))
        if cust is None:
            raise _http_error(SAP_CONN, 404, f"Customer {m.group(1)} not found")
        return {"CustomerID": m.group(1), "Name": cust["Name"],
                "Tier": cust["Tier"]}
    raise _http_error(SAP_CONN, 404, f"no mock route for {method} {path}")


def _mock_apply_discount(order_id, order, headers, body):
    key = headers.get("Idempotency-Key")
    if not key:
        raise _http_error(SAP_CONN, 400, "Idempotency-Key header required")
    stored = _STATE["idempotency"].get(key)
    if stored is not None:
        return {**stored, "Replayed": True}  # same key: no second discount
    if order["Status"] not in ("OPEN", "PENDING"):
        raise _http_error(SAP_CONN, 409, f"order is {order['Status']}")
    pct = float(body["DiscountPct"])
    order["TotalAmount"] = round(order["TotalAmount"] * (1 - pct / 100), 2)
    res = {"NewTotal": order["TotalAmount"],
           "ConfirmationID": "CNF-" + key.replace("-", "")[:8].upper(),
           "Replayed": False}
    _STATE["idempotency"][key] = res
    apps = _STATE["applications"]
    apps[order_id] = apps.get(order_id, 0) + 1
    if _STATE["lose_next_response"]:
        _STATE["lose_next_response"] = False
        raise _http_error(SAP_CONN, 504, "gateway timeout (simulated)")
    return res


def _mock_salesforce(method, path, params):
    m = re.fullmatch(r"/sobjects/Account/Customer_Id__c/([^/]+)", path)
    cust = _STATE["customers"].get(m.group(1)) if m else None
    if method != "GET" or cust is None:
        raise _http_error(SALESFORCE_CONN, 404, "NOT_FOUND: no matching Account")
    return {"Account_Tier__c": cust["Tier"],
            "Contract_End_Date__c": cust["ContractEnd"],
            "Volume_Discount_Agreement__c": cust["Volume"]}


def _mock_rpa(method, path, body):
    m = re.fullmatch(r"/api/v1/bots/([A-Za-z0-9_]+)/trigger", path)
    if method != "POST" or not m:
        raise _http_error(RPA_CONN, 404, f"no mock route for {method} {path}")
    digest = uuid.uuid5(uuid.NAMESPACE_URL, m.group(1) + body["action_params"])
    return {"status": "COMPLETED",
            "output": {"bot": m.group(1), "plant": body["plant_code"],
                       "rows_processed": 12},
            "execution_id": "EXE-" + digest.hex[:8].upper()}


def _mock_call(conn, method, path, **kw):
    headers = kw.get("headers", {})
    if conn == SAP_CONN:
        return _mock_sap(method, path, headers, kw.get("json"))
    if conn == SALESFORCE_CONN:
        return _mock_salesforce(method, path, kw.get("params"))
    if conn == RPA_CONN:
        return _mock_rpa(method, path, kw.get("json"))
    raise _http_error(conn, 404, "unknown connection")


def _connection_call(conn, method, path, **kw):
    """Call a Unity Catalog HTTP connection through the proxy."""
    import requests
    from databricks.sdk import WorkspaceClient

    w = WorkspaceClient()
    url = (f"{w.config.host}/api/2.0/unity-catalog/connections/"
           f"{conn}/proxy{path}")
    hdr = {**w.config.authenticate(), **kw.pop("headers", {})}
    r = requests.request(method, url, headers=hdr, timeout=15, **kw)
    if not r.ok:
        raise RuntimeError(f"{conn} {r.status_code}: {r.text[:200]}")
    return r.json()


def _call(conn, method, path, **kw):
    if TOOL_MODE == "mock":
        return _mock_call(conn, method, path, **kw)
    return _connection_call(conn, method, path, **kw)


# ---------------------------------------------------------------------
# Tools (same logic as code/ch09/rest_tools.py)
# ---------------------------------------------------------------------
def idempotency_key(order_id, discount_pct, reason):
    """One key per logical request, so a retry cannot double-apply."""
    return str(uuid.uuid5(uuid.NAMESPACE_URL,
                          f"{order_id}|{discount_pct:.2f}|{reason}"))


@tool
def check_order_status(order_id: str) -> dict:
    """Live status of a CobaltWorks purchase order (e.g. 'PO-7723').

    Call this BEFORE any order modification. Returns live SAP data,
    not the Lakehouse copy.
    """
    if not PO.fullmatch(order_id):
        return {"error": "bad order_id", "code": "VALIDATION_ERROR"}
    d = _call(SAP_CONN, "GET", f"/PurchaseOrderSet('{order_id}')")
    return {
        "order_id": order_id,
        "status": d["Status"],
        "expected_ship_date": d.get("ExpectedShipDate"),
        "delay_days": d.get("DelayDays", 0),
        "delay_reason": d.get("DelayReason"),
    }


def _validate_discount(order_id, discount_pct, reason):
    if not PO.fullmatch(order_id) or not (
            MIN_DISCOUNT <= discount_pct <= MAX_DISCOUNT):
        return {"error": "invalid input", "code": "VALIDATION_ERROR"}
    if len(reason.strip()) < 10:
        return {"error": "reason too short", "code": "VALIDATION_ERROR"}
    return None


def _apply_discount(order_id, discount_pct, reason, approved_action_id=None):
    bad = _validate_discount(order_id, discount_pct, reason)
    if bad:
        return bad

    po = f"/PurchaseOrderSet('{order_id}')"
    order = _call(SAP_CONN, "GET", po)
    if order["Status"] not in ("OPEN", "PENDING"):
        return {"error": f"order is {order['Status']}",
                "code": "INVALID_STATE"}

    if discount_pct > AUTONOMOUS_LIMIT and not approved_action_id:
        cust = _call(SAP_CONN, "GET",
                     f"/CustomerSet('{order['CustomerID']}')")
        if cust.get("Tier") != "PLATINUM":
            return {"status": "human_approval_required",
                    "code": "POLICY_APPROVAL_NEEDED",
                    "message": f"{discount_pct}% needs PLATINUM tier."}

    key = idempotency_key(order_id, discount_pct, reason)
    res = _call(SAP_CONN, "POST", f"{po}/ApplyDiscount",
                json={"DiscountPct": discount_pct, "Reason": reason},
                headers={"Idempotency-Key": key})
    return {"status": "success", "order_id": order_id,
            "new_total": res["NewTotal"],
            "confirmation_id": res["ConfirmationID"]}


@tool
def apply_discount(order_id: str, discount_pct: float, reason: str) -> dict:
    """Apply a 0.1-20% discount to an OPEN or PENDING order.

    Policy enforced in code: above 10% requires PLATINUM tier, and
    high-risk requests return 'human_approval_required'.
    """
    return _apply_discount(order_id, discount_pct, reason)


def apply_discount_after_approval(order_id, discount_pct, reason,
                                  approved_action_id):
    """Ring 5 execute step. NOT an agent tool.

    The caller must already have checked that a human wrote this
    approved_action_id to the approvals table (see notebook 02). All input
    and order-state checks still run; only the tier rule is waived.
    """
    if not approved_action_id:
        return {"error": "approved_action_id required",
                "code": "VALIDATION_ERROR"}
    return _apply_discount(order_id, discount_pct, reason, approved_action_id)


def build_discount_proposal(order_id, discount_pct, reason):
    """Ring 5 propose step: a structured plan with impact analysis.

    Changes nothing in SAP. Returns ``requires_human_approval`` so the caller
    knows whether to record an approval request.
    """
    bad = _validate_discount(order_id, discount_pct, reason)
    if bad:
        return bad
    order = _call(SAP_CONN, "GET", f"/PurchaseOrderSet('{order_id}')")
    if order["Status"] not in ("OPEN", "PENDING"):
        return {"error": f"order is {order['Status']}",
                "code": "INVALID_STATE"}
    cust = _call(SAP_CONN, "GET", f"/CustomerSet('{order['CustomerID']}')")
    tier = cust.get("Tier")
    needs = discount_pct > AUTONOMOUS_LIMIT and tier != "PLATINUM"
    total = order.get("TotalAmount")
    plan = {
        "order_id": order_id,
        "customer_id": order["CustomerID"],
        "customer_tier": tier,
        "discount_pct": discount_pct,
        "reason": reason,
        "requires_human_approval": needs,
        "policy_reason": (
            f"{discount_pct}% exceeds the {AUTONOMOUS_LIMIT}% limit for "
            f"{tier} customers" if needs else "within autonomous authority"),
    }
    if total is not None:
        plan["current_total"] = total
        plan["projected_total"] = round(total * (1 - discount_pct / 100), 2)
        plan["revenue_impact"] = round(total - plan["projected_total"], 2)
    return plan


@tool
def get_customer_account(customer_id: str) -> dict:
    """Tier, volume-discount flag and contract end date from Salesforce.

    Use this to verify discount eligibility or active terms.
    """
    if not CUST.fullmatch(customer_id):
        return {"error": "bad customer_id", "code": "VALIDATION_ERROR"}
    d = _call(SALESFORCE_CONN, "GET",
              f"/sobjects/Account/Customer_Id__c/{customer_id}",
              params={"fields": "Account_Tier__c,Contract_End_Date__c,"
                                "Volume_Discount_Agreement__c"})
    return {
        "customer_id": customer_id,
        "tier": d.get("Account_Tier__c") or "STANDARD",
        "volume_discount_agreement": d.get("Volume_Discount_Agreement__c"),
        "contract_end_date": d.get("Contract_End_Date__c"),
    }


@tool
def run_shopfloor_rpa_bot(bot_id: str, plant_code: str, action: str) -> dict:
    """Run a legacy RPA bot. Chicago (CHI) legacy MES only.

    `action` is a JSON string. For all other plants, use the direct
    ERP API tools instead.
    """
    if plant_code != "CHI" or not BOT.fullmatch(bot_id):
        return {"error": "CHI plant and CHI_MES_* bots only",
                "code": "VALIDATION_ERROR"}
    try:
        json.loads(action)  # reject malformed JSON before triggering the bot
    except ValueError:
        return {"error": "action must be a JSON string",
                "code": "VALIDATION_ERROR"}
    r = _call(RPA_CONN, "POST", f"/api/v1/bots/{bot_id}/trigger",
              json={"plant_code": plant_code, "action_params": action})
    return {"bot_id": bot_id, "status": r["status"],
            "output_data": r.get("output"),
            "execution_id": r["execution_id"]}


# ---------------------------------------------------------------------
# Audit helpers (used by notebooks 02 and 05)
# ---------------------------------------------------------------------
_SECRET_KEYS = re.compile(r"token|secret|password|authorization|api_key", re.I)
_EMAIL = re.compile(r"[\w.+-]+@[\w-]+\.[\w.-]+")


def mask_params(params):
    """Mask sensitive values before they are written to the audit table.

    Numeric parameters such as discount_pct stay readable, because the
    anomaly query depends on them.
    """
    out = {}
    for k, v in params.items():
        if _SECRET_KEYS.search(k):
            out[k] = "***"
        elif k == "reason" and isinstance(v, str):
            out[k] = f"[masked:{len(v)} chars]"
        elif k == "customer_id" and isinstance(v, str):
            out[k] = v[:3] + "-***"
        elif isinstance(v, str):
            out[k] = _EMAIL.sub("***@***", v)
        else:
            out[k] = v
    return out


def classify_result(tool_name, result):
    """Map a tool result to (status, policy_basis) for the audit table."""
    if not isinstance(result, dict):
        return "success", "read_only"
    code, status = result.get("code"), result.get("status")
    if code == "VALIDATION_ERROR":
        return "policy_rejected", "ring3_input_validation"
    if code == "INVALID_STATE":
        return "policy_rejected", "ring4_order_state"
    if status == "human_approval_required" or code in (
            "POLICY_APPROVAL_NEEDED", "APPROVAL_REQUIRED"):
        return "policy_rejected", "ring5_human_approval_gate"
    if status == "pending_human_approval":
        return "success", "ring5_proposal_recorded"
    if "error" in result:
        return "error", code or "tool_error"
    if tool_name in ("apply_discount", "execute_approved_discount"):
        return "success", "ring4_within_policy"
    return "success", "read_only"


# ---------------------------------------------------------------------
# Notebook helpers
# ---------------------------------------------------------------------
_NAME = re.compile(r"[A-Za-z0-9_]+")


def qualified(catalog, schema, table=None):
    """Backtick-quoted three-part name. Rejects anything but [A-Za-z0-9_]."""
    parts = [catalog, schema] + ([table] if table else [])
    for p in parts:
        if not _NAME.fullmatch(p or ""):
            raise ValueError(f"invalid identifier {p!r}: use letters, digits, _")
    return ".".join(f"`{p}`" for p in parts)


def ensure_catalog_schema(spark, catalog, schemas):
    """Create the catalog (if allowed) and schemas, with actionable errors."""
    qualified(catalog, "x")  # validates the catalog name
    try:
        spark.sql(f"CREATE CATALOG IF NOT EXISTS `{catalog}`")
    except Exception as exc:  # noqa: BLE001 - message is shown to the reader
        print(f"Could not create catalog {catalog!r}: {str(exc)[:200]}\n"
              "-> Set the `catalog` widget to a catalog you can already "
              "use (for example your workspace's default catalog) and "
              "re-run. Continuing with the existing catalog if it exists.")
    for schema in schemas:
        try:
            spark.sql(f"CREATE SCHEMA IF NOT EXISTS {qualified(catalog, schema)}")
        except Exception as exc:
            raise RuntimeError(
                f"Cannot use schema {catalog}.{schema}: {str(exc)[:200]}\n"
                "Set the `catalog` widget to a catalog where you have "
                "CREATE SCHEMA permission and re-run.") from exc


_NOT_CHAT = ("embedding", "bge", "gte", "rerank", "whisper", "image")
_PREFERRED = ("claude-sonnet", "claude", "gpt-5", "gemini", "gpt-oss", "llama")


def resolve_chat_endpoint(endpoints, preferred):
    """Pick a chat endpoint. Returns (name, message).

    ``endpoints`` is the result of WorkspaceClient().serving_endpoints.list().
    If ``preferred`` exists it is used; otherwise the best available
    ``databricks-*`` chat endpoint is chosen and the message explains why.
    """
    chat = []
    for e in endpoints:
        task = getattr(e, "task", None)
        state = getattr(getattr(e, "state", None), "ready", None)
        if state is not None and "NOT_READY" in str(state):
            continue
        chat.append((e.name, task))
    names = [n for n, _ in chat]
    if preferred in names:
        return preferred, f"Using chat endpoint {preferred!r}."
    candidates = [
        n for n, t in chat
        if n.startswith("databricks-")
        and (t is None or "chat" in str(t))
        and not any(x in n.lower() for x in _NOT_CHAT)
    ]

    def rank(n):
        low = n.lower()
        for i, p in enumerate(_PREFERRED):
            if p in low:
                return (i, n)
        return (len(_PREFERRED), n)

    if not candidates:
        return None, (
            f"Endpoint {preferred!r} was not found and no databricks-* chat "
            "endpoint is visible. Open Serving in your workspace, copy the "
            "name of a tool-calling chat endpoint and paste it into the "
            "`chat_endpoint` widget.")
    pick = sorted(candidates, key=rank)[0]
    return pick, (
        f"Endpoint {preferred!r} was not found in this workspace. Falling "
        f"back to {pick!r}. Set the `chat_endpoint` widget to silence this.")


def message_text(msg):
    """Plain text of a chat message (content may be a list of blocks)."""
    content = getattr(msg, "content", msg)
    if isinstance(content, str):
        return content
    parts = []
    for block in content or []:
        if isinstance(block, str):
            parts.append(block)
        elif isinstance(block, dict) and block.get("type") == "text":
            parts.append(block.get("text", ""))
    return "".join(parts)


def tool_call_summary(messages):
    """One line per tool call and result, for printing an agent run."""
    lines = []
    for m in messages:
        for tc in getattr(m, "tool_calls", None) or []:
            lines.append(f"CALL   {tc['name']}({json.dumps(tc['args'])})")
        if getattr(m, "type", "") == "tool":
            lines.append(f"RESULT {message_text(m)[:300]}")
    return lines
