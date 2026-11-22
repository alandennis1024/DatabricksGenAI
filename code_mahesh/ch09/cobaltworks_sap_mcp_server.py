"""CobaltWorks SAP MCP server (Chapter 9), stdio transport, for demos.

Exposes two tools from cobaltworks_tools: check_order_status and
apply_discount. Ring 2 (tool exposure control) in action: the Salesforce
and RPA tools are deliberately not advertised here.

Started by notebook 03_mcp_stdio_demo as a child process:

    python cobaltworks_sap_mcp_server.py

stdout carries the MCP protocol, so this file must never print to it.
A deployed agent should use a server hosted on Databricks Apps instead.
"""
import os
import sys

# Make `import cobaltworks_tools` work wherever the process was started.
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import cobaltworks_tools as cw  # noqa: E402
from mcp.server.fastmcp import FastMCP  # noqa: E402

mcp = FastMCP("cobaltworks-sap")


@mcp.tool()
def check_order_status(order_id: str) -> dict:
    """Live status of a CobaltWorks purchase order (e.g. 'PO-7723').

    Call this BEFORE any order modification.
    """
    return cw.check_order_status.invoke({"order_id": order_id})


@mcp.tool()
def apply_discount(order_id: str, discount_pct: float, reason: str) -> dict:
    """Apply a 0.1-20% discount to an OPEN or PENDING order.

    Above 10% requires PLATINUM tier; high-risk requests return
    'human_approval_required'. Policy is enforced in code.
    """
    return cw.apply_discount.invoke({
        "order_id": order_id, "discount_pct": discount_pct, "reason": reason})


if __name__ == "__main__":
    mcp.run(transport="stdio")
