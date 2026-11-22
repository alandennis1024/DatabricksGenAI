# Chapter 9: Agents' Access to Transactional and Operational Systems

Runnable notebooks for Chapter 9 of *Generative AI Solutions with
Databricks*. They build the CobaltWorks procurement agent's operational
tools, guardrails, audit trail, MCP integration and memory.

Chapter 9 uses fictional SAP, Salesforce and RPA systems
(`sap.cobaltworks.internal`, and so on) that you do not have. So every
notebook runs against **mock systems by default**, and nothing needs a real
ERP.

## Notebooks

Run them in this order. Each one is re-runnable.

| Notebook | Book section | What it does |
|---|---|---|
| `00_setup_audit_trail` | Governance, Auditing, and Visibility > Unity Catalog Governance | Creates catalog `cobalt_ops`, schemas, the append-only `tool_actions` audit table and the `human_approvals` table. Shows guarded grants. |
| `01_operational_tools` | Integration Patterns in Practice > Direct Tool Integration via REST APIs; Salesforce REST API Integration | Calls the SAP, Salesforce and RPA tools directly: status, accepted discount, rejections, the human-approval gate, idempotent retry, injection-style IDs. |
| `02_agent_with_guardrails` | Importance of Guardrails; Logs, Compliance, and Inference Tables; case study | `create_agent` with the tools and a system prompt, MLflow tracing, every tool call written to the audit table, and the propose / approve / execute human gate. |
| `03_mcp_stdio_demo` | The Model Context Protocol (MCP) | Runs a FastMCP server as a child process over stdio and builds an agent on its tools. |
| `04_lakebase_memory` | Lakebase for Agent Memory | Multi-turn memory with a LangGraph checkpointer: Lakebase if you give an instance name, otherwise in-process `MemorySaver`. |
| `05_audit_queries` | Logs, Compliance, and Inference Tables; Data Profiling and Genie Agents | The anomaly query (`input_params:discount_pct::double`), guardrail and latency summaries, proof that the log is append-only. |
| `06_uc_http_connection_example` | Direct Tool Integration via REST APIs (UC connection NOTE) | Optional and guarded: `CREATE CONNECTION ... TYPE HTTP` and `GRANT USE CONNECTION`. Does nothing unless `run_example = yes`. |
| `99_cleanup` | none | Drops what the notebooks created, only when `confirm = yes`. |

Supporting files: `cobaltworks_tools.py` (the tools, mock systems and small
helpers), `cobaltworks_sap_mcp_server.py` (the MCP server used by notebook
03), `requirements.txt`.

## Mock mode and connection mode

The tool code is the book's code. Only the transport differs, selected by the
`tool_mode` widget:

* **`mock`** (default): deterministic, in-process fake systems. Order states
  `OPEN`, `PENDING`, `SHIPPED` and `CLOSED`; customer tiers `STANDARD`, `GOLD`
  and `PLATINUM`; idempotency keys that replay the same confirmation. No
  network, no credentials. Sample data: `PO-7723` (PENDING, standard customer,
  6 days late because of Apex Haulers), `PO-7701` (OPEN, platinum customer),
  `PO-7650` (SHIPPED), `PO-7599` (CLOSED).
* **`connection`**: calls Unity Catalog HTTP connections through the
  workspace proxy, exactly as in `code/ch09/rest_tools.py`. Use it if you own
  real systems. Create connections named `sap_s4`, `salesforce` and
  `rpa_platform` (see notebook 06; the names are constants at the top of
  `cobaltworks_tools.py`). Checks that assume the mock data become
  informational in this mode.

Notebook 03 always uses mock mode: a child process does not inherit your
notebook's workspace credentials.

## Prerequisites

* A Databricks workspace with Unity Catalog. Serverless compute or a DBR 17+
  cluster. Clone this repo with **Workspace > Create > Git folder** and open
  the `Chapters/Chapter 9` folder.
* Permission to create a catalog, or an existing catalog you can use: set the
  `catalog` widget to it.
* For notebooks 02, 03 and 04: a tool-calling chat model endpoint. The default
  is `databricks-claude-sonnet-4-5`. Model names change, so each notebook lists
  your workspace's endpoints and falls back to another `databricks-*` chat
  endpoint if the configured one is missing. You can always set the
  `chat_endpoint` widget yourself.
* Notebook 04 with Lakebase only: a Lakebase database instance (or project)
  you can connect to.

Every notebook starts with `%pip install -q -r requirements.txt` and a Python
restart, so the first run takes a minute longer.

## Shared widgets

| Widget | Default | Used by |
|---|---|---|
| `catalog` | `cobalt_ops` | 00, 02, 05, 99 |
| `audit_schema` | `agent_audit` | 00, 02, 05, 99 |
| `chat_endpoint` | `databricks-claude-sonnet-4-5` | 02, 03, 04 |
| `tool_mode` | `mock` | 01, 02, 04 |

Notebook-specific widgets: `agent_principal` (00), `lakebase_instance`,
`lakebase_project`, `lakebase_branch` (04), the connection settings and
`run_example` (06), and `confirm` and `drop_connection` (99).

## Free Edition notes

* Free Edition is serverless only, which is what these notebooks target.
* Pay-per-token model endpoints are available but have usage limits. Notebook
  02 makes about five short agent calls and notebooks 03 and 04 a few more.
  If you hit a limit, wait and re-run from the failed cell.
* Free Edition includes **one Lakebase project**. Notebook 04 works without
  it (in-process memory). To try Lakebase, use the project and branch widgets.
* Outbound internet access is restricted. Mock mode needs none, but a Unity
  Catalog connection to your own host (notebook 06, connection mode) may not
  be reachable.
* Creating catalogs may be restricted. If `CREATE CATALOG` fails, set the
  `catalog` widget to a catalog you can use.

## Cost

Mock mode costs only compute time and a few foundation-model tokens. Notebook
00 to 01 and 05 use no model. Notebook 04 with Lakebase uses your Lakebase
instance, which bills while it runs; stop or delete it when done.

## Cleanup

Run `99_cleanup` with `confirm = yes`. It drops the two tables and, if they
are empty, the two schemas. It never drops the catalog and never uses
`CASCADE`. Lakebase checkpoint tables and MLflow traces are not touched.

## Troubleshooting

| Symptom | Cause and fix |
|---|---|
| `cobaltworks_tools.py not found` | The notebook is not running from the cloned `Chapter 9` Git folder. Open it from there. |
| `CREATE CATALOG` error message, then schema error | Your workspace blocks catalog creation. Set the `catalog` widget to a catalog you can use and re-run 00. |
| `Table ... is not available` in 02 or 05 | Run `00_setup_audit_trail` first, with the same `catalog` and `audit_schema`. |
| `Endpoint ... was not found` | The model name changed. The notebook picks a fallback; set `chat_endpoint` to silence the message. If none exist, open **Serving** and copy a tool-calling chat endpoint name. |
| `The agent call failed on endpoint ...` | The endpoint is rate limited, not ready, or does not support tool calling. Try another endpoint. |
| MCP step in 03 fails and prints `--- server stderr ---` | Read the stderr text. Usually the folder was moved so the server file is missing, or the package install did not finish; re-run the first cells. |
| `Lakebase connection failed` in 04 | Check the instance name or project and branch, and your connect permission. The notebook then continues with in-process memory. |
| `DELETE blocked, as intended` in 05 | That is the success case: the audit table is append-only. |
| `Cannot read secret` in 06 | Create the secret scope and key first, or leave `run_example` at `no`. |
| `PERMISSION_DENIED` on `CREATE CONNECTION` | You need the `CREATE CONNECTION` privilege on the metastore. |
| A GRANT fails in 00 | The principal name is wrong, or you do not own the objects. Grants only run when `agent_principal` is set. |
