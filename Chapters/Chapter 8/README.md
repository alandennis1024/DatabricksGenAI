# Chapter 8: Agents, Tools and Function Calling

Runnable Databricks notebooks for Chapter 8 of *Generative AI Solutions with Databricks*.
They build, step by step, the agents described in the chapter: a Unity Catalog function used
as a tool, a tool calling (ReAct) agent, a router agent, a human-in-the-loop approval flow, and
the guided versus unguided data deletion example.

## Notebooks

Run them in order. Each notebook says what it needs at the top.

| Notebook | Book section | What it does |
|---|---|---|
| `00_setup_sample_data.py` | Tools (setup) | Creates the catalog and schema (`cobalt_ops.sales` by default) and small sample tables: sales, help articles, orders, customer records, a refund approval table and a refund ledger. |
| `01_uc_function_tool.py` | Tools > Defining a Tool in Python | Creates the Unity Catalog SQL function `region_revenue(region_name STRING)`, tests it (including an injection attempt), and shows an optional `EXECUTE` grant. |
| `02_tool_calling_agent.py` | Tools; Types of Agents > Tool Calling Agents | Gives the function to an LLM with `UCFunctionToolkit` and `create_agent`, asks *"What was revenue in EMEA?"*, prints the tool call messages, and turns on MLflow tracing. |
| `03_router_agent.py` | Types of Agents > Router Agents | A router plus three specialists (customer support, analytics, API) exposed to the router as tools. Uses a Delta keyword search instead of AI Search and a mock API. |
| `04_human_in_the_loop.py` | Types of Agents > Human-in-the-Loop Agents | A LangGraph refund workflow: a $20 refund runs automatically, a $12,000 refund pauses, writes a pending row, and resumes after approve, edit, or reject. Decisions go to a Delta approval table. |
| `05_guided_vs_unguided.py` | Guided vs. Unguided Agents > Data Deletion Request | The same deletion request done two ways on a scratch table: unguided deletes at once; guided does a dry run and deletes only after you set the `approve` widget. |
| `99_cleanup.py` | none | Drops what the folder created. Does nothing unless `confirm` is `yes`. |

`requirements.txt` lists the Python packages. Notebooks 02, 03 and 04 install it with
`%pip install -q -r requirements.txt` and restart Python. Notebooks 00, 01, 05 and 99 use only
Spark SQL, so they need no install.

## Prerequisites

* A Databricks workspace with **Unity Catalog**.
* **Serverless compute** (recommended) or a cluster on **Databricks Runtime 17 or later**.
* Permission to create tables and functions in a schema. Creating a catalog is optional (see
  the troubleshooting table).
* For notebooks 02 and 03: **Foundation Model APIs** with a chat endpoint that supports
  **tool calling**. The default is `databricks-claude-sonnet-4-5`. Model endpoint names change.
  If your workspace does not have that name, the notebook lists your endpoints and picks
  another `databricks-*` chat endpoint, printing which one.
* Notebooks 00, 01, 04 and 05 call no model, so they work without Foundation Model APIs.

## How to use this folder

1. In your workspace choose **Workspace > Create > Git folder** and paste this repository's URL.
2. Open `Chapters/Chapter 8/00_setup_sample_data.py`, attach serverless compute, and
   **Run all**.
3. Continue with `01` through `05` in order.
4. Run `99_cleanup.py` with `confirm` set to `yes` when you are done.

### Widgets

Every notebook that needs them has the same widgets with the same defaults. Use the same values
in every notebook.

| Widget | Default | Meaning |
|---|---|---|
| `catalog` | `cobalt_ops` | Catalog for the sample data and the function |
| `schema` | `sales` | Schema for the sample data and the function |
| `chat_endpoint` | `databricks-claude-sonnet-4-5` | Chat model endpoint (02 and 03 only) |

A few notebooks have one extra widget: `grant_to` (01), `approval_threshold` (04), `approve` and
`months` (05), and `confirm` and `drop_schema` (99).

Unity Catalog function names have three levels. The notebooks always build them from the
`catalog` and `schema` widgets, for example `cobalt_ops.sales.region_revenue`.

## Cost and run time

* Compute: serverless, usually a few minutes in total. The first run of 02, 03 and 04 spends one
  to three minutes installing packages.
* Model calls: only 02 and 03 call a model. 02 makes a few calls and 03 makes roughly 10 to 20 on
  a pay-per-token endpoint. That is normally a few cents.
* Storage: a few kilobytes of Delta tables.

## Free Edition notes

Not tested here. Databricks Free Edition is serverless only and has daily usage quotas, and
compute can stop for the rest of the day when you pass them. You may not be able to create a new
catalog, so set the `catalog` widget to one you can use (for example `workspace`). Check
**Serving** for the Foundation Model endpoints your edition offers and for their limits. See the
[Free Edition limitations](https://docs.databricks.com/aws/en/getting-started/free-edition-limitations)
page.

## Optional: the Agent Bricks CLI (Beta)

The chapter's last section describes building an agent with the Agent Bricks CLI. These steps
**cannot run in a notebook**, and **no notebook in this folder runs them**. The Agent Bricks CLI
is in **Beta**, and these commands were **not verified when this folder was written**. Check the
[Agent Bricks CLI documentation](https://docs.databricks.com/aws/en/agents/custom-agents/agent-bricks-cli)
for the current commands and options before you use them. Run them in a terminal on your own
computer:

```bash
# 1. Sign in to your workspace
databricks auth login

# 2. Sign in for the Agent Bricks CLI
agentbricks login

# 3. Scaffold a project from the LangGraph template
agentbricks init --framework langgraph my-agent

# 4. Run and test the agent locally, then open the MLflow trace
agentbricks dev

# 5. Deploy the agent
agentbricks deploy my-agent
```

Declare the agent's tools, such as the `region_revenue` function from notebook 01, in the
project's `agent.toml` file. The CLI is the way to deploy an agent you want other apps to call;
the notebooks here are for learning how the pieces work.

## Clean up

Run `99_cleanup.py` and set the `confirm` widget to `yes`. It drops the `region_revenue`
function and these tables: `sales`, `help_articles`, `orders`, `customer_records`,
`refund_approvals`, `refund_ledger` and `deletion_demo_records`. It keeps the schema and the
catalog unless you also set `drop_schema` to `yes`, and it drops the schema only if it is empty.
It never drops a catalog. MLflow traces stay in the notebook experiments.

The approval table is an audit trail. Do not run the cleanup on a schema that holds data you
want to keep.

## Troubleshooting

| Symptom | Likely cause | What to do |
|---|---|---|
| `Cannot create or use schema ...` in 00 | You cannot create catalogs, or the metastore has no default storage | Set the `catalog` widget to a catalog you own (for example `workspace`) in every notebook. |
| `Table ... was not found. Run 00_setup_sample_data first` | Notebook 00 was not run, or the widgets differ | Run 00 first and use the same `catalog` and `schema` values everywhere. |
| `Function ... was not found` in 02 or 03 | Notebook 01 was not run | Run 01. |
| `WARNING: no endpoint named '...'` | The default model endpoint is not in your workspace | The notebook picks another `databricks-*` chat endpoint. Or set the `chat_endpoint` widget. |
| `No databricks-* chat endpoint is available` | Foundation Model APIs are off or not offered in your region | Open **Serving**, check which endpoints exist, and set `chat_endpoint` to one that supports tools. |
| 02 prints "the model did not call region_revenue" | The chosen endpoint does not support tool calling well | Set `chat_endpoint` to a model that supports tool calling. |
| Permission error on `EXECUTE` or on the function | Your identity cannot run the function | Ask the owner to run `GRANT EXECUTE ON FUNCTION ... TO ...` (see 01). |
| `cannot import name 'ExecutionInfo' from 'langgraph.runtime'` | Mismatched `langgraph` and `langgraph-prebuilt` versions | Install from this folder's `requirements.txt`, which pins a matching pair. |
| A pending refund in 04 is gone after a restart | The checkpointer is in memory | Expected. Run the 04 cells again, or use a database-backed checkpointer. |
| 05 deletes nothing | `approve` is empty on purpose | Type `approve` in the `approve` widget and run the guided section again. |
| 04 raises `approval_threshold must be above 20...` | The demo refunds need a threshold between 20 and 7500 | Use a value in that range. |

## Package notes

The notebooks were checked offline against these versions: `databricks-langchain` 0.23.0,
`unitycatalog-ai` 0.4.1, `langchain` 1.2.10, `langgraph` 1.0.10, `langgraph-prebuilt` 1.0.8,
`mlflow` 3.18.0 and `databricks-sdk` 0.94.0. `requirements.txt` pins `langgraph-prebuilt` below
1.0.9 on purpose: `unitycatalog-langchain` caps `langchain` at 1.2.10, which needs `langgraph`
below 1.1, and newer `langgraph-prebuilt` releases need `langgraph` 1.1 or later. Remove that
pin when the `unitycatalog-langchain` cap is lifted.
