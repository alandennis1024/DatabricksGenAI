# Databricks notebook source
# MAGIC %md
# MAGIC # 00 - MLflow Prompt Registry (Chapter 15)
# MAGIC
# MAGIC **Book section:** Looking to the Future > Capabilities on Databricks >
# MAGIC *MLflow Prompt Registry*, plus *Treat prompts and instructions as code*.
# MAGIC
# MAGIC **What it does.** Registers a CobaltWorks procurement-assistant prompt in
# MAGIC Unity Catalog, adds versions, points the `staging` and `production`
# MAGIC aliases at them, and loads the prompt by alias
# MAGIC (`prompts:/<name>@<alias>`). Moving `production` is a deliberate step: it
# MAGIC needs a passing evaluation gate and a named approver, and a failing
# MAGIC candidate is blocked. Rolling back is one alias change.
# MAGIC
# MAGIC **Prerequisites**
# MAGIC * The MLflow Prompt Registry is a **Beta** feature. A workspace admin may
# MAGIC   need to enable it on the Previews page. If it is off, the notebook
# MAGIC   stops early with a message instead of failing.
# MAGIC * `CREATE FUNCTION`, `EXECUTE`, and `MANAGE` on the target schema (the
# MAGIC   registry stores prompts as Unity Catalog objects).
# MAGIC * Permission to create the catalog and schema, or an existing catalog you
# MAGIC   can use. Set the `catalog` and `schema` widgets above.
# MAGIC
# MAGIC **Runtime.** Serverless compute, or a cluster on DBR 17 or later. The
# MAGIC first cell installs `mlflow[databricks]>=3.14` from `requirements.txt`.
# MAGIC
# MAGIC **Creates.** One prompt, `<catalog>.<schema>.procurement_assistant`, with
# MAGIC three versions and the aliases `staging` and `production`. It also writes
# MAGIC one MLflow trace. Run `99_cleanup` to remove them.
# MAGIC
# MAGIC The notebook is safe to run again: versions that already exist are reused.

# COMMAND ----------

# MAGIC %pip install -q -r requirements.txt

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

dbutils.widgets.text("catalog", "cobalt_ops", "Catalog")
dbutils.widgets.text("schema", "future", "Schema")
dbutils.widgets.text("approver", "", "Approver (blank = current user)")

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
        if os.path.exists(os.path.join(folder, "cobalt_common.py")):
            return folder
    raise RuntimeError(
        "cobalt_common.py was not found next to this notebook. Clone the whole "
        "repository into a Databricks Git folder and open the notebook there."
    )


NOTEBOOK_DIR = _find_notebook_dir()
if NOTEBOOK_DIR not in sys.path:
    sys.path.insert(0, NOTEBOOK_DIR)

# COMMAND ----------

import mlflow
from mlflow import MlflowClient

import cobalt_common as cc

CATALOG = cc.check_identifier(dbutils.widgets.get("catalog"), "catalog")
SCHEMA = cc.check_identifier(dbutils.widgets.get("schema"), "schema")
PROMPT = cc.prompt_name(CATALOG, SCHEMA)
APPROVER = dbutils.widgets.get("approver").strip()
if not APPROVER:
    APPROVER = spark.sql("SELECT current_user()").collect()[0][0]

cc.ensure_catalog_schema(spark, CATALOG, SCHEMA)

ready, message = cc.prompt_registry_ready(mlflow, CATALOG, SCHEMA)
if not ready:
    print(message)
    dbutils.notebook.exit("SKIPPED: Prompt Registry is not available. See output.")
print(f"Prompt: {PROMPT}")
print(f"Approver recorded on promotions: {APPROVER}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Register versions and point `staging` at one
# MAGIC
# MAGIC Prompt versions are immutable. Every change creates a new version. The
# MAGIC prompt name needs all three Unity Catalog levels (`catalog.schema.name`).
# MAGIC
# MAGIC Version 1 is the current baseline. Version 2 is the code from the
# MAGIC chapter. `ensure_version` registers a version only if it does not exist
# MAGIC yet, so you can run the notebook more than once.

# COMMAND ----------


def ensure_version(version, template, commit_message):
    try:
        existing = mlflow.genai.load_prompt(PROMPT, version=version, allow_missing=True)
    except Exception:
        existing = None
    if existing is not None:
        print(f"Version {version} already exists; reusing it.")
        return existing
    created = mlflow.genai.register_prompt(
        name=PROMPT, template=template, commit_message=commit_message
    )
    if created.version != version:
        raise RuntimeError(
            f"Expected to create version {version} but got {created.version}. "
            f"{PROMPT} already has other versions: run 99_cleanup or use a "
            "different schema."
        )
    print(f"Registered version {created.version}.")
    return created


v1 = ensure_version(
    1,
    [
        {"role": "system", "content": "Answer procurement questions."},
        {"role": "user", "content": "{{question}}"},
    ],
    "Initial version",
)

# The chapter's example: a system message plus a templated user message.
prompt = ensure_version(
    2,
    [
        {
            "role": "system",
            "content": "Follow the approved procurement policy.",
        },
        {"role": "user", "content": "{{question}}"},
    ],
    "Add explicit approval boundary",
)

mlflow.genai.set_prompt_alias(
    name=PROMPT,
    alias="staging",
    version=prompt.version,
)

staged = mlflow.genai.load_prompt(f"prompts:/{PROMPT}@staging", cache_ttl_seconds=0)
messages = staged.format(question="Can I apply a 15% discount?")
print(f"Loaded {staged.uri} through the staging alias.")
for message in messages:
    print(f"  {message['role']:>6}: {message['content']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. An evaluation gate guards the `production` alias
# MAGIC
# MAGIC An alias makes a prompt change easy, but it must not make it automatic.
# MAGIC `should_promote` is the chapter's gate: quality may not fall, no unsafe
# MAGIC actions are allowed, and latency and cost stay inside limits. The
# MAGIC thresholds are placeholders. The metrics below are **illustrative
# MAGIC numbers**, not results from a real evaluation run. In your own system
# MAGIC they come from `mlflow.genai.evaluate` on the same dataset for every
# MAGIC prompt version.

# COMMAND ----------


def should_promote(candidate: dict, baseline: dict) -> bool:
    """Promote only when quality improves without violating safety or SLOs."""
    return (
        candidate["task_success"] >= baseline["task_success"]
        and candidate["groundedness"] >= baseline["groundedness"]
        and candidate["unsafe_action_rate"] <= 0.0
        and candidate["p95_latency_ms"] <= 4000
        and candidate["cost_per_success"] <= baseline["cost_per_success"] * 1.15
    )


METRICS = {
    1: {"task_success": 0.90, "groundedness": 0.91, "unsafe_action_rate": 0.0,
        "p95_latency_ms": 3200, "cost_per_success": 0.050},
    2: {"task_success": 0.93, "groundedness": 0.94, "unsafe_action_rate": 0.0,
        "p95_latency_ms": 3300, "cost_per_success": 0.052},
    3: {"task_success": 0.95, "groundedness": 0.95, "unsafe_action_rate": 0.04,
        "p95_latency_ms": 3100, "cost_per_success": 0.049},
}


def set_tag(version, key, value):
    try:
        MlflowClient().set_prompt_version_tag(PROMPT, version, key, value)
    except Exception as exc:  # tags are helpful, but not essential
        print(f"  (could not tag version {version}: {type(exc).__name__}: {exc})")


def alias_version(alias):
    prompt_version = mlflow.genai.load_prompt(
        f"prompts:/{PROMPT}@{alias}", allow_missing=True, cache_ttl_seconds=0
    )
    return None if prompt_version is None else prompt_version.version


def promote(version, baseline_version, approver):
    """Move `production` only if the gate passes and a person is named."""
    if not approver.strip():
        raise ValueError("A named approver is required to move production.")
    passed = should_promote(METRICS[version], METRICS[baseline_version])
    if not passed:
        print(f"BLOCKED: version {version} fails the gate; "
              f"production stays at version {alias_version('production')}.")
        return False
    mlflow.genai.set_prompt_alias(name=PROMPT, alias="production", version=version)
    set_tag(version, "approved_by", approver)
    set_tag(version, "gate", "should_promote passed (illustrative metrics)")
    print(f"PROMOTED: production now points at version {version} "
          f"(approved by {approver}).")
    return True


# Bootstrap: version 1 is the baseline, compared with itself.
promote(1, 1, APPROVER)
# Candidate: version 2 improves on the baseline and passes.
promote(2, 1, APPROVER)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. A bad candidate is blocked
# MAGIC
# MAGIC Version 3 loosens the approval boundary. Its (illustrative) evaluation
# MAGIC shows an unsafe-action rate above zero, so the gate refuses to move
# MAGIC `production`. The version still exists and `staging` can point at it for
# MAGIC testing, but customers keep getting version 2.

# COMMAND ----------

bad = ensure_version(
    3,
    [
        {
            "role": "system",
            "content": "Offer discounts without waiting for approval.",
        },
        {"role": "user", "content": "{{question}}"},
    ],
    "Experiment: skip approval for small discounts",
)
mlflow.genai.set_prompt_alias(name=PROMPT, alias="staging", version=bad.version)

promoted = promote(3, 2, APPROVER)
assert not promoted, "the unsafe candidate must not reach production"
print(f"staging -> v{alias_version('staging')}, "
      f"production -> v{alias_version('production')}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. The application loads by alias, and the trace records the URI
# MAGIC
# MAGIC The application never hard-codes instruction text. It loads
# MAGIC `prompts:/<name>@production` and records the exact versioned URI in the
# MAGIC trace, so a reviewer can see which instructions produced an answer.
# MAGIC Loaded prompts are cached; `cache_ttl_seconds=0` makes this demo read the
# MAGIC alias fresh each time.

# COMMAND ----------


@mlflow.trace(name="procurement_answer")
def answer(question: str):
    loaded = mlflow.genai.load_prompt(
        f"prompts:/{PROMPT}@production", cache_ttl_seconds=0
    )
    mlflow.update_current_trace(tags={"prompt_uri": loaded.uri})
    return loaded.format(question=question)


try:
    for message in answer("Can I apply a 15% discount?"):
        print(f"{message['role']:>6}: {message['content']}")
    print("Trace written with tag prompt_uri.")
except Exception as exc:  # tracing needs an experiment; the demo still holds
    print(f"Tracing was skipped ({type(exc).__name__}: {exc}).")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Roll back with one alias change
# MAGIC
# MAGIC The decision record for supplier suspension says "move the prompt alias
# MAGIC back to the previous version". This is that step. Run the notebook again
# MAGIC to promote version 2 once more.

# COMMAND ----------

before = alias_version("production")
mlflow.genai.set_prompt_alias(name=PROMPT, alias="production", version=1)
set_tag(1, "rollback_reason", "demo rollback from notebook 00")
print(f"production moved from v{before} back to v{alias_version('production')}.")

print("\nAliases now:")
for alias in ("staging", "production"):
    print(f"  {alias:<10} -> v{alias_version(alias)}")
print(f"\nPrompt URI for the application: prompts:/{PROMPT}@production")
