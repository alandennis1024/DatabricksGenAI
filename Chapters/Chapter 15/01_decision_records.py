# Databricks notebook source
# MAGIC %md
# MAGIC # 01 - Decision records (Chapter 15)
# MAGIC
# MAGIC **Book section:** Looking to the Future > CobaltWorks in 2028 > *Data,
# MAGIC model, and tool lineage* (the bullet "Write a decision record for each new
# MAGIC capability").
# MAGIC
# MAGIC **What it does.** Renders the blank decision-record template for a new
# MAGIC capability, shows what a validator reports while placeholders remain,
# MAGIC loads the filled-in supplier-suspension example, and checks that every
# MAGIC required field and section is present and filled. It finishes with three
# MAGIC deliberately broken copies to show what the checks catch.
# MAGIC
# MAGIC **Prerequisites.** None. No tables, models, or permissions are needed.
# MAGIC
# MAGIC **Runtime.** Serverless compute, or a cluster on DBR 17 or later.
# MAGIC
# MAGIC **Creates.** Nothing. The records are read from this folder and checked in
# MAGIC memory.

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
        if os.path.exists(os.path.join(folder, "decision_records.py")):
            return folder
    raise RuntimeError(
        "decision_records.py was not found next to this notebook. Clone the "
        "whole repository into a Databricks Git folder and open the notebook "
        "there."
    )


NOTEBOOK_DIR = _find_notebook_dir()
if NOTEBOOK_DIR not in sys.path:
    sys.path.insert(0, NOTEBOOK_DIR)

# COMMAND ----------

import os

import decision_records as dr

TEMPLATE = dr.read_text(os.path.join(NOTEBOOK_DIR, "decision_record_template.md"))
EXAMPLE = dr.read_text(
    os.path.join(NOTEBOOK_DIR, "decision_record_supplier_suspension.md")
)


def show(title, report):
    status = "PASS" if report.ok else "FAIL"
    print(f"{title}: {status} ({len(report.errors)} errors, "
          f"{len(report.warnings)} warnings)")
    for line in report.errors:
        print(f"  error:   {line}")
    for line in report.warnings:
        print(f"  warning: {line}")


# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Start a new record from the template
# MAGIC
# MAGIC `render_draft` puts the capability name in the title. Every other `<...>`
# MAGIC placeholder is yours to replace. Until you do, validation fails, which is
# MAGIC the point: a template is not a decision.

# COMMAND ----------

draft = dr.render_draft(TEMPLATE, "Recommend supplier suspension")
print("\n".join(draft.splitlines()[:10]))
print("...")
print()
show("Draft", dr.validate_record(draft))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Validate the filled-in example
# MAGIC
# MAGIC The supplier-suspension record from the chapter has no placeholders, a
# MAGIC valid status and version, all ten sections, and a threat model that names
# MAGIC each abuse path the chapter lists.

# COMMAND ----------

record = dr.parse_record(EXAMPLE)
report = dr.validate_record(EXAMPLE)
show("Supplier suspension", report)
assert report.ok and not report.warnings, "the shipped example must be clean"

print()
for name, value in record.fields.items():
    print(f"{name:<18} {value}")
print()
for name in dr.REQUIRED_SECTIONS:
    first_line = record.sections[name].splitlines()[0]
    print(f"{name:<20} {first_line[:60]}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. See what the checks catch
# MAGIC
# MAGIC Each copy below breaks one rule. All three must fail.

# COMMAND ----------

broken = {
    "Rollback section removed": EXAMPLE.split("## Rollback")[0]
    + "## Review"
    + EXAMPLE.split("## Review")[1],
    "Status is 'Maybe'": EXAMPLE.replace("| Status | Proposed |", "| Status | Maybe |"),
    "Placeholder left in": EXAMPLE.replace(
        "Head of Procurement Operations.", "<named owner>."
    ),
}
for title, text in broken.items():
    result = dr.validate_record(text)
    show(title, result)
    assert not result.ok, f"{title} should have failed validation"
