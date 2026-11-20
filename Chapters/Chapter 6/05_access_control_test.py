# Databricks notebook source
# MAGIC %md
# MAGIC # Chapter 6 - Access-control validation test (A1)
# MAGIC
# MAGIC Post-deploy validation for the metadata-filter pattern. Run after
# MAGIC `00_setup_sample_data`, `01_create_index`, and `04_search_as_user`.
# MAGIC
# MAGIC Cases:
# MAGIC 1. **Positive** - an authorized user (leadership member) retrieves the
# MAGIC    restricted document `POL-100`.
# MAGIC 2. **Negative** - an unauthorized user (procurement-chi only) does not.
# MAGIC 3. **No widening** - a user-supplied `plant_id` outside the user's
# MAGIC    authorized plants yields no results.
# MAGIC 4. **Sanity** - an authorized in-scope query returns `POL-001`.
# MAGIC
# MAGIC Prints PASS/FAIL per case and raises (failing the job) on any failure.
# MAGIC
# MAGIC Prerequisites: two workspace users, one in the `leadership` group and
# MAGIC one in `procurement-chi` only. Their real group membership is resolved
# MAGIC through SCIM - nothing here trusts user input for identity.

# COMMAND ----------

# Set these to real users in your workspace (see README prerequisites).
AUTHORIZED_USER = ""    # member of "leadership"
UNAUTHORIZED_USER = ""  # member of "procurement-chi" only

assert AUTHORIZED_USER and UNAUTHORIZED_USER, (
    "Set AUTHORIZED_USER and UNAUTHORIZED_USER to workspace users "
    "with the group memberships described above."
)

# COMMAND ----------

# Import search_as_user, get_user_groups, and GROUP_PLANTS from 04.
# (When run as a job, make 04 a preceding task instead, or keep both
# notebooks in the same Repo so this relative path resolves.)
%run ./04_search_as_user

# COMMAND ----------

RESTRICTED_QUERY = "Apex Fasteners rebate terms"
POLICY_QUERY = "discount approval limits"

failures = []


def check(name, condition, detail=""):
    status = "PASS" if condition else "FAIL"
    print(f"[{status}] {name}" + (f" - {detail}" if detail else ""))
    if not condition:
        failures.append(name)

# COMMAND ----------

# Case 1: authorized user retrieves the restricted document
rows = search_as_user(RESTRICTED_QUERY, user_name=AUTHORIZED_USER)
check(
    "positive: authorized user retrieves POL-100",
    any(r["document_id"] == "POL-100" for r in rows),
    f"documents returned: {sorted({r['document_id'] for r in rows})}",
)

# Case 2: unauthorized user does not
rows = search_as_user(RESTRICTED_QUERY, user_name=UNAUTHORIZED_USER)
check(
    "negative: unauthorized user denied POL-100",
    all(r["document_id"] != "POL-100" for r in rows),
    f"documents returned: {sorted({r['document_id'] for r in rows})}",
)

# Case 3: user-supplied plant_id cannot widen scope
rows = search_as_user(
    POLICY_QUERY, user_name=UNAUTHORIZED_USER, requested_plant_id="DET"
)
check(
    "no widening: plant_id=DET outside user's scope returns nothing",
    len(rows) == 0,
    f"rows returned: {len(rows)}",
)

# Case 4: in-scope query still works
rows = search_as_user(
    POLICY_QUERY, user_name=UNAUTHORIZED_USER, requested_plant_id="CHI"
)
check(
    "sanity: in-scope CHI query returns POL-001",
    any(r["document_id"] == "POL-001" for r in rows),
    f"documents returned: {sorted({r['document_id'] for r in rows})}",
)

# COMMAND ----------

if failures:
    raise RuntimeError(
        f"Access-control validation FAILED: {', '.join(failures)}"
    )
print("All access-control validation cases PASSED.")
