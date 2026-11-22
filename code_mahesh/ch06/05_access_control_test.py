# Databricks notebook source
# MAGIC %md
# MAGIC # Chapter 6 - Access-control validation test
# MAGIC
# MAGIC **Book section:** the closing test in "Enforcing access control with
# MAGIC metadata filters": an authorized user retrieves the restricted document,
# MAGIC and an unauthorized user does not. This notebook is new.
# MAGIC
# MAGIC **What it does:** post-deploy validation for the metadata-filter
# MAGIC pattern in `04_search_as_user`. Cases:
# MAGIC 1. **Positive** - an authorized user (a member of the leadership group)
# MAGIC    retrieves the restricted document `POL-100`.
# MAGIC 2. **Negative** - an unauthorized user (a member of the Chicago
# MAGIC    procurement group only) does not.
# MAGIC 3. **No widening** - a user-supplied `plant_id` outside the user's
# MAGIC    authorized plants yields no results.
# MAGIC 4. **Sanity** - an authorized in-scope query returns `POL-001`.
# MAGIC
# MAGIC It prints PASS or FAIL per case and raises (failing a job) on any
# MAGIC failure.
# MAGIC
# MAGIC **Prerequisites:** `00` and `01` finished, plus two real workspace users
# MAGIC whose group membership you set up beforehand: `authorized_user` is in
# MAGIC the leadership group, `unauthorized_user` is in the Chicago procurement
# MAGIC group but **not** in the leadership group. Enter their user names
# MAGIC (emails) in the widgets. Their groups are resolved through SCIM, and you
# MAGIC need permission to read users. Nothing trusts input for identity.
# MAGIC
# MAGIC **If you leave the user widgets empty,** the notebook prints this setup
# MAGIC guidance and your own groups, skips the test, and does not fail. You can
# MAGIC also point the group widgets (the same values as in `00`) at groups that
# MAGIC already exist in your workspace.
# MAGIC
# MAGIC **Runtime:** under a minute. **Creates:** nothing.

# COMMAND ----------

# MAGIC %pip install -q -r requirements.txt

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

dbutils.widgets.text("catalog", "cobalt_ops", "Catalog")
dbutils.widgets.text("schema", "serving", "Schema")
dbutils.widgets.text("endpoint_name", "cobalt-policy-search", "AI Search endpoint")
dbutils.widgets.text("index_name", "policy_search", "Index name (short)")
dbutils.widgets.text("leadership_group", "leadership", "Group for POL-100")
dbutils.widgets.text("chi_group", "procurement-chi", "Chicago procurement group")
dbutils.widgets.text("det_group", "procurement-det", "Detroit procurement group")
dbutils.widgets.text("authorized_user", "", "Authorized user (email)")
dbutils.widgets.text("unauthorized_user", "", "Unauthorized user (email)")

CATALOG = dbutils.widgets.get("catalog").strip()
SCHEMA = dbutils.widgets.get("schema").strip()
ENDPOINT_NAME = dbutils.widgets.get("endpoint_name").strip()
INDEX_NAME = f"{CATALOG}.{SCHEMA}.{dbutils.widgets.get('index_name').strip()}"
LEADERSHIP_GROUP = dbutils.widgets.get("leadership_group").strip()
CHI_GROUP = dbutils.widgets.get("chi_group").strip()
DET_GROUP = dbutils.widgets.get("det_group").strip()
AUTHORIZED_USER = dbutils.widgets.get("authorized_user").strip()
UNAUTHORIZED_USER = dbutils.widgets.get("unauthorized_user").strip()

# COMMAND ----------

import re

from databricks.ai_search.client import AISearchClient
from databricks.sdk import WorkspaceClient

COLUMNS = ["chunk_id", "content", "document_id", "section_title",
           "effective_date", "plant_id", "access_group"]

# Which plants each group may see. Built from the widgets; edit if needed.
GROUP_PLANTS = {}
for _group, _plants in ((CHI_GROUP, {"CHI"}), (DET_GROUP, {"DET"}),
                        (LEADERSHIP_GROUP, {"CHI", "DET"})):
    GROUP_PLANTS[_group] = GROUP_PLANTS.get(_group, set()) | _plants

workspace = WorkspaceClient()
client = AISearchClient()
if not client.index_exists(endpoint_name=ENDPOINT_NAME, index_name=INDEX_NAME):
    raise RuntimeError(
        f"Index {INDEX_NAME} was not found on endpoint {ENDPOINT_NAME}. Run "
        "01_create_index first, and check the widgets match."
    )
index = client.get_index(
    endpoint_name=ENDPOINT_NAME,
    index_name=INDEX_NAME,
)

# COMMAND ----------

# --- shared with 05_access_control_test: keep both copies identical ---
def get_user_groups(user_name=None):
    """Resolve group display names from SCIM. An empty list means no access."""
    if user_name is None:
        me = workspace.current_user.me()
        return [g.display for g in (me.groups or []) if g.display]
    if not re.fullmatch(r"[A-Za-z0-9_.+@'-]+", user_name):
        return []  # never build a SCIM filter from an unexpected string
    matches = list(workspace.users.list(filter=f'userName eq "{user_name}"'))
    if not matches:
        return []
    user = workspace.users.get(matches[0].id)
    return [g.display for g in (user.groups or []) if g.display]


def _rows(results):
    manifest = results.get("manifest") or {}
    names = [c.get("name") for c in manifest.get("columns", [])] or COLUMNS
    data = (results.get("result") or {}).get("data_array") or []
    return [dict(zip(names, row)) for row in data]


def search_as_user(index, question, user=None, requested_plant_id=None,
                   num_results=10):
    groups = get_user_groups(user)  # from the platform, never the request body
    if not groups:
        return []  # closed failure mode: no groups, no results

    filters = {"access_group": groups}  # a list matches any of its values

    if requested_plant_id is not None:
        allowed = set().union(*(GROUP_PLANTS.get(g, set()) for g in groups))
        if requested_plant_id not in allowed:
            return []  # a user-supplied plant cannot widen the scope
        filters["plant_id"] = requested_plant_id

    results = index.similarity_search(
        query_text=question,
        columns=COLUMNS,
        filters=filters,
        num_results=num_results,
        query_type="hybrid",
    )
    return _rows(results)
# --- end shared code ---

# COMMAND ----------

def check_preconditions():
    """Fail with a clear message if the two users are not set up correctly."""
    auth_groups = get_user_groups(AUTHORIZED_USER)
    unauth_groups = get_user_groups(UNAUTHORIZED_USER)
    print(f"{AUTHORIZED_USER} groups: {auth_groups}")
    print(f"{UNAUTHORIZED_USER} groups: {unauth_groups}")
    problems = []
    if LEADERSHIP_GROUP not in auth_groups:
        problems.append(f"{AUTHORIZED_USER} must be in '{LEADERSHIP_GROUP}'")
    if CHI_GROUP not in unauth_groups:
        problems.append(f"{UNAUTHORIZED_USER} must be in '{CHI_GROUP}'")
    if LEADERSHIP_GROUP in unauth_groups:
        problems.append(
            f"{UNAUTHORIZED_USER} must NOT be in '{LEADERSHIP_GROUP}'"
        )
    if problems:
        raise ValueError(
            "Test setup is wrong: " + "; ".join(problems) + ". Fix the group "
            "membership, or change the group widgets (use the same values "
            "as in 00_setup_sample_data). A user not found in SCIM also "
            "looks like this."
        )


RESTRICTED_QUERY = "Apex Fasteners rebate terms"
POLICY_QUERY = "discount approval limits"

failures = []


def check(name, condition, detail=""):
    status = "PASS" if condition else "FAIL"
    print(f"[{status}] {name}" + (f" - {detail}" if detail else ""))
    if not condition:
        failures.append(name)


def docs(rows):
    return sorted({r["document_id"] for r in rows})


def run_cases():
    # Case 1: authorized user retrieves the restricted document
    rows = search_as_user(index, RESTRICTED_QUERY, user=AUTHORIZED_USER)
    check("positive: authorized user retrieves POL-100",
          "POL-100" in docs(rows), f"documents returned: {docs(rows)}")

    # Case 2: unauthorized user does not
    rows = search_as_user(index, RESTRICTED_QUERY, user=UNAUTHORIZED_USER)
    check("negative: unauthorized user denied POL-100",
          "POL-100" not in docs(rows), f"documents returned: {docs(rows)}")

    # Case 3: a user-supplied plant_id cannot widen scope
    rows = search_as_user(index, POLICY_QUERY, user=UNAUTHORIZED_USER,
                          requested_plant_id="DET")
    check("no widening: plant_id=DET outside user's scope returns nothing",
          len(rows) == 0, f"rows returned: {len(rows)}")

    # Case 4: an in-scope query still works
    rows = search_as_user(index, POLICY_QUERY, user=UNAUTHORIZED_USER,
                          requested_plant_id="CHI")
    check("sanity: in-scope CHI query returns POL-001",
          "POL-001" in docs(rows), f"documents returned: {docs(rows)}")

    if failures:
        raise RuntimeError(
            f"Access-control validation FAILED: {', '.join(failures)}"
        )
    print("All access-control validation cases PASSED.")

# COMMAND ----------

if AUTHORIZED_USER and UNAUTHORIZED_USER:
    check_preconditions()
    run_cases()
else:
    print("SKIPPED: set the 'authorized_user' and 'unauthorized_user' widgets "
          "to two workspace users (see the notes at the top of this "
          "notebook), then run this cell again.")
    print(f"Your own groups, for reference: {get_user_groups()}")
