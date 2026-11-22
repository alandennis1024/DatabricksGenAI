# Databricks notebook source
# MAGIC %md
# MAGIC # Chapter 6 - Enforcing access control with metadata filters
# MAGIC
# MAGIC **Book section:** "Enforcing access control with metadata filters" (the
# MAGIC `search_as_user` listing), with `get_user_groups` implemented.
# MAGIC
# MAGIC **What it does:** resolves the caller's identity and groups from the
# MAGIC platform (SCIM), never from request input, and passes the groups as an
# MAGIC `access_group` filter on every search. A user with no groups gets no
# MAGIC results (closed failure mode), and a user-supplied `plant_id` can only
# MAGIC narrow the search to plants the user's groups are allowed to see.
# MAGIC
# MAGIC **Prerequisites:** `00_setup_sample_data` and `01_create_index` finished.
# MAGIC The `leadership_group`, `chi_group` and `det_group` widgets must match
# MAGIC the values used in `00`. The groups need not exist for this notebook,
# MAGIC but your own user only sees chunks if you belong to one of them.
# MAGIC
# MAGIC **Runtime:** under a minute.
# MAGIC
# MAGIC **Creates:** nothing.
# MAGIC
# MAGIC **Reference design:**
# MAGIC * In a notebook or job, the caller is the current user
# MAGIC   (`workspace.current_user.me()`). Passing `user` looks up another user
# MAGIC   by `userName`; this needs permission to read users and is meant for
# MAGIC   tests and admin tools.
# MAGIC * In a Databricks App, take the user from the app's request headers and
# MAGIC   resolve their groups the same way.
# MAGIC * SCIM returns the user's direct group memberships. Add your own
# MAGIC   expansion if you rely on nested groups.
# MAGIC * Group membership is resolved per call. Cache it only as long as your
# MAGIC   risk tolerance allows: the filter is only as current as the group
# MAGIC   data and the index sync.

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

CATALOG = dbutils.widgets.get("catalog").strip()
SCHEMA = dbutils.widgets.get("schema").strip()
ENDPOINT_NAME = dbutils.widgets.get("endpoint_name").strip()
INDEX_NAME = f"{CATALOG}.{SCHEMA}.{dbutils.widgets.get('index_name').strip()}"
LEADERSHIP_GROUP = dbutils.widgets.get("leadership_group").strip()
CHI_GROUP = dbutils.widgets.get("chi_group").strip()
DET_GROUP = dbutils.widgets.get("det_group").strip()

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

my_groups = get_user_groups()
print(f"Your groups: {my_groups}")
print(f"Groups this demo knows about: {sorted(GROUP_PLANTS)}")
if not set(my_groups) & set(GROUP_PLANTS):
    print("You are in none of those groups, so the search below returns "
          "nothing. That is the closed failure mode working. To see rows, "
          "add yourself to one of the groups (or set the group widgets to "
          "groups you belong to, in 00 and here) and run again.")

for row in search_as_user(index, "discount approval limits"):
    print(f"{row['document_id']} | {row['section_title']} "
          f"| {row['plant_id']} | {row['access_group']}")

# COMMAND ----------

# An unknown user resolves to no groups, so the index is never queried.
try:
    print(search_as_user(index, "discount approval limits",
                         user="nobody@example.invalid"))
except Exception as err:
    print("Looking up another user needs permission to read users in this "
          f"workspace: {err}")
