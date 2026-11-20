# Databricks notebook source
# MAGIC %md
# MAGIC # Chapter 6 - Enforcing access control with metadata filters
# MAGIC
# MAGIC The pattern from the chapter, with `get_user_groups` implemented:
# MAGIC identity and group membership come from the platform (SCIM), never
# MAGIC from request input.
# MAGIC
# MAGIC Reference design:
# MAGIC * In a notebook/job, the caller's identity is the current user
# MAGIC   (`workspace.current_user.me()`).
# MAGIC * In a Databricks App, take the user from the app session headers and
# MAGIC   resolve their groups the same way.
# MAGIC * Group membership is resolved per call. Cache it only for as long as
# MAGIC   your risk tolerance allows - the filter is only as current as the
# MAGIC   group data and the index sync.

# COMMAND ----------

from databricks.sdk import WorkspaceClient
from databricks.vector_search.client import VectorSearchClient

ENDPOINT_NAME = "cobalt-policy-search"
INDEX_NAME = "cobalt_ops.serving.policy_search"

COLUMNS = ["chunk_id", "content", "document_id", "section_title",
           "effective_date", "plant_id", "access_group"]

# Which plants each group may see. Edit to match your environment.
GROUP_PLANTS = {
    "procurement-chi": {"CHI"},
    "procurement-det": {"DET"},
    "leadership": {"CHI", "DET"},
}

workspace = WorkspaceClient()
index = VectorSearchClient().get_index(
    endpoint_name=ENDPOINT_NAME,
    index_name=INDEX_NAME,
)

# COMMAND ----------

def get_user_groups(user_name=None):
    """Resolve group display names from SCIM. Empty list means no access."""
    if user_name is None:
        me = workspace.current_user.me()
        return [g.display for g in (me.groups or []) if g.display]
    matches = list(
        workspace.users.list(filter=f'userName eq "{user_name}"')
    )
    if not matches:
        return []
    user = workspace.users.get(matches[0].id)
    return [g.display for g in (user.groups or []) if g.display]

# COMMAND ----------

def _rows(results):
    manifest = results.get("manifest") or {}
    names = [c.get("name") for c in manifest.get("columns", [])] or COLUMNS
    return [dict(zip(names, row))
            for row in results["result"].get("data_array") or []]


def search_as_user(question, user_name=None, requested_plant_id=None,
                   num_results=10):
    groups = get_user_groups(user_name)
    if not groups:
        return []  # closed failure mode: no groups, no results

    filters = {"access_group": groups}

    if requested_plant_id is not None:
        allowed = set().union(
            *(GROUP_PLANTS.get(g, set()) for g in groups)
        )
        if requested_plant_id not in allowed:
            return []  # user-supplied plant cannot widen scope
        filters["plant_id"] = requested_plant_id

    results = index.similarity_search(
        query_text=question,
        columns=COLUMNS,
        filters=filters,
        num_results=num_results,
        query_type="hybrid",
    )
    return _rows(results)

# COMMAND ----------

for row in search_as_user("discount approval limits"):
    print(f"{row['document_id']} | {row['section_title']} "
          f"| {row['plant_id']} | {row['access_group']}")
