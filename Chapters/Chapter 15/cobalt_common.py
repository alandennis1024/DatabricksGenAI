"""Shared helpers for the Chapter 15 notebooks.

Nothing here reads notebook globals: ``spark`` and ``mlflow`` are passed in.
"""
from __future__ import annotations

import os
import re

DEFAULT_CATALOG = "cobalt_ops"
DEFAULT_SCHEMA = "future"
PROMPT_BASENAME = "procurement_assistant"

# Every table that notebook 04 creates, and every view built on them.
SAMPLE_TABLES = (
    "request_traces",
    "eval_scores",
    "endpoint_metrics",
    "retrieval_events",
    "agent_actions",
)
VIEWS = (
    "v_quality_daily",
    "v_task_success_daily",
    "v_endpoint_reliability_daily",
    "v_tool_timeouts_hourly",
    "v_queue_depth_hourly",
    "v_economics_daily",
    "v_control_daily",
    "v_suspicious_retrieval",
)
# Written into each sample table's COMMENT so notebooks 04 and 99 never
# overwrite or drop a table that holds real data.
SAMPLE_MARKER = "Chapter 15 sample data"

_IDENTIFIER = re.compile(r"^[A-Za-z0-9_\-]+$")


def check_identifier(value: str, label: str) -> str:
    """Return a catalog or schema name that is safe to put in SQL."""
    value = (value or "").strip()
    if not _IDENTIFIER.match(value):
        raise ValueError(
            f"The {label} widget must contain only letters, digits, "
            f"underscores, and hyphens (got {value!r})."
        )
    return value


def quote(name: str) -> str:
    return "`" + name.replace("`", "``") + "`"


def fq(catalog: str, schema: str, name: str) -> str:
    """Backtick-quoted three-level name for SQL."""
    return ".".join(quote(part) for part in (catalog, schema, name))


def prompt_name(catalog: str, schema: str) -> str:
    """Unity Catalog prompt names must have all three levels."""
    return f"{catalog}.{schema}.{PROMPT_BASENAME}"


def on_databricks() -> bool:
    return "DATABRICKS_RUNTIME_VERSION" in os.environ


def _names(rows) -> set[str]:
    return {str(row[0]) for row in rows}


def ensure_catalog_schema(spark, catalog: str, schema: str) -> None:
    """Create the catalog and schema if possible, or explain what to change."""
    catalog = check_identifier(catalog, "catalog")
    schema = check_identifier(schema, "schema")
    try:
        spark.sql(f"CREATE CATALOG IF NOT EXISTS {quote(catalog)}")
    except Exception as exc:  # creation is often not allowed
        if catalog not in _names(spark.sql("SHOW CATALOGS").collect()):
            raise RuntimeError(
                f"Catalog {catalog!r} does not exist and could not be "
                f"created ({type(exc).__name__}). Set the `catalog` widget to "
                "a catalog you can use (on Databricks Free Edition, "
                "use `workspace`), then run the notebook again."
            ) from exc
    try:
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {quote(catalog)}.{quote(schema)}")
    except Exception as exc:
        rows = spark.sql(f"SHOW SCHEMAS IN {quote(catalog)}").collect()
        if schema not in _names(rows):
            raise RuntimeError(
                f"Schema {catalog}.{schema} does not exist and could not be "
                f"created ({type(exc).__name__}). Ask for CREATE SCHEMA on "
                "the catalog, or set the `schema` widget to an existing schema."
            ) from exc


def is_sample_table(spark, fqn: str) -> bool | None:
    """True for a Chapter 15 sample table, False for any other table.

    Returns None when the table does not exist.
    """
    try:
        rows = spark.sql(f"DESCRIBE TABLE EXTENDED {fqn}").collect()
    except Exception as exc:
        if "TABLE_OR_VIEW_NOT_FOUND" in str(exc) or "cannot be found" in str(exc):
            return None
        raise
    for row in rows:
        if str(row[0]).strip().lower() == "comment":
            return SAMPLE_MARKER in str(row[1])
    return False


PROMPT_REGISTRY_HELP = (
    "The MLflow Prompt Registry is a Beta feature. This notebook could not "
    "use it in your workspace. Check that:\n"
    "  1. Beta previews are enabled for your account or workspace admin "
    "(look for 'Prompt Registry' on the workspace Previews page);\n"
    "  2. you have CREATE FUNCTION, EXECUTE, and MANAGE on the schema "
    "{catalog}.{schema} (or pick a schema where you do, using the "
    "`catalog` and `schema` widgets);\n"
    "  3. the cluster runs mlflow >= 3.14 (the first cell installs it)."
)


def prompt_registry_ready(mlflow, catalog: str, schema: str):
    """Probe the registry once. Returns (ready, message)."""
    if on_databricks():
        mlflow.set_registry_uri("databricks-uc")
    try:
        mlflow.genai.search_prompts(
            filter_string=f"catalog = '{catalog}' AND schema = '{schema}'",
            max_results=1,
        )
    except Exception as exc:
        text = PROMPT_REGISTRY_HELP.format(catalog=catalog, schema=schema)
        return False, f"{text}\n\nOriginal error: {type(exc).__name__}: {exc}"
    return True, "Prompt Registry is reachable."
