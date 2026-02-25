"""
Data Quality Agent - orchestrates schema analysis and LLM-based rule generation.

This module examines a table's schema, column names, and descriptive statistics
to suggest basic validation rules. It then invokes an LLM to reason about more
complex patterns such as detecting outliers, inferring business logic, or
proposing cross-column constraints.
"""

import json
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.serving import ChatMessage, ChatMessageRole


class DataQualityAgent:
    """Generates data quality rules for a given Databricks table."""

    def __init__(self, system_prompt: str, example_rules: list[dict]):
        self.system_prompt = system_prompt
        self.example_rules = example_rules
        self.client = WorkspaceClient()

    # ------------------------------------------------------------------ #
    # Step 1: Schema analysis - basic rules derived from metadata
    # ------------------------------------------------------------------ #
    def analyze_schema(self, table_name: str, catalog: str, schema: str) -> dict:
        """Read the table schema and descriptive statistics from Unity Catalog."""
        full_name = f"{catalog}.{schema}.{table_name}"

        # Fetch column metadata from Unity Catalog
        table_info = self.client.tables.get(full_name)
        columns = [
            {
                "name": col.name,
                "type": str(col.type_name),
                "nullable": col.nullable,
            }
            for col in table_info.columns
        ]

        # Generate basic rules from schema metadata
        basic_rules = []
        for col in columns:
            if not col["nullable"]:
                basic_rules.append(
                    {
                        "column": col["name"],
                        "rule": f"{col['name']} IS NOT NULL",
                        "source": "schema",
                    }
                )
            if col["type"] in ("INT", "LONG", "DOUBLE", "FLOAT", "DECIMAL"):
                basic_rules.append(
                    {
                        "column": col["name"],
                        "rule": f"{col['name']} IS NOT NULL OR {col['name']} >= 0",
                        "source": "schema",
                        "note": "Numeric column - consider non-negative constraint",
                    }
                )

        return {"full_name": full_name, "columns": columns, "basic_rules": basic_rules}

    # ------------------------------------------------------------------ #
    # Step 2: LLM reasoning - advanced rules from an LLM
    # ------------------------------------------------------------------ #
    def generate_advanced_rules(self, schema_context: dict) -> list[dict]:
        """Invoke an LLM to propose advanced data quality rules."""
        user_message = self._build_user_prompt(schema_context)

        response = self.client.serving_endpoints.query(
            name="databricks-meta-llama-3-3-70b-instruct",
            messages=[
                ChatMessage(
                    role=ChatMessageRole.SYSTEM,
                    content=self.system_prompt,
                ),
                ChatMessage(
                    role=ChatMessageRole.USER,
                    content=user_message,
                ),
            ],
            max_tokens=1024,
            temperature=0.1,
        )

        raw = response.choices[0].message.content
        return self._parse_llm_response(raw)

    # ------------------------------------------------------------------ #
    # Step 3: Combine and return all rules
    # ------------------------------------------------------------------ #
    def run(self, table_name: str, catalog: str, schema: str) -> list[dict]:
        """Full pipeline: schema analysis then LLM reasoning."""
        schema_context = self.analyze_schema(table_name, catalog, schema)
        advanced_rules = self.generate_advanced_rules(schema_context)

        all_rules = schema_context["basic_rules"] + advanced_rules
        return all_rules

    # ------------------------------------------------------------------ #
    # Internal helpers
    # ------------------------------------------------------------------ #
    def _build_user_prompt(self, schema_context: dict) -> str:
        """Assemble the user prompt sent to the LLM."""
        columns_desc = "\n".join(
            f"  - {c['name']} ({c['type']}, nullable={c['nullable']})"
            for c in schema_context["columns"]
        )
        examples_desc = json.dumps(self.example_rules, indent=2)
        basic_desc = json.dumps(schema_context["basic_rules"], indent=2)

        return (
            f"Table: {schema_context['full_name']}\n\n"
            f"Columns:\n{columns_desc}\n\n"
            f"Basic rules already generated from schema:\n{basic_desc}\n\n"
            f"Example rules for reference:\n{examples_desc}\n\n"
            "Propose additional data quality rules. Focus on:\n"
            "- Outlier detection for numeric columns\n"
            "- Business logic constraints (e.g., end_date > start_date)\n"
            "- Cross-column consistency checks\n"
            "- String format validation where applicable\n\n"
            "Return your rules as a JSON array."
        )

    @staticmethod
    def _parse_llm_response(raw: str) -> list[dict]:
        """Extract a JSON array of rules from the LLM response."""
        cleaned = raw.strip()
        if cleaned.startswith("```"):
            cleaned = cleaned.split("\n", 1)[1]
        if cleaned.endswith("```"):
            cleaned = cleaned.rsplit("```", 1)[0]
        cleaned = cleaned.strip()

        try:
            rules = json.loads(cleaned)
        except json.JSONDecodeError:
            rules = [{"column": "_parse_error", "rule": cleaned, "source": "llm"}]

        for rule in rules:
            rule.setdefault("source", "llm")
        return rules
