"""
Utility helpers for rule formatting and validation.
"""

import json


def format_rules_as_json(rules: list[dict]) -> str:
    """Return rules as a pretty-printed JSON string."""
    return json.dumps(rules, indent=2)


def validate_rule_structure(rules: list[dict]) -> list[dict]:
    """
    Validate that each rule has the required keys and return only well-formed
    rules.  Malformed entries are logged but silently dropped so that a
    single bad rule does not break the entire output.

    Required keys: column, rule
    Optional keys: source, note
    """
    valid = []
    for idx, rule in enumerate(rules):
        if not isinstance(rule, dict):
            print(f"[WARN] Rule at index {idx} is not a dict - skipping: {rule}")
            continue
        if "column" not in rule or "rule" not in rule:
            print(
                f"[WARN] Rule at index {idx} missing 'column' or 'rule' - "
                f"skipping: {rule}"
            )
            continue
        valid.append(rule)
    return valid
