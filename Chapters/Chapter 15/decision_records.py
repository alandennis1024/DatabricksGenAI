"""Read and check Chapter 15 decision records (Markdown files).

A decision record is a Markdown file with a field table and ten sections.
``validate_record`` reports what is missing, empty, or still a placeholder.
"""
from __future__ import annotations

import re
from dataclasses import dataclass, field
from pathlib import Path

REQUIRED_FIELDS = ("Capability", "Status", "Date and version")
REQUIRED_SECTIONS = (
    "Intended use",
    "Excluded use",
    "Owner",
    "Data classification",
    "Threat model",
    "Evaluation evidence",
    "Operational SLOs",
    "Approval boundary",
    "Rollback",
    "Review",
)
VALID_STATUSES = {"Proposed", "Approved", "Retired"}
# The five abuse paths that the chapter says every threat model should cover.
THREAT_KEYWORDS = {
    "indirect injection": "injection",
    "cross-tenant retrieval": "cross-tenant",
    "tool parameter smuggling": "smuggling",
    "excessive agency": "excessive agency",
    "denial of service": "denial of service",
}
_PLACEHOLDER = re.compile(r"<[^<>]{2,}>")
_INLINE_CODE = re.compile(r"`[^`\n]*`")
_VERSION = re.compile(r"^\d{4}-\d{2}-\d{2}, v\d+$")


@dataclass
class Record:
    title: str
    fields: dict[str, str]
    sections: dict[str, str]


@dataclass
class Report:
    errors: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return not self.errors


def read_text(path: str | Path) -> str:
    return Path(path).read_text(encoding="utf-8")


def render_draft(template: str, capability_name: str) -> str:
    """Start a new record: put the capability name in the title."""
    if not capability_name.strip():
        raise ValueError("capability_name must not be empty")
    return template.replace("<capability name>", capability_name.strip(), 1)


def parse_record(text: str) -> Record:
    lines = text.splitlines()
    title = ""
    fields: dict[str, str] = {}
    sections: dict[str, list[str]] = {}
    current: str | None = None
    for line in lines:
        if line.startswith("# ") and not title:
            title = line[2:].strip()
        elif line.startswith("## "):
            current = line[3:].strip()
            sections[current] = []
        elif current is None and line.startswith("|"):
            cells = [cell.strip() for cell in line.strip().strip("|").split("|")]
            if len(cells) == 2 and cells[0] not in ("Field", "---") \
                    and not set(cells[0]) <= {"-"}:
                fields[cells[0]] = cells[1]
        elif current is not None:
            sections[current].append(line)
    return Record(
        title=title,
        fields=fields,
        sections={name: "\n".join(body).strip() for name, body in sections.items()},
    )


def find_placeholders(text: str) -> list[str]:
    """Return every unreplaced <placeholder>, ignoring inline code."""
    return _PLACEHOLDER.findall(_INLINE_CODE.sub("", text))


def validate_record(text: str) -> Report:
    report = Report()
    record = parse_record(text)
    if not record.title.startswith("Decision record:"):
        report.errors.append("Title must start with 'Decision record:'")
    for name in REQUIRED_FIELDS:
        if not record.fields.get(name):
            report.errors.append(f"Missing field: {name}")
    for name in REQUIRED_SECTIONS:
        if name not in record.sections:
            report.errors.append(f"Missing section: {name}")
        elif not record.sections[name]:
            report.errors.append(f"Empty section: {name}")
    for placeholder in find_placeholders(text):
        short = " ".join(placeholder.split())[:50]
        report.errors.append(f"Unreplaced placeholder: {short}")
    status = record.fields.get("Status")
    if status and status not in VALID_STATUSES:
        report.errors.append(
            f"Status must be one of {sorted(VALID_STATUSES)} (got {status!r})"
        )
    version = record.fields.get("Date and version")
    if version and not _VERSION.match(version) and "<" not in version:
        report.errors.append("Date and version must look like 2028-04-02, v1")
    threats = record.sections.get("Threat model", "").lower()
    for label, keyword in THREAT_KEYWORDS.items():
        if threats and keyword not in threats:
            report.warnings.append(f"Threat model does not mention {label}")
    return report
