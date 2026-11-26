"""Chapter 11: pure governance contracts and *synthetic* training fixtures.

This library is intentionally independent of Databricks/Spark for offline CI.
No function grants enterprise permissions or authorizes physical maintenance.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timezone
from hashlib import sha256
import io
import math
import re
import struct
import wave
import zlib
from typing import Iterable, Mapping

SQL_ID_RE = re.compile(r"^[a-z][a-z0-9_]*$")
PNG_SIGNATURE = b"\x89PNG\r\n\x1a\n"


@dataclass(frozen=True)
class Manual:
    manual_id: str
    revision: str
    machine_family: str
    classification: str
    approval_status: str
    content: str


MANUALS: tuple[Manual, ...] = (
    Manual("XM400-OPS", "2026-11", "XM-400", "INTERNAL", "APPROVED",
           "XM-400: Stop and isolate the system. Follow approved site LOTO. "
           "Only qualified personnel may perform mechanical inspection."),
    Manual("XM400-PUBLIC", "2026-10", "XM-400", "PUBLIC", "APPROVED",
           "XM-400 synthetic reference: identify the manual revision before review."),
    Manual("XM400-RETIRED", "2018-01", "XM-400", "INTERNAL", "RETIRED",
           "OBSOLETE DRAFT: bypass the guard and force restart. NEVER USE."),
    Manual("XM400-RESTRICTED", "2026-11", "XM-400", "RESTRICTED", "APPROVED",
           "Restricted internal engineering drawings. Not for general access."),
)


def validate_sql_identifier(identifier: str) -> str:
    if not isinstance(identifier, str) or not SQL_ID_RE.fullmatch(identifier):
        raise ValueError("Expected simple lowercase SQL identifier [a-z][a-z0-9_]*")
    return identifier


def qualified_schema(catalog: str, schema: str) -> str:
    return f"{validate_sql_identifier(catalog)}.{validate_sql_identifier(schema)}"


def csv_manual_records() -> list[dict[str, str]]:
    return [vars(m).copy() for m in MANUALS]


def machine_family(machine_id: str) -> str | None:
    match = re.match(r"^(XM-\d+)(?:-[A-Za-z0-9]+)*$", machine_id or "")
    return match.group(1) if match else None


def authorized_manuals(
    manuals: Iterable[Manual], *, verified_identity: bool,
    allowed_classifications: Iterable[str], family: str,
) -> list[Manual]:
    """Fail closed. caller's entitlement must come from authenticated server context.

    `family` is a *relevance* filter, not proof of authorization. In production,
    authorize every retrieved document using identity-aware service controls.
    """
    if not verified_identity:
        return []
    allowed = set(allowed_classifications)
    return [m for m in manuals
            if m.approval_status == "APPROVED"
            and m.machine_family == family
            and m.classification in allowed]


def chunk_manual(manual: Manual, max_words: int = 16) -> list[dict[str, str | int]]:
    if max_words < 1:
        raise ValueError("max_words must be positive")
    words = manual.content.split()
    records = []
    for n, start in enumerate(range(0, len(words), max_words)):
        text = " ".join(words[start:start + max_words])
        records.append({
            "chunk_id": sha256(f"{manual.manual_id}:{manual.revision}:{n}".encode()).hexdigest(),
            "manual_id": manual.manual_id,
            "manual_revision": manual.revision,
            "machine_family": manual.machine_family,
            "classification": manual.classification,
            "approval_status": manual.approval_status,
            "chunk_number": n,
            "text_chunk": text,
        })
    return records


def png_synthetic_bytes(width: int = 8, height: int = 8) -> bytes:
    """Produce a valid flat-color PNG with no realistic person or PII."""
    if not 1 <= width <= 128 or not 1 <= height <= 128:
        raise ValueError("Image dimensions outside demo bounds")
    def chunk(kind: bytes, body: bytes) -> bytes:
        return (struct.pack(">I", len(body)) + kind + body
                + struct.pack(">I", zlib.crc32(kind + body) & 0xFFFFFFFF))
    rows = b"".join(b"\x00" + bytes([90, 130, 180]) * width
                    for _ in range(height))
    ihdr = struct.pack(">2I5B", width, height, 8, 2, 0, 0, 0)
    return PNG_SIGNATURE + chunk(b"IHDR", ihdr) + chunk(b"IDAT", zlib.compress(rows)) + chunk(b"IEND", b"")


def png_integrity_gate(data: bytes | None) -> str:
    """Valid header/CRC check, **NOT** a face detector or PII approval."""
    if not isinstance(data, bytes) or not data.startswith(PNG_SIGNATURE):
        return "QUARANTINE"
    pos = len(PNG_SIGNATURE)
    valid_ihdr = False
    seen_iend = False
    try:
        while pos + 12 <= len(data):
            length = struct.unpack(">I", data[pos:pos + 4])[0]
            if length > 5_000_000 or pos + 12 + length > len(data):
                return "QUARANTINE"
            typ = data[pos + 4:pos + 8]
            body = data[pos + 8:pos + 8 + length]
            checksum = struct.unpack(">I", data[pos + 8 + length:pos + 12 + length])[0]
            if checksum != zlib.crc32(typ + body) & 0xFFFFFFFF:
                return "QUARANTINE"
            if typ == b"IHDR":
                if len(body) != 13:
                    return "QUARANTINE"
                width, height = struct.unpack(">2I", body[:8])
                valid_ihdr = width > 0 and height > 0
            if typ == b"IEND":
                seen_iend = True
                break
            pos += 12 + length
    except (ValueError, TypeError, struct.error):
        return "QUARANTINE"
    return "REVIEW_REQUIRED" if valid_ihdr and seen_iend else "QUARANTINE"


def synthetic_wav_bytes(sample_rate: int = 8000, duration_s: float = 0.15) -> bytes:
    n = int(sample_rate * duration_s)
    pcm = b"".join(struct.pack("<h", int(1800 * math.sin(2 * math.pi * 300 * k / sample_rate)))
                   for k in range(n))
    out = io.BytesIO()
    with wave.open(out, "wb") as handle:
        handle.setnchannels(1)
        handle.setsampwidth(2)
        handle.setframerate(sample_rate)
        handle.writeframes(pcm)
    return out.getvalue()


def wav_rms(wav_bytes: bytes | None) -> float | None:
    """PCM16 mono demonstration; fail closed for unsupported or broken data."""
    if not isinstance(wav_bytes, bytes):
        return None
    try:
        with wave.open(io.BytesIO(wav_bytes), "rb") as sound:
            if sound.getcomptype() != "NONE" or sound.getsampwidth() != 2 or sound.getnchannels() != 1:
                return None
            frames = sound.readframes(sound.getnframes())
        if len(frames) == 0 or len(frames) % 2:
            return None
        values = struct.unpack("<" + "h" * (len(frames) // 2), frames)
        return math.sqrt(sum(x * x for x in values) / len(values)) / 32768.0
    except (wave.Error, ValueError, TypeError, EOFError, struct.error):
        return None


def safety_screen_signal(text: str) -> str:
    """Keyword demo: BLOCK an explicit unsafe phrase; otherwise REVIEW, never APPROVE."""
    if not isinstance(text, str):
        return "REVIEW_REQUIRED"
    norm = " ".join(text.lower().split())
    if "bypass" in norm and ("loto" in norm or "lockout" in norm or "interlock" in norm):
        return "BLOCK_SAFETY_VIOLATION"
    return "REVIEW_REQUIRED"


def audited_request_match(inference: Mapping, audit: Mapping) -> bool:
    """Match only a *verified shared request key* and endpoint.

    Temporal proximity alone is insufficient to attribute an inference to a user.
    These mappings are controlled fixtures, not the system.access.audit schema.
    """
    key = inference.get("request_id")
    principal = audit.get("authenticated_principal")
    return bool(key and isinstance(key, str) and principal
                and audit.get("request_id") == key
                and audit.get("endpoint") == inference.get("endpoint"))


def incident_disposition(manual: Manual) -> str:
    return "ALLOW_RETRIEVAL" if manual.approval_status == "APPROVED" else "QUARANTINE"
