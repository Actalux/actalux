"""Ingest-time guard against private personal information.

Actalux publishes board/administration *policy* records only -- never
individual student or non-official-personnel private records, and never
closed-session deliberation content (see the content policy in CLAUDE.md). The
real enforcement point for that guarantee is **ingestion**: if a private record
never enters the database, search can never surface it.

This guard scans a document's text before it is stored and flags **high-
precision PII tokens** -- patterns that genuinely never appear in public board
records, so a match almost certainly means a private record slipped in:

- Social Security numbers (`123-45-6789`)
- An explicit date-of-birth label followed by an actual date (`DOB: 03/15/2010`)

It is deliberately **narrow, not comprehensive**. It does NOT try to detect
contextual PII -- a named individual in a disciplinary, medical, or grade
context -- because that cannot be told apart from legitimate *public policy
about* those topics by pattern or even by an LLM relevance judge (the retrieval
eval confirmed this: every "leak" there traced to the public Missouri Sunshine
Law text, not an actual record). Catching contextual PII is a human-review job.
This guard is a backstop for the clear-cut cases, not a guarantee.

Matched values are never logged or returned in the clear -- only the pattern
name, character offset, and a digit-masked shape -- so the guard never copies
PII into logs or the database.
"""

from __future__ import annotations

import re
from dataclasses import dataclass

# High-precision patterns only. Each must be specific enough that a public
# board/admin record would not match it. Undashed SSNs and bare 9-digit runs
# are intentionally NOT matched -- they collide with budget codes / IDs.
PII_PATTERNS: dict[str, re.Pattern[str]] = {
    "ssn": re.compile(r"\b\d{3}-\d{2}-\d{4}\b"),
    "date_of_birth": re.compile(
        r"\b(?:DOB|D\.O\.B\.|date of birth)\b\s*[:\-]?\s*\d{1,2}[/-]\d{1,2}[/-]\d{2,4}",
        re.IGNORECASE,
    ),
}

GUARD_MODES = ("block", "warn", "off")


@dataclass(frozen=True)
class PIIFinding:
    """One high-precision PII match, with the value masked.

    `masked` replaces every digit in the match with 'X', preserving shape (so
    the operator can recognise the pattern) without copying the value anywhere.
    """

    pattern_name: str
    char_offset: int
    masked: str


def scan_text(text: str) -> list[PIIFinding]:
    """Return high-precision PII findings in `text` (empty list if clean)."""
    findings: list[PIIFinding] = []
    for name, pattern in PII_PATTERNS.items():
        for match in pattern.finditer(text):
            masked = re.sub(r"\d", "X", match.group(0))
            findings.append(PIIFinding(pattern_name=name, char_offset=match.start(), masked=masked))
    findings.sort(key=lambda f: f.char_offset)
    return findings


def should_block(findings: list[PIIFinding], mode: str) -> bool:
    """Whether findings should block ingestion under `mode`.

    Only "block" mode stops ingestion; "warn" and "off" never do. The default
    mode is "block" (see config.Config.pii_guard_mode) -- fail safe.
    """
    return bool(findings) and mode == "block"


def summarize(findings: list[PIIFinding]) -> str:
    """One-line, value-free summary for logs (pattern names + counts)."""
    counts: dict[str, int] = {}
    for f in findings:
        counts[f.pattern_name] = counts.get(f.pattern_name, 0) + 1
    return ", ".join(f"{name}×{n}" for name, n in sorted(counts.items()))
