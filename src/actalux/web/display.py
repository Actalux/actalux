"""Homogenized document display titles, computed at render time (never stored).

The stored ``meeting_title`` is the raw source filename and stays untouched as
provenance. For display we present one consistent name derived from the
document's ``document_type`` and ``meeting_date`` — so a board meeting reads
"April 12, 2023 — Meeting Minutes" no matter how its file was named.

Because the title is computed at render time, every document — already in the
corpus or newly ingested tomorrow — is homogenized automatically; there is no
stored display name that can drift out of sync.
"""

from __future__ import annotations

import re
from collections.abc import Mapping
from datetime import date
from typing import Any

_MONTHS = [
    "",
    "January",
    "February",
    "March",
    "April",
    "May",
    "June",
    "July",
    "August",
    "September",
    "October",
    "November",
    "December",
]

_TYPE_LABELS = {
    "minutes": "Meeting Minutes",
    "transcript": "Meeting Transcript",
    "agenda": "Agenda",
    "packet": "Board Packet",
    "budget": "Budget",
    "resolution": "Resolution",
    "warrants": "Warrants",
    "expenditure_summary": "Expenditure Summary",
    "revenue_summary": "Revenue Summary",
    "audit": "Audit",
    "per_pupil": "Per-Pupil Spending",
    "presentation": "Presentation",
    "schedule": "Meeting Schedule",
    "curriculum_map": "Curriculum Map",
    "curriculum": "Curriculum",
    "governance": "Governance",
    "strategic_plan": "Strategic Plan",
    "facilities_plan": "Facilities Plan",
    "assessment": "Assessment",
    "ballot": "Ballot",
    "other": "Record",
}

# Types whose natural identifier is the meeting date -> date-led title. Other
# types (curriculum, governance, plans, ...) read better as a cleaned filename.
_DATED_TYPES = {
    "minutes",
    "transcript",
    "agenda",
    "budget",
    "resolution",
    "warrants",
    "expenditure_summary",
    "revenue_summary",
}

# Qualifiers worth surfacing on a minutes title, detected from the raw filename.
_DESCRIPTORS: list[tuple[re.Pattern[str], str]] = [
    (re.compile(r"retreat", re.I), "Retreat"),
    (re.compile(r"special", re.I), "Special"),
    (re.compile(r"work(?:ing)?\s+session", re.I), "Work Session"),
    (re.compile(r"community\s+forum", re.I), "Community Forum"),
    (re.compile(r"closed\s+session", re.I), "Closed Session"),
    (re.compile(r"\bjoint\b", re.I), "Joint"),
]

_EXT_RE = re.compile(r"\.(pdf|txt|html?|docx?|md|markdown)$", re.I)
_CANVA_RE = re.compile(r"^canva[ _]+", re.I)


def _coerce_date(value: Any) -> date | None:
    if isinstance(value, date):
        return value
    if isinstance(value, str) and value:
        try:
            return date.fromisoformat(value[:10])
        except ValueError:
            return None
    return None


def _clean_filename(title: str) -> str:
    """Strip extension, the 'canva' scrape prefix, and tidy a raw filename."""
    name = _EXT_RE.sub("", title)
    name = _CANVA_RE.sub("", name)
    return re.sub(r"\s+", " ", name).strip()


def _minutes_descriptors(raw_title: str) -> list[str]:
    parts = [label for pattern, label in _DESCRIPTORS if pattern.search(raw_title)]
    if re.search(r"draft", raw_title, re.I) and not re.search(r"signed", raw_title, re.I):
        parts.append("draft")
    return parts


def display_title(doc: Mapping[str, Any]) -> str:
    """A homogenized display title for a document/result mapping.

    Expects ``document_type``, ``meeting_date``, and ``meeting_title`` keys
    (the shapes returned by db.get_document, list_documents, and enrich_results).
    """
    raw = (doc.get("meeting_title") or "").strip()
    dtype = doc.get("document_type") or "other"
    label = _TYPE_LABELS.get(dtype, "Record")

    if dtype in _DATED_TYPES:
        d = _coerce_date(doc.get("meeting_date"))
        if d and 1 <= d.month <= 12:
            base = f"{_MONTHS[d.month]} {d.day}, {d.year} — {label}"
            descriptors = _minutes_descriptors(raw) if dtype == "minutes" else []
            return f"{base} ({', '.join(descriptors)})" if descriptors else base

    return _clean_filename(raw) or label
