"""Group documents into reader-facing topics for the digest roundup.

The operator chose a *themed* roundup (one section per topic) over a flat
per-document changelog, so the drafter needs a stable mapping from a document's
``document_type`` (as produced by ``ingest.classify``) to a topic heading, and a
fixed section order. Anything unmapped falls into "Other records" rather than
being dropped.
"""

from __future__ import annotations

from collections import OrderedDict
from typing import Protocol

# Topic headings in the order they appear in the post. Board meetings and budget
# lead because they carry the most timely public-record activity.
THEME_MEETINGS = "Board meetings"
THEME_BUDGET = "Budget & spending"
THEME_FACILITIES = "Facilities"
THEME_CURRICULUM = "Curriculum & instruction"
THEME_OTHER = "Other records"

THEME_ORDER: tuple[str, ...] = (
    THEME_MEETINGS,
    THEME_BUDGET,
    THEME_FACILITIES,
    THEME_CURRICULUM,
    THEME_OTHER,
)

# document_type -> theme. Types come from ingest.classify.classify_document_type.
_TYPE_TO_THEME: dict[str, str] = {
    # Board meetings
    "agenda": THEME_MEETINGS,
    "minutes": THEME_MEETINGS,
    "packet": THEME_MEETINGS,
    "resolution": THEME_MEETINGS,
    "transcript": THEME_MEETINGS,
    "schedule": THEME_MEETINGS,
    "governance": THEME_MEETINGS,
    "ballot": THEME_MEETINGS,
    # Budget & spending
    "budget": THEME_BUDGET,
    "audit": THEME_BUDGET,
    "per_pupil": THEME_BUDGET,
    "warrants": THEME_BUDGET,
    "expenditure_summary": THEME_BUDGET,
    "revenue_summary": THEME_BUDGET,
    # Facilities
    "facilities_plan": THEME_FACILITIES,
    # Curriculum & instruction
    "curriculum_map": THEME_CURRICULUM,
    "curriculum": THEME_CURRICULUM,
    "assessment": THEME_CURRICULUM,
    "strategic_plan": THEME_CURRICULUM,
    # Other records
    "presentation": THEME_OTHER,
    "other": THEME_OTHER,
}


def theme_for(document_type: str) -> str:
    """Map a ``document_type`` to its topic heading; unknown types -> "Other records"."""
    return _TYPE_TO_THEME.get((document_type or "").strip().lower(), THEME_OTHER)


class _HasTheme(Protocol):
    theme: str


def group_by_theme(docs: list[_HasTheme]) -> OrderedDict[str, list[_HasTheme]]:
    """Group items (each carrying a ``theme``) into THEME_ORDER, dropping empties.

    Returns an ordered mapping so the drafter walks topics in a fixed sequence,
    and only includes a heading when at least one document fell under it.
    """
    grouped: OrderedDict[str, list[_HasTheme]] = OrderedDict()
    for theme in THEME_ORDER:
        members = [d for d in docs if d.theme == theme]
        if members:
            grouped[theme] = members
    return grouped
