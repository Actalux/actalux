"""Canonical filename/title -> (document_type, meeting_date) derivation.

Single source of truth shared by the ingest pipeline (scripts/ingest.py) and the
retroactive corrector (scripts/recategorize_documents.py), so a document gets the
same type and date whether it is ingested today or relabelled later. Keeping this
in one place is what stops new ingests from re-creating the "typed 'other' /
dated to the ingest day" mess that the recategorize pass cleaned up — and it
keeps the render-time display titles (web.display) homogeneous going forward.
"""

from __future__ import annotations

import re
from datetime import date

_MONTHS = {
    "jan": 1,
    "feb": 2,
    "mar": 3,
    "apr": 4,
    "may": 5,
    "jun": 6,
    "jul": 7,
    "aug": 8,
    "sep": 9,
    "oct": 10,
    "nov": 11,
    "dec": 12,
}
_SANE_YEARS = range(2015, 2031)

# --- date patterns, tried in this order. Digit lookarounds (not \b) so a date
# followed by "_" — common in filenames like "2024-03-15_board" — still matches. ---
_ISO_RE = re.compile(r"(?<!\d)(\d{4})-(\d{2})-(\d{2})(?!\d)")
_MONTH_DATE_RE = re.compile(
    r"(jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)[a-z]*\.?\s+"
    r"(\d{1,2})(?:st|nd|rd|th)?,?\s+(\d{4})",
    re.I,
)
_MMDDYY_DOT_RE = re.compile(r"(?<!\d)(\d{1,2})\.(\d{1,2})\.(\d{2})(?!\d)")
_MMDDYY_DASH_RE = re.compile(r"(?<!\d)(\d{1,2})-(\d{1,2})-(\d{2})(?!\d)")
_MMDDYY_SPACE_RE = re.compile(r"(?<!\d)(\d{1,2})\s+(\d{1,2})\s+(\d{2})(?!\d)")
_MONTH_YEAR_RE = re.compile(
    r"(jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)[a-z]*(\d{4})(?!\d)", re.I
)
_FISCAL_RE = re.compile(r"(\d{4})-(\d{4})(?=[\s_]|$)")
# Space-separated fiscal year, e.g. "Clayton 2019 2020 Budget". Requires the two
# years to be consecutive so it can't fire on two unrelated 4-digit numbers.
_FISCAL_SPACE_RE = re.compile(r"(?<!\d)(\d{4})\s+(\d{4})(?!\d)")
# Compact MM DD YYYY with no separators, e.g. "BOE_Adopt 20-21 Budget_06242020".
_MMDDYYYY_RE = re.compile(r"(?<!\d)(\d{2})(\d{2})(\d{4})(?!\d)")
_COMPACT_RE = re.compile(r"(jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)(\d{1,2})(?!\d)", re.I)

_ANNUAL_RE = re.compile(r"20\d{2}\s*[ _-]\s*20\d{2}")


def _safe_date(year: int, month: int, day: int) -> date | None:
    if year not in _SANE_YEARS or not (1 <= month <= 12) or not (1 <= day <= 31):
        return None
    try:
        return date(year, month, day)
    except ValueError:
        return None


def is_annual_schedule(text: str) -> bool:
    """An annual board-meeting schedule/calendar (a span of years), not one meeting."""
    s = (text or "").replace("-", " ")
    low = s.lower()
    return bool(_ANNUAL_RE.search(s)) and (
        "board of education meeting" in low or "meetings" in low or "calendar" in low
    )


def parse_meeting_date(name: str, today: date | None = None) -> date | None:
    """Parse a meeting date from a filename/title, or None if none is confident.

    Handles ISO (2024-03-15), "Apr 12, 2023", "11.16.22", "10-29-25", "10 26 22",
    compact "06242020" (MMDDYYYY), "Feb2025" (day defaults to 1), fiscal
    "2024-2025" or "2024 2025" (-> Jul 1 of start year), and compact "jan21" (year
    inferred as the most recent past occurrence; needs ``today``). Fiscal/compact
    are last so explicit full dates win.
    """
    name = name or ""

    m = _ISO_RE.search(name)
    if m:
        d = _safe_date(int(m.group(1)), int(m.group(2)), int(m.group(3)))
        if d:
            return d
    m = _MONTH_DATE_RE.search(name)
    if m:
        d = _safe_date(int(m.group(3)), _MONTHS[m.group(1).lower()[:3]], int(m.group(2)))
        if d:
            return d
    for pat in (_MMDDYY_DOT_RE, _MMDDYY_DASH_RE, _MMDDYY_SPACE_RE):
        m = pat.search(name)
        if m:
            d = _safe_date(2000 + int(m.group(3)), int(m.group(1)), int(m.group(2)))
            if d:
                return d
    m = _MMDDYYYY_RE.search(name)
    if m:
        d = _safe_date(int(m.group(3)), int(m.group(1)), int(m.group(2)))
        if d:
            return d
    m = _MONTH_YEAR_RE.search(name)
    if m:
        d = _safe_date(int(m.group(2)), _MONTHS[m.group(1).lower()[:3]], 1)
        if d:
            return d
    m = _FISCAL_RE.search(name)
    if m:
        d = _safe_date(int(m.group(1)), 7, 1)
        if d:
            return d
    # Space-separated fiscal, but only when the two years are consecutive (so a
    # "2019 2020" budget parses while two unrelated years do not).
    m = _FISCAL_SPACE_RE.search(name)
    if m and int(m.group(2)) == int(m.group(1)) + 1:
        d = _safe_date(int(m.group(1)), 7, 1)
        if d:
            return d
    if today is not None:
        m = _COMPACT_RE.search(name)
        if m:
            month = _MONTHS[m.group(1).lower()[:3]]
            day = int(m.group(2))
            candidate = _safe_date(today.year, month, day)
            if candidate and candidate > today:
                candidate = _safe_date(today.year - 1, month, day)
            if candidate:
                return candidate
    return None


# Specific-type patterns, checked in order (first match wins). The original
# ingest types come first in their original order (so a doc that already
# classified correctly still does); newer types are appended. The one change to
# an existing type is that minutes now also covers the "BOE MM signed" naming
# scheme that previously fell through to 'other'.
_TYPE_PATTERNS: list[tuple[str, re.Pattern[str]]] = [
    ("agenda", re.compile(r"agenda", re.I)),
    (
        "minutes",
        re.compile(
            r"minutes|\bMM\b|business meeting|board retreat|boe meeting"
            r"|work(?:ing)? session|special meeting",
            re.I,
        ),
    ),
    ("packet", re.compile(r"packet|board.?pack", re.I)),
    ("resolution", re.compile(r"resolution", re.I)),
    ("audit", re.compile(r"audit|audited|cafr|acfr|comprehensive\s+annual\s+financial", re.I)),
    ("per_pupil", re.compile(r"per[-_\s]?pupil", re.I)),
    ("warrants", re.compile(r"warrants?", re.I)),
    ("expenditure_summary", re.compile(r"expenditure\s+summary", re.I)),
    ("revenue_summary", re.compile(r"revenue\s+summary", re.I)),
    ("budget", re.compile(r"budget", re.I)),
    ("presentation", re.compile(r"presentation|preliminary.?plan", re.I)),
    ("ballot", re.compile(r"ballot", re.I)),
    # newer types (formerly 'other')
    ("curriculum_map", re.compile(r"curriculum.{0,3}map", re.I)),
    ("curriculum", re.compile(r"curriculum", re.I)),
    ("facilities_plan", re.compile(r"master\s*plan|facilities\s+plan", re.I)),
    ("strategic_plan", re.compile(r"strategic|\bcsip\b", re.I)),
    ("assessment", re.compile(r"assessment", re.I)),
    (
        "governance",
        re.compile(
            r"sunshine|candidate|orientation|livestream|public comment|governance"
            r"|gifted|resource guide|open meetings|\bpolicy\b",
            re.I,
        ),
    ),
]


def classify_document_type(name: str, *, is_text_file: bool = False) -> str:
    """Derive a document_type from a filename/title; 'other' if nothing matches.

    Annual schedules are checked first (they contain "Meeting Minutes" but are
    not minutes). ``is_text_file`` marks a .txt source so board-meeting
    transcripts (which carry no type keyword) classify as 'transcript'.
    """
    name = name or ""
    if is_annual_schedule(name):
        return "schedule"
    for doc_type, pattern in _TYPE_PATTERNS:
        if pattern.search(name):
            return doc_type
    if is_text_file and re.search(r"board", name, re.I):
        return "transcript"
    return "other"
