#!/usr/bin/env python3
"""Re-derive document_type and meeting_date from filenames/titles.

Many documents were ingested as document_type='other' because the ingest
classifier didn't recognise their naming scheme (e.g. minutes saved as
"Apr 12 2023 BOE MM signed.pdf"), and several minutes fell back to the ingest
date because their "MM.DD.YY" titles weren't parsed. This corrects both,
deriving values from the (verifiable) filename/title.

Conservative by design:
  * Re-typing applies ONLY to docs currently typed 'other' (specific types are
    left alone). If a doc can't be classified it stays 'other'.
  * Re-dating applies to any doc where a confident date parses AND differs from
    the stored date, EXCEPT annual schedule docs (no single meeting date).
  * The original meeting_title is never modified — it is the provenance record.

Dry-run by default; --apply writes (needs ACTALUX_SUPABASE_SERVICE_KEY).
Idempotent: re-running after --apply proposes no further changes.

Usage:
  doppler run --project mac --config dev -- uv run python scripts/recategorize_documents.py
  doppler run --project mac --config dev -- uv run python scripts/recategorize_documents.py --apply
"""

from __future__ import annotations

import argparse
import logging
import os
import re
from datetime import date

from actalux.db import get_client

logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger(__name__)

_ANNUAL_RE = re.compile(r"20\d{2}\s*[ _-]\s*20\d{2}")
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

# Date patterns, most-specific first: "Apr 12 2023" / "December 10, 2025";
# "11.16.22" (MM.DD.YY); "10 26 22" (MM DD YY).
_MONTH_DATE_RE = re.compile(
    r"(jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)[a-z]*\.?\s+"
    r"(\d{1,2})(?:st|nd|rd|th)?[, ]+\s*(\d{4})",
    re.I,
)
_MMDDYY_DOT_RE = re.compile(r"\b(\d{1,2})\.(\d{1,2})\.(\d{2})\b")
_MMDDYY_SPACE_RE = re.compile(r"\b(\d{1,2})\s+(\d{1,2})\s+(\d{2})\b")


def is_annual_schedule(meeting_title: str) -> bool:
    """An annual board-meeting schedule/calendar, not a single meeting."""
    s = (meeting_title or "").replace("-", " ")
    return bool(_ANNUAL_RE.search(s)) and (
        "board of education meeting" in s.lower() or "meetings" in s.lower()
    )


def derive_document_type(meeting_title: str, source_file: str, current_type: str) -> str | None:
    """Proposed document_type for an 'other' doc, or None to leave it unchanged.

    Only reclassifies docs currently typed 'other'; specific types are trusted.
    Returns None when the doc is not 'other' or cannot be confidently classified.
    """
    if current_type != "other":
        return None
    title, sf = meeting_title or "", source_file or ""
    s = f"{title} {sf}".lower()

    if is_annual_schedule(title) or "board of education meetings" in s:
        return "schedule"
    # Minutes: "MM" token, "meeting minutes", "business meeting", retreats,
    # "BOE Meeting", session minutes. Checked before curriculum/etc.
    if (
        re.search(r"\bmm\b", s)
        or "meeting minutes" in s
        or "business meeting" in s
        or "board retreat" in s
        or "boe meeting" in s
        or "working session" in s
        or "work session" in s
        or "special meeting" in s
    ):
        return "minutes"
    if "curriculum" in s and "map" in s:
        return "curriculum_map"
    if "curriculum" in s:
        return "curriculum"
    if "master plan" in s or "masterplan" in s:
        return "facilities_plan"
    if "strategic" in s or "csip" in s:
        return "strategic_plan"
    if "assessment" in s:
        return "assessment"
    if "audit" in s or "acfr" in s:
        return "audit"
    if "calendar" in s:
        return "schedule"
    if any(
        kw in s
        for kw in (
            "sunshine",
            "candidate",
            "orientation",
            "livestream",
            "public comment",
            "governance",
            "gifted",
            "resource guide",
            "open meetings",
            "policy",
        )
    ):
        return "governance"
    return None  # leave as 'other'


def derive_meeting_date(meeting_title: str, source_file: str) -> date | None:
    """Parse a full meeting date from the title/filename, or None if unconfident."""
    for text in (meeting_title or "", source_file or ""):
        m = _MONTH_DATE_RE.search(text)
        if m:
            d = _safe_date(int(m.group(3)), _MONTHS[m.group(1).lower()[:3]], int(m.group(2)))
            if d:
                return d
        m = _MMDDYY_DOT_RE.search(text)
        if m:
            d = _safe_date(2000 + int(m.group(3)), int(m.group(1)), int(m.group(2)))
            if d:
                return d
        m = _MMDDYY_SPACE_RE.search(text)
        if m:
            d = _safe_date(2000 + int(m.group(3)), int(m.group(1)), int(m.group(2)))
            if d:
                return d
    return None


def _safe_date(year: int, month: int, day: int) -> date | None:
    """Construct a date, returning None for out-of-range / impossible values."""
    if year not in _SANE_YEARS or not (1 <= month <= 12) or not (1 <= day <= 31):
        return None
    try:
        return date(year, month, day)
    except ValueError:
        return None


def plan_changes(docs: list[dict]) -> list[dict]:
    """Compute the (type, date) changes for each doc. Read-only."""
    changes = []
    for d in docs:
        title, sf = d.get("meeting_title", ""), d.get("source_file", "")
        update: dict[str, str] = {}

        new_type = derive_document_type(title, sf, d["document_type"])
        if new_type and new_type != d["document_type"]:
            update["document_type"] = new_type

        # Don't re-date annual schedules (no single meeting date).
        effective_type = new_type or d["document_type"]
        if effective_type != "schedule" and not is_annual_schedule(title):
            parsed = derive_meeting_date(title, sf)
            if parsed and parsed.isoformat() != str(d.get("meeting_date")):
                update["meeting_date"] = parsed.isoformat()

        if update:
            changes.append({"id": d["id"], "title": title, "from": d, "update": update})
    return changes


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--apply", action="store_true", help="Write changes (default: dry-run).")
    args = parser.parse_args()

    url = os.environ["ACTALUX_SUPABASE_URL"]
    key_var = "ACTALUX_SUPABASE_SERVICE_KEY" if args.apply else "ACTALUX_SUPABASE_KEY"
    try:
        key = os.environ[key_var]
    except KeyError as exc:
        raise SystemExit(
            f"Missing {exc}; run under doppler run --project mac --config dev -- ..."
        ) from exc

    client = get_client(url, key)
    docs = (
        client.table("documents")
        .select("id, document_type, meeting_date, meeting_title, source_file")
        .is_("replaces_id", "null")
        .execute()
    ).data or []

    changes = plan_changes(docs)
    retypes = [c for c in changes if "document_type" in c["update"]]
    redates = [c for c in changes if "meeting_date" in c["update"]]

    logger.info(
        "Scanned %d documents; %d changes (%d re-type, %d re-date).",
        len(docs),
        len(changes),
        len(retypes),
        len(redates),
    )

    logger.info("\n-- RE-TYPE (%d) --", len(retypes))
    type_counts: dict[str, int] = {}
    for c in retypes:
        t = c["update"]["document_type"]
        type_counts[t] = type_counts.get(t, 0) + 1
    for t, n in sorted(type_counts.items(), key=lambda x: -x[1]):
        logger.info("   other -> %-16s %d", t, n)
    for c in retypes:
        logger.info("   #%s  -> %-15s | %s", c["id"], c["update"]["document_type"], c["title"][:48])

    logger.info("\n-- RE-DATE (%d) --", len(redates))
    for c in redates:
        logger.info(
            "   #%s  %s -> %s | %s",
            c["id"],
            c["from"].get("meeting_date"),
            c["update"]["meeting_date"],
            c["title"][:42],
        )

    if not args.apply:
        logger.info("\nDRY RUN — no changes written. Re-run with --apply to write.")
        return 0

    for c in changes:
        client.table("documents").update(c["update"]).eq("id", c["id"]).execute()
    logger.info("\nApplied %d changes.", len(changes))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
