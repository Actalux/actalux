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
from datetime import date

from actalux.db import get_client
from actalux.ingest.classify import (
    classify_document_type,
    is_annual_schedule,
    parse_meeting_date,
)

logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger(__name__)


def derive_document_type(meeting_title: str, source_file: str, current_type: str) -> str | None:
    """Proposed document_type, or None to leave it unchanged.

    Uses the shared classifier (so ingest and this corrector agree). Reclassifies
    docs currently typed 'other' (specific types are trusted), with one extra
    correction: an annual schedule that the old ingest mis-typed as 'minutes' is
    moved to 'schedule'.
    """
    name = f"{(meeting_title or '').strip()} {(source_file or '').strip()}"
    is_text = (source_file or "").lower().endswith(".txt")
    proposed = classify_document_type(name, is_text_file=is_text)
    if current_type == "other":
        return proposed if proposed != "other" else None
    if proposed == "schedule" and current_type == "minutes":
        return "schedule"
    return None


def derive_meeting_date(meeting_title: str, source_file: str) -> date | None:
    """Parse a meeting date from the title, then the filename. None if unconfident.

    No ``today`` is passed, so compact "jan21"-style inference is skipped — a
    retroactive corrector must be deterministic, not dependent on the run date.
    """
    return parse_meeting_date(meeting_title or "") or parse_meeting_date(source_file or "")


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
            if parsed:
                date_matches = parsed.isoformat() == str(d.get("meeting_date"))
                current_source = d.get("date_source")
                # Trusted values: date was set from a verifiable source better than
                # filename parsing.  Never downgrade 'content' or 'manual' to
                # 'filename' even when the filename happens to parse to the same date.
                _trusted = {"content", "manual"}
                if not date_matches:
                    # Date differs: write both the corrected date and its provenance,
                    # but only when we're not clobbering a more-reliable source.
                    if current_source not in _trusted:
                        update["meeting_date"] = parsed.isoformat()
                        update["date_source"] = "filename"
                elif current_source not in _trusted and current_source != "filename":
                    # Date already correct but provenance is stale or absent
                    # ('default', 'unknown', None — all pre-A3 legacy signals).
                    # Write provenance only so the column converges after --apply.
                    update["date_source"] = "filename"

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
        .select("id, document_type, meeting_date, date_source, meeting_title, source_file")
        .is_("replaces_id", "null")
        .execute()
    ).data or []

    changes = plan_changes(docs)
    retypes = [c for c in changes if "document_type" in c["update"]]
    redates = [c for c in changes if "meeting_date" in c["update"]]
    # Provenance-only fixes: date_source corrected without changing the date
    # itself (a legacy 'default'/'unknown' row whose date was already right).
    reprovenance = [
        c for c in changes if "date_source" in c["update"] and "meeting_date" not in c["update"]
    ]

    logger.info(
        "Scanned %d documents; %d changes (%d re-type, %d re-date, %d re-provenance).",
        len(docs),
        len(changes),
        len(retypes),
        len(redates),
        len(reprovenance),
    )

    logger.info("\n-- RE-TYPE (%d) --", len(retypes))
    type_counts: dict[str, int] = {}
    for c in retypes:
        transition = f"{c['from']['document_type']} -> {c['update']['document_type']}"
        type_counts[transition] = type_counts.get(transition, 0) + 1
    for transition, n in sorted(type_counts.items(), key=lambda x: -x[1]):
        logger.info("   %-26s %d", transition, n)
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

    if reprovenance:
        logger.info("\n-- RE-PROVENANCE (date unchanged, %d) --", len(reprovenance))
        for c in reprovenance:
            logger.info(
                "   #%s  date_source %s -> %s | %s",
                c["id"],
                c["from"].get("date_source"),
                c["update"]["date_source"],
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
