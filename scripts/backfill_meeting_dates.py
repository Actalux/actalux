"""Repair documents whose meeting_date is an ingest-day placeholder.

Ingest used to substitute ``date.today()`` when it could not parse a date, and
because ``meeting_date`` is what the meetings endpoint keys on, those rows did
not merely sit under the wrong date — they manufactured meetings. Sixty-five
Clayton schools documents landed on 2026-07-19 (a Sunday) and 2026-08-03 (a
Monday); the board meets Wednesdays.

The repair reads each document's own text. Board packet cover sheets state the
meeting date in a header block, and that is the authority used here — not the
filename, which routinely carries a different date: 20250430FinancialReport is
the April report *presented on June 4*, and 20251210PolicyKB-2ndRead is the
second reading of a policy first read on December 10 and read again January 21.
Dating those from the filename would replace one wrong date with another.

Two guards keep the derivation honest:

  * Dates carrying a provenance label ("Last Revised Date: 05/13/2020") are
    refused. Policies are revised at board meetings, so those dates are real
    meeting dates and pass a calendar check — they are simply the wrong
    document's meeting.
  * A derived date must appear in the body's own meeting calendar, built from
    documents whose date provenance is already trusted.

A document that never names its meeting keeps its current value and is
reported, not guessed. For those the authority is the Diligent parent packet,
which the crawler does not yet record.

Run (prefix with `doppler run --project mac --config dev --`):
  uv run python scripts/backfill_meeting_dates.py                # dry run
  uv run python scripts/backfill_meeting_dates.py --entity 1     # one body
  uv run python scripts/backfill_meeting_dates.py --apply        # write
"""

from __future__ import annotations

import argparse
import logging
import sys
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from actalux.config import load_config  # noqa: E402
from actalux.db import fetch_all_rows, get_client  # noqa: E402
from actalux.ingest.classify import meeting_date_from_text  # noqa: E402

logger = logging.getLogger(__name__)

# Provenance values whose meeting_date is trustworthy enough to define the
# body's real meeting calendar.
TRUSTED_SOURCES = frozenset({"filename", "content", "civicplus", "manual"})
# Provenance values meaning "ingest could not determine a date". 'default' is the
# historical label for the date.today() fallback; 'undetermined' is what ingest
# writes now that it no longer fabricates one.
SUSPECT_SOURCES = frozenset({"default", "undetermined"})
# Document types that establish that a meeting took place on a date.
CALENDAR_TYPES = ("minutes", "transcript", "agenda")


def build_calendar(rows: list[dict], entity_id: int) -> set[date]:
    """Dates on which this body demonstrably met, from trusted-provenance rows."""
    out: set[date] = set()
    for r in rows:
        if r.get("entity_id") != entity_id or r.get("replaces_id"):
            continue
        if r.get("document_type") not in CALENDAR_TYPES:
            continue
        if r.get("date_source") not in TRUSTED_SOURCES:
            continue
        md = r.get("meeting_date")
        if md:
            out.add(date.fromisoformat(str(md)[:10]))
    return out


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--apply", action="store_true", help="write to the DB (default: dry run)")
    parser.add_argument("--entity", type=int, help="restrict to one entity id")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    config = load_config()
    if args.apply and not config.supabase_service_key:
        raise SystemExit("ACTALUX_SUPABASE_SERVICE_KEY is required to --apply.")
    client = get_client(config.supabase_url, config.supabase_service_key or config.supabase_key)

    rows = fetch_all_rows(
        lambda: client.table("documents").select(
            "id,entity_id,document_type,meeting_date,date_source,source_file,content,replaces_id"
        )
    )
    entity_ids = (
        [args.entity]
        if args.entity
        else sorted({r["entity_id"] for r in rows if r.get("entity_id")})
    )

    resolved: list[tuple[dict, date]] = []
    unresolved: list[dict] = []
    for entity_id in entity_ids:
        calendar = build_calendar(rows, entity_id)
        logger.info("entity %s: %d known meeting dates", entity_id, len(calendar))
        for r in rows:
            if r.get("entity_id") != entity_id or r.get("replaces_id"):
                continue
            if r.get("date_source") not in SUSPECT_SOURCES:
                continue
            derived = meeting_date_from_text(r.get("content") or "", calendar.__contains__)
            if derived and str(derived) != str(r.get("meeting_date")):
                resolved.append((r, derived))
            elif not derived:
                unresolved.append(r)

    for r, d in sorted(resolved, key=lambda x: (x[0]["entity_id"], str(x[1]))):
        logger.info(
            "  doc %-5s %s -> %s  %-10s %s",
            r["id"],
            r["meeting_date"],
            d,
            r["document_type"],
            (r["source_file"] or "")[:44],
        )
    for r in unresolved:
        logger.info(
            "  doc %-5s UNRESOLVED (text names no meeting)  %-10s %s",
            r["id"],
            r["document_type"],
            (r["source_file"] or "")[:44],
        )

    logger.info("resolved: %d   unresolved: %d", len(resolved), len(unresolved))
    if not args.apply:
        logger.info("Dry run. Re-run with --apply to write.")
        return 0

    for r, d in resolved:
        client.table("documents").update({"meeting_date": str(d), "date_source": "content"}).eq(
            "id", r["id"]
        ).execute()
    logger.info("Wrote %d corrected meeting dates.", len(resolved))
    logger.info(
        "%d documents still carry a placeholder date; their meeting is not stated "
        "in their text and must come from the portal packet.",
        len(unresolved),
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
