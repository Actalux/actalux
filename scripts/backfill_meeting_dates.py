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

A document that never names its meeting falls back to the packet that linked to
it. Packet attachments — exec summaries, policy PDFs, signed MOUs — frequently
carry no date of their own, but the packet does, and every attachment it links
belongs to that meeting. The parent map is rebuilt offline by rescanning the
downloaded PDFs' link annotations, so this needs no network and no portal call.
It is skipped when the download directory is absent.

Anything still unresolved after both strategies keeps its current value and is
reported, not guessed.

Run (prefix with `doppler run --project mac --config dev --`):
  uv run python scripts/backfill_meeting_dates.py                # dry run
  uv run python scripts/backfill_meeting_dates.py --entity 1     # one body
  uv run python scripts/backfill_meeting_dates.py --apply        # write
"""

from __future__ import annotations

import argparse
import logging
import re
import sys
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))
# The crawler's link scanner is reused to rebuild the packet map offline.
sys.path.insert(0, str(Path(__file__).resolve().parent))

from actalux.config import load_config  # noqa: E402
from actalux.db import fetch_all_rows, get_client  # noqa: E402
from actalux.ingest.classify import meeting_date_from_text  # noqa: E402

logger = logging.getLogger(__name__)

# Provenance values whose meeting_date is trustworthy enough to define the
# body's real meeting calendar.
TRUSTED_SOURCES = frozenset({"filename", "content", "civicplus", "manual", "packet", "diligent"})
# Downloaded portal PDFs, rescanned to rebuild which packet links which attachment.
DOWNLOAD_DIR = Path("data/documents")
_GUID_IN_URL = re.compile(r"/document/([0-9a-f-]{36})", re.I)
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


def build_parent_dates(rows: list[dict]) -> dict[str, date]:
    """Map a document's portal GUID to the meeting date of the packet linking it.

    Rebuilt by rescanning downloaded PDFs for link annotations — the same scan the
    crawler does — so it is deterministic and offline. A GUID linked from packets
    that disagree on the date is left out rather than resolved by preference.
    """
    if not DOWNLOAD_DIR.is_dir():
        logger.info("%s absent — skipping packet resolution", DOWNLOAD_DIR)
        return {}
    try:
        from download_documents import extract_linked_guids
    except ImportError:
        logger.info("crawler not importable — skipping packet resolution")
        return {}

    date_by_file: dict[str, date] = {}
    for r in rows:
        if r.get("replaces_id") or not r.get("source_file") or not r.get("meeting_date"):
            continue
        if r.get("date_source") in TRUSTED_SOURCES:
            date_by_file[r["source_file"]] = date.fromisoformat(str(r["meeting_date"])[:10])

    candidates: dict[str, set[date]] = {}
    pdfs = sorted(DOWNLOAD_DIR.glob("*.pdf"))
    for pdf in pdfs:
        parent_date = date_by_file.get(pdf.name)
        if not parent_date:
            continue
        for guid in extract_linked_guids(pdf):
            candidates.setdefault(guid.lower(), set()).add(parent_date)
    resolved = {g: next(iter(ds)) for g, ds in candidates.items() if len(ds) == 1}
    conflicted = len(candidates) - len(resolved)
    logger.info(
        "packet map: scanned %d PDFs, %d attachments resolved, %d linked from packets "
        "that disagree (left alone)",
        len(pdfs),
        len(resolved),
        conflicted,
    )
    return resolved


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

    # Metadata only. Selecting `content` for the whole corpus alongside these
    # columns overruns the server statement timeout, and only the suspect rows —
    # a few dozen — are ever read for their text.
    rows = fetch_all_rows(
        lambda: client.table("documents").select(
            "id,entity_id,document_type,meeting_date,date_source,source_file,source_url,replaces_id"
        )
    )
    suspect_ids = [
        r["id"]
        for r in rows
        if r.get("date_source") in SUSPECT_SOURCES and not r.get("replaces_id")
    ]
    content_by_id: dict[int, str] = {}
    for start in range(0, len(suspect_ids), 25):
        batch = suspect_ids[start : start + 25]
        got = client.table("documents").select("id,content").in_("id", batch).execute().data or []
        content_by_id.update({row["id"]: row.get("content") or "" for row in got})
    logger.info("read text for %d candidate documents", len(content_by_id))
    entity_ids = (
        [args.entity]
        if args.entity
        else sorted({r["entity_id"] for r in rows if r.get("entity_id")})
    )

    parent_dates = build_parent_dates(rows)

    resolved: list[tuple[dict, date, str]] = []
    unresolved: list[dict] = []
    for entity_id in entity_ids:
        calendar = build_calendar(rows, entity_id)
        logger.info("entity %s: %d known meeting dates", entity_id, len(calendar))
        for r in rows:
            if r.get("entity_id") != entity_id or r.get("replaces_id"):
                continue
            if r.get("date_source") not in SUSPECT_SOURCES:
                continue
            # The document's own statement first: a second-reading cover sheet names
            # the meeting it is being read at, which is not the packet that first
            # carried the policy. Only fall back to the packet when the text is silent.
            derived = meeting_date_from_text(content_by_id.get(r["id"], ""), calendar.__contains__)
            how = "text"
            if not derived:
                m = _GUID_IN_URL.search(r.get("source_url") or "")
                if m:
                    derived = parent_dates.get(m.group(1).lower())
                    how = "packet"
            if derived and str(derived) != str(r.get("meeting_date")):
                resolved.append((r, derived, how))
            elif not derived:
                unresolved.append(r)

    for r, d, how in sorted(resolved, key=lambda x: (x[0]["entity_id"], str(x[1]))):
        logger.info(
            "  doc %-5s %s -> %s  via %-6s %-10s %s",
            r["id"],
            r["meeting_date"],
            d,
            how,
            r["document_type"],
            (r["source_file"] or "")[:40],
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

    for r, d, how in resolved:
        client.table("documents").update(
            {"meeting_date": str(d), "date_source": "content" if how == "text" else "packet"}
        ).eq("id", r["id"]).execute()
    logger.info("Wrote %d corrected meeting dates.", len(resolved))
    if unresolved:
        logger.info(
            "%d documents still carry a placeholder date: their text names no meeting "
            "and no packet in data/documents/ links to them.",
            len(unresolved),
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
