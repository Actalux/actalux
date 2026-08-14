"""Fail if any meeting document is filed under a date nothing vouches for.

The invariant: a document the meetings endpoint will serve must have a
meeting_date with trustworthy provenance. When it does not, the endpoint does
not merely return the document under the wrong date — it reports a meeting on
that date, and anything consuming the API takes that as the district having met.
That is how December 2025 minutes were served as the August 3, 2026 board record
and a newsletter draft was written from them.

Scope is deliberately narrow. Non-meeting documents — curriculum maps, strategic
plans, presentations — are never returned by the meetings endpoint, so an
imprecise date on one is untidy rather than dangerous. Those are reported and do
not fail the run, which is what keeps this check quiet enough to be believed
when it does fire.

Run (prefix with `doppler run --project mac --config dev --`):
  uv run python scripts/check_meeting_dates.py
"""

from __future__ import annotations

import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from actalux.config import load_config  # noqa: E402
from actalux.db import fetch_all_rows, get_client  # noqa: E402

logger = logging.getLogger(__name__)

# Types the meetings endpoint serves (web/api.py MEETING_TYPES). A bad date on one
# of these asserts that a meeting happened.
MEETING_TYPES = frozenset({"minutes", "transcript", "resolution", "agenda"})
# Provenance values that actually vouch for a date.
TRUSTED_SOURCES = frozenset({"filename", "content", "civicplus", "manual", "packet", "diligent"})


def untrusted(row: dict) -> bool:
    """True when nothing vouches for this row's meeting_date.

    Either the provenance is explicitly a non-answer, or it is unverified legacy
    ('unknown') and the date happens to equal the ingest day — the signature of
    the old date.today() fallback.
    """
    source = row.get("date_source")
    if source in ("default", "undetermined"):
        return True
    if source in TRUSTED_SOURCES:
        return False
    meeting_date, created_at = row.get("meeting_date"), row.get("created_at")
    if not meeting_date or not created_at:
        return True
    return str(meeting_date)[:10] == str(created_at)[:10]


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    config = load_config()
    client = get_client(config.supabase_url, config.supabase_service_key or config.supabase_key)

    rows = fetch_all_rows(
        lambda: client.table("documents").select(
            "id,entity_id,document_type,meeting_date,date_source,source_file,created_at,replaces_id"
        )
    )
    live = [r for r in rows if not r.get("replaces_id")]
    suspect = [r for r in live if untrusted(r)]
    serving = [r for r in suspect if r.get("document_type") in MEETING_TYPES]
    other = [r for r in suspect if r.get("document_type") not in MEETING_TYPES]

    logger.info("checked %d live documents", len(live))
    if other:
        logger.info(
            "%d non-meeting documents have an imprecise date (not served as meetings, "
            "not failing this check)",
            len(other),
        )
    if not serving:
        logger.info("OK: every meeting document has a vouched-for date.")
        return 0

    logger.error(
        "%d meeting documents are filed under a date nothing vouches for. Each one "
        "makes the meetings endpoint report a meeting on that date:",
        len(serving),
    )
    for r in sorted(serving, key=lambda r: str(r["meeting_date"])):
        logger.error(
            "  doc %-5s %s  %-10s src=%-13s %s",
            r["id"],
            r["meeting_date"],
            r["document_type"],
            r.get("date_source"),
            (r.get("source_file") or "")[:44],
        )
    logger.error("Repair with: uv run python scripts/backfill_meeting_dates.py --apply")
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
