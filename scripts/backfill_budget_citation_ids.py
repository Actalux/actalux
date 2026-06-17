"""Backfill budget_line_items.citation_id from each figure's current chunk (migration 019).

After migration 019 adds budget_line_items.citation_id, this copies the stable
citation_id of each figure's referenced chunk onto the figure, so the figure's
citation survives the source document's re-ingest (which nulls chunk_id). Figures
whose chunk_id is already NULL (source re-ingested before this ran) are reported
but cannot be backfilled here -- they need a content re-link, out of scope.

Additive and idempotent: only sets citation_id; re-running is a no-op for rows
that already match. Dry-run by default.

Run (prefix with `doppler run --project mac --config dev --`):
  uv run python scripts/backfill_budget_citation_ids.py            # dry run
  uv run python scripts/backfill_budget_citation_ids.py --apply    # write citation_id
"""

from __future__ import annotations

import argparse
import logging

from actalux.config import load_config
from actalux.db import get_chunk_citation_ids, get_client

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

PAGE = 1000  # PostgREST default row cap; page explicitly so large tables aren't truncated


def _all_budget_rows(client) -> list[dict]:
    """All budget_line_items (id, chunk_id, citation_id), paged past the row cap."""
    rows: list[dict] = []
    start = 0
    while True:
        page = (
            client.table("budget_line_items")
            .select("id, chunk_id, citation_id")
            .order("id")
            .range(start, start + PAGE - 1)
            .execute()
            .data
        ) or []
        rows.extend(page)
        if len(page) < PAGE:
            break
        start += PAGE
    return rows


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--apply", action="store_true", help="write (default: dry run)")
    args = parser.parse_args()

    config = load_config()
    read_client = get_client(config.supabase_url, config.supabase_key)
    write_client = None
    if args.apply:
        if not config.supabase_service_key:
            raise SystemExit("ACTALUX_SUPABASE_SERVICE_KEY is required to --apply.")
        write_client = get_client(config.supabase_url, config.supabase_service_key)

    rows = _all_budget_rows(read_client)
    logger.info("Loaded %d budget_line_items.", len(rows))

    chunk_ids = [r["chunk_id"] for r in rows if r.get("chunk_id") is not None]
    chunk_citation = get_chunk_citation_ids(read_client, chunk_ids)

    updates: list[tuple[int, str]] = []
    null_chunk = 0
    no_chunk_citation = 0
    for r in rows:
        cid = r.get("chunk_id")
        if cid is None:
            null_chunk += 1
            continue
        stable = chunk_citation.get(cid, "")
        if not stable:
            no_chunk_citation += 1
            continue
        if r.get("citation_id") != stable:
            updates.append((r["id"], stable))

    logger.info(
        "%d need a citation_id write; %d already current; %d have no chunk_id; "
        "%d chunk has no citation_id.",
        len(updates),
        len(rows) - len(updates) - null_chunk - no_chunk_citation,
        null_chunk,
        no_chunk_citation,
    )

    if not args.apply:
        logger.info("Dry run: would write %d citation_id value(s).", len(updates))
        return 0

    assert write_client is not None
    for item_id, stable in updates:
        write_client.table("budget_line_items").update({"citation_id": stable}).eq(
            "id", item_id
        ).execute()
    logger.info("Done: wrote %d citation_id value(s).", len(updates))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
