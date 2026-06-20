#!/usr/bin/env python3
"""One-off: repair YouTube transcript dedup identity and clear bogus version chains.

``normalize_source_ref`` used to drop the URL query string, but a YouTube video's
stable id lives there (``watch?v=ID``). So every meeting collapsed to the single
ref ``https://www.youtube.com/watch``, and each newly ingested meeting matched the
previously ingested one and was recorded as a new *version* of it -- chaining
unrelated meetings (5/13 "replaced by" 6/3 "replaced by" 4/29 ...). Documents with
``replaces_id`` set are excluded from both browse and search, so the bulk of the
transcript archive was invisible.

This backfill, run after the ``normalize_source_ref`` fix:
  1. recomputes each YouTube transcript's ``source_ref`` from its ``source_url``
     (now per-video, so meetings are distinct), and
  2. clears ``replaces_id`` where it points at a document with a *different*
     stable identity -- i.e. a bogus supersession, not a real content update.

A real version chain (same ``source_ref`` old->new) is left untouched, so the
script is safe to re-run and never collapses genuine updates.

Usage:
    doppler run --project mac --config dev -- \
        uv run python scripts/fix_youtube_source_ref.py --dry-run
    doppler run --project mac --config dev -- \
        uv run python scripts/fix_youtube_source_ref.py
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parent))
from ingest import normalize_source_ref  # noqa: E402  (sibling script)

from actalux.config import load_config  # noqa: E402
from actalux.db import get_client  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def _planned_change(
    doc: dict[str, Any], all_docs_by_id: dict[int, dict[str, Any]]
) -> dict[str, Any]:
    """Compute the fields to update for one YouTube transcript (empty = no change).

    ``source_ref`` is recomputed from the stored ``source_url``. ``replaces_id`` is
    cleared only when its target has a different recomputed stable identity (a bogus
    supersession); a genuine same-ref version chain is preserved.
    """
    change: dict[str, Any] = {}
    new_ref = normalize_source_ref(doc.get("source_url") or "")
    if new_ref and new_ref != (doc.get("source_ref") or ""):
        change["source_ref"] = new_ref

    replaces_id = doc.get("replaces_id")
    if replaces_id is not None:
        target = all_docs_by_id.get(replaces_id)
        target_ref = normalize_source_ref((target or {}).get("source_url") or "")
        # Bogus when the chain links two different documents (dangling target, or a
        # different stable identity). A real update keeps the same ref old->new.
        if target is None or target_ref != new_ref:
            change["replaces_id"] = None
    return change


def main() -> None:
    parser = argparse.ArgumentParser(description="Repair YouTube transcript dedup identity.")
    parser.add_argument("--dry-run", action="store_true", help="report changes; write nothing")
    args = parser.parse_args()

    config = load_config()
    # Writer: the service key bypasses RLS (the publishable key cannot write).
    client = get_client(config.supabase_url, config.supabase_service_key)

    all_docs = (
        client.table("documents")
        .select(
            "id, source_url, source_ref, replaces_id, source_portal, document_type, source_file"
        )
        .execute()
        .data
    )
    by_id = {d["id"]: d for d in all_docs}
    youtube_transcripts = [
        d
        for d in all_docs
        if d.get("source_portal") == "youtube" and d.get("document_type") == "transcript"
    ]

    refs_fixed = chains_cleared = 0
    for doc in sorted(youtube_transcripts, key=lambda d: d["id"]):
        change = _planned_change(doc, by_id)
        if not change:
            continue
        if "source_ref" in change:
            refs_fixed += 1
        if "replaces_id" in change:
            chains_cleared += 1
        verb = "would update" if args.dry_run else "updating"
        logger.info("  %s doc %s: %s", verb, doc["id"], change)
        if not args.dry_run:
            client.table("documents").update(change).eq("id", doc["id"]).execute()

    verb = "would fix" if args.dry_run else "fixed"
    logger.info(
        "Done: %s %d source_ref(s); %s %d bogus version chain(s) across %d YouTube transcripts.",
        verb,
        refs_fixed,
        "would clear" if args.dry_run else "cleared",
        chains_cleared,
        len(youtube_transcripts),
    )


if __name__ == "__main__":
    main()
