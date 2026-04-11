#!/usr/bin/env python3
"""One-off: backfill content_hash and source_portal for existing documents.

Usage:
    doppler run --project mac --config dev -- uv run python scripts/backfill_provenance.py
"""

from __future__ import annotations

import logging
import re

from actalux.config import load_config
from actalux.db import get_client
from actalux.ingest.hashing import content_hash

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# Patterns to infer source_portal from existing filenames
PORTAL_PATTERNS: list[tuple[str, re.Pattern[str]]] = [
    ("diligent", re.compile(r"Meeting Minutes", re.IGNORECASE)),
    ("diligent", re.compile(r"BOE.*Forum", re.IGNORECASE)),
    ("claytonschools", re.compile(r"Budget\.html$", re.IGNORECASE)),
    (
        "claytonschools",
        re.compile(
            r"Volume\d|LRFMP|Glenridge|Ballot|CSIP|Strategic",
            re.IGNORECASE,
        ),
    ),
    ("youtube", re.compile(r"Board of Education.*\.txt$", re.IGNORECASE)),
    ("youtube", re.compile(r"jan\d+.*board.*\.txt$", re.IGNORECASE)),
]


def infer_portal(source_file: str) -> str:
    """Guess the source portal from a filename."""
    for portal, pattern in PORTAL_PATTERNS:
        if pattern.search(source_file):
            return portal
    return "manual"


def main() -> None:
    config = load_config()
    client = get_client(config.supabase_url, config.supabase_key)

    cols = "id, source_file, content, content_hash, source_portal"
    result = client.table("documents").select(cols).execute()
    docs = result.data

    updated = 0
    for doc in docs:
        changes: dict[str, str | int] = {}

        # Compute hash if missing
        if not doc.get("content_hash"):
            changes["content_hash"] = content_hash(doc["content"])

        # Infer portal if missing
        if not doc.get("source_portal"):
            changes["source_portal"] = infer_portal(doc["source_file"])

        if changes:
            client.table("documents").update(changes).eq("id", doc["id"]).execute()
            updated += 1
            logger.info(
                "Updated doc %d (%s): %s",
                doc["id"],
                doc["source_file"],
                ", ".join(f"{k}={v!r:.40}" for k, v in changes.items()),
            )

    logger.info("Backfill complete: %d/%d documents updated", updated, len(docs))


if __name__ == "__main__":
    main()
