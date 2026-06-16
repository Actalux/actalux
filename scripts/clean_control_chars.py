#!/usr/bin/env python3
"""Strip control-character extraction artifacts from stored content.

Some PDFs extract with stray C0/C1 control characters (0x08, 0x01, file-separator
runs, broken-font C1 bytes). They are never document text, but they were stored in
``chunks.content`` and ``documents.content`` and so leaked into the JSON API (as
``\\b`` etc.) and the rendered pages. This rewrites the stored text through the
same cleaner the ingest parser now applies (``parser.strip_control_chars``) and
recomputes each document's ``content_hash`` so a future re-crawl matches and does
not spuriously re-version the document.

Embeddings are NOT recomputed: control characters contribute nothing to the
bge-small embedding, so the existing vectors stay valid for the cleaned text.

Dry-run by default; --apply writes (needs ACTALUX_SUPABASE_SERVICE_KEY).
Idempotent: rows already free of control characters are skipped.

Usage:
  doppler run --project mac --config dev -- uv run python scripts/clean_control_chars.py
  doppler run --project mac --config dev -- uv run python scripts/clean_control_chars.py --apply
"""

from __future__ import annotations

import argparse
import logging
import os
from typing import Any

from actalux.db import get_client
from actalux.ingest.hashing import content_hash
from actalux.ingest.parser import strip_control_chars

logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger(__name__)

_PAGE = 1000


def _iter_rows(client, table: str, columns: str):
    """Yield every row of a table, paged so large tables don't load at once."""
    page = 0
    while True:
        rows = (
            client.table(table)
            .select(columns)
            .range(page * _PAGE, page * _PAGE + _PAGE - 1)
            .execute()
        ).data or []
        if not rows:
            return
        yield from rows
        page += 1


def plan_chunk_cleanups(client) -> list[dict[str, Any]]:
    """Chunks whose content carries control characters, with cleaned text."""
    out = []
    for r in _iter_rows(client, "chunks", "id, content"):
        cleaned = strip_control_chars(r["content"] or "")
        if cleaned != (r["content"] or ""):
            out.append({"id": r["id"], "content": cleaned})
    return out


def plan_doc_cleanups(client) -> list[dict[str, Any]]:
    """Documents whose content carries control chars, with cleaned text + new hash."""
    out = []
    for r in _iter_rows(client, "documents", "id, content, content_hash"):
        original = r["content"] or ""
        cleaned = strip_control_chars(original)
        if cleaned == original:
            continue
        new_hash = content_hash(cleaned)
        out.append(
            {
                "id": r["id"],
                "content": cleaned,
                "content_hash": new_hash,
                "hash_changed": new_hash != r.get("content_hash"),
            }
        )
    return out


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
    chunk_changes = plan_chunk_cleanups(client)
    doc_changes = plan_doc_cleanups(client)
    hash_changes = sum(1 for d in doc_changes if d["hash_changed"])

    logger.info(
        "Control-char cleanup: %d chunks, %d documents (%d with hash changes).",
        len(chunk_changes),
        len(doc_changes),
        hash_changes,
    )
    for d in doc_changes:
        logger.info("   doc #%s  hash_changed=%s", d["id"], d["hash_changed"])

    if not args.apply:
        logger.info("\nDRY RUN — no changes written. Re-run with --apply to write.")
        return 0

    for c in chunk_changes:
        client.table("chunks").update({"content": c["content"]}).eq("id", c["id"]).execute()
    for d in doc_changes:
        client.table("documents").update(
            {"content": d["content"], "content_hash": d["content_hash"]}
        ).eq("id", d["id"]).execute()
    logger.info("\nApplied: %d chunks, %d documents.", len(chunk_changes), len(doc_changes))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
