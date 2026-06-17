"""Backfill the stable citation_id on every existing chunk (migration 018).

After migration 018 adds chunks.citation_id (nullable), existing rows read NULL.
This computes each chunk's content-addressed citation id from its document's
stable key and writes it, so citations can render and route on the stable id
instead of the SERIAL row id.

The id is derived exactly as ingest does (ingest.hashing.assign_citation_ids):
per document, in chunk_index order, disambiguating verbatim-repeated passages by
appearance order. Processing per document keeps the dup-ordinal logic identical
to ingest, so a future re-ingest of an unchanged document reproduces these ids.

Additive and idempotent: only writes citation_id, never deletes; re-running is a
no-op for rows that already match. Dry-run by default.

Run (prefix with `doppler run --project mac --config dev --`):
  uv run python scripts/backfill_citation_ids.py            # dry run: counts only
  uv run python scripts/backfill_citation_ids.py --apply    # write citation_id
"""

from __future__ import annotations

import argparse
import logging
from collections import defaultdict

from actalux.config import load_config
from actalux.db import get_client
from actalux.ingest.hashing import assign_citation_ids, doc_stable_key

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

PAGE = 1000  # PostgREST default row cap; page through chunks explicitly


def _fetch_all_chunks(client) -> list[dict]:
    """All chunks (id, document_id, chunk_index, content, citation_id), paged."""
    rows: list[dict] = []
    start = 0
    while True:
        page = (
            client.table("chunks")
            .select("id, document_id, chunk_index, content, citation_id")
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


def _doc_keys(client) -> dict[int, str]:
    """Map document id -> stable citation key (source_ref/content_hash/source_file)."""
    rows: list[dict] = []
    start = 0
    while True:
        page = (
            client.table("documents")
            .select("id, source_ref, content_hash, source_file")
            .order("id")
            .range(start, start + PAGE - 1)
            .execute()
            .data
        ) or []
        rows.extend(page)
        if len(page) < PAGE:
            break
        start += PAGE
    return {
        d["id"]: doc_stable_key(
            d.get("source_ref") or "", d.get("content_hash") or "", d.get("source_file") or ""
        )
        for d in rows
    }


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

    doc_keys = _doc_keys(read_client)
    chunks = _fetch_all_chunks(read_client)
    logger.info("Loaded %d chunks across %d documents.", len(chunks), len(doc_keys))

    # Group by document and order by chunk_index so the dup-ordinal logic matches
    # ingest exactly (assign_citation_ids depends on within-document order).
    by_doc: dict[int, list[dict]] = defaultdict(list)
    for c in chunks:
        by_doc[c["document_id"]].append(c)

    updates: list[tuple[int, str]] = []  # (chunk_id, citation_id) needing a write
    missing_doc = 0
    for doc_id, doc_chunks in by_doc.items():
        doc_chunks.sort(key=lambda c: (c.get("chunk_index") or 0, c["id"]))
        doc_key = doc_keys.get(doc_id)
        if doc_key is None:
            missing_doc += len(doc_chunks)
            logger.warning(
                "No document row for doc_id=%s (%d chunks); skipping.", doc_id, len(doc_chunks)
            )
            continue
        cids = assign_citation_ids(doc_key, [c["content"] or "" for c in doc_chunks])
        for c, cid in zip(doc_chunks, cids, strict=True):
            if c.get("citation_id") != cid:
                updates.append((c["id"], cid))

    # Collision visibility: count distinct citation_ids that map to >1 chunk
    # globally (after the proposed updates merge with existing values).
    final_map: dict[int, str] = {c["id"]: c.get("citation_id") or "" for c in chunks}
    for cid_id, cid in updates:
        final_map[cid_id] = cid
    cid_counts: dict[str, int] = defaultdict(int)
    for cid in final_map.values():
        if cid:
            cid_counts[cid] += 1
    collisions = {cid: n for cid, n in cid_counts.items() if n > 1}

    logger.info(
        "%d chunk(s) need a citation_id write; %d already current; %d orphaned (no doc).",
        len(updates),
        len(chunks) - len(updates) - missing_doc,
        missing_doc,
    )
    if collisions:
        logger.warning(
            "%d citation_id value(s) shared by >1 chunk (routing prefers current "
            "version; logs ambiguity): %s",
            len(collisions),
            dict(list(collisions.items())[:10]),
        )

    if not args.apply:
        logger.info("Dry run: would write %d citation_id value(s).", len(updates))
        return 0

    assert write_client is not None
    written = 0
    for chunk_id, cid in updates:
        write_client.table("chunks").update({"citation_id": cid}).eq("id", chunk_id).execute()
        written += 1
        if written % 500 == 0:
            logger.info("  wrote %d/%d ...", written, len(updates))
    logger.info("Done: wrote %d citation_id value(s).", written)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
