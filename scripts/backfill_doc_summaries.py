"""Backfill documents.summary with one-sentence LLM summaries.

For each document, fetch the first ~3 chunks (by id, which roughly
preserves source order), generate a one-sentence factual summary via
gpt-4o-mini, and write it to documents.summary.

Idempotent: skips documents that already have a non-empty summary.
Pass --force to regenerate everything.

Run via doppler so OPENAI_API_KEY and Supabase creds are present:
  doppler run --project mac --config dev -- \\
      uv run python scripts/backfill_doc_summaries.py
  doppler run --project mac --config dev -- \\
      uv run python scripts/backfill_doc_summaries.py --force --limit 5
"""

from __future__ import annotations

import argparse
import logging
import sys
import time

from actalux.config import load_config
from actalux.db import get_client
from actalux.errors import SummaryError
from actalux.search.summarize import generate_doc_summary

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

EXCERPT_CHUNK_COUNT = 3


def fetch_excerpts(client, doc_id: int) -> list[str]:
    res = (
        client.table("chunks")
        .select("content")
        .eq("document_id", doc_id)
        .order("id", desc=False)
        .limit(EXCERPT_CHUNK_COUNT)
        .execute()
    )
    return [r["content"] for r in res.data if r.get("content")]


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--force", action="store_true", help="regenerate even if a summary already exists"
    )
    parser.add_argument(
        "--limit", type=int, default=0, help="process at most N documents (0 = all)"
    )
    parser.add_argument(
        "--sleep", type=float, default=0.0, help="seconds to sleep between API calls"
    )
    args = parser.parse_args()

    cfg = load_config()
    if not cfg.openai_api_key:
        logger.error("OPENAI_API_KEY not set; aborting")
        return 1
    client = get_client(cfg.supabase_url, cfg.supabase_key)

    res = (
        client.table("documents")
        .select("id,meeting_title,document_type,meeting_date,source_portal,summary")
        .order("id", desc=False)
        .execute()
    )
    docs = res.data
    if args.limit > 0:
        docs = docs[: args.limit]

    todo = [d for d in docs if args.force or not (d.get("summary") or "").strip()]
    logger.info(
        "Documents total=%d; needing summary=%d (force=%s)", len(docs), len(todo), args.force
    )

    written = 0
    failed = 0
    for d in todo:
        doc_id = d["id"]
        try:
            excerpts = fetch_excerpts(client, doc_id)
        except Exception:
            logger.exception("Failed to fetch excerpts for doc %d", doc_id)
            failed += 1
            continue
        if not excerpts:
            logger.warning("doc %d has no chunks; skipping", doc_id)
            continue

        try:
            summary = generate_doc_summary(
                title=d.get("meeting_title") or "",
                doc_type=d.get("document_type") or "",
                date=str(d.get("meeting_date") or ""),
                portal=d.get("source_portal") or "",
                excerpts=excerpts,
                api_key=cfg.openai_api_key,
                model=cfg.summary_model,
            )
        except SummaryError as exc:
            logger.error("doc %d summary failed: %s", doc_id, exc)
            failed += 1
            continue

        try:
            client.table("documents").update({"summary": summary}).eq("id", doc_id).execute()
            written += 1
            logger.info("doc %d -> %s", doc_id, summary[:100])
        except Exception:
            logger.exception("Failed to write summary for doc %d", doc_id)
            failed += 1

        if args.sleep > 0:
            time.sleep(args.sleep)

    logger.info("Done. written=%d failed=%d skipped=%d", written, failed, len(docs) - len(todo))
    return 0


if __name__ == "__main__":
    sys.exit(main())
