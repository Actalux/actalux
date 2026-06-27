"""Backfill documents.chapters with topic chapters for YouTube transcripts.

For each board-meeting transcript, build a timestamped transcript from its chunks
(each chunk carries start_seconds from the Whisper sidecar), ask the LLM to divide
it into agenda/topic sections, and store the result on documents.chapters as
[{"t": <seconds>, "title": "<neutral label>"}, ...]. The reader then offers a
clickable chapter list that jumps the video to each topic.

Idempotent: skips transcripts that already have chapters. Pass --force to redo.
Only transcripts whose chunks carry start_seconds can be processed (the offsets
are what the chapters link to); others are skipped and reported.

Run via doppler so OPENAI_API_KEY and Supabase creds are present:
  doppler run --project mac --config dev -- \\
      uv run python scripts/backfill_transcript_chapters.py
  doppler run --project mac --config dev -- \\
      uv run python scripts/backfill_transcript_chapters.py --force --limit 3
"""

from __future__ import annotations

import argparse
import logging
import sys
import time

from actalux.config import load_config
from actalux.db import fetch_all_rows, get_client
from actalux.errors import SummaryError
from actalux.search.summarize import generate_chapters

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

MIN_TIMESTAMPED_CHUNKS = 4  # too few offsets to chapter meaningfully


def build_timestamped_transcript(client, doc_id: int) -> tuple[str, int]:
    """Return ('[secs] text ...' joined, max_seconds) for a transcript's chunks.

    Only chunks with a start_seconds offset are included (chapters link to those
    offsets). Returns ("", 0) when there are too few timestamped chunks.
    """
    res = (
        client.table("chunks")
        .select("content,start_seconds,chunk_index")
        .eq("document_id", doc_id)
        .order("chunk_index", desc=False)
        .execute()
    )
    timed = [
        r
        for r in (res.data or [])
        if r.get("start_seconds") is not None and (r.get("content") or "").strip()
    ]
    if len(timed) < MIN_TIMESTAMPED_CHUNKS:
        return "", 0
    lines = [f"[{int(r['start_seconds'])}] {r['content'].strip()}" for r in timed]
    max_seconds = max(int(r["start_seconds"]) for r in timed)
    return "\n".join(lines), max_seconds


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--force", action="store_true", help="regenerate even if chapters exist")
    parser.add_argument("--limit", type=int, default=0, help="process at most N (0 = all)")
    parser.add_argument("--sleep", type=float, default=0.0, help="seconds between API calls")
    args = parser.parse_args()

    cfg = load_config()
    if not cfg.openrouter_api_key:
        logger.error("OpenRouter key not set; aborting")
        return 1
    # Writer: service key bypasses RLS.
    client = get_client(cfg.supabase_url, cfg.supabase_service_key)

    # Page past PostgREST's row cap (keeps newest meetings in scope as the
    # transcript corpus grows past ~1000 docs).
    docs = fetch_all_rows(
        lambda: (
            client.table("documents")
            .select("id,meeting_title,meeting_date,chapters")
            .eq("source_portal", "youtube")
            .eq("document_type", "transcript")
            .is_("replaces_id", "null")
        ),
        order="meeting_date",
        desc=True,
    )
    todo = [d for d in docs if args.force or not d.get("chapters")]
    if args.limit > 0:
        todo = todo[: args.limit]
    logger.info("transcripts=%d needing chapters=%d (force=%s)", len(docs), len(todo), args.force)

    written = skipped = failed = 0
    for d in todo:
        doc_id = d["id"]
        transcript, max_seconds = build_timestamped_transcript(client, doc_id)
        if not transcript:
            logger.info("doc %d: too few timestamped chunks; skipping", doc_id)
            skipped += 1
            continue
        try:
            chapters = generate_chapters(
                title=d.get("meeting_title") or "",
                date=str(d.get("meeting_date") or ""),
                timestamped_transcript=transcript,
                api_key=cfg.openrouter_api_key,
                model=cfg.summary_model,
                base_url=cfg.openrouter_base_url,
                max_seconds=max_seconds,
            )
        except SummaryError as exc:
            logger.error("doc %d chapters failed: %s", doc_id, exc)
            failed += 1
            continue
        client.table("documents").update({"chapters": chapters}).eq("id", doc_id).execute()
        written += 1
        logger.info("doc %d -> %d chapters (%s ...)", doc_id, len(chapters), chapters[0]["title"])
        if args.sleep > 0:
            time.sleep(args.sleep)

    logger.info("Done. written=%d skipped=%d failed=%d", written, skipped, failed)
    return 0


if __name__ == "__main__":
    sys.exit(main())
