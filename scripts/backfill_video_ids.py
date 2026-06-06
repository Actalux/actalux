#!/usr/bin/env python3
"""One-off: backfill documents.video_id for YouTube board-meeting docs.

Matches each youtube-portal document to a channel video from the discovery
manifest (refresh it first with scripts/crawl_youtube.py) by meeting_date.
Same-date meetings (e.g. a regular and a joint board meeting on one day) are
disambiguated by title word-overlap. Docs whose meeting has no public channel
video are left with an empty video_id and keep their transcript-text view.

Usage:
    python scripts/crawl_youtube.py            # refresh the manifest first
    doppler run --project mac --config dev -- \
        uv run python scripts/backfill_video_ids.py --dry-run
    doppler run --project mac --config dev -- \
        uv run python scripts/backfill_video_ids.py
"""

from __future__ import annotations

import argparse
import json
import logging
from pathlib import Path

from actalux.config import load_config
from actalux.db import get_client, set_document_video_id

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

MANIFEST_PATH = Path("data/youtube_discovery.json")


def _words(text: str) -> set[str]:
    """Lowercased alphanumeric word set for title-overlap scoring."""
    return {w for w in "".join(c if c.isalnum() else " " for c in text.lower()).split() if w}


def _pick_video(doc_title: str, candidates: list[dict]) -> dict:
    """Choose the same-date video whose title best matches the doc title.

    With one candidate this is a no-op. With several (same-date regular vs joint
    meeting) use Jaccard similarity, not raw overlap: a regular-meeting title is a
    subset of the joint-meeting title, so raw overlap ties and would mis-route the
    regular doc to the joint video. Jaccard divides by the union, so the joint
    video's extra words ("joint", "alderman") penalize it for the regular doc.
    """
    if len(candidates) == 1:
        return candidates[0]
    doc_words = _words(doc_title)

    def jaccard(video: dict) -> float:
        video_words = _words(video.get("title", ""))
        union = doc_words | video_words
        return len(doc_words & video_words) / len(union) if union else 0.0

    return max(candidates, key=jaccard)


def main() -> None:
    parser = argparse.ArgumentParser(description="Backfill documents.video_id from the manifest.")
    parser.add_argument("--dry-run", action="store_true", help="report matches; write nothing")
    parser.add_argument("--manifest", type=Path, default=MANIFEST_PATH)
    args = parser.parse_args()

    meetings = json.loads(args.manifest.read_text())["meetings"]
    by_date: dict[str, list[dict]] = {}
    for m in meetings:
        if m.get("video_id") and m.get("meeting_date"):
            by_date.setdefault(m["meeting_date"], []).append(m)

    config = load_config()
    # Writer: the service key bypasses RLS (the publishable key cannot write).
    client = get_client(config.supabase_url, config.supabase_service_key)
    docs = [
        d
        for d in client.table("documents")
        .select("id, source_portal, meeting_date, meeting_title, video_id")
        .execute()
        .data
        if d.get("source_portal") == "youtube"
    ]

    assigned = 0
    no_video = 0
    for d in sorted(docs, key=lambda x: str(x.get("meeting_date"))):
        candidates = by_date.get(str(d.get("meeting_date")), [])
        if not candidates:
            no_video += 1
            logger.info("  no video   | doc %s | %s", d["id"], d.get("meeting_title"))
            continue
        video = _pick_video(d.get("meeting_title") or "", candidates)
        vid = video["video_id"]
        if d.get("video_id") == vid:
            logger.info("  unchanged  | doc %s | %s", d["id"], vid)
            continue
        if args.dry_run:
            logger.info("  would set  | doc %s | %s | %s", d["id"], vid, video.get("title"))
        else:
            set_document_video_id(client, d["id"], vid)
            logger.info("  set        | doc %s | %s | %s", d["id"], vid, video.get("title"))
        assigned += 1

    verb = "would assign" if args.dry_run else "assigned"
    logger.info("Done: %s %d video_ids; %d docs have no public video.", verb, assigned, no_video)


if __name__ == "__main__":
    main()
