#!/usr/bin/env python3
"""Backfill chunks.start_seconds for YouTube board-meeting docs.

For each transcript with a Whisper sidecar (a ``<stem>.segments.json`` written by
``transcribe_meetings.py`` next to the transcript), align each chunk to a time
offset so the reader pane can cue the player to the cited moment. The stored
transcript IS this Whisper text, so the alignment is exact (near-100% coverage):
each chunk is matched by a short word-window probe (several positions, first exact
hit wins) read off the timed text. Chunks that don't align keep start_seconds =
NULL and play from 0:00.

Sidecars are only on disk right after a transcription run, so this is run as a
step of ``transcribe.yml`` once the audio has been transcribed. Docs without a
sidecar present are skipped — their offsets were already persisted when they were
first transcribed.

Usage:
    doppler run --project mac --config dev -- \
        uv run python scripts/backfill_chunk_timestamps.py --dry-run
    doppler run --project mac --config dev -- \
        uv run python scripts/backfill_chunk_timestamps.py
"""

from __future__ import annotations

import argparse
import json
import logging
import re
from pathlib import Path

from actalux.config import load_config
from actalux.db import get_client, set_chunk_start_seconds

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

PROBE_WORDS = 12  # length of the word-window matched against the timed text
PROBE_FRACTIONS = (0.4, 0.2, 0.6, 0.1, 0.8)  # where in the chunk to take a probe
MIN_CHUNK_WORDS = 8  # chunks shorter than this have no reliable probe
SEGMENTS_DIR = Path("data/documents")  # where transcribe_meetings writes the sidecars


def _norm(text: str) -> str:
    """Lowercase, keep alphanumerics + spaces, collapse whitespace."""
    return re.sub(r"[^a-z0-9 ]", "", re.sub(r"\s+", " ", text.lower())).strip()


def load_segment_sidecar(source_file: str) -> list[dict] | None:
    """Load a Whisper ``<stem>.segments.json`` sidecar for a transcript, if present."""
    if not source_file:
        return None
    sidecar = SEGMENTS_DIR / f"{Path(source_file).stem}.segments.json"
    return json.loads(sidecar.read_text()) if sidecar.exists() else None


def build_timed_index_from_segments(segments: list[dict]) -> tuple[str, list[int]]:
    """Timed index from Whisper segments (exact: the transcript IS this text)."""
    parts: list[str] = []
    char_ms: list[int] = []
    for s in segments:
        nt = _norm(s.get("text", ""))
        if not nt:
            continue
        nt += " "
        parts.append(nt)
        char_ms.extend([int(float(s.get("start", 0)) * 1000)] * len(nt))
    return "".join(parts), char_ms


def align_chunk(content: str, timed_text: str, char_ms: list[int]) -> int | None:
    """Find the chunk in the timed text via a word-window probe; return start sec."""
    words = _norm(content).split()
    if len(words) < MIN_CHUNK_WORDS:
        return None
    for frac in PROBE_FRACTIONS:
        i = int(len(words) * frac)
        probe = " ".join(words[i : i + PROBE_WORDS])
        pos = timed_text.find(probe)
        if pos != -1:
            return char_ms[pos] // 1000
    return None


def main() -> None:
    parser = argparse.ArgumentParser(description="Backfill chunks.start_seconds for transcripts.")
    parser.add_argument("--dry-run", action="store_true", help="report coverage; write nothing")
    args = parser.parse_args()

    config = load_config()
    client = get_client(config.supabase_url, config.supabase_service_key)  # writer
    docs = [
        d
        for d in client.table("documents").select("id, source_portal, source_file").execute().data
        if d.get("source_portal") == "youtube"
    ]
    logger.info("youtube docs to process: %d", len(docs))

    total_aligned = total_chunks = 0
    for d in docs:
        segments = load_segment_sidecar(d.get("source_file", ""))
        if segments is None:
            continue  # no sidecar on disk -> offsets already persisted at transcribe time
        timed_text, char_ms = build_timed_index_from_segments(segments)
        chunks = (
            client.table("chunks")
            .select("id, content, start_seconds")
            .eq("document_id", d["id"])
            .order("chunk_index")
            .execute()
            .data
        )
        aligned = 0
        for ch in chunks:
            sec = align_chunk(ch["content"], timed_text, char_ms)
            if sec is None or ch.get("start_seconds") == sec:
                continue
            if not args.dry_run:
                set_chunk_start_seconds(client, ch["id"], sec)
            aligned += 1
        total_aligned += aligned
        total_chunks += len(chunks)
        pct = (100 * aligned // len(chunks)) if chunks else 0
        logger.info("doc %s: aligned %d/%d (%d%%)", d["id"], aligned, len(chunks), pct)

    verb = "would set" if args.dry_run else "set"
    logger.info(
        "Done: %s %d/%d chunk timestamps across %d docs.",
        verb,
        total_aligned,
        total_chunks,
        len(docs),
    )


if __name__ == "__main__":
    main()
