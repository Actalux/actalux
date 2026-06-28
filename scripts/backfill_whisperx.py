#!/usr/bin/env python3
"""Backfill a body's whole meeting corpus with clean WhisperX + pyannote, in parallel.

Same per-meeting work as ``transcribe_whisperx.py`` (download -> clean WhisperX +
pyannote on Modal -> canonicalize -> stage), but built for the one-time corpus
re-transcribe: it **fans the GPU work out across Modal** instead of doing one
blocking meeting at a time. The serial path costs ~(transcribe + diarize) per
meeting (~8 min x ~100 meetings ~= many hours); fanning out collapses that to the
download wall-clock plus the GPU tail.

Why a re-transcribe at all: the existing Groq transcripts carry decode-time
prompt-leakage contamination and have no word-level timestamps or speaker turns.
Re-transcribing clean (no name biasing) supersedes each Groq version via the
document version chain (``replaces_id``) and scrubs the leakage; PDFs/minutes are
untouched (transcripts only).

Three phases:
  1. Download audio concurrently (a thread pool over the WARP proxy) and, as each
     lands, ``spawn`` its transcribe + diarize on Modal (non-blocking). WARP egress
     rotation is global and not thread-safe, so the pool never rotates — a download
     that fails on the current egress is deferred, not retried in-pool.
  2. Retry the deferred downloads serially, this time rotating the WARP egress
     between attempts (safe now that nothing else is streaming through it).
  3. ``collect`` every spawned GPU result (they ran in parallel while we downloaded),
     assemble the speaker layer, and stage the same four artifacts the serial path
     writes, plus the ingest manifest.

A later step runs ``scripts/ingest.py --manifest`` then
``scripts/persist_whisperx.py`` — identical to the serial path. Single body per run
(matches ``ingest --body``); the CI workflow loops the bodies.

Run (DB creds from Doppler mac; Modal/HF tokens from actalux):
  MODAL_TOKEN_ID=... MODAL_TOKEN_SECRET=... HF_TOKEN=... \
  doppler run --project mac --config dev -- \
    uv run --group diarization python scripts/backfill_whisperx.py \
      --body council --workers 4
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path
from types import SimpleNamespace
from typing import Any

from actalux.config import load_config
from actalux.db import get_client, get_entity_by_path, get_name_corrections
from actalux.diarization.modal_runner import ModalRunner
from actalux.errors import ActaluxError
from actalux.glossary.canonicalize import CorrectionRule, build_rules
from actalux.graph.store import place_lexicon
from actalux.ingest.bodies import get_body
from actalux.ingest.youtube import BoardMeeting, download_audio
from actalux.transcription.modal_whisperx import WhisperXRunner
from actalux.transcription.pipeline import assemble_speaker_layer

# Reuse the serial path's staging helpers + the Groq path's discovery/WARP helpers,
# same-dir import like transcribe_whisperx.py does.
sys.path.insert(0, str(Path(__file__).resolve().parent))
from transcribe_meetings import (  # noqa: E402
    WARP_DOWNLOAD_RETRIES,
    existing_transcript_dates,
    reconnect_warp,
    select_meetings,
)
from transcribe_whisperx import (  # noqa: E402
    AUDIO_DIR,
    MANIFEST_PATH,
    OUTPUT_DIR,
    canonical_segments,
    stage_meeting,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# Same-egress retries inside the concurrent pool (no WARP rotation — that is global
# and would break the other in-flight downloads). A download still failing falls to
# the serial retry phase, which rotates the egress between attempts.
POOL_DOWNLOAD_RETRIES = 2


@dataclass
class _Pending:
    """A meeting whose GPU work is spawned and awaiting collection."""

    meeting: BoardMeeting
    transcribe_call: Any  # opaque modal.FunctionCall handle (kept Modal-agnostic here)
    diarize_call: Any


def _spawn(
    meeting: BoardMeeting, audio: Path, transcriber: WhisperXRunner, diarizer: ModalRunner
) -> _Pending:
    """Ship a downloaded meeting's audio to Modal (transcribe + diarize) without blocking.

    Reads the audio once, spawns both GPU jobs, and deletes the local file — the audio
    is transient (only the text is kept) and the bytes are already on their way to Modal.
    The read is inside the ``finally`` so a read failure still unlinks the file; if the
    second spawn fails, the first is cancelled so a half-failed pair leaves no orphan.
    """
    try:
        audio_bytes = audio.read_bytes()
        transcribe_call = transcriber.spawn(audio_bytes)
        try:
            diarize_call = diarizer.spawn(audio_bytes)
        except Exception:
            transcriber.cancel(transcribe_call)  # don't leave the transcribe job orphaned
            raise
    finally:
        audio.unlink(missing_ok=True)
    return _Pending(meeting, transcribe_call, diarize_call)


def download_and_spawn(
    meetings: list[BoardMeeting],
    download: Callable[[str, bool], Path],
    transcriber: WhisperXRunner,
    diarizer: ModalRunner,
    *,
    workers: int,
) -> tuple[list[_Pending], list[BoardMeeting]]:
    """Phase 1: download concurrently and spawn GPU work; return (pending, deferred).

    ``download(video_id, reconnect)`` downloads one meeting; ``reconnect=False`` here
    because WARP rotation is global and would break the other concurrent downloads.
    Spawning happens on this (main) thread as each download completes, so the Modal
    client is only ever called single-threaded. A download failure defers the meeting
    to the serial retry phase; a spawn failure is logged and dropped (a re-download
    can't fix it).
    """
    pending: list[_Pending] = []
    deferred: list[BoardMeeting] = []
    with ThreadPoolExecutor(max_workers=max(1, workers)) as pool:
        futures = {pool.submit(download, m.video_id, False): m for m in meetings}
        for future in as_completed(futures):
            meeting = futures[future]
            try:
                audio = future.result()
            except ActaluxError:
                logger.warning("pool download failed for %s; will retry serially", meeting.video_id)
                deferred.append(meeting)
                continue
            try:
                pending.append(_spawn(meeting, audio, transcriber, diarizer))
            except Exception:
                logger.exception(
                    "spawn failed for %s (%s); skipping", meeting.title, meeting.video_id
                )
    return pending, deferred


def retry_serial(
    deferred: list[BoardMeeting],
    download: Callable[[str, bool], Path],
    transcriber: WhisperXRunner,
    diarizer: ModalRunner,
) -> list[_Pending]:
    """Phase 2: re-download the deferred meetings serially (WARP rotation enabled)."""
    pending: list[_Pending] = []
    for meeting in deferred:
        try:
            audio = download(meeting.video_id, True)
        except ActaluxError:
            logger.exception(
                "download failed permanently for %s (%s)", meeting.title, meeting.video_id
            )
            continue
        try:
            pending.append(_spawn(meeting, audio, transcriber, diarizer))
        except Exception:
            logger.exception("spawn failed for %s (%s); skipping", meeting.title, meeting.video_id)
    return pending


def collect_and_stage(
    pending: list[_Pending],
    entity_id: int,
    rules: list[CorrectionRule],
    transcriber: WhisperXRunner,
    diarizer: ModalRunner,
    out_dir: Path,
) -> list[dict[str, str]]:
    """Phase 3: collect each spawned GPU result, assemble the layer, stage artifacts.

    Every meeting is fully guarded — a failure in collect, assemble, or stage drops just
    that meeting (logged) instead of crashing a run that already staged others — and the
    two GPU jobs are collected independently so a failed one can't orphan its sibling.
    Processed in deterministic ``(meeting_date, video_id)`` order so reruns produce stable
    artifacts (downloads otherwise finish in arbitrary order).
    """
    entries: list[dict[str, str]] = []
    for p in sorted(pending, key=lambda x: (x.meeting.meeting_date, x.meeting.video_id)):
        try:
            raw = transcriber.collect(p.transcribe_call)
        except Exception:
            logger.exception(
                "transcribe collect failed for %s (%s); skipping",
                p.meeting.title,
                p.meeting.video_id,
            )
            diarizer.cancel(p.diarize_call)  # don't leave the paired diarize job orphaned
            continue
        try:
            timeline = diarizer.collect(p.diarize_call)
        except Exception:
            logger.exception(
                "diarize collect failed for %s (%s); skipping", p.meeting.title, p.meeting.video_id
            )
            continue
        try:
            layer = assemble_speaker_layer(raw, timeline, rules)
            segments = canonical_segments(raw, rules)
            entry = stage_meeting(p.meeting, layer, segments, entity_id, out_dir)
        except Exception:
            logger.exception(
                "assemble/stage failed for %s (%s); skipping", p.meeting.title, p.meeting.video_id
            )
            continue
        logger.info(
            "  %s: %d words, %d turns, %d name fixes",
            p.meeting.video_id,
            len(raw.all_words()),
            len(layer.turns),
            len(layer.canonicalizations),
        )
        entries.append(entry)
    return entries


def make_downloader(audio_dir: Path, proxy: str | None) -> Callable[[str, bool], Path]:
    """Build the ``download(video_id, reconnect)`` callable bound to this run's proxy.

    ``reconnect=True`` rotates the WARP egress between attempts (serial phase only);
    ``reconnect=False`` retries the current egress a couple of times (concurrent pool).
    """

    def download(video_id: str, reconnect: bool) -> Path:
        if reconnect and proxy:
            return download_audio(
                video_id,
                audio_dir,
                proxy=proxy,
                retries=WARP_DOWNLOAD_RETRIES,
                on_retry=reconnect_warp,
            )
        retries = POOL_DOWNLOAD_RETRIES if proxy else 1
        return download_audio(video_id, audio_dir, proxy=proxy, retries=retries, on_retry=None)

    return download


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Backfill a body's corpus with clean WhisperX + pyannote (Modal fan-out)."
    )
    parser.add_argument("--body", required=True, help="public body key (e.g. council, schools)")
    parser.add_argument("--since", help="only meetings on/after this date (YYYY-MM-DD)")
    parser.add_argument("--limit", type=int, help="cap the number of meetings (for testing)")
    parser.add_argument(
        "--incremental",
        action="store_true",
        help="skip already-ingested meetings (default: re-transcribe all, superseding old)",
    )
    parser.add_argument(
        "--workers", type=int, default=4, help="concurrent downloads (default: %(default)s)"
    )
    parser.add_argument("--proxy", help="SOCKS proxy for yt-dlp (WARP endpoint in CI)")
    args = parser.parse_args()

    force = not args.incremental
    body = get_body(args.body)
    cfg = load_config()
    client = get_client(cfg.supabase_url, cfg.supabase_key)
    entity = get_entity_by_path(client, *body.entity_path.split("/"))
    if not entity:
        raise SystemExit(f"Unknown entity {body.entity_path!r}; seed it first.")
    # Place-scoped corrections (cardinal rule): a mangling here is never applied elsewhere.
    rules = build_rules(
        get_name_corrections(client, entity["place_id"]),
        place_lexicon(client, entity["place_id"]),
    )

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    AUDIO_DIR.mkdir(parents=True, exist_ok=True)
    existing = set() if force else existing_transcript_dates(client, entity["id"])
    # select_meetings reads a fixed arg shape; the backfill always discovers (no single
    # --video-id), so the video_id/title/date fields are absent.
    sel_args = SimpleNamespace(
        video_id=None,
        title=None,
        date=None,
        since=args.since,
        force=force,
        limit=args.limit,
        proxy=args.proxy,
    )
    meetings = select_meetings(sel_args, OUTPUT_DIR, existing, body)
    logger.info("meetings to transcribe (parallel fan-out): %d", len(meetings))
    if not meetings:
        MANIFEST_PATH.write_text(json.dumps([], indent=2))
        return

    transcriber = WhisperXRunner()
    diarizer = ModalRunner()
    download = make_downloader(AUDIO_DIR, args.proxy)

    logger.info("phase 1/3: download + spawn GPU (workers=%d)", args.workers)
    pending, deferred = download_and_spawn(
        meetings, download, transcriber, diarizer, workers=args.workers
    )
    if deferred:
        logger.info(
            "phase 2/3: serial retry of %d deferred download(s) (WARP rotate)", len(deferred)
        )
        pending += retry_serial(deferred, download, transcriber, diarizer)
    logger.info("phase 3/3: collect %d GPU result(s) + stage", len(pending))
    entries = collect_and_stage(pending, entity["id"], rules, transcriber, diarizer, OUTPUT_DIR)

    source_files = [e["source_file"] for e in entries]
    if len(set(source_files)) != len(source_files):
        raise SystemExit(f"duplicate source_file in manifest (stem collision): {source_files}")
    MANIFEST_PATH.write_text(json.dumps(entries, indent=2))

    dropped = len(meetings) - len(entries)
    logger.info(
        "staged %d/%d transcript(s) (%d dropped); manifest: %s",
        len(entries),
        len(meetings),
        dropped,
        MANIFEST_PATH,
    )
    # A run that selected meetings but staged none failed wholesale (e.g. WARP flagged,
    # Modal down) — fail loudly so the empty manifest isn't mistaken for "nothing to do".
    # Partial drops are tolerated (some videos go private/unavailable); they are logged
    # above and a rerun re-attempts the still-missing dates.
    if meetings and not entries:
        raise SystemExit(
            f"backfill staged 0 of {len(meetings)} selected meeting(s); see errors above"
        )
    if dropped:
        logger.warning("%d meeting(s) dropped; rerun to re-attempt the missing dates", dropped)


if __name__ == "__main__":
    main()
