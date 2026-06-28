#!/usr/bin/env python3
"""Transcribe board-meeting videos with clean WhisperX + pyannote and stage them.

The going-forward speaker-attribution pipeline (design:
docs/architecture/speaker-attribution.md). For each meeting it downloads audio
(WARP egress in CI), runs WhisperX (word-level, no name biasing — clean verbatim)
and pyannote diarization on Modal, canonicalizes proper nouns against the place's
vetted name-corrections, and stages four artifacts under data/documents/:

  <stem>.txt              canonical (name-corrected) text   -> documents.content
  <stem>.segments.json    canonical segment timings         -> chunk start_seconds
  <stem>.attribution.json raw text + word-level speaker turns + the correction
                          audit + media metadata            -> the new tables
  whisperx_manifest.json  the ingest manifest

A later step runs scripts/ingest.py --manifest (documents + chunks + version
chain), then scripts/persist_whisperx.py attaches the attribution layer keyed to
the new document id. Mirrors transcribe_meetings.py (the Groq path) so the two
read the same way. Runs entirely off-Mac (GitHub Actions over Modal).

Prereq: the Modal apps must already be deployed
(``modal deploy src/actalux/diarization/modal_runner.py`` and
``modal deploy src/actalux/transcription/modal_whisperx.py``).

Run (DB creds from Doppler mac; Modal/HF tokens from actalux):
  MODAL_TOKEN_ID=... MODAL_TOKEN_SECRET=... HF_TOKEN=... \
  doppler run --project mac --config dev -- \
    uv run --group diarization python scripts/transcribe_whisperx.py \
      --discover --body council --limit 3
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path

from actalux.config import load_config
from actalux.db import get_client, get_entity_by_path, get_name_corrections
from actalux.diarization.modal_runner import ModalRunner
from actalux.errors import ActaluxError
from actalux.glossary.canonicalize import CorrectionRule, build_rules, canonicalize_text
from actalux.graph.store import place_lexicon
from actalux.ingest.bodies import get_body
from actalux.ingest.youtube import BoardMeeting, download_audio
from actalux.transcription.backend import WordTranscript
from actalux.transcription.modal_whisperx import WhisperXRunner
from actalux.transcription.pipeline import SpeakerLayer, assemble_speaker_layer

# Reuse the proven Groq-path helpers (same-dir import, like discover_corrections.py).
sys.path.insert(0, str(Path(__file__).resolve().parent))
from transcribe_meetings import (  # noqa: E402
    WARP_DOWNLOAD_RETRIES,
    existing_transcript_dates,
    manifest_entry,
    reconnect_warp,
    safe_stem,
    select_meetings,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

OUTPUT_DIR = Path("data/documents")
AUDIO_DIR = Path("data/audio")  # transient; gitignored, deleted after transcription
MANIFEST_PATH = OUTPUT_DIR / "whisperx_manifest.json"


def meeting_stem(meeting: BoardMeeting) -> str:
    """A per-meeting artifact stem that is unique within a run.

    Title alone collides when two meetings share a title (e.g. recurring agendas), so
    the video id — unique per meeting — is appended; otherwise the .txt/.segments/
    .attribution artifacts and the manifest source_file would clash and persist could
    attach a layer to the wrong document.
    """
    return f"{safe_stem(meeting.title)}_{meeting.video_id}"


def canonical_segments(raw: WordTranscript, rules: list[CorrectionRule]) -> list[dict]:
    """Per-segment canonical timings for the chunk-timestamp sidecar.

    Canonicalized to match the canonical chunks (documents.content), so the
    chunk->segment timestamp matching aligns on the same spellings.
    """
    return [
        {"start": s.start_s, "end": s.end_s, "text": canonicalize_text(s.text, rules)[0]}
        for s in raw.segments
    ]


def stage_meeting(
    meeting: BoardMeeting,
    layer: SpeakerLayer,
    segments: list[dict],
    entity_id: int,
    out_dir: Path,
) -> dict[str, str]:
    """Write the canonical text, segment sidecar, and attribution sidecar; return manifest row.

    The attribution sidecar carries everything the post-ingest persist step needs
    (it can't write yet — the document id doesn't exist until ingest runs).
    """
    stem = meeting_stem(meeting)
    (out_dir / f"{stem}.txt").write_text(layer.canonical_text)
    (out_dir / f"{stem}.segments.json").write_text(json.dumps(segments, indent=2))
    attribution = {
        "video_id": meeting.video_id,
        "source_url": meeting.url,
        "entity_id": entity_id,
        "duration_seconds": None,  # optional; a later media-metadata pass can fill it
        "layer": layer.to_dict(),
    }
    (out_dir / f"{stem}.attribution.json").write_text(json.dumps(attribution, indent=2))
    return manifest_entry(meeting, f"{stem}.txt")


def process_meeting(
    meeting: BoardMeeting,
    entity_id: int,
    rules: list[CorrectionRule],
    transcriber: WhisperXRunner,
    diarizer: ModalRunner,
    out_dir: Path,
    audio_dir: Path,
    *,
    proxy: str | None,
) -> dict[str, str]:
    """Download, transcribe+diarize+attribute, and stage one meeting; return its manifest row."""
    logger.info("processing %s (%s)", meeting.title, meeting.video_id)
    retries = WARP_DOWNLOAD_RETRIES if proxy else 1
    audio = download_audio(
        meeting.video_id,
        audio_dir,
        proxy=proxy,
        retries=retries,
        on_retry=reconnect_warp if proxy else None,
    )
    try:
        raw = transcriber.transcribe(str(audio))
        timeline = diarizer.run(str(audio))
    finally:
        audio.unlink(missing_ok=True)  # audio is transient; only the text is kept
    layer = assemble_speaker_layer(raw, timeline, rules)
    segments = canonical_segments(raw, rules)
    logger.info(
        "  %d words, %d turns, %d name fixes",
        len(raw.all_words()),
        len(layer.turns),
        len(layer.canonicalizations),
    )
    return stage_meeting(meeting, layer, segments, entity_id, out_dir)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Transcribe meetings with clean WhisperX + pyannote."
    )
    parser.add_argument("--discover", action="store_true", help="list channel meeting videos")
    parser.add_argument(
        "--body", default="schools", help="which public body (default: %(default)s)"
    )
    parser.add_argument("--since", help="only meetings on/after this date (YYYY-MM-DD)")
    parser.add_argument("--limit", type=int, help="cap the number of meetings processed")
    parser.add_argument("--force", action="store_true", help="re-transcribe even if a file exists")
    parser.add_argument("--video-id", help="transcribe a single video id")
    parser.add_argument("--date", help="meeting date for --video-id (YYYY-MM-DD)")
    parser.add_argument("--title", help="meeting title for --video-id")
    parser.add_argument("--proxy", help="SOCKS proxy for yt-dlp (WARP endpoint in CI)")
    args = parser.parse_args()

    if not args.discover and not args.video_id:
        parser.error("pass --discover or --video-id")

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
    existing = (
        existing_transcript_dates(client, entity["id"])
        if args.discover and not args.force
        else set()
    )
    meetings = select_meetings(args, OUTPUT_DIR, existing, body)
    logger.info("meetings to transcribe: %d", len(meetings))
    if not meetings:
        MANIFEST_PATH.write_text(json.dumps([], indent=2))
        return

    # Backends bind to deployed Modal apps; constructing them is cheap (no GPU).
    transcriber = WhisperXRunner()
    diarizer = ModalRunner()
    entries: list[dict[str, str]] = []
    for meeting in meetings:
        try:
            entries.append(
                process_meeting(
                    meeting,
                    entity["id"],
                    rules,
                    transcriber,
                    diarizer,
                    OUTPUT_DIR,
                    AUDIO_DIR,
                    proxy=args.proxy,
                )
            )
        except ActaluxError:
            logger.exception("failed: %s (%s)", meeting.title, meeting.video_id)

    source_files = [e["source_file"] for e in entries]
    if len(set(source_files)) != len(source_files):
        raise SystemExit(f"duplicate source_file in manifest (stem collision): {source_files}")
    MANIFEST_PATH.write_text(json.dumps(entries, indent=2))
    logger.info("staged %d transcript(s); manifest: %s", len(entries), MANIFEST_PATH)


if __name__ == "__main__":
    main()
