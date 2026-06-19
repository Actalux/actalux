#!/usr/bin/env python3
"""Transcribe Clayton board-meeting videos with Whisper and stage them for ingest.

Downloads meeting audio from YouTube (via a WARP SOCKS proxy in the cloud — see
``actalux.ingest.youtube``), transcribes it with Groq's ``whisper-large-v3``, and
writes each transcript plus a timestamp sidecar (``<stem>.segments.json``) and a
manifest the ingest reads. The sidecar feeds ``backfill_chunk_timestamps`` so the
reader can cue the video to a cited moment — now aligned to the exact ingested
text, not a separate caption pass.

Modes:
  --discover [--since YYYY-MM-DD] [--limit N]   list channel board meetings,
                                                skip ones already staged, do the rest
  --video-id ID [--date YYYY-MM-DD] [--title T] transcribe one meeting

Cloud runs pass --proxy (the WARP SOCKS endpoint); locally (residential IP) it is
omitted. Re-running is safe: an existing transcript is skipped unless --force.

Usage:
  doppler run --project mac --config dev -- \
    uv run python scripts/transcribe_meetings.py --discover --limit 3
"""

from __future__ import annotations

import argparse
import json
import logging
import re
from pathlib import Path

from actalux.config import load_config
from actalux.errors import ActaluxError
from actalux.ingest.transcribe import Transcript, transcribe_audio
from actalux.ingest.youtube import BoardMeeting, download_audio, list_board_meetings

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

OUTPUT_DIR = Path("data/documents")
MANIFEST_PATH = Path("data/documents/youtube_manifest.json")
AUDIO_DIR = Path("data/audio")  # transient; gitignored, deleted after transcription

# Biases Whisper toward the meeting's proper nouns (names mis-heard otherwise).
TRANSCRIBE_PROMPT = (
    "School District of Clayton Board of Education meeting. "
    "Superintendent, Board of Education, Proposition O, levy, agenda, motion carried."
)


def safe_stem(title: str) -> str:
    """A filesystem-safe filename stem from a video title."""
    return re.sub(r'[<>:"/\\|?*]', "_", title).strip()


def manifest_entry(meeting: BoardMeeting, source_file: str) -> dict[str, str]:
    """The ingest manifest row for a transcribed meeting."""
    return {
        "source_file": source_file,
        "source_url": meeting.url,
        "source_portal": "youtube",
        "document_type": "transcript",
        "meeting_date": meeting.meeting_date,
        "meeting_title": meeting.title,
        "date_source": "filename" if meeting.meeting_date else "manual",
        "video_id": meeting.video_id,
    }


def write_outputs(transcript: Transcript, stem: str, out_dir: Path) -> str:
    """Write the transcript text + segment sidecar; return the transcript filename."""
    txt_name = f"{stem}.txt"
    (out_dir / txt_name).write_text(transcript.text)
    sidecar = [{"start": s.start, "end": s.end, "text": s.text} for s in transcript.segments]
    (out_dir / f"{stem}.segments.json").write_text(json.dumps(sidecar, indent=2))
    return txt_name


def process_meeting(
    meeting: BoardMeeting, out_dir: Path, audio_dir: Path, *, proxy: str | None
) -> dict[str, str]:
    """Download, transcribe, and stage one meeting; return its manifest entry."""
    cfg = load_config()
    if not cfg.groq_api_key:
        raise ActaluxError("GROQ_ACTALUX_API_KEY is not set; cannot transcribe")
    logger.info("processing %s (%s)", meeting.title, meeting.video_id)
    audio = download_audio(meeting.video_id, audio_dir, proxy=proxy)
    try:
        transcript = transcribe_audio(
            audio,
            cfg.groq_api_key,
            model=cfg.transcribe_model,
            base_url=cfg.transcribe_base_url,
            prompt=TRANSCRIBE_PROMPT,
        )
    finally:
        audio.unlink(missing_ok=True)  # the audio is transient; only the text is kept
    stem = safe_stem(meeting.title)
    source_file = write_outputs(transcript, stem, out_dir)
    logger.info("  wrote %s (%d segments)", source_file, len(transcript.segments))
    return manifest_entry(meeting, source_file)


def select_meetings(args: argparse.Namespace, out_dir: Path) -> list[BoardMeeting]:
    """Resolve the meetings to process from --discover or explicit --video-id."""
    if args.video_id:
        return [
            BoardMeeting(
                video_id=args.video_id,
                title=args.title or args.video_id,
                meeting_date=args.date or "",
                url=f"https://www.youtube.com/watch?v={args.video_id}",
            )
        ]
    meetings = list_board_meetings(proxy=args.proxy)
    if args.since:
        meetings = [m for m in meetings if m.meeting_date >= args.since]
    if not args.force:
        meetings = [m for m in meetings if not (out_dir / f"{safe_stem(m.title)}.txt").exists()]
    if args.limit:
        meetings = meetings[: args.limit]
    return meetings


def main() -> None:
    parser = argparse.ArgumentParser(description="Transcribe board meetings with Whisper.")
    parser.add_argument("--discover", action="store_true", help="list channel board meetings")
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

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    meetings = select_meetings(args, OUTPUT_DIR)
    logger.info("meetings to transcribe: %d", len(meetings))

    entries: list[dict[str, str]] = []
    for meeting in meetings:
        try:
            entries.append(process_meeting(meeting, OUTPUT_DIR, AUDIO_DIR, proxy=args.proxy))
        except ActaluxError:
            logger.exception("failed: %s (%s)", meeting.title, meeting.video_id)

    MANIFEST_PATH.write_text(json.dumps(entries, indent=2))
    logger.info("staged %d transcript(s); manifest: %s", len(entries), MANIFEST_PATH)


if __name__ == "__main__":
    main()
