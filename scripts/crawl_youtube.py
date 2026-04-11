#!/usr/bin/env python3
"""Discover Clayton School District YouTube board meeting videos.

Lists all board meeting videos, identifies which have transcripts
in the corpus, and flags gaps that need transcription.

Usage:
    python scripts/crawl_youtube.py

Requires yt-dlp installed.
"""

from __future__ import annotations

import json
import logging
import re
import subprocess
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

CHANNEL_URL = "https://www.youtube.com/@SchoolDistrictofClayton/videos"
MANIFEST_PATH = Path("data/youtube_discovery.json")

# Pattern to identify board meeting videos
BOARD_MEETING_RE = re.compile(
    r"board of education|BOE meeting",
    re.IGNORECASE,
)

# Date patterns in video titles
DATE_PATTERNS = [
    # "2/19/26" or "10/8/25"
    re.compile(r"(\d{1,2})/(\d{1,2})/(\d{2})\b"),
    # "Nov. 13, 2019"
    re.compile(
        r"(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\.?\s+(\d{1,2}),?\s+(\d{4})",
        re.IGNORECASE,
    ),
    # "11112020"
    re.compile(r"(\d{2})(\d{2})(\d{4})\b"),
]

MONTH_NAMES = {
    "jan": 1,
    "feb": 2,
    "mar": 3,
    "apr": 4,
    "may": 5,
    "jun": 6,
    "jul": 7,
    "aug": 8,
    "sep": 9,
    "oct": 10,
    "nov": 11,
    "dec": 12,
}


def parse_date_from_title(title: str) -> str | None:
    """Extract a date string (YYYY-MM-DD) from a video title."""
    # Try M/D/YY
    match = DATE_PATTERNS[0].search(title)
    if match:
        month, day, year = int(match.group(1)), int(match.group(2)), int(match.group(3))
        year = 2000 + year if year < 50 else 1900 + year
        return f"{year}-{month:02d}-{day:02d}"

    # Try "Nov. 13, 2019"
    match = DATE_PATTERNS[1].search(title)
    if match:
        month = MONTH_NAMES[match.group(1).lower()[:3]]
        day = int(match.group(2))
        year = int(match.group(3))
        return f"{year}-{month:02d}-{day:02d}"

    # Try "MMDDYYYY"
    match = DATE_PATTERNS[2].search(title)
    if match:
        month, day, year = int(match.group(1)), int(match.group(2)), int(match.group(3))
        return f"{year}-{month:02d}-{day:02d}"

    return None


def list_channel_videos() -> list[dict[str, str]]:
    """List all videos from the Clayton SD YouTube channel."""
    result = subprocess.run(
        [
            "yt-dlp",
            "--flat-playlist",
            "--print",
            "%(id)s|%(title)s|%(duration_string)s",
            CHANNEL_URL,
        ],
        capture_output=True,
        text=True,
        timeout=60,
    )

    videos: list[dict[str, str]] = []
    for line in result.stdout.strip().split("\n"):
        if not line.strip():
            continue
        parts = line.split("|", 2)
        if len(parts) < 2:
            continue
        video_id = parts[0]
        title = parts[1]
        duration = parts[2] if len(parts) > 2 else ""
        videos.append(
            {
                "video_id": video_id,
                "title": title,
                "duration": duration,
                "url": f"https://www.youtube.com/watch?v={video_id}",
            }
        )

    return videos


def main() -> None:
    logger.info("Listing videos from Clayton SD YouTube channel...")
    all_videos = list_channel_videos()
    logger.info("Found %d total videos", len(all_videos))

    # Filter to board meetings
    board_meetings: list[dict[str, str]] = []
    for video in all_videos:
        if BOARD_MEETING_RE.search(video["title"]):
            video["meeting_date"] = parse_date_from_title(video["title"]) or ""
            board_meetings.append(video)

    board_meetings.sort(
        key=lambda v: v.get("meeting_date", ""),
        reverse=True,
    )

    logger.info("Identified %d board meeting videos", len(board_meetings))

    # Check which we already have transcripts for
    existing_transcripts = set()
    docs_dir = Path("data/documents")
    for f in docs_dir.glob("*.txt"):
        existing_transcripts.add(f.stem.lower())

    for meeting in board_meetings:
        title_key = meeting["title"].replace("/", "-").replace("⧸", "-").lower()
        meeting["has_transcript"] = any(
            title_key in t or t in title_key for t in existing_transcripts
        )

    # Report
    needs_transcription = [m for m in board_meetings if not m["has_transcript"]]
    has_transcription = [m for m in board_meetings if m["has_transcript"]]

    logger.info("\n=== BOARD MEETINGS WITH TRANSCRIPTS (%d) ===", len(has_transcription))
    for m in has_transcription:
        logger.info("  %s  %s", m.get("meeting_date", "????-??-??"), m["title"])

    logger.info("\n=== NEEDS TRANSCRIPTION (%d) ===", len(needs_transcription))
    for m in needs_transcription:
        logger.info("  %s  %s  %s", m.get("meeting_date", "????-??-??"), m["title"], m["url"])

    # Write manifest
    output = {
        "total_videos": len(all_videos),
        "board_meetings": len(board_meetings),
        "has_transcript": len(has_transcription),
        "needs_transcription": len(needs_transcription),
        "meetings": board_meetings,
    }
    MANIFEST_PATH.write_text(json.dumps(output, indent=2))
    logger.info("\nDiscovery manifest written to %s", MANIFEST_PATH)


if __name__ == "__main__":
    main()
