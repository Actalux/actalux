"""Shared YouTube helpers: list Clayton board-meeting videos and download audio.

Used by the transcription orchestrator (``scripts/transcribe_meetings.py``) and
the discovery/gap report (``scripts/crawl_youtube.py``).

Downloads must route around YouTube's datacenter-IP bot-check when run in the
cloud. The proven recipe (verified June 2026 from a GitHub runner):

* route yt-dlp through a SOCKS proxy — Cloudflare WARP in CI clears the
  "Sign in to confirm you're not a bot" block that flags datacenter IPs;
* use the ``android`` player client — the one that resolves a downloadable
  format through the WARP egress;
* let yt-dlp's OWN downloader fetch the file. NEVER ``--download-sections``: it
  hands the media fetch to ffmpeg, which ignores ``--proxy`` and 403s from the
  raw datacenter IP. Trim/transcode locally instead.

Locally (a residential IP) the proxy is unnecessary, so ``proxy`` is optional.
"""

from __future__ import annotations

import json
import logging
import re
import subprocess
from dataclasses import dataclass
from pathlib import Path

from actalux.errors import IngestError

logger = logging.getLogger(__name__)

CHANNEL_URL = "https://www.youtube.com/@SchoolDistrictofClayton/videos"
# Player client that resolves a downloadable format through the WARP egress.
PLAYER_CLIENT = "android"
# bestaudio if the client exposes audio-only, else format 18 (progressive
# 360p+audio), else anything; audio is extracted locally afterward.
AUDIO_FORMAT = "bestaudio/18/best"

BOARD_MEETING_RE = re.compile(r"board of education|BOE meeting", re.IGNORECASE)

DATE_PATTERNS = [
    re.compile(r"(\d{1,2})/(\d{1,2})/(\d{2})\b"),  # 2/19/26
    re.compile(
        r"(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\.?\s+(\d{1,2}),?\s+(\d{4})",
        re.IGNORECASE,
    ),  # Nov. 13, 2019
    re.compile(r"(\d{2})(\d{2})(\d{4})\b"),  # 11132019
]

MONTH_NAMES = {
    "jan": 1, "feb": 2, "mar": 3, "apr": 4, "may": 5, "jun": 6,
    "jul": 7, "aug": 8, "sep": 9, "oct": 10, "nov": 11, "dec": 12,
}  # fmt: skip


@dataclass(frozen=True)
class BoardMeeting:
    """A board-meeting video discovered on the channel."""

    video_id: str
    title: str
    meeting_date: str  # "YYYY-MM-DD", or "" when the title carries no date
    url: str


def parse_date_from_title(title: str) -> str | None:
    """Extract a meeting date (YYYY-MM-DD) from a video title, or None."""
    m = DATE_PATTERNS[0].search(title)
    if m:
        month, day, year = int(m.group(1)), int(m.group(2)), int(m.group(3))
        year = 2000 + year if year < 50 else 1900 + year
        return f"{year}-{month:02d}-{day:02d}"
    m = DATE_PATTERNS[1].search(title)
    if m:
        return f"{int(m.group(3))}-{MONTH_NAMES[m.group(1).lower()[:3]]:02d}-{int(m.group(2)):02d}"
    m = DATE_PATTERNS[2].search(title)
    if m:
        return f"{int(m.group(3))}-{int(m.group(1)):02d}-{int(m.group(2)):02d}"
    return None


def _proxy_args(proxy: str | None) -> list[str]:
    """yt-dlp args for the WARP egress: SOCKS proxy + the android player client."""
    args = ["--extractor-args", f"youtube:player_client={PLAYER_CLIENT}"]
    if proxy:
        args += ["--proxy", proxy]
    return args


def list_board_meetings(
    channel_url: str = CHANNEL_URL, *, proxy: str | None = None
) -> list[BoardMeeting]:
    """List the channel's board-meeting videos (newest first), with parsed dates."""
    cmd = [
        "yt-dlp",
        "--flat-playlist",
        "--print",
        "%(id)s|%(title)s",
        *_proxy_args(proxy),
        channel_url,
    ]
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=120, check=True)
    except (subprocess.CalledProcessError, subprocess.TimeoutExpired) as exc:
        raise IngestError(f"yt-dlp channel listing failed: {exc}") from exc

    meetings: list[BoardMeeting] = []
    for line in result.stdout.strip().splitlines():
        vid, _, title = line.partition("|")
        if not vid or not BOARD_MEETING_RE.search(title):
            continue
        meetings.append(
            BoardMeeting(
                video_id=vid,
                title=title,
                meeting_date=parse_date_from_title(title) or "",
                url=f"https://www.youtube.com/watch?v={vid}",
            )
        )
    meetings.sort(key=lambda m: m.meeting_date, reverse=True)
    return meetings


def download_audio(video_id: str, dest_dir: Path, *, proxy: str | None = None) -> Path:
    """Download a video's audio to ``dest_dir`` as MP3; return the file path.

    Uses yt-dlp's native downloader (honors ``--proxy``) + local ffmpeg audio
    extraction. Raises ``IngestError`` on failure or if no MP3 is produced.
    """
    dest_dir.mkdir(parents=True, exist_ok=True)
    out_template = str(dest_dir / f"{video_id}.%(ext)s")
    url = f"https://www.youtube.com/watch?v={video_id}"
    try:
        subprocess.run(
            ["yt-dlp", *_proxy_args(proxy), "-f", AUDIO_FORMAT,
             "-x", "--audio-format", "mp3", "--no-warnings",
             "-o", out_template, url],
            capture_output=True, text=True, timeout=1800, check=True,
        )  # fmt: skip
    except (subprocess.CalledProcessError, subprocess.TimeoutExpired) as exc:
        stderr = getattr(exc, "stderr", "") or ""
        raise IngestError(f"yt-dlp audio download failed for {video_id}: {stderr[-500:]}") from exc

    mp3 = dest_dir / f"{video_id}.mp3"
    if not mp3.exists():
        raise IngestError(f"audio download produced no MP3 for {video_id}")
    return mp3


def load_discovery(path: Path) -> list[dict]:
    """Load a previously written discovery manifest (list of meeting dicts)."""
    return json.loads(path.read_text()) if path.exists() else []
