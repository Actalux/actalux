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
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path

from actalux.errors import IngestError

logger = logging.getLogger(__name__)

CHANNEL = "https://www.youtube.com/@SchoolDistrictofClayton"
# Board meetings are LIVESTREAMED, so most live under the channel's /streams tab;
# /videos catches any non-livestream uploads. Enumerate both and dedup by id.
CHANNEL_TABS = ("streams", "videos")
# Player client that resolves a downloadable format through the WARP egress.
PLAYER_CLIENT = "android"
# bestaudio if the client exposes audio-only, else format 18 (progressive
# 360p+audio), else anything; audio is extracted locally afterward.
AUDIO_FORMAT = "bestaudio/18/best"

BOARD_MEETING_RE = re.compile(r"board of education|BOE meeting", re.IGNORECASE)

DATE_PATTERNS = [
    re.compile(r"(\d{1,2})[/-](\d{1,2})[/-](\d{2,4})\b"),  # 2/19/26, 12/13/2023, 06-09-2026
    re.compile(
        r"(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*\.?\s+(\d{1,2}),?\s+(\d{4})",
        re.IGNORECASE,
    ),  # Nov. 13, 2019 or August 17, 2022
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


def _iso_if_plausible(year: int, month: int, day: int) -> str | None:
    """Format a plausible board-meeting date, or None (rejects mis-parsed titles)."""
    if 2000 <= year <= 2100 and 1 <= month <= 12 and 1 <= day <= 31:
        return f"{year}-{month:02d}-{day:02d}"
    return None


def parse_date_from_title(title: str) -> str | None:
    """Extract a meeting date (YYYY-MM-DD) from a video title, or None."""
    m = DATE_PATTERNS[0].search(title)
    if m:
        month, day, year = int(m.group(1)), int(m.group(2)), int(m.group(3))
        if year < 100:  # 2-digit year (e.g. 26 -> 2026); 4-digit years pass through
            year = 2000 + year if year < 50 else 1900 + year
        if iso := _iso_if_plausible(year, month, day):
            return iso
    m = DATE_PATTERNS[1].search(title)
    if m:
        iso = _iso_if_plausible(
            int(m.group(3)), MONTH_NAMES[m.group(1).lower()[:3]], int(m.group(2))
        )
        if iso:
            return iso
    m = DATE_PATTERNS[2].search(title)
    if m:
        iso = _iso_if_plausible(int(m.group(3)), int(m.group(1)), int(m.group(2)))
        if iso:
            return iso
    return None


def _proxy_args(proxy: str | None) -> list[str]:
    """yt-dlp args for the WARP egress: SOCKS proxy + the android player client."""
    args = ["--extractor-args", f"youtube:player_client={PLAYER_CLIENT}"]
    if proxy:
        args += ["--proxy", proxy]
    return args


def _list_channel_tab(tab_url: str, proxy: str | None) -> list[str]:
    """Return ``id|title`` lines for one channel tab via yt-dlp."""
    cmd = ["yt-dlp", "--flat-playlist", "--print", "%(id)s|%(title)s", *_proxy_args(proxy), tab_url]
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=180, check=True)
    except (subprocess.CalledProcessError, subprocess.TimeoutExpired) as exc:
        raise IngestError(f"yt-dlp channel listing failed for {tab_url}: {exc}") from exc
    return result.stdout.strip().splitlines()


def list_board_meetings(
    channel: str = CHANNEL,
    *,
    title_filter: re.Pattern[str] = BOARD_MEETING_RE,
    proxy: str | None = None,
) -> list[BoardMeeting]:
    """List a channel's meeting videos (newest first), with parsed dates.

    ``title_filter`` selects which videos count as this body's meetings — the city
    channel hosts several bodies (City Council, Plan Commission, committees), so the
    caller passes a body-specific pattern (see ``actalux.ingest.bodies``). Enumerates
    the /streams (livestreamed meetings) and /videos tabs and dedups by video id —
    board meetings are livestreams, so /videos alone misses most of them.

    Not every channel has every tab: a channel that never livestreams has no
    /streams tab, which yt-dlp reports as an error. A per-tab failure is skipped
    (logged) so one missing tab doesn't abort discovery; only a total failure
    (no tab could be listed at all) is fatal.
    """
    seen: dict[str, BoardMeeting] = {}
    listed_any = False
    for tab in CHANNEL_TABS:
        try:
            lines = _list_channel_tab(f"{channel}/{tab}", proxy)
        except IngestError as exc:
            logger.warning("skipping channel tab %s/%s: %s", channel, tab, exc)
            continue
        listed_any = True
        for line in lines:
            vid, _, title = line.partition("|")
            if not vid or vid in seen or not title_filter.search(title):
                continue
            seen[vid] = BoardMeeting(
                video_id=vid,
                title=title,
                meeting_date=parse_date_from_title(title) or "",
                url=f"https://www.youtube.com/watch?v={vid}",
            )
    if not listed_any:
        raise IngestError(f"no channel tab could be listed for {channel}")
    return sorted(seen.values(), key=lambda m: m.meeting_date, reverse=True)


def download_audio(
    video_id: str,
    dest_dir: Path,
    *,
    proxy: str | None = None,
    retries: int = 1,
    on_retry: Callable[[], None] | None = None,
) -> Path:
    """Download a video's audio to ``dest_dir`` as MP3; return the file path.

    Uses yt-dlp's native downloader (honors ``--proxy``) + local ffmpeg audio
    extraction. A single Cloudflare WARP session is one egress IP, and YouTube
    flags some WARP IPs (bot-check) while others are clean — so on failure we
    call ``on_retry`` (which rotates the WARP egress) and try again, up to
    ``retries`` attempts. Raises ``IngestError`` after the last attempt.
    """
    dest_dir.mkdir(parents=True, exist_ok=True)
    out_template = str(dest_dir / f"{video_id}.%(ext)s")
    url = f"https://www.youtube.com/watch?v={video_id}"
    cmd = ["yt-dlp", *_proxy_args(proxy), "-f", AUDIO_FORMAT,
           "-x", "--audio-format", "mp3", "--no-warnings", "-o", out_template, url]  # fmt: skip

    last_error = ""
    for attempt in range(1, max(1, retries) + 1):
        try:
            subprocess.run(cmd, capture_output=True, text=True, timeout=1800, check=True)
            mp3 = dest_dir / f"{video_id}.mp3"
            if mp3.exists():
                return mp3
            last_error = "no MP3 produced"
        except (subprocess.CalledProcessError, subprocess.TimeoutExpired) as exc:
            last_error = (getattr(exc, "stderr", "") or str(exc))[-300:]
        logger.warning("download attempt %d/%d failed for %s", attempt, retries, video_id)
        if attempt < retries and on_retry is not None:
            on_retry()  # rotate the WARP egress IP before retrying
    raise IngestError(f"yt-dlp audio download failed for {video_id}: {last_error}")


def load_discovery(path: Path) -> list[dict]:
    """Load a previously written discovery manifest (list of meeting dicts)."""
    return json.loads(path.read_text()) if path.exists() else []
