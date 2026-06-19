"""Transcribe board-meeting audio with OpenAI hosted Whisper.

Replaces the YouTube auto-caption transcript source. Produces the transcript text
plus per-segment timestamps; the segments drive the reader's "cue the video to the
cited moment" deep-links (``chunks.start_seconds``), now aligned against the exact
text we ingest rather than a second, fuzzily-matched ASR pass.

A long meeting exceeds OpenAI's 25 MB per-file limit, so audio is re-encoded to
16 kHz mono MP3 (Whisper's native rate) and split into time windows comfortably
under the cap. Each window is transcribed independently and its segment times are
shifted back by the window offset, so the merged timeline is continuous.
"""

from __future__ import annotations

import logging
import subprocess
import tempfile
from dataclasses import dataclass
from pathlib import Path

from openai import OpenAI

from actalux.errors import TranscriptionError

logger = logging.getLogger(__name__)

DEFAULT_MODEL = "whisper-1"  # the model that returns segment timestamps (verbose_json)
# OpenAI caps a single upload at 25 MB. 20-min windows at 16 kHz mono / 64 kbps are
# ~10 MB — well under the cap — and short windows keep segment timestamps accurate.
WINDOW_SECONDS = 20 * 60
WHISPER_RATE = 16_000
WHISPER_BITRATE = "64k"
MAX_UPLOAD_BYTES = 24 * 1024 * 1024
# Whisper rejects clips under 0.1s; a sub-second trailing remainder (rounding at
# the end of a meeting) is folded into the prior window rather than sent alone.
MIN_WINDOW_SECONDS = 1.0


@dataclass(frozen=True)
class Segment:
    """One timestamped transcript segment (seconds from the start of the meeting)."""

    start: float
    end: float
    text: str


@dataclass(frozen=True)
class Transcript:
    """A full transcript: prose text plus the timed segments behind it."""

    text: str
    segments: list[Segment]


def _plan_windows(duration: float, window_seconds: int) -> list[tuple[float, float]]:
    """Split ``duration`` seconds into ``(offset, length)`` windows of ``window_seconds``."""
    if duration <= 0:
        return [(0.0, 0.0)]
    windows: list[tuple[float, float]] = []
    offset = 0.0
    while offset < duration:
        windows.append((offset, min(float(window_seconds), duration - offset)))
        offset += window_seconds
    # Fold a sub-minimum trailing remainder into the previous window so we never
    # send Whisper a clip too short to transcribe (it 400s under 0.1s).
    if len(windows) >= 2 and windows[-1][1] < MIN_WINDOW_SECONDS:
        prev_offset, prev_len = windows[-2]
        windows[-2] = (prev_offset, prev_len + windows[-1][1])
        windows.pop()
    return windows


def _merge_windows(results: list[tuple[float, str, list[Segment]]]) -> Transcript:
    """Concatenate per-window (offset, text, segments) into one continuous transcript.

    Each window's segment times are window-relative; shifting them by the window
    offset stitches the windows into a single timeline.
    """
    texts: list[str] = []
    segments: list[Segment] = []
    for offset, text, segs in results:
        if text:
            texts.append(text)
        for s in segs:
            segments.append(Segment(s.start + offset, s.end + offset, s.text))
    return Transcript(text=" ".join(texts).strip(), segments=segments)


def _probe_duration(path: Path) -> float:
    """Return the audio duration in seconds via ffprobe."""
    try:
        out = subprocess.run(
            ["ffprobe", "-v", "error", "-show_entries", "format=duration",
             "-of", "default=nokey=1:noprint_wrappers=1", str(path)],
            check=True, capture_output=True, text=True,
        )  # fmt: skip
        return float(out.stdout.strip())
    except (subprocess.CalledProcessError, ValueError) as exc:
        raise TranscriptionError(f"ffprobe failed for {path.name}: {exc}") from exc


def _encode_window(src: Path, offset: float, length: float, dst: Path) -> None:
    """Re-encode one window of ``src`` to 16 kHz mono MP3 at ``dst`` via ffmpeg."""
    try:
        subprocess.run(
            ["ffmpeg", "-nostdin", "-loglevel", "error",
             "-ss", str(offset), "-t", str(length), "-i", str(src),
             "-ac", "1", "-ar", str(WHISPER_RATE), "-c:a", "libmp3lame",
             "-b:a", WHISPER_BITRATE, "-y", str(dst)],
            check=True, capture_output=True,
        )  # fmt: skip
    except subprocess.CalledProcessError as exc:
        stderr = exc.stderr.decode("utf-8", "replace") if exc.stderr else ""
        raise TranscriptionError(f"ffmpeg failed encoding window at {offset}s: {stderr}") from exc


def _transcribe_one(
    client: OpenAI, path: Path, model: str, language: str, prompt: str | None
) -> tuple[str, list[Segment]]:
    """Transcribe a single audio file, returning (text, window-relative segments)."""
    if path.stat().st_size > MAX_UPLOAD_BYTES:
        logger.warning("audio window %s exceeds the upload cap; Whisper may reject it", path.name)
    try:
        with path.open("rb") as fh:
            resp = client.audio.transcriptions.create(
                model=model,
                file=fh,
                response_format="verbose_json",
                timestamp_granularities=["segment"],
                language=language,
                prompt=prompt or "",  # empty prompt is a no-op; keeps args static for typing
            )
    except Exception as exc:  # OpenAI SDK raises many concrete types; one boundary here
        raise TranscriptionError(f"Whisper API failed for {path.name}: {exc}") from exc
    segments = [
        Segment(float(s.start), float(s.end), (s.text or "").strip()) for s in (resp.segments or [])
    ]
    return (resp.text or "").strip(), segments


def transcribe_audio(
    audio_path: Path,
    api_key: str,
    model: str = DEFAULT_MODEL,
    *,
    base_url: str | None = None,
    language: str = "en",
    prompt: str | None = None,
    window_seconds: int = WINDOW_SECONDS,
) -> Transcript:
    """Transcribe an audio file with OpenAI hosted Whisper, returning text + segments.

    The file is split into ``window_seconds`` windows (re-encoded to 16 kHz mono MP3
    so each stays under OpenAI's 25 MB cap), each transcribed independently, then
    merged onto one continuous timeline. ``prompt`` biases spelling of names/terms
    (e.g. board members, "Proposition O"). Raises ``TranscriptionError`` on any
    ffmpeg/ffprobe or API failure.
    """
    duration = _probe_duration(audio_path)
    windows = _plan_windows(duration, window_seconds)
    client = OpenAI(api_key=api_key, base_url=base_url)
    logger.info(
        "transcribing %s (%.0fs) in %d window(s) via %s",
        audio_path.name,
        duration,
        len(windows),
        model,
    )

    results: list[tuple[float, str, list[Segment]]] = []
    with tempfile.TemporaryDirectory() as tmp:
        for i, (offset, length) in enumerate(windows):
            window_path = Path(tmp) / f"window_{i:03d}.mp3"
            _encode_window(audio_path, offset, length, window_path)
            text, segments = _transcribe_one(client, window_path, model, language, prompt)
            results.append((offset, text, segments))
            logger.info(
                "  window %d/%d (%.0fs): %d segments", i + 1, len(windows), offset, len(segments)
            )

    return _merge_windows(results)
