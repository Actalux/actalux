"""Provider-agnostic word-level transcription seam.

The rest of the system depends only on ``TranscriptionBackend`` + the domain types
here, never on a specific GPU provider — ``WhisperXRunner`` (``modal_whisperx``) is
the first adapter, and a local/Replicate/RunPod runner could implement the same port.

Word-level timestamps are the reason this exists (segment-level Whisper, e.g. the
hosted Groq path in ``ingest.transcribe``, can't cut clips precisely or align to
speaker turns word-by-word).
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Protocol


@dataclass(frozen=True)
class Word:
    """One transcribed word with its time span (seconds from the start of the audio)."""

    text: str
    start_s: float
    end_s: float


@dataclass(frozen=True)
class TranscriptSegment:
    """A transcript segment: prose plus the words behind it (forced-aligned)."""

    text: str
    start_s: float
    end_s: float
    words: list[Word]


@dataclass(frozen=True)
class WordTranscript:
    """A full transcript: segments (each carrying its words) plus provenance."""

    segments: list[TranscriptSegment]
    language: str
    source_model: str  # e.g. "whisperx/large-v3"

    @classmethod
    def from_payload(cls, payload: dict[str, Any], source_model: str) -> WordTranscript:
        """Build from the wire format a remote backend returns.

        Shape: ``{"language": str, "segments": [{"start","end","text","words":
        [{"word","start","end"}, ...]}, ...]}``. This is the single place that shape
        is interpreted, so the rest of the code only sees typed objects.
        """
        segments = [
            TranscriptSegment(
                text=(s.get("text") or "").strip(),
                start_s=float(s.get("start", 0.0)),
                end_s=float(s.get("end", 0.0)),
                words=[
                    Word(str(w.get("word", "")), float(w["start"]), float(w["end"]))
                    for w in s.get("words", [])
                    if w.get("start") is not None and w.get("end") is not None
                ],
            )
            for s in payload.get("segments", [])
        ]
        return cls(
            segments=segments, language=payload.get("language", ""), source_model=source_model
        )

    @property
    def text(self) -> str:
        """The full transcript prose (segment texts joined)."""
        return " ".join(s.text for s in self.segments if s.text).strip()

    def all_words(self) -> list[Word]:
        """Every word across all segments, in order — the unit aligned to speaker turns."""
        return [w for s in self.segments for w in s.words]


class TranscriptionBackend(Protocol):
    """Transcribes an audio source into a word-level ``WordTranscript``.

    Implementations decide where compute happens (a serverless GPU, a local process,
    ...). ``audio_uri`` is whatever the backend understands — a local path for an
    in-process runner, an uploaded handle for a remote one.
    """

    def transcribe(self, audio_uri: str) -> WordTranscript: ...
