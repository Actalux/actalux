"""Provider-agnostic diarization seam.

Diarization turns audio into anonymous speaker-turn time ranges. The rest of the
system depends only on ``DiarizationBackend`` + the domain types here, never on a
specific GPU provider — ``ModalRunner`` (``modal_runner``) is the first adapter,
and a local/Replicate/RunPod runner could implement the same port unchanged.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Protocol


@dataclass(frozen=True)
class SpeakerTurn:
    """A contiguous stretch of audio attributed to one anonymous cluster."""

    cluster_label: str  # e.g. "SPEAKER_00"
    start_s: float
    end_s: float


@dataclass(frozen=True)
class SpeakerTimeline:
    """The diarization of one meeting: ordered turns plus provenance."""

    turns: list[SpeakerTurn]
    num_speakers: int
    source_model: str  # e.g. "pyannote/speaker-diarization-3.1"

    @classmethod
    def from_segments(cls, segments: list[dict[str, Any]], source_model: str) -> SpeakerTimeline:
        """Build a timeline from raw ``[{speaker,start,end}, ...]`` segments.

        The wire format a remote backend returns is plain JSON dicts; this is the
        single place that shape is interpreted, so the rest of the code only sees
        typed turns.
        """
        turns = [
            SpeakerTurn(str(s["speaker"]), float(s["start"]), float(s["end"])) for s in segments
        ]
        num = len({t.cluster_label for t in turns})
        return cls(turns=turns, num_speakers=num, source_model=source_model)


class DiarizationBackend(Protocol):
    """Runs diarization on an audio source, returning a ``SpeakerTimeline``.

    Implementations decide where compute happens (a serverless GPU, a local
    process, ...). ``audio_uri`` is whatever the backend understands — a local
    path for an in-process runner, an uploaded handle for a remote one.
    """

    def run(self, audio_uri: str, *, hint_num_speakers: int | None = None) -> SpeakerTimeline: ...
