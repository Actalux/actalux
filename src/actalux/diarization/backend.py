"""Provider-agnostic diarization seam.

Diarization turns audio into anonymous speaker-turn time ranges. The rest of the
system depends only on ``DiarizationBackend`` + the domain types here, never on a
specific GPU provider — ``ModalRunner`` (``modal_runner``) is the first adapter,
and a local/Replicate/RunPod runner could implement the same port unchanged.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Protocol


@dataclass(frozen=True)
class SpeakerTurn:
    """A contiguous stretch of audio attributed to one anonymous cluster."""

    cluster_label: str  # e.g. "SPEAKER_00"
    start_s: float
    end_s: float


@dataclass(frozen=True)
class ClusterEmbedding:
    """One anonymous cluster's voiceprint: an L2-normalized voice vector + provenance.

    Extracted during the diarization pass (the embedding half of the pyannote
    pipeline) over that cluster's own speech, then L2-normalized so cosine
    similarity == dot product. Anonymous by itself — it names no one. It becomes a
    gallery sample only after a human confirms the cluster to a *publishable
    official* (enrollment, officials-only + DB-enforced). See
    docs/architecture/voiceprint-speaker-id-plan.md.
    """

    cluster_label: str
    vector: tuple[float, ...]  # L2-normalized 256-d (frozen by the Phase 0 spike)
    seconds: float  # speech behind the sample (quality signal)
    model: str  # embedding model id + version


@dataclass(frozen=True)
class SpeakerTimeline:
    """The diarization of one meeting: ordered turns plus provenance.

    ``embeddings`` carries an optional per-cluster voiceprint (keyed by
    ``cluster_label``). A backend that only diarizes leaves it empty; one that also
    extracts embeddings fills it. Nothing here decides *who* a cluster is.
    """

    turns: list[SpeakerTurn]
    num_speakers: int
    source_model: str  # e.g. "pyannote/speaker-diarization-3.1"
    embeddings: dict[str, ClusterEmbedding] = field(default_factory=dict)

    @classmethod
    def from_segments(cls, segments: list[dict[str, Any]], source_model: str) -> SpeakerTimeline:
        """Build a timeline from raw ``[{speaker,start,end}, ...]`` segments (no embeddings).

        The wire format a remote backend returns is plain JSON dicts; this is the
        single place that shape is interpreted, so the rest of the code only sees
        typed turns.
        """
        turns = [
            SpeakerTurn(str(s["speaker"]), float(s["start"]), float(s["end"])) for s in segments
        ]
        num = len({t.cluster_label for t in turns})
        return cls(turns=turns, num_speakers=num, source_model=source_model)

    @classmethod
    def from_remote(cls, payload: Any, source_model: str) -> SpeakerTimeline:
        """Build a timeline from a remote backend's payload, tolerating both shapes.

        A pre-embeddings backend returns a bare ``[{speaker,start,end}, ...]`` list;
        an embeddings-emitting one returns ``{"segments": [...], "embeddings":
        [{cluster_label,vector,seconds,model}, ...]}``. Accepting both means client
        code keeps working against a function that has not yet been redeployed.
        """
        if isinstance(payload, dict):
            segments = payload.get("segments", [])
            emb_rows = payload.get("embeddings", [])
        else:
            segments, emb_rows = payload, []
        turns = [
            SpeakerTurn(str(s["speaker"]), float(s["start"]), float(s["end"])) for s in segments
        ]
        num = len({t.cluster_label for t in turns})
        embeddings = {
            str(e["cluster_label"]): ClusterEmbedding(
                cluster_label=str(e["cluster_label"]),
                vector=tuple(float(x) for x in e["vector"]),
                seconds=float(e["seconds"]),
                model=str(e["model"]),
            )
            for e in emb_rows
        }
        return cls(turns=turns, num_speakers=num, source_model=source_model, embeddings=embeddings)


class DiarizationBackend(Protocol):
    """Runs diarization on an audio source, returning a ``SpeakerTimeline``.

    Implementations decide where compute happens (a serverless GPU, a local
    process, ...). ``audio_uri`` is whatever the backend understands — a local
    path for an in-process runner, an uploaded handle for a remote one.
    """

    def run(
        self,
        audio_uri: str,
        *,
        hint_num_speakers: int | None = None,
        return_embeddings: bool = False,
    ) -> SpeakerTimeline: ...
