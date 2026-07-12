"""Voice observations — the cached, per-cluster centroid records the linker operates on.

A ``VoiceObservation`` is one diarization cluster from one recording, reduced to its pooled
centroid embedding plus the metadata the scorer and evaluator need (how much speech backs it,
which acoustic condition it was recorded under, when the meeting was). These are the cache
artifact produced by the heavy Modal embed step ``[E]`` and consumed by the pure
scoring/clustering/evaluation stages ``[S]``/``[C]``/``[V]`` — so the expensive embedding is
paid once and the linking parameters can be swept freely offline.

A ``VoiceNode`` is a set of observations the linker has judged to be one physical voice.

The cache is a single ``.npz`` holding a stacked ``float32`` embedding matrix plus a JSON blob
of the parallel metadata, so it round-trips without pickling. Pure numpy — no ``modal``, no
GPU, no DB — see docs/architecture/linking-prototype-phase1.md.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path

import numpy as np


@dataclass(frozen=True)
class VoiceObservation:
    """One diarization cluster reduced to a pooled centroid plus its provenance.

    Attributes
    ----------
    document_id
        The recording (meeting document) this cluster came from.
    cluster_label
        The per-recording diarization label, e.g. ``"SPEAKER_03"`` — only unique within
        ``document_id``.
    embedding
        The pooled cluster centroid, shape ``(D,)``.
    speech_seconds
        Total speech behind the cluster (the enrollment weight / reliability proxy).
    acoustic_condition
        Recording condition: one of ``"zoom_gallery"``, ``"zoom_share"``, ``"in_person"``,
        ``"phone"``, ``"unknown"``. The decisive axis for cross-condition drift.
    meeting_date
        ISO date of the meeting, or ``None`` when unknown.
    """

    document_id: int
    cluster_label: str
    embedding: np.ndarray
    speech_seconds: float
    acoustic_condition: str
    meeting_date: str | None


@dataclass
class VoiceNode:
    """A set of observations the linker has judged to be one physical voice."""

    node_id: int
    observations: list[VoiceObservation]

    def member_keys(self) -> list[tuple[int, str]]:
        """Return the ``(document_id, cluster_label)`` key of each member observation."""
        return [(o.document_id, o.cluster_label) for o in self.observations]


def embedding_matrix(obs: list[VoiceObservation]) -> np.ndarray:
    """Stack the observations' embeddings into an ``(N, D)`` matrix in list order.

    Returns an empty ``(0, 0)`` ``float32`` array for an empty input.
    """
    if not obs:
        return np.zeros((0, 0), dtype=np.float32)
    return np.stack([np.asarray(o.embedding) for o in obs])


def _observation_meta(obs: list[VoiceObservation]) -> list[dict[str, object]]:
    """Serialize the non-embedding fields of each observation to JSON-safe dicts."""
    return [
        {
            "document_id": int(o.document_id),
            "cluster_label": o.cluster_label,
            "speech_seconds": float(o.speech_seconds),
            "acoustic_condition": o.acoustic_condition,
            "meeting_date": o.meeting_date,
        }
        for o in obs
    ]


def save_observations(obs: list[VoiceObservation], path: Path) -> None:
    """Persist observations to a single ``.npz`` (stacked ``float32`` embeddings + JSON meta).

    Parameters
    ----------
    obs
        The observations to cache; their order is preserved.
    path
        Destination file. Written via a file handle so the exact path is used (numpy's
        ``.npz`` auto-suffixing is bypassed) and load round-trips it.
    """
    embeddings = embedding_matrix(obs).astype(np.float32)
    meta = json.dumps(_observation_meta(obs))
    with Path(path).open("wb") as fh:
        np.savez(fh, embeddings=embeddings, meta=np.array(meta))


def load_observations(path: Path) -> list[VoiceObservation]:
    """Load observations previously written by :func:`save_observations`.

    The embeddings come back as standalone ``float32`` arrays (copied out of the archive), so
    the returned objects are valid after the underlying file is closed.
    """
    with np.load(Path(path)) as data:
        embeddings = np.asarray(data["embeddings"])
        meta = json.loads(str(data["meta"].item()))
    return [
        VoiceObservation(
            document_id=int(m["document_id"]),
            cluster_label=str(m["cluster_label"]),
            embedding=np.ascontiguousarray(embeddings[i]),
            speech_seconds=float(m["speech_seconds"]),
            acoustic_condition=str(m["acoustic_condition"]),
            meeting_date=m["meeting_date"],
        )
        for i, m in enumerate(meta)
    ]
