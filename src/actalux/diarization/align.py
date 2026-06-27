"""Align diarization speaker turns to existing transcript chunks by time overlap.

This is the overlay step that keeps verbatim integrity: the canonical Whisper
transcript text is never changed — diarization only attaches a speaker label to
each chunk, chosen as the cluster whose turns overlap the chunk's [start, end]
window the most. A chunk with no overlapping turn is left unattributed rather
than guessed.
"""

from __future__ import annotations

from actalux.diarization.backend import SpeakerTimeline


def assign_clusters(
    timeline: SpeakerTimeline, chunks: list[tuple[int, float, float]]
) -> dict[int, str]:
    """Map ``chunk_id -> cluster_label`` by maximum temporal overlap.

    ``chunks`` is ``(chunk_id, start_s, end_s)``. A chunk that overlaps no turn is
    omitted from the result (unattributed, not defaulted to a speaker).
    """
    assigned: dict[int, str] = {}
    for chunk_id, start_s, end_s in chunks:
        best_label: str | None = None
        best_overlap = 0.0
        for turn in timeline.turns:
            overlap = min(end_s, turn.end_s) - max(start_s, turn.start_s)
            if overlap > best_overlap:
                best_overlap = overlap
                best_label = turn.cluster_label
        if best_label is not None:
            assigned[chunk_id] = best_label
    return assigned


def chunk_windows(chunks: list[dict[str, object]]) -> list[tuple[int, float, float]]:
    """Derive ``(id, start_s, end_s)`` windows from chunks ordered by chunk_index.

    Each chunk carries ``start_seconds``; its end is the next chunk's start (the
    last chunk runs to +inf so trailing turns still attribute). Chunks without a
    ``start_seconds`` are dropped — they can't be placed on the timeline.
    """
    timed = [
        (int(c["id"]), float(c["start_seconds"]))  # type: ignore[arg-type]
        for c in chunks
        if c.get("start_seconds") is not None
    ]
    timed.sort(key=lambda t: t[1])
    windows: list[tuple[int, float, float]] = []
    for i, (chunk_id, start_s) in enumerate(timed):
        end_s = timed[i + 1][1] if i + 1 < len(timed) else float("inf")
        windows.append((chunk_id, start_s, end_s))
    return windows
