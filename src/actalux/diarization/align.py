"""Align diarization speaker turns to transcript content by time overlap.

Verbatim integrity is preserved throughout: transcript text is never changed —
alignment only attaches anonymous speaker labels by time.

Two granularities:

* ``assign_clusters`` / ``chunk_windows`` — coarse chunk-level overlay (one label
  per ~200-word chunk). Kept for the A/B eval; too coarse for production (a chunk
  spans many speakers), so the going-forward path uses the word level below.
* ``attribute_words`` — the production attribution layer: assign each WhisperX word
  to its max-overlap pyannote turn, then merge consecutive same-cluster words into
  word-level speaker turns (``diarization_turns`` rows, for reader labels + clip
  cutting). Words overlapping no turn fall back to the temporally nearest turn so no
  verbatim word is dropped and no turn is left without a (NOT NULL) cluster label.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from actalux.diarization.backend import SpeakerTimeline, SpeakerTurn
from actalux.transcription.backend import Word


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


@dataclass(frozen=True)
class AttributedTurn:
    """A merged run of consecutive words sharing one anonymous speaker cluster."""

    cluster_label: str
    start_s: float
    end_s: float
    words: list[Word]

    @property
    def text(self) -> str:
        """A readable join of the turn's words (display convenience).

        The verbatim record is ``words`` (and ``documents.raw_content``); this trims
        and single-spaces for display, so it is not the byte-exact source of truth.
        """
        return " ".join(w.text.strip() for w in self.words if w.text.strip()).strip()

    def to_row(self, document_id: int, source_model: str) -> dict[str, Any]:
        """Row for the ``diarization_turns`` table (words as the JSONB wire shape)."""
        return {
            "document_id": document_id,
            "cluster_label": self.cluster_label,
            "start_seconds": self.start_s,
            "end_seconds": self.end_s,
            "words": [{"word": w.text, "start": w.start_s, "end": w.end_s} for w in self.words],
            "source_model": source_model,
        }


def _interval_gap(word: Word, turn: SpeakerTurn) -> float:
    """Time gap between a word's span and a turn's span (0.0 if they touch/overlap).

    Uses the full word interval (not just its start) so a word sitting in a gap goes to
    whichever turn is closest by its nearest edge.
    """
    if word.end_s < turn.start_s:
        return turn.start_s - word.end_s
    if word.start_s > turn.end_s:
        return word.start_s - turn.end_s
    return 0.0


def _word_cluster(word: Word, turns: list[SpeakerTurn]) -> str | None:
    """The cluster for one word: max temporal overlap, else the nearest turn in time.

    Overlap wins. A word that overlaps no turn (a brief diarization gap) attaches to
    the temporally closest turn — never dropped, never left without a cluster — so the
    verbatim text stays intact. Returns ``None`` only when there are no turns at all.
    Ties (equal overlap or equal gap) break on turn start/end/label so the result is
    deterministic regardless of the turn list's order.
    """
    best_label: str | None = None
    best_overlap = 0.0
    for turn in turns:
        overlap = min(word.end_s, turn.end_s) - max(word.start_s, turn.start_s)
        if overlap > best_overlap:
            best_overlap = overlap
            best_label = turn.cluster_label
    if best_label is not None:
        return best_label
    if not turns:
        return None
    nearest = min(
        turns,
        key=lambda t: (_interval_gap(word, t), t.start_s, t.end_s, t.cluster_label),
    )
    return nearest.cluster_label


def attribute_words(words: list[Word], timeline: SpeakerTimeline) -> list[AttributedTurn]:
    """Assign words to pyannote turns, then merge same-cluster runs into speaker turns.

    Words are processed in time order; each is labeled by :func:`_word_cluster`, and
    consecutive words with the same label collapse into one ``AttributedTurn`` (the
    verbatim re-segmentation — text is partitioned, never edited). With no words or no
    turns, returns ``[]`` (nothing to attribute).
    """
    if not words or not timeline.turns:
        return []
    ordered = sorted(words, key=lambda w: (w.start_s, w.end_s))
    runs: list[tuple[str, list[Word]]] = []
    for word in ordered:
        label = _word_cluster(word, timeline.turns)
        if label is None:
            continue
        if runs and runs[-1][0] == label:
            runs[-1][1].append(word)
        else:
            runs.append((label, [word]))
    return [
        AttributedTurn(label, group[0].start_s, group[-1].end_s, group) for label, group in runs
    ]
