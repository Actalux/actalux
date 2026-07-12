"""The measurement harness ``[C]``+``[V]`` — turn observations + labels into a go/no-go number.

Given a set of :class:`~actalux.diarization.linking.observations.VoiceObservation` and a parallel
list of ground-truth labels, this derives the same-meeting ``cannot_link`` constraint, sweeps a
scale-adaptive threshold grid for a scoring backend, and reports the operating point that maximizes
across-*meeting* pairwise F1 at a purity floor. Backend-agnostic (cosine vs AS-norm plug in as an
``(N, D) -> (N, N)`` callable) and label-agnostic (labels are opaque hashables). Pure — no DB, no
Modal. See docs/architecture/linking-prototype-phase1.md.
"""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Hashable
from itertools import combinations

import numpy as np

from actalux.diarization.linking.cluster import constrained_complete_linkage
from actalux.diarization.linking.evaluate import (
    coverage,
    pairwise_prf,
    per_condition_pair_f1,
    purity,
)
from actalux.diarization.linking.observations import VoiceObservation

# Threshold-grid resolution: percentiles of the upper-triangle scores, so the grid adapts to each
# backend's own scale (cosine in [-1, 1] vs z-scored AS-norm) rather than a fixed magic range.
DEFAULT_N_THRESHOLDS = 30


def cannot_link_same_meeting(obs: list[VoiceObservation]) -> set[frozenset[int]]:
    """Every pair of clusters sharing a ``document_id`` is a cannot-link (different people).

    This is the one structural constraint that leaks no cross-meeting identity label: two voices
    in the same recording are, by diarization, distinct speakers.
    """
    by_doc: dict[int, list[int]] = defaultdict(list)
    for i, o in enumerate(obs):
        by_doc[o.document_id].append(i)
    forbidden: set[frozenset[int]] = set()
    for idxs in by_doc.values():
        forbidden.update(frozenset(pair) for pair in combinations(idxs, 2))
    return forbidden


def candidate_thresholds(
    scores: np.ndarray, *, n_thresholds: int = DEFAULT_N_THRESHOLDS
) -> list[float]:
    """Distinct percentile thresholds over the upper-triangle off-diagonal scores (adaptive)."""
    n = int(scores.shape[0])
    if n < 2:
        return []
    upper = scores[np.triu_indices(n, k=1)]
    if upper.size == 0:
        return []
    return sorted({float(v) for v in np.percentile(upper, np.linspace(0, 100, n_thresholds))})


def evaluate_point(
    pred: list[int],
    true: list[Hashable | None],
    meeting_cond: list[str],
    acoustic_cond: list[str],
) -> dict[str, float]:
    """All metrics for one node assignment.

    ``true`` labels are opaque (person ids) or ``None`` (not in the benchmark); they are stringified
    internally so the metric helpers, which key on equality, treat them consistently.
    """
    true_str: list[str | None] = [None if t is None else str(t) for t in true]
    p, r, f1 = pairwise_prf(pred, true_str)
    by_meeting = per_condition_pair_f1(pred, true_str, meeting_cond)
    by_acoustic = per_condition_pair_f1(pred, true_str, acoustic_cond)
    return {
        "n_nodes": float(len(set(pred))),
        "purity": purity(pred, true_str),
        "coverage": coverage(pred, true_str),
        "pair_precision": p,
        "pair_recall": r,
        "pair_f1": f1,
        "across_meeting_f1": by_meeting["across"],
        "within_meeting_f1": by_meeting["within"],
        "across_condition_f1": by_acoustic["across"],
    }


def sweep_backend(
    scores: np.ndarray,
    cannot_link: set[frozenset[int]],
    true: list[Hashable | None],
    meeting_cond: list[str],
    acoustic_cond: list[str],
    *,
    purity_floor: float,
    must_link: set[frozenset[int]] | None = None,
    n_thresholds: int = DEFAULT_N_THRESHOLDS,
) -> tuple[dict[str, float] | None, list[dict[str, float]]]:
    """Sweep thresholds for one backend's score matrix.

    Returns ``(best, sweep)`` where ``best`` is the threshold maximizing across-meeting F1 among
    points whose purity clears ``purity_floor`` (``None`` if none clear it — an honest miss, never a
    silent fallback to a low-purity point), and ``sweep`` is every evaluated point.
    """
    sweep: list[dict[str, float]] = []
    for thr in candidate_thresholds(scores, n_thresholds=n_thresholds):
        pred = constrained_complete_linkage(
            scores, threshold=thr, cannot_link=cannot_link, must_link=must_link
        )
        sweep.append({"threshold": thr, **evaluate_point(pred, true, meeting_cond, acoustic_cond)})
    eligible = [p for p in sweep if p["purity"] >= purity_floor]
    best = max(eligible, key=lambda p: p["across_meeting_f1"]) if eligible else None
    return best, sweep


def label_stats(true: list[Hashable | None], acoustic_cond: list[str]) -> dict[str, int]:
    """Benchmark shape: labeled clusters, distinct officials, recurring + cross-condition counts."""
    conds: dict[Hashable, set[str]] = defaultdict(set)
    counts: dict[Hashable, int] = defaultdict(int)
    for t, cond in zip(true, acoustic_cond, strict=True):
        if t is None:
            continue
        counts[t] += 1
        conds[t].add(cond)
    return {
        "clusters": sum(counts.values()),
        "officials": len(counts),
        "recurring_officials": sum(1 for c in counts.values() if c >= 2),
        "cross_condition_officials": sum(1 for cs in conds.values() if len(cs) >= 2),
    }
