"""The measurement harness ``[C]``+``[V]`` — turn observations + labels into a go/no-go number.

Given a set of :class:`~actalux.diarization.linking.observations.VoiceObservation` and a parallel
list of ground-truth labels, this derives the same-meeting ``cannot_link`` constraint, sweeps a
scale-adaptive threshold grid for a scoring backend, and reports the operating point that maximizes
across-*meeting* pairwise F1 at a purity floor. Backend-agnostic (cosine vs AS-norm plug in as an
``(N, D) -> (N, N)`` callable) and label-agnostic (labels are opaque hashables). Pure — no DB, no
Modal. See docs/architecture/linking-prototype-phase1.md.
"""

from __future__ import annotations

from collections import Counter, defaultdict
from collections.abc import Hashable
from itertools import combinations

import numpy as np

from actalux.diarization.linking.cluster import constrained_complete_linkage
from actalux.diarization.linking.evaluate import (
    bcubed_prf,
    coverage,
    macro_recall_by_official,
    pairwise_prf,
    per_condition_pair_f1,
    purity,
)
from actalux.diarization.linking.observations import VoiceObservation

# Threshold-grid resolution: percentiles of the upper-triangle scores, so the grid adapts to each
# backend's own scale (cosine in [-1, 1] vs z-scored AS-norm) rather than a fixed magic range. Set
# fine (not ~30) because a coarse grid can skip the best operating point at a strict purity floor
# and understate a backend — observed 30 vs 80 swinging schools cosine F1@0.95 from 0.45 to 0.54.
DEFAULT_N_THRESHOLDS = 80


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
    bp, br, bf = bcubed_prf(pred, true_str)
    by_meeting = per_condition_pair_f1(pred, true_str, meeting_cond)
    by_acoustic = per_condition_pair_f1(pred, true_str, acoustic_cond)
    return {
        "n_nodes": float(len(set(pred))),
        "purity": purity(pred, true_str),
        "coverage": coverage(pred, true_str),
        "pair_precision": p,
        "pair_recall": r,
        "pair_f1": f1,
        # B-cubed + macro-per-official recall don't overweight prolific officials the way pairwise
        # counting does — the phase-2 reviewers flagged pairwise F1's quadratic bias.
        "bcubed_precision": bp,
        "bcubed_recall": br,
        "bcubed_f1": bf,
        "macro_official_recall": macro_recall_by_official(pred, true_str),
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


def best_at_floors(
    sweep: list[dict[str, float]], floors: list[float]
) -> dict[float, dict[str, float] | None]:
    """For each purity floor, the swept point maximizing across-meeting F1 that clears it.

    Reporting the whole frontier — not one floor — is what keeps a single strict floor from
    misreading as a failure: a backend can look degenerate at 0.99 yet be strong at 0.90.
    """
    out: dict[float, dict[str, float] | None] = {}
    for floor in floors:
        eligible = [p for p in sweep if p["purity"] >= floor]
        out[floor] = max(eligible, key=lambda p: p["across_meeting_f1"]) if eligible else None
    return out


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


def _false_enrollments(pred: list[int], true: list[Hashable | None]) -> int:
    """Labeled clusters in a node whose majority official differs from their own.

    A node enrolls under its dominant official; any minority-official cluster sharing that node
    would be enrolled as the wrong person — a false enrollment. Counts those over labeled items
    only; ties in the majority are broken by first appearance (arbitrary but consistent).
    """
    by_node: dict[int, list[Hashable]] = defaultdict(list)
    for i, t in enumerate(true):
        if t is not None:
            by_node[pred[i]].append(t)
    false = 0
    for members in by_node.values():
        majority = Counter(members).most_common(1)[0][0]
        false += sum(1 for m in members if m != majority)
    return false


def poison_blast_radius(
    scores: np.ndarray,
    cannot_link: set[frozenset[int]],
    true: list[Hashable | None],
    *,
    threshold: float,
    max_trials: int = 50,
) -> dict[str, float]:
    """Downstream harm of one wrong merge under the actual linkage + operating threshold.

    Complete-linkage is precision-biased: one spurious edge rarely fuses two whole identities
    (every cross pair must also clear the threshold). This quantifies that robustness — it forces
    one cross-official, cross-meeting pair to merge (a ``must_link``) at ``threshold`` and counts
    the extra false enrollments versus the un-poisoned assignment, over a deterministic sample of
    the first ``max_trials`` such pairs (index order). Reports mean and worst-case blast radius.
    """
    labeled = [i for i, t in enumerate(true) if t is not None]
    base_pred = constrained_complete_linkage(scores, threshold=threshold, cannot_link=cannot_link)
    base_false = _false_enrollments(base_pred, true)
    poisons: list[frozenset[int]] = []
    for a, b in combinations(labeled, 2):
        if true[a] == true[b] or frozenset((a, b)) in cannot_link:
            continue  # need a cross-official pair that cannot_link does not already forbid
        poisons.append(frozenset((a, b)))
        if len(poisons) >= max_trials:
            break
    if not poisons:
        return {"n_trials": 0.0, "mean_false_enrollments": 0.0, "max_false_enrollments": 0.0}
    deltas = [
        _false_enrollments(
            constrained_complete_linkage(
                scores, threshold=threshold, cannot_link=cannot_link, must_link={pair}
            ),
            true,
        )
        - base_false
        for pair in poisons
    ]
    return {
        "n_trials": float(len(deltas)),
        "mean_false_enrollments": float(np.mean(deltas)),
        "max_false_enrollments": float(max(deltas)),
    }


def loo_threshold_ci(
    scores: np.ndarray,
    cannot_link: set[frozenset[int]],
    true: list[Hashable | None],
    meeting_cond: list[str],
    acoustic_cond: list[str],
    *,
    purity_floor: float,
    n_thresholds: int = DEFAULT_N_THRESHOLDS,
) -> dict[str, float | list[float]]:
    """Leave-one-official-out operating threshold with a spread band.

    The operating point is a point estimate on ~21 officials; retuning it on the same anchors that
    define ground truth would overfit. This holds out each official in turn (drops their clusters
    from the labeled benchmark), re-selects the across-meeting-F1-maximizing threshold at
    ``purity_floor`` on the rest, and summarizes the held-out thresholds (mean + a 2.5/97.5
    percentile band). A fold that clears no point contributes nothing.
    """
    officials = sorted({t for t in true if t is not None}, key=str)
    thresholds: list[float] = []
    for held in officials:
        reduced: list[Hashable | None] = [None if t == held else t for t in true]
        best, _ = sweep_backend(
            scores,
            cannot_link,
            reduced,
            meeting_cond,
            acoustic_cond,
            purity_floor=purity_floor,
            n_thresholds=n_thresholds,
        )
        if best is not None:
            thresholds.append(float(best["threshold"]))
    if not thresholds:
        return {
            "n_folds": 0.0,
            "mean_threshold": 0.0,
            "ci95_lo": 0.0,
            "ci95_hi": 0.0,
            "thresholds": [],
        }
    arr = np.asarray(thresholds)
    return {
        "n_folds": float(len(thresholds)),
        "mean_threshold": float(np.mean(arr)),
        "ci95_lo": float(np.percentile(arr, 2.5)),
        "ci95_hi": float(np.percentile(arr, 97.5)),
        "thresholds": thresholds,
    }


def cannot_link_audit(
    obs: list[VoiceObservation], scores: np.ndarray, *, threshold: float
) -> list[dict[str, object]]:
    """Same-meeting cluster pairs scoring above ``threshold`` — suspected diarization fragmentation.

    ``cannot_link_same_meeting`` assumes two clusters in one recording are different speakers. That
    breaks if diarization over-segmented a single speaker: the two fragments then look alike. A
    same-meeting pair scoring at or above the operating ``threshold`` is exactly a would-be merge
    the constraint is blocking, so it is the pair to audit by ear. Returns flagged pairs, highest
    score first; empty when none are suspicious.
    """
    by_doc: dict[int, list[int]] = defaultdict(list)
    for i, o in enumerate(obs):
        by_doc[o.document_id].append(i)
    flagged: list[tuple[float, dict[str, object]]] = []
    for idxs in by_doc.values():
        for a, b in combinations(idxs, 2):
            score = float(scores[a, b])
            if score >= threshold:
                flagged.append(
                    (
                        score,
                        {
                            "document_id": obs[a].document_id,
                            "cluster_a": obs[a].cluster_label,
                            "cluster_b": obs[b].cluster_label,
                            "score": score,
                        },
                    )
                )
    flagged.sort(key=lambda r: r[0], reverse=True)  # sort on the float score, highest first
    return [record for _, record in flagged]
