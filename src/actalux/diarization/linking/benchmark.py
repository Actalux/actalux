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

    This is an explicit UPPER BOUND, not an expectation: the real proposer refuses to name any node
    holding two anchored officials (``proposer.resolve_node_official``), so a contaminated node of
    anchored clusters yields no proposal at all. The bound is what would happen if that guard were
    removed — which is exactly the quantity a poisoning study should report.
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


def _stratified_poison_pairs(
    labeled: list[int],
    true: list[Hashable | None],
    cannot_link: set[frozenset[int]],
    max_trials: int,
) -> list[frozenset[int]]:
    """A deterministic, official-pair-stratified sample of cross-official poison candidates.

    Plain index-order sampling degenerates: ``combinations`` yields ``(0,1), (0,2), (0,3)…``, so the
    first ``max_trials`` candidates nearly all poison cluster 0 — the study then measures one
    official. Grouping by the unordered official pair and round-robining one candidate per group
    spreads the trials across the roster while staying fully deterministic (groups in sorted key
    order; candidates within a group in index order).
    """
    by_official_pair: dict[tuple[str, str], list[frozenset[int]]] = defaultdict(list)
    for a, b in combinations(labeled, 2):
        if true[a] == true[b] or frozenset((a, b)) in cannot_link:
            continue  # need a cross-official pair that cannot_link does not already forbid
        key = (str(true[a]), str(true[b]))
        by_official_pair[tuple(sorted(key))].append(frozenset((a, b)))
    groups = [by_official_pair[k] for k in sorted(by_official_pair)]
    out: list[frozenset[int]] = []
    depth = 0
    while len(out) < max_trials and any(depth < len(g) for g in groups):
        for group in groups:
            if depth < len(group):
                out.append(group[depth])
                if len(out) >= max_trials:
                    break
        depth += 1
    return out


def _node_holds_two_officials(pred: list[int], true: list[Hashable | None], node: int) -> bool:
    """Does this node hold >= 2 distinct labeled officials — the shape the proposer won't name?"""
    officials = {true[i] for i, p in enumerate(pred) if p == node and true[i] is not None}
    return len(officials) >= 2


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
    the extra false enrollments versus the un-poisoned assignment, over a stratified deterministic
    sample of up to ``max_trials`` such pairs (see :func:`_stratified_poison_pairs`).

    Reports the mean/worst-case blast radius (the :func:`_false_enrollments` upper bound) plus
    ``ambiguity_caught`` — the fraction of poisoned runs whose merged node holds two officials and
    would therefore be REFUSED by the proposer's ambiguity guard. Every sampled poison joins two
    anchored officials, so a healthy run reports 1.0 by construction: the value is an invariant
    check that the forced merge actually landed (and that the guard stands between the bound and
    reality), not a discovered quantity.
    """
    labeled = [i for i, t in enumerate(true) if t is not None]
    base_pred = constrained_complete_linkage(scores, threshold=threshold, cannot_link=cannot_link)
    base_false = _false_enrollments(base_pred, true)
    poisons = _stratified_poison_pairs(labeled, true, cannot_link, max_trials)
    if not poisons:
        return {
            "n_trials": 0.0,
            "mean_false_enrollments": 0.0,
            "max_false_enrollments": 0.0,
            "ambiguity_caught": 0.0,
        }
    deltas: list[int] = []
    caught = 0
    for pair in poisons:
        pred = constrained_complete_linkage(
            scores, threshold=threshold, cannot_link=cannot_link, must_link={pair}
        )
        deltas.append(_false_enrollments(pred, true) - base_false)
        if _node_holds_two_officials(pred, true, pred[next(iter(pair))]):
            caught += 1
    return {
        "n_trials": float(len(deltas)),
        "mean_false_enrollments": float(np.mean(deltas)),
        "max_false_enrollments": float(max(deltas)),
        "ambiguity_caught": caught / len(deltas),
    }


def _official_pair_recall(
    pred: list[int], true: list[Hashable | None], official: Hashable
) -> float | None:
    """One official's pair recall: their same-official pairs sharing a node. None if singleton."""
    members = [i for i, t in enumerate(true) if t == official]
    if len(members) < 2:
        return None  # a single cluster forms no same-official pair -> recall is undefined
    total = len(members) * (len(members) - 1) // 2
    counts = Counter(pred[i] for i in members)
    same = sum(k * (k - 1) // 2 for k in counts.values())
    return same / total


def _official_false_merge(pred: list[int], true: list[Hashable | None], official: Hashable) -> bool:
    """Is any of this official's clusters sitting in a node whose majority official differs?"""
    by_node: dict[int, list[Hashable]] = defaultdict(list)
    for i, t in enumerate(true):
        if t is not None:
            by_node[pred[i]].append(t)
    for i, t in enumerate(true):
        if t != official:
            continue
        majority = Counter(by_node[pred[i]]).most_common(1)[0][0]
        if majority != official:
            return True
    return False


def loo_operating_point(
    scores: np.ndarray,
    cannot_link: set[frozenset[int]],
    true: list[Hashable | None],
    meeting_cond: list[str],
    acoustic_cond: list[str],
    *,
    purity_floor: float,
    n_thresholds: int = DEFAULT_N_THRESHOLDS,
) -> dict[str, object]:
    """Leave-one-official-out operating point — how the threshold generalizes to an UNSEEN official.

    Selecting the operating threshold on the same anchors that define ground truth overfits, and a
    threshold's *stability* says nothing about its *performance*. So: hold each official out, choose
    the threshold on the remaining roster (max across-meeting F1 subject to ``purity_floor``), then
    score the held-out official at that threshold under the FULL labels — their pair recall, and
    whether they were falsely merged into another official's node. That is the honest estimate of
    what the rollout threshold buys on someone it never saw.

    Clustering depends only on ``(scores, threshold)`` — never on labels — so every fold reuses one
    precomputed linkage per threshold instead of re-clustering the identical grid per official.

    Returns the per-fold records plus a summary. ``threshold_spread_lo/hi`` is the observed min/max
    across folds — a SPREAD BAND, not a confidence interval: with ~21 folds there is no distribution
    to interval-estimate, and calling it a CI would overstate it. A fold clearing no point at the
    floor contributes nothing; a singleton official reports ``heldout_recall`` of ``None``.
    """
    officials = sorted({t for t in true if t is not None}, key=str)
    thresholds = candidate_thresholds(scores, n_thresholds=n_thresholds)
    preds = {
        thr: constrained_complete_linkage(scores, threshold=thr, cannot_link=cannot_link)
        for thr in thresholds
    }
    folds: list[dict[str, object]] = []
    for held in officials:
        # drop the held-out official from the labels the threshold is chosen on
        reduced: list[str | None] = [None if (t is None or t == held) else str(t) for t in true]
        best_thr: float | None = None
        best_f1 = -1.0
        for thr in thresholds:
            pred = preds[thr]
            if purity(pred, reduced) < purity_floor:
                continue
            f1 = per_condition_pair_f1(pred, reduced, meeting_cond)["across"]
            if f1 > best_f1:  # ties keep the first (lowest) threshold, matching sweep_backend
                best_thr, best_f1 = thr, f1
        if best_thr is None:
            continue  # this fold clears no point at the floor — an honest miss
        pred = preds[best_thr]
        folds.append(
            {
                "official": held,
                "threshold": best_thr,
                "heldout_recall": _official_pair_recall(pred, true, held),
                "false_merge": _official_false_merge(pred, true, held),
            }
        )
    recalls = [f["heldout_recall"] for f in folds if f["heldout_recall"] is not None]
    thr_vals = [float(f["threshold"]) for f in folds]  # type: ignore[arg-type]
    return {
        "folds": folds,
        "n_folds": float(len(folds)),
        "mean_heldout_recall": float(np.mean(recalls)) if recalls else 0.0,
        "n_false_merge_folds": float(sum(1 for f in folds if f["false_merge"])),
        "mean_threshold": float(np.mean(thr_vals)) if thr_vals else 0.0,
        "threshold_spread_lo": float(np.min(thr_vals)) if thr_vals else 0.0,
        "threshold_spread_hi": float(np.max(thr_vals)) if thr_vals else 0.0,
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


def proposer_outcome(pred: list[int], true: list[Hashable | None], index: int) -> str:
    """What the proposer would do for ONE labeled cluster played as the un-named one.

    The cluster's node-mates (other labeled members of its cluster) vote: exactly one official and
    it matches -> ``'correct'``; exactly one and it differs -> ``'wrong'`` (the proposer would
    propose the wrong name); two+ officials -> ``'ambiguous'`` (the ambiguity guard refuses);
    none -> ``'alone'`` (nothing to inherit).
    """
    node = pred[index]
    officials = {
        true[j] for j, n in enumerate(pred) if n == node and j != index and true[j] is not None
    }
    if not officials:
        return "alone"
    if len(officials) > 1:
        return "ambiguous"
    return "correct" if officials == {true[index]} else "wrong"


def proposer_outcomes(pred: list[int], true: list[Hashable | None]) -> dict[str, int]:
    """Per-cluster proposer simulation over one clustering: would each cluster be named right?

    Aggregates :func:`proposer_outcome` over every labeled cluster. This is the operational view of
    a threshold — pairwise F1 rewards recovering whole cliques, but a proposal only needs ONE
    correct anchored sibling, so the two metrics can rank thresholds differently.

    Unlabeled clusters are skipped (their outcome is unknowable), so on an all-cluster cache the
    counts cover only the anchored subset.
    """
    outcomes = {"correct": 0, "wrong": 0, "ambiguous": 0, "alone": 0}
    for i in range(len(pred)):
        if true[i] is None:
            continue
        outcomes[proposer_outcome(pred, true, i)] += 1
    return outcomes


def proposer_tradeoff(
    scores: np.ndarray,
    cannot_link: set[frozenset[int]],
    true: list[Hashable | None],
    *,
    n_thresholds: int = DEFAULT_N_THRESHOLDS,
) -> list[dict[str, float]]:
    """The correct-vs-wrong-IDs curve across the threshold grid (the operating-point evidence).

    One row per candidate threshold: ``threshold``, ``purity``, and the :func:`proposer_outcomes`
    counts. This is the table an operating-point decision is made from — loosening the threshold
    trades wrong names for correct ones until the ambiguity guard converts further merges into
    abstentions and the curve plateaus.
    """
    rows: list[dict[str, float]] = []
    for threshold in candidate_thresholds(scores, n_thresholds=n_thresholds):
        pred = constrained_complete_linkage(scores, threshold=threshold, cannot_link=cannot_link)
        row: dict[str, float] = {"threshold": float(threshold), "purity": purity(pred, true)}
        row.update({k: float(v) for k, v in proposer_outcomes(pred, true).items()})
        rows.append(row)
    return rows


def best_proposer_point(
    rows: list[dict[str, float]], *, purity_floor: float
) -> dict[str, float] | None:
    """The tradeoff row maximizing correct proposals at a purity floor.

    ``None`` when no threshold clears the floor. Ties keep the first (lowest) threshold, matching
    :func:`sweep_backend`'s convention.
    """
    best: dict[str, float] | None = None
    for row in rows:
        if row["purity"] < purity_floor:
            continue
        if best is None or row["correct"] > best["correct"]:
            best = row
    return best
