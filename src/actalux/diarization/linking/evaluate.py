"""Linking evaluation ``[V]`` — how well a node assignment recovers the ground-truth voices.

The benchmark is a *subset* of all observations (only clusters carrying a very-high-confidence
anchor get a ground-truth roster slug), so every metric here is computed over the labeled
items only — ``true[i] is None`` means "not in the benchmark" and is skipped. Metrics:

- ``purity`` — do predicted nodes mix people? (majority-true mass per predicted cluster)
- ``coverage`` — inverse purity: is one person's clusters gathered into one node?
- ``pairwise_prf`` — precision/recall/F1 over same-node pairs.
- ``per_condition_pair_f1`` — the decisive drift view: pairwise F1 *within* one acoustic
  condition vs *across* conditions. The across number is where raw cosine fails and calibrated
  scoring must hold.

Pure Python. See docs/architecture/linking-prototype-phase1.md.
"""

from __future__ import annotations

from collections import Counter, defaultdict
from itertools import combinations


def _labeled_indices(true: list[str | None]) -> list[int]:
    """Indices whose ground-truth label is present (in the benchmark)."""
    return [i for i, label in enumerate(true) if label is not None]


def purity(pred: list[int], true: list[str | None]) -> float:
    """Cluster purity over labeled items: mass of the majority true class per predicted node.

    Returns 0.0 when no items are labeled.
    """
    labeled = _labeled_indices(true)
    if not labeled:
        return 0.0
    by_pred: dict[int, list[str | None]] = defaultdict(list)
    for i in labeled:
        by_pred[pred[i]].append(true[i])
    dominant = sum(max(Counter(labels).values()) for labels in by_pred.values())
    return dominant / len(labeled)


def coverage(pred: list[int], true: list[str | None]) -> float:
    """Inverse purity over labeled items: max overlap of each true class with one predicted node.

    Returns 0.0 when no items are labeled.
    """
    labeled = _labeled_indices(true)
    if not labeled:
        return 0.0
    by_true: dict[str | None, list[int]] = defaultdict(list)
    for i in labeled:
        by_true[true[i]].append(pred[i])
    gathered = sum(max(Counter(preds).values()) for preds in by_true.values())
    return gathered / len(labeled)


def _pair_counts(
    pairs: list[tuple[int, int]],
    pred: list[int],
    true: list[str | None],
) -> tuple[int, int, int]:
    """(TP, FP, FN) over the given index pairs: same-pred/same-true agreement counting."""
    tp = fp = fn = 0
    for a, b in pairs:
        same_pred = pred[a] == pred[b]
        same_true = true[a] == true[b]
        if same_pred and same_true:
            tp += 1
        elif same_pred:
            fp += 1
        elif same_true:
            fn += 1
    return tp, fp, fn


def _prf_from_counts(tp: int, fp: int, fn: int) -> tuple[float, float, float]:
    """Precision, recall, F1 from pair counts; a zero denominator yields 0.0."""
    precision = tp / (tp + fp) if (tp + fp) else 0.0
    recall = tp / (tp + fn) if (tp + fn) else 0.0
    f1 = 2 * precision * recall / (precision + recall) if (precision + recall) else 0.0
    return precision, recall, f1


def pairwise_prf(pred: list[int], true: list[str | None]) -> tuple[float, float, float]:
    """Pairwise precision/recall/F1 over all unordered pairs of labeled items.

    TP = same predicted node & same true class; FP = same node & different class;
    FN = different node & same class. Zero-denominator metrics return 0.0.
    """
    labeled = _labeled_indices(true)
    pairs = list(combinations(labeled, 2))
    return _prf_from_counts(*_pair_counts(pairs, pred, true))


def per_condition_pair_f1(
    pred: list[int],
    true: list[str | None],
    conditions: list[str],
) -> dict[str, float]:
    """Pairwise F1 split into within-condition and across-condition labeled pairs.

    Parameters
    ----------
    pred
        Predicted node id per index.
    true
        Ground-truth label per index (``None`` = not in the benchmark).
    conditions
        Acoustic condition per index, parallel to ``pred``/``true``.

    Returns
    -------
    dict[str, float]
        ``{"within": f1, "across": f1}``. The ``"across"`` value is the decisive
        cross-condition drift metric.
    """
    labeled = _labeled_indices(true)
    within, across = [], []
    for a, b in combinations(labeled, 2):
        (within if conditions[a] == conditions[b] else across).append((a, b))
    return {
        "within": _prf_from_counts(*_pair_counts(within, pred, true))[2],
        "across": _prf_from_counts(*_pair_counts(across, pred, true))[2],
    }


def bcubed_prf(pred: list[int], true: list[str | None]) -> tuple[float, float, float]:
    """B-cubed precision/recall/F1 over labeled items (Amigó et al. 2009).

    Per item, precision is the fraction of its predicted node sharing its true class and recall is
    the fraction of its true class gathered into its predicted node; the metric averages those
    per-item ratios. Unlike :func:`pairwise_prf`, it does not weight prolific officials
    quadratically — each cluster contributes once. Every item counts itself, so both denominators
    are >= 1. Returns ``(0.0, 0.0, 0.0)`` when no items are labeled.
    """
    labeled = _labeled_indices(true)
    if not labeled:
        return 0.0, 0.0, 0.0
    pred_sizes = Counter(pred[i] for i in labeled)
    true_sizes = Counter(true[i] for i in labeled)
    both = Counter((pred[i], true[i]) for i in labeled)  # items sharing node AND true class
    precision = sum(both[(pred[i], true[i])] / pred_sizes[pred[i]] for i in labeled)
    recall = sum(both[(pred[i], true[i])] / true_sizes[true[i]] for i in labeled)
    n = len(labeled)
    precision /= n
    recall /= n
    f1 = 2 * precision * recall / (precision + recall) if (precision + recall) else 0.0
    return precision, recall, f1


def macro_recall_by_official(pred: list[int], true: list[str | None]) -> float:
    """Mean per-official pairwise recall, weighting every official equally (macro).

    Pairwise recall is dominated by prolific officials (their same-official pair count grows
    quadratically); this averages each official's own recall so a chair with 20 meetings counts the
    same as an official with two. An official with a single labeled cluster has no same-official
    pair and is excluded. Returns 0.0 when no official has >= 2 labeled clusters.
    """
    labeled = _labeled_indices(true)
    by_true: dict[str | None, list[int]] = defaultdict(list)
    for i in labeled:
        by_true[true[i]].append(i)
    recalls: list[float] = []
    for members in by_true.values():
        n = len(members)
        if n < 2:
            continue
        total_pairs = n * (n - 1) // 2
        node_counts = Counter(pred[i] for i in members)
        same_node = sum(k * (k - 1) // 2 for k in node_counts.values())
        recalls.append(same_node / total_pairs)
    return sum(recalls) / len(recalls) if recalls else 0.0
