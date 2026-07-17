"""Light condition-aware score calibration — optional headroom over AS-norm.

AS-norm already fixes most of the cross-condition (Zoom<->in-person) drift
(docs/architecture/linking-backend-decision-2026-07-12.md). A quality-aware logistic calibrator can
recover a little more by learning, from anchor-derived same/different pairs, how to combine the raw
cosine, the AS-norm score, a cross-condition indicator, and a duration (quality) feature into one
calibrated pair score — the QMF idea (Mandasari 2013; Thienpondt IDLab VoxSRC-20 2021), kept
deliberately LIGHT given weak in-domain ground truth.

Upstream-native note: wespeaker's calibration ships as a Kaldi/shell recipe, not a pip API, so a
small ridge-regularized IRLS logistic regression (pure numpy, no new dependency) is the faithful,
non-forking equivalent of that method. It returns an ``(N, N)`` score matrix behind the same
backend contract as :func:`actalux.diarization.linking.scoring.asnorm_matrix`, so the benchmark
harness scores it identically. This is MEASURE-GATED: adopt it only if it beats AS-norm on the
leave-one-official-out eval; otherwise it stays an unused, tested module.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from itertools import combinations

import numpy as np

from actalux.diarization.linking.benchmark import proposer_outcome
from actalux.diarization.linking.cluster import constrained_complete_linkage
from actalux.diarization.linking.scoring import asnorm_matrix, cosine_matrix

# Pair features. cross_condition lets the model learn a separate offset for Zoom<->in-person pairs;
# log_min_seconds down-weights pairs where one cluster has little speech (a noisy centroid) — logged
# because raw speech seconds have a heavy right tail (tens to thousands), and a linear term would
# let one marathon cluster dominate the standardization.
FEATURE_NAMES = ("cosine", "asnorm", "cross_condition", "log_min_seconds")

# Ridge strength (on standardized features; the bias is never regularized) and IRLS iteration count.
# Light regularization keeps the calibrator from separating perfectly on a handful of anchor pairs.
DEFAULT_L2 = 1.0
DEFAULT_IRLS_ITERS = 25


def _sigmoid(z: np.ndarray) -> np.ndarray:
    """Numerically stable logistic sigmoid."""
    out = np.empty_like(z, dtype=np.float64)
    pos = z >= 0
    out[pos] = 1.0 / (1.0 + np.exp(-z[pos]))
    ez = np.exp(z[~pos])
    out[~pos] = ez / (1.0 + ez)
    return out


def balanced_sample_weight(y: np.ndarray) -> np.ndarray:
    """Class-balanced weights so the rare same-official pairs are not drowned by the negatives.

    Different-official pairs grow ~quadratically with the roster while an official's same-official
    pairs grow only with their own meeting count, so the fit is dominated by negatives unless the
    classes are re-weighted. Uses the standard ``n / (2 * n_class)`` convention (total mass stays
    ``n``). A single-class target is returned unweighted — there is nothing to balance.
    """
    y = np.asarray(y, dtype=np.float64)
    n = y.size
    n_pos = float((y == 1.0).sum())
    n_neg = float(n - n_pos)
    if n_pos == 0.0 or n_neg == 0.0:
        return np.ones(n)
    return np.where(y == 1.0, n / (2.0 * n_pos), n / (2.0 * n_neg))


def _fit_logistic(
    design: np.ndarray,
    y: np.ndarray,
    *,
    l2: float,
    iters: int,
    sample_weight: np.ndarray | None = None,
) -> np.ndarray:
    """Ridge-regularized logistic regression by IRLS (Newton-Raphson).

    ``design`` includes the leading bias column of ones. The ridge term stabilizes the Hessian on
    separable or collinear data and is not applied to the bias. ``sample_weight`` (default: uniform)
    scales each observation's contribution to both the gradient and the Hessian.
    """
    n, d = design.shape
    s = np.ones(n) if sample_weight is None else np.asarray(sample_weight, dtype=np.float64)
    w = np.zeros(d)
    reg = l2 * np.eye(d)
    reg[0, 0] = 0.0  # never regularize the bias term
    for _ in range(iters):
        p = _sigmoid(design @ w)
        irls_w = s * p * (1.0 - p)
        grad = design.T @ (s * (y - p)) - reg @ w
        hess = design.T @ (design * irls_w[:, None]) + reg
        w = w + np.linalg.solve(hess, grad)
    return w


@dataclass(frozen=True)
class Calibrator:
    """A fitted pair-score calibrator: standardization stats + logistic weights (bias first)."""

    weights: np.ndarray  # (n_features + 1,); weights[0] is the bias
    mean: np.ndarray  # (n_features,) feature means used to standardize
    std: np.ndarray  # (n_features,) feature stds used to standardize
    feature_names: tuple[str, ...] = field(default=FEATURE_NAMES)

    def predict_logit(self, feats: np.ndarray) -> np.ndarray:
        """Calibrated pair scores (logits — monotonic with probability) for raw feature rows."""
        z = (np.asarray(feats, dtype=np.float64) - self.mean) / self.std
        design = np.column_stack([np.ones(z.shape[0]), z])
        return design @ self.weights

    def to_dict(self) -> dict[str, object]:
        """JSON-able form for freezing in ``linking_operating_points.calibrator`` (migrate_048).

        A calibrator refit at propose time on whatever anchors exist would drift as anchors accrue —
        the same transductive instability the frozen cohort exists to prevent — so the fitted
        weights are frozen alongside the threshold they were measured with.
        """
        return {
            "weights": [float(w) for w in self.weights],
            "mean": [float(m) for m in self.mean],
            "std": [float(s) for s in self.std],
            "feature_names": list(self.feature_names),
        }

    @classmethod
    def from_dict(cls, data: dict[str, object]) -> Calibrator:
        """Rebuild a frozen calibrator; refuses a feature layout this code no longer computes."""
        names = tuple(data["feature_names"])  # type: ignore[arg-type]
        if names != FEATURE_NAMES:
            raise ValueError(
                f"stored calibrator features {names!r} != current {FEATURE_NAMES!r} — "
                f"refit and re-freeze the operating point"
            )
        return cls(
            weights=np.asarray(data["weights"], dtype=np.float64),
            mean=np.asarray(data["mean"], dtype=np.float64),
            std=np.asarray(data["std"], dtype=np.float64),
            feature_names=names,
        )


def pair_features(
    embeddings: np.ndarray,
    cohort: np.ndarray,
    conditions: list[str],
    seconds: list[float],
) -> tuple[np.ndarray, list[tuple[int, int]]]:
    """Feature rows (parallel to :data:`FEATURE_NAMES`) for every ``i < j`` cluster pair."""
    cos = cosine_matrix(embeddings)
    asn = asnorm_matrix(embeddings, cohort)
    rows: list[list[float]] = []
    pairs: list[tuple[int, int]] = []
    for i, j in combinations(range(embeddings.shape[0]), 2):
        cross = 0.0 if conditions[i] == conditions[j] else 1.0
        rows.append([cos[i, j], asn[i, j], cross, float(np.log1p(min(seconds[i], seconds[j])))])
        pairs.append((i, j))
    return np.asarray(rows, dtype=np.float64), pairs


def labeled_pair_targets(
    true: list[object | None],
    pairs: list[tuple[int, int]],
    *,
    exclude: set[frozenset[int]] | None = None,
) -> tuple[list[int], np.ndarray]:
    """Same-official (1) / different-official (0) targets over labeled, fittable pairs.

    Returns the indices into ``pairs`` that are labeled on both ends plus the target vector, so the
    caller fits on ``feats[keep]``. Pairs touching an unlabeled cluster are dropped, never guessed.

    ``exclude`` (pass ``benchmark.cannot_link_same_meeting``) drops structurally-negative pairs from
    the FIT: two clusters in one recording are different speakers by construction, and the linker
    already forbids merging them — so they are free easy negatives that flatter the calibrator and
    drag its bias without ever being scored at inference. Prediction still fills the full matrix;
    linkage ignores those entries regardless.
    """
    keep: list[int] = []
    y: list[float] = []
    for k, (i, j) in enumerate(pairs):
        if true[i] is None or true[j] is None:
            continue
        if exclude is not None and frozenset((i, j)) in exclude:
            continue
        keep.append(k)
        y.append(1.0 if true[i] == true[j] else 0.0)
    return keep, np.asarray(y, dtype=np.float64)


def fit_calibrator(
    feats: np.ndarray,
    y: np.ndarray,
    *,
    l2: float = DEFAULT_L2,
    iters: int = DEFAULT_IRLS_ITERS,
    balanced: bool = True,
) -> Calibrator:
    """Fit the calibrator on labeled pair features (standardized) and same/different targets.

    ``balanced`` (default) class-weights the fit so the quadratically-more-numerous negatives do not
    swamp the same-official pairs; pass ``False`` for a raw unweighted fit.
    """
    feats = np.asarray(feats, dtype=np.float64)
    y = np.asarray(y, dtype=np.float64)
    mean = feats.mean(axis=0)
    std = feats.std(axis=0)
    std[std == 0] = 1.0  # a constant feature standardizes to 0; guard the divide
    z = (feats - mean) / std
    design = np.column_stack([np.ones(z.shape[0]), z])
    w = _fit_logistic(
        design,
        y,
        l2=l2,
        iters=iters,
        sample_weight=balanced_sample_weight(y) if balanced else None,
    )
    return Calibrator(weights=w, mean=mean, std=std)


def _matrix_from_pairs(n: int, pairs: list[tuple[int, int]], logits: np.ndarray) -> np.ndarray:
    """Fold per-pair logits back into a symmetric ``(N, N)`` score matrix."""
    mat = np.zeros((n, n))
    for (i, j), s in zip(pairs, logits, strict=True):
        mat[i, j] = mat[j, i] = float(s)
    return mat


def calibrated_matrix(
    embeddings: np.ndarray,
    cohort: np.ndarray,
    conditions: list[str],
    seconds: list[float],
    calibrator: Calibrator,
) -> np.ndarray:
    """Symmetric ``(N, N)`` calibrated score matrix — a drop-in backend for the benchmark sweep."""
    feats, pairs = pair_features(embeddings, cohort, conditions, seconds)
    return _matrix_from_pairs(embeddings.shape[0], pairs, calibrator.predict_logit(feats))


def loo_refit_outcomes(
    feats: np.ndarray,
    pairs: list[tuple[int, int]],
    true: list[object | None],
    cannot_link: set[frozenset[int]],
    *,
    threshold: float,
    l2: float = DEFAULT_L2,
    iters: int = DEFAULT_IRLS_ITERS,
) -> dict[str, int]:
    """Held-out proposer outcomes for the calibrator at a FIXED threshold.

    The in-sample tradeoff curve flatters a fitted scorer — the calibrator saw the judged pairs. So
    for each labeled cluster this refits WITHOUT any pair touching it, re-links everything at the
    fixed operating threshold, and judges only that cluster
    (:func:`~actalux.diarization.linking.benchmark.proposer_outcome`). The gap between this and the
    in-sample curve is the fit leakage; adoption decisions read THIS number (the phase-2 adoption
    gate: adopt only if it beats AS-norm held-out).
    """
    n = len(true)
    keep, y = labeled_pair_targets(true, pairs, exclude=cannot_link)
    y_of = dict(zip(keep, y, strict=True))
    outcomes = {"correct": 0, "wrong": 0, "ambiguous": 0, "alone": 0}
    for i in range(n):
        if true[i] is None:
            continue
        mask = [k for k in keep if i not in pairs[k]]
        calib = fit_calibrator(feats[mask], np.asarray([y_of[k] for k in mask]), l2=l2, iters=iters)
        scores = _matrix_from_pairs(n, pairs, calib.predict_logit(feats))
        pred = constrained_complete_linkage(scores, threshold=threshold, cannot_link=cannot_link)
        outcomes[proposer_outcome(pred, true, i)] += 1
    return outcomes
