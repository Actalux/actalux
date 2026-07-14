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

from actalux.diarization.linking.scoring import asnorm_matrix, cosine_matrix

# Pair features. cross_condition lets the model learn a separate offset for Zoom<->in-person pairs;
# min_seconds down-weights pairs where one cluster has little speech (a noisy centroid).
FEATURE_NAMES = ("cosine", "asnorm", "cross_condition", "min_seconds")

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


def _fit_logistic(design: np.ndarray, y: np.ndarray, *, l2: float, iters: int) -> np.ndarray:
    """Ridge-regularized logistic regression by IRLS (Newton-Raphson).

    ``design`` includes the leading bias column of ones. The ridge term stabilizes the Hessian on
    separable or collinear data and is not applied to the bias.
    """
    d = design.shape[1]
    w = np.zeros(d)
    reg = l2 * np.eye(d)
    reg[0, 0] = 0.0  # never regularize the bias term
    for _ in range(iters):
        p = _sigmoid(design @ w)
        weights = p * (1.0 - p)
        grad = design.T @ (y - p) - reg @ w
        hess = design.T @ (design * weights[:, None]) + reg
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
        rows.append([cos[i, j], asn[i, j], cross, min(seconds[i], seconds[j])])
        pairs.append((i, j))
    return np.asarray(rows, dtype=np.float64), pairs


def labeled_pair_targets(
    true: list[object | None], pairs: list[tuple[int, int]]
) -> tuple[list[int], np.ndarray]:
    """Same-official (1) / different-official (0) targets over labeled pairs only.

    Returns the indices into ``pairs`` that are labeled on both ends plus the target vector, so the
    caller fits on ``feats[keep]``. Pairs touching an unlabeled cluster are dropped, never guessed.
    """
    keep: list[int] = []
    y: list[float] = []
    for k, (i, j) in enumerate(pairs):
        if true[i] is None or true[j] is None:
            continue
        keep.append(k)
        y.append(1.0 if true[i] == true[j] else 0.0)
    return keep, np.asarray(y, dtype=np.float64)


def fit_calibrator(
    feats: np.ndarray, y: np.ndarray, *, l2: float = DEFAULT_L2, iters: int = DEFAULT_IRLS_ITERS
) -> Calibrator:
    """Fit the calibrator on labeled pair features (standardized) and same/different targets."""
    feats = np.asarray(feats, dtype=np.float64)
    y = np.asarray(y, dtype=np.float64)
    mean = feats.mean(axis=0)
    std = feats.std(axis=0)
    std[std == 0] = 1.0  # a constant feature standardizes to 0; guard the divide
    z = (feats - mean) / std
    design = np.column_stack([np.ones(z.shape[0]), z])
    w = _fit_logistic(design, y, l2=l2, iters=iters)
    return Calibrator(weights=w, mean=mean, std=std)


def calibrated_matrix(
    embeddings: np.ndarray,
    cohort: np.ndarray,
    conditions: list[str],
    seconds: list[float],
    calibrator: Calibrator,
) -> np.ndarray:
    """Symmetric ``(N, N)`` calibrated score matrix — a drop-in backend for the benchmark sweep."""
    feats, pairs = pair_features(embeddings, cohort, conditions, seconds)
    logits = calibrator.predict_logit(feats)
    n = embeddings.shape[0]
    mat = np.zeros((n, n))
    for (i, j), s in zip(pairs, logits, strict=True):
        mat[i, j] = mat[j, i] = float(s)
    return mat
