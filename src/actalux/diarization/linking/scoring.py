"""Calibrated pair scoring — the drift fix that replaces raw cosine at the linking stage.

Raw cosine similarity between cross-recording cluster centroids produces spurious high scores
under condition mismatch (a far-field Zoom cluster spuriously hitting 0.85-0.95 against an
in-person cluster). ``asnorm_matrix`` applies AS-norm (adaptive symmetric score
normalization): each pair score is standardized against the two clusters' own score
distributions over an impostor cohort, so a centroid that scores high against *everything*
(the far-field failure mode) is pulled down, while a distinctive centroid that only matches
its true twin stays high. Theory: Swart & Brummer 2017; recipe: VBx / 3D-Speaker.

Pure numpy — no torch, no GPU, no network. The backend is deliberately narrow (a callable
returning an ``(N, N)`` similarity matrix) so a calibrated ``plda_matrix`` backend can be
added later behind the same interface. See docs/architecture/linking-prototype-phase1.md.
"""

from __future__ import annotations

import numpy as np

# AS-norm cohort size: the number of top impostor scores per cluster used to estimate the
# score distribution. VBx-style default; swept during calibration, not a magic inline literal.
AS_NORM_COHORT_TOPK = 100

# Numerical floor added to every standard deviation so a degenerate (zero-variance) cohort
# never divides by zero.
EPS = 1e-8

# A cohort score at (or numerically indistinguishable from) 1.0 is a self-match — the cluster
# compared against itself or an exact duplicate — and is excluded from the impostor cohort so
# it cannot inflate the mean. The tolerance is loose enough to survive float32 round-off.
SELF_MATCH_TOL = 1e-6


def _l2_normalize_rows(mat: np.ndarray) -> np.ndarray:
    """Row-wise L2 normalize; a zero row stays zero (so its cosine is 0, never NaN)."""
    norms = np.linalg.norm(mat, axis=1, keepdims=True)
    norms[norms == 0] = 1.0
    return mat / norms


def _cosine_between(a: np.ndarray, b: np.ndarray) -> np.ndarray:
    """Cosine similarity of every row of ``a`` against every row of ``b`` -> ``(len a, len b)``."""
    return _l2_normalize_rows(a) @ _l2_normalize_rows(b).T


def cosine_matrix(embeddings: np.ndarray) -> np.ndarray:
    """L2-normalized cosine similarity of every pair of rows.

    Parameters
    ----------
    embeddings
        The ``(N, D)`` centroid stack.

    Returns
    -------
    np.ndarray
        The symmetric ``(N, N)`` cosine similarity matrix; zero-norm rows score 0.
    """
    mat = np.asarray(embeddings, dtype=np.float64)
    normed = _l2_normalize_rows(mat)
    return normed @ normed.T


def asnorm_matrix(
    embeddings: np.ndarray,
    cohort: np.ndarray | None = None,
    *,
    topk: int = AS_NORM_COHORT_TOPK,
) -> np.ndarray:
    """Adaptive symmetric score normalization of the pairwise cosine matrix.

    Parameters
    ----------
    embeddings
        The ``(N, D)`` centroid stack to score against itself.
    cohort
        The ``(M, D)`` impostor cohort each score is normalized against. Defaults to
        ``embeddings`` itself when ``None`` (leave-one-out via the self-match exclusion).
    topk
        Use the top ``min(topk, available)`` cohort scores per cluster to estimate its
        score distribution.

    Returns
    -------
    np.ndarray
        The symmetric, finite ``(N, N)`` AS-norm score matrix. For a pair ``(i, j)`` the
        score is ``0.5 * ((S-mu_i)/sigma_i + (S-mu_j)/sigma_j)`` where ``S`` is the raw
        cosine and ``mu``/``sigma`` are the cluster's top-cohort mean/std.
    """
    mat = np.asarray(embeddings, dtype=np.float64)
    s = cosine_matrix(mat)
    cohort_mat = mat if cohort is None else np.asarray(cohort, dtype=np.float64)
    cohort_scores = _cosine_between(mat, cohort_mat)

    n = mat.shape[0]
    mu = np.empty(n)
    sigma = np.empty(n)
    for i in range(n):
        row = cohort_scores[i]
        impostors = row[row < 1.0 - SELF_MATCH_TOL]  # drop self / exact-duplicate columns
        if impostors.size == 0:
            mu[i], sigma[i] = 0.0, 1.0  # no impostors -> neutral (identity) normalization
            continue
        k = min(topk, impostors.size)
        top = np.sort(impostors)[::-1][:k]
        mu[i] = float(np.mean(top))
        sigma[i] = float(np.std(top)) + EPS  # population std; EPS guards divide-by-zero

    z = (s - mu[:, None]) / sigma[:, None]
    return 0.5 * (z + z.T)


# Future backend: ``plda_matrix(embeddings, ...) -> np.ndarray`` slots in here behind the same
# "(N, D) centroids -> (N, N) similarity" contract as a calibrated cosine replacement.
