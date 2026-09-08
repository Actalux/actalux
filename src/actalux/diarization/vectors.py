"""Shared pure-numpy vector math used across the voiceprint pipeline.

Pure numpy — no torch, no GPU, no DB — so every diarization module (pooling, label QA,
hygiene, matching, linking) can depend on it without pulling in a heavy import.
"""

from __future__ import annotations

import numpy as np


def l2_normalize_rows(mat: np.ndarray) -> np.ndarray:
    """Row-wise L2 normalize; a zero row stays zero (so its cosine is 0, never NaN)."""
    norms = np.linalg.norm(mat, axis=1, keepdims=True)
    norms[norms == 0] = 1.0
    return mat / norms


def medoid_cosines(vecs: np.ndarray) -> tuple[int, np.ndarray]:
    """Find the medoid row of L2-normalized ``vecs`` and its cosine to every row.

    The medoid is the row with highest mean cosine similarity to the rest (the group's
    center). A singleton anchors on itself (cosine 1.0).
    """
    n = vecs.shape[0]
    sim = vecs @ vecs.T
    if n == 1:
        return 0, sim[0]
    mean_to_others = (sim.sum(axis=1) - 1.0) / (n - 1)
    medoid = int(np.argmax(mean_to_others))
    return medoid, sim[medoid]
