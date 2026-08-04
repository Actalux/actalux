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
