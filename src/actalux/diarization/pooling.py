"""Gate B — embedding purity: pool a cluster's per-turn voiceprints into one robust vector.

A diarization cluster's turns are embedded individually (on the GPU); this module turns
that per-turn list into a single enrolled voiceprint, robust to a few contaminated turns
(crosstalk, a bled-in "here" from another speaker). The method is a trimmed, medoid-
anchored, length-weighted mean:

  1. medoid turn = the one most similar on average to the rest (the cluster's center);
  2. drop the bottom ``trim_fraction`` of turns by cosine-to-medoid (contamination);
  3. length-weighted-average the survivors and L2-normalize.

A cluster with no coherent core (too few survivors, or survivors that still disagree with
the medoid) is REJECTED (returns ``None``) rather than enrolled — a crosstalk / mis-split
cluster self-eliminates instead of poisoning the gallery. This is Gate B in
docs/architecture/voiceprint-recalibration-plan.md §4; Gate A (labelqa.py) is the
independent backstop against a clean-but-mislabeled cluster.

Pure numpy — no ``modal``, no GPU, no ``actalux`` heavy imports — so it is unit-tested in
the normal suite and shared by the enroller and the recalibration harness.
"""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np


@dataclass(frozen=True)
class Pooled:
    """One enrolled voiceprint plus the provenance of how clean it is.

    ``purity`` is the median cosine-to-medoid of the kept turns (1.0 = a single voice;
    low = the turns disagree). ``coherent_turns`` kept of ``n_turns`` total; ``seconds`` is
    the speech behind the kept turns.
    """

    vector: tuple[float, ...]
    purity: float
    n_turns: int
    coherent_turns: int
    seconds: float


def _l2_normalize_rows(mat: np.ndarray) -> np.ndarray:
    """Row-wise L2 normalize; zero rows stay zero (so cosine == dot product)."""
    norms = np.linalg.norm(mat, axis=1, keepdims=True)
    norms[norms == 0] = 1.0
    return mat / norms


def pool_turn_embeddings(
    vectors: list[tuple[float, ...]],
    durations: list[float],
    *,
    trim_fraction: float,
    min_coherent_turns: int,
    purity_floor: float,
) -> Pooled | None:
    """Pool per-turn embeddings into one voiceprint, or ``None`` if the cluster has no core.

    Parameters
    ----------
    vectors
        Per-turn embeddings (each ~256-d; L2-normalized defensively here anyway).
    durations
        Per-turn speech seconds, parallel to ``vectors`` (the pooling weight).
    trim_fraction
        Drop this bottom fraction of turns by cosine-to-medoid (0.0 keeps all). Swept +
        reported by calibration — not a magic constant (plan §7).
    min_coherent_turns
        Reject the cluster if fewer than this many turns survive the trim.
    purity_floor
        Reject if the survivors' median cosine-to-medoid is below this.

    Returns
    -------
    Pooled | None
        ``None`` marks a rejected (no-coherent-core) cluster — do not enroll it.
    """
    if len(vectors) != len(durations):
        raise ValueError("vectors and durations must be the same length")
    n = len(vectors)
    if n == 0 or n < min_coherent_turns:
        return None

    vecs = _l2_normalize_rows(np.asarray(vectors, dtype=np.float64))
    durs = np.asarray(durations, dtype=np.float64)

    # Pairwise cosine (rows are normalized). Medoid = highest mean similarity to others.
    sim = vecs @ vecs.T
    if n == 1:
        mean_to_others = np.array([1.0])  # a singleton is its own (weak) center
    else:
        mean_to_others = (sim.sum(axis=1) - 1.0) / (n - 1)
    medoid = int(np.argmax(mean_to_others))
    cos_to_medoid = sim[medoid]

    # Trim the bottom fraction by cosine-to-medoid (the contaminated tail).
    #
    # NOTE the interaction with ``min_coherent_turns``: a quantile cut on exactly 2 turns lands
    # between their two similarities, so the tail-drop keeps 1 and the cluster is rejected. With
    # the default trim the EFFECTIVE floor is therefore 3 turns, not 2. That is deliberate: the
    # only way to keep a 2-turn cluster would be to skip the trim for it, and with
    # ``purity_floor=0`` (the production setting, where coherence is delegated to Gate A) two
    # turns of DIFFERENT voices — a diarization error — would pool into a blended voiceprint.
    # Rejecting thin clusters is the precision-safe side of that trade.
    keep_at = float(np.quantile(cos_to_medoid, trim_fraction)) if trim_fraction > 0 else -np.inf
    kept = np.flatnonzero(cos_to_medoid >= keep_at)
    if kept.size < min_coherent_turns:
        return None

    purity = float(np.median(cos_to_medoid[kept]))
    if purity < purity_floor:
        return None

    weights = durs[kept]
    if weights.sum() <= 0:
        weights = np.ones_like(weights)  # degenerate durations -> unweighted
    pooled = (vecs[kept] * weights[:, None]).sum(axis=0)
    norm = float(np.linalg.norm(pooled))
    if norm == 0 or not np.isfinite(norm):
        return None
    pooled = pooled / norm

    return Pooled(
        vector=tuple(float(x) for x in pooled),
        purity=purity,
        n_turns=n,
        coherent_turns=int(kept.size),
        seconds=float(durs[kept].sum()),
    )
