"""Gate A — label quality: is a pooled voiceprint actually the person it's labeled with?

Gate B (pooling.py) makes a cluster's embedding internally clean; it cannot tell whether
the *name* on that cluster is right. Roll-call anchoring occasionally attaches an
official's name to the wrong voice (a clerk / a bled-in neighbor), which shows up as a
gallery that disagrees with itself across meetings (negative same-person cosines). Gate A
screens those out with two independent, purely-geometric checks — no ground truth needed:

  - ``coherent_core``: an official must have a subset of meetings whose voiceprints
    mutually agree, or they are not trusted as a positive (catches mislabeled / clerk
    galleries like the diagnosed Kami Waldman / Bridget McAndrew).
  - ``collapse_suspects``: if two clusters labeled with DIFFERENT people are near-
    duplicates, that's one voice wearing several names (a roll-call caller labeled as many
    members) — both names are suspect.

Applied ONLY within training folds in the nested-LOMO harness (plan §5), never to filter a
held-out test sample — that is what keeps the calibration metric honest. Pure numpy.
"""

from __future__ import annotations

import numpy as np


def _normalize(mat: np.ndarray) -> np.ndarray:
    norms = np.linalg.norm(mat, axis=1, keepdims=True)
    norms[norms == 0] = 1.0
    return mat / norms


def mean_cosine_to_others(vectors: list[tuple[float, ...]]) -> list[float]:
    """Per-sample mean cosine to the other samples (a self-consistency score)."""
    n = len(vectors)
    if n == 0:
        return []
    if n == 1:
        return [0.0]  # a singleton has no corroboration
    vecs = _normalize(np.asarray(vectors, dtype=np.float64))
    sim = vecs @ vecs.T
    return [float((sim[i].sum() - 1.0) / (n - 1)) for i in range(n)]


def coherent_core(
    vectors: list[tuple[float, ...]], *, core_floor: float, min_core: int
) -> list[int]:
    """Indices of an official's mutually-agreeing samples, or [] if there is no core.

    A sample is in the core if its mean cosine to the official's other samples is at least
    ``core_floor``. If fewer than ``min_core`` samples clear that, the official has no
    trustworthy core and is not enabled as a positive. ``core_floor`` / ``min_core`` are
    swept per fold (plan §7), not hardcoded.
    """
    means = mean_cosine_to_others(vectors)
    core = [i for i, m in enumerate(means) if m >= core_floor]
    return core if len(core) >= min_core else []


def collapse_suspects(
    labeled: list[tuple[int, tuple[float, ...]]], *, collapse_bound: float
) -> set[int]:
    """person_ids implicated in a "one voice, many names" collapse.

    ``labeled`` is ``[(person_id, vector), ...]`` across officials. If two samples with
    DIFFERENT person_ids have cosine >= ``collapse_bound``, that single voice is anchored
    to multiple names (a roll-call caller labeled as several members) — both person_ids are
    flagged. Returns the set of suspect person_ids to exclude from positives.
    """
    if len(labeled) < 2:
        return set()
    persons = np.array([p for p, _ in labeled])
    vecs = _normalize(np.asarray([v for _, v in labeled], dtype=np.float64))
    sim = vecs @ vecs.T
    suspects: set[int] = set()
    n = len(labeled)
    for i in range(n):
        for j in range(i + 1, n):
            if persons[i] != persons[j] and sim[i, j] >= collapse_bound:
                suspects.add(int(persons[i]))
                suspects.add(int(persons[j]))
    return suspects
