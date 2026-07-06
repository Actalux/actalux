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


def coherent_core_asnorm(
    own_vectors: list[tuple[float, ...]],
    cohort_vectors: list[tuple[float, ...]],
    *,
    z_floor: float,
    min_core: int,
    min_cohort: int,
    sigma_eps: float,
    raw_fallback_floor: float,
) -> list[int]:
    """Indices of an official's samples that stand clear of the impostor cohort (AS-norm core).

    The genuine statistic per own sample is its mean cosine to the official's OTHER samples (the
    same self-coherence ``coherent_core`` thresholds raw). Here it is z-scored against the impostor
    cohort — that sample's cosines to every OTHER official's vectors — before comparison, so an
    official whose absolute coherence is modest but clearly above the cross-official cloud can still
    form a core. A raw cosine floor is meaningless on the z-scale, so asnorm gets its own
    ``z_floor``. A degenerate cohort (fewer than ``min_cohort`` scores, or a spread below
    ``sigma_eps``) has no z-scale, so that sample falls back to the raw self-coherence test at
    ``raw_fallback_floor`` rather than dividing by ~0. Population std (ddof=0) matches AS-norm's
    cohort statistic. Cohort is caller-supplied and excludes negatives.
    """
    if not own_vectors:
        return []
    own_means = mean_cosine_to_others(own_vectors)
    own = _normalize(np.asarray(own_vectors, dtype=np.float64))
    cohort = _normalize(np.asarray(cohort_vectors, dtype=np.float64)) if cohort_vectors else None
    core: list[int] = []
    for i in range(len(own_vectors)):
        if cohort is None or len(cohort_vectors) < min_cohort:
            in_core = own_means[i] >= raw_fallback_floor
        else:
            cohort_cos = own[i] @ cohort.T  # impostor scores for own sample i
            sigma = float(cohort_cos.std())
            if sigma < sigma_eps:
                in_core = own_means[i] >= raw_fallback_floor
            else:
                z = (own_means[i] - float(cohort_cos.mean())) / sigma
                in_core = z >= z_floor
        if in_core:
            core.append(i)
    return core if len(core) >= min_core else []


def coherent_subset(
    vectors: list[tuple[float, ...]],
    *,
    core_floor: float,
    min_core: int,
    cohort_vectors: list[tuple[float, ...]] | None = None,
    z_floor: float | None = None,
    min_cohort: int = 3,
    sigma_eps: float = 1e-6,
) -> list[int]:
    """Grow the largest mutually-coherent subset from the medoid (the "Hummell fix").

    ``coherent_core`` thresholds each sample's mean cosine to *all* the official's other samples,
    so a scattered minority of anchors (e.g. six unverified discourse labels pointing at
    inconsistent voices) drags a coherent majority's mean below the floor and knocks the whole
    official out. This grows the core instead: the medoid (the sample most similar on average to
    the rest) anchors the coherent voice, and a sample joins the core when it sits within the core
    radius of the medoid. Anchors outside the radius are discarded — the coherent majority survives
    the noisy minority.

    Raw mode: a sample joins when its cosine-to-medoid ≥ ``core_floor``. AS-norm mode (an impostor
    ``cohort_vectors`` + ``z_floor`` given): it joins when its cosine-to-medoid, z-scored against
    that sample's cohort cosines, ≥ ``z_floor``; a degenerate cohort (fewer than ``min_cohort``
    scores or spread below ``sigma_eps``) falls back to the raw radius rather than dividing by ~0.
    The medoid is always in its own core. Returns ``[]`` if fewer than ``min_core`` survive.

    The returned indices are the *retained* anchors; the caller computes discarded ones as the
    complement (reported per evidence family in the audit block). Pure numpy; leakage-safe (called
    on training folds only, like the rest of Gate A).
    """
    n = len(vectors)
    if n == 0:
        return []
    vecs = _normalize(np.asarray(vectors, dtype=np.float64))
    sim = vecs @ vecs.T
    if n == 1:
        mean_to_others = np.array([1.0])
    else:
        mean_to_others = (sim.sum(axis=1) - 1.0) / (n - 1)
    medoid = int(np.argmax(mean_to_others))
    cos_to_medoid = sim[medoid]

    asnorm = (
        cohort_vectors is not None and z_floor is not None and len(cohort_vectors) >= min_cohort
    )
    cohort = _normalize(np.asarray(cohort_vectors, dtype=np.float64)) if asnorm else None

    core: list[int] = []
    for i in range(n):
        if i == medoid:
            in_core = True  # the medoid anchors its own core
        elif asnorm:
            cohort_cos = vecs[i] @ cohort.T  # type: ignore[union-attr]
            sigma = float(cohort_cos.std())
            if sigma < sigma_eps:
                in_core = cos_to_medoid[i] >= core_floor  # no z-scale -> raw fallback radius
            else:
                z = (cos_to_medoid[i] - float(cohort_cos.mean())) / sigma
                in_core = z >= z_floor
        else:
            in_core = cos_to_medoid[i] >= core_floor
        if in_core:
            core.append(i)
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
