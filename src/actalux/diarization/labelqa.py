"""Gate A — label quality: is a pooled voiceprint actually the person it's labeled with?

Gate B (pooling.py) makes a cluster's embedding internally clean; it cannot tell whether
the *name* on that cluster is right. Roll-call anchoring occasionally attaches an
official's name to the wrong voice (a clerk / a bled-in neighbor), which shows up as a
gallery that disagrees with itself across meetings (negative same-person cosines). Gate A
screens those out with two independent, purely-geometric checks — no ground truth needed:

  - ``coherent_subset``: an official must have a subset of meetings whose voiceprints
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

from actalux.diarization.vectors import l2_normalize_rows


def mean_cosine_to_others(vectors: list[tuple[float, ...]]) -> list[float]:
    """Per-sample mean cosine to the other samples (a self-consistency score)."""
    n = len(vectors)
    if n == 0:
        return []
    if n == 1:
        return [0.0]  # a singleton has no corroboration
    vecs = l2_normalize_rows(np.asarray(vectors, dtype=np.float64))
    sim = vecs @ vecs.T
    return [float((sim[i].sum() - 1.0) / (n - 1)) for i in range(n)]


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

    Thresholding each sample's mean cosine to *all* the official's other samples means a
    scattered minority of anchors (e.g. six unverified discourse labels pointing at
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
    vecs = l2_normalize_rows(np.asarray(vectors, dtype=np.float64))
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
    cohort = l2_normalize_rows(np.asarray(cohort_vectors, dtype=np.float64)) if asnorm else None

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


def collapse_pairs(
    labeled: list[tuple[int, tuple[float, ...]]], *, collapse_bound: float
) -> list[tuple[int, int, float]]:
    """The offending sample pairs behind a "one voice, many names" collapse.

    ``labeled`` is ``[(person_id, vector), ...]`` across officials. Returns
    ``(i, j, cosine)`` index pairs into ``labeled`` where the two samples carry DIFFERENT
    person_ids yet cosine >= ``collapse_bound`` — each pair is one voice wearing two names.
    Indices (not person_ids) so the caller can attach sample provenance (which meeting,
    which anchor) and a human can break the collapse by ear; a bare suspect set cannot be
    reviewed (the cal-15 Garganigo/Doherty/Wilson veto was unactionable for exactly that
    reason).
    """
    if len(labeled) < 2:
        return []
    persons = np.array([p for p, _ in labeled])
    vecs = l2_normalize_rows(np.asarray([v for _, v in labeled], dtype=np.float64))
    sim = vecs @ vecs.T
    pairs: list[tuple[int, int, float]] = []
    n = len(labeled)
    for i in range(n):
        for j in range(i + 1, n):
            if persons[i] != persons[j] and sim[i, j] >= collapse_bound:
                pairs.append((i, j, float(sim[i, j])))
    return pairs


def collapse_suspects(
    labeled: list[tuple[int, tuple[float, ...]]], *, collapse_bound: float
) -> set[int]:
    """person_ids implicated in a "one voice, many names" collapse.

    The suspect set is exactly the union of ``collapse_pairs`` members (one mechanism —
    the veto and the audit's pair evidence can never disagree). Both person_ids of every
    pair are flagged: a confirmation cannot split one voice into two people, so neither
    name is trustworthy until a human breaks the pair.
    """
    suspects: set[int] = set()
    for i, j, _ in collapse_pairs(labeled, collapse_bound=collapse_bound):
        suspects.add(int(labeled[i][0]))
        suspects.add(int(labeled[j][0]))
    return suspects
