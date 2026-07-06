"""Unit tests for Gate A label-quality predicates. Pure math, no GPU/DB."""

from __future__ import annotations

import pytest

from actalux.diarization.labelqa import (
    coherent_core,
    coherent_subset,
    collapse_suspects,
    mean_cosine_to_others,
)

A = (1.0, 0.0, 0.0)
B = (0.0, 1.0, 0.0)
C = (0.0, 0.0, 1.0)


def test_mean_cosine_to_others():
    means = mean_cosine_to_others([A, A, B])
    assert means[0] == pytest.approx(0.5)
    assert means[1] == pytest.approx(0.5)
    assert means[2] == pytest.approx(0.0)


def test_singleton_has_no_corroboration():
    assert mean_cosine_to_others([A]) == [0.0]


def test_coherent_core_keeps_agreeing_samples():
    # Three consistent meetings + one mislabeled outlier -> core is the three.
    core = coherent_core([A, A, A, B], core_floor=0.5, min_core=2)
    assert core == [0, 1, 2]


def test_coherent_core_empty_when_no_agreement():
    # Kami/Bridget signature: samples that don't agree across meetings -> no core.
    assert coherent_core([A, B, C], core_floor=0.5, min_core=2) == []


def test_coherent_core_respects_min_core():
    assert coherent_core([A, A], core_floor=0.5, min_core=3) == []


def test_collapse_detects_one_voice_many_names():
    # person 1 and person 2 have the same voice -> a roll-call caller labeled twice.
    suspects = collapse_suspects([(1, A), (2, A), (3, B)], collapse_bound=0.9)
    assert suspects == {1, 2}


def test_collapse_ignores_distinct_voices():
    assert collapse_suspects([(1, A), (2, B)], collapse_bound=0.9) == set()


def test_collapse_ignores_same_person_duplicates():
    # The same person appearing twice is expected, not a collapse.
    assert collapse_suspects([(1, A), (1, A)], collapse_bound=0.9) == set()


# --- coherent_subset (the medoid-grown, Hummell-robust core) ----------------------------------


def test_coherent_subset_grows_from_medoid_and_discards_outliers():
    # Three agreeing samples + one orthogonal outlier: the outlier is outside the medoid's radius.
    assert coherent_subset([A, A, A, B], core_floor=0.5, min_core=2) == [0, 1, 2]


def test_coherent_subset_survives_scattered_majority():
    # The Hummell shape reduced to hand vectors: two coherent anchors (A) + THREE mutually-
    # orthogonal scatter vectors. mean_cosine_to_others is highest for an A (it has one perfect
    # match), so the medoid is an A and the core is exactly the two A's — the scattered majority is
    # discarded rather than dragging the coherent pair below the floor.
    import numpy as np

    e = [tuple(1.0 if j == i else 0.0 for j in range(5)) for i in range(5)]
    samples = [
        e[0],
        e[0],
        e[2],
        e[3],
        e[4],
    ]  # two coherent (e0) + three mutually-orthogonal scatter
    core = coherent_subset(samples, core_floor=0.5, min_core=2)
    assert core == [0, 1]
    assert np.allclose([samples[i] for i in core], [e[0], e[0]])


def test_coherent_subset_respects_min_core():
    assert coherent_subset([A, B, C], core_floor=0.5, min_core=2) == []  # medoid alone, min 2


def test_coherent_subset_asnorm_radius_and_fallback():
    import math

    r84, r99 = math.sqrt(0.84), math.sqrt(0.99)
    s1a = (1.0, 0.0, 0.0, 0.0, 0.0)
    s1b = (0.4, r84, 0.0, 0.0, 0.0)  # cosine 0.4 with s1a
    cohort = [(0.1, 0.0, r99, 0.0, 0.0), (-0.1, 0.0, 0.0, r99, 0.0)]  # near-orthogonal impostors
    kw = {"min_core": 2, "min_cohort": 2, "sigma_eps": 1e-6}
    # Raw radius 0.5 rejects s1b (0.4 < 0.5) -> only the medoid survives -> below min_core -> [].
    assert coherent_subset([s1a, s1b], core_floor=0.5, min_core=2) == []
    # AS-norm: s1b sits well above the impostor cloud, clearing a modest z-floor -> core is both.
    assert coherent_subset(
        [s1a, s1b], core_floor=0.5, cohort_vectors=cohort, z_floor=2.0, **kw
    ) == [
        0,
        1,
    ]
    # A too-small cohort has no z-scale -> raw fallback at core_floor (0.4 < 0.5 rejects s1b).
    assert (
        coherent_subset([s1a, s1b], core_floor=0.5, cohort_vectors=cohort[:1], z_floor=2.0, **kw)
        == []
    )
