"""Unit tests for Gate B pooling (pool_turn_embeddings). Pure math, no GPU/DB."""

from __future__ import annotations

import pytest

from actalux.diarization.pooling import Pooled, pool_turn_embeddings

A = (1.0, 0.0, 0.0)
B = (0.0, 1.0, 0.0)
C = (0.0, 0.0, 1.0)


def test_clean_cluster_survives_with_full_purity():
    p = pool_turn_embeddings(
        [A, A, A], [10.0, 10.0, 10.0], trim_fraction=0.25, min_coherent_turns=2, purity_floor=0.5
    )
    assert isinstance(p, Pooled)
    assert p.coherent_turns == 3 and p.n_turns == 3
    assert p.purity == pytest.approx(1.0)
    assert p.vector[0] == pytest.approx(1.0)


def test_contaminant_is_trimmed():
    # Three A's + one intruder B; the bottom-quartile trim drops B, pooled stays A.
    p = pool_turn_embeddings(
        [A, A, A, B], [10.0] * 4, trim_fraction=0.25, min_coherent_turns=2, purity_floor=0.5
    )
    assert p is not None
    assert p.n_turns == 4 and p.coherent_turns == 3
    assert p.vector[0] == pytest.approx(1.0)
    assert p.vector[1] == pytest.approx(0.0, abs=1e-9)


def test_no_coherent_core_is_rejected():
    # Three mutually orthogonal turns -> survivors disagree with the medoid -> reject.
    p = pool_turn_embeddings(
        [A, B, C], [10.0, 10.0, 10.0], trim_fraction=0.0, min_coherent_turns=2, purity_floor=0.5
    )
    assert p is None


def test_too_few_turns_rejected():
    assert (
        pool_turn_embeddings(
            [A], [10.0], trim_fraction=0.25, min_coherent_turns=2, purity_floor=0.5
        )
        is None
    )


def test_length_weighting_pulls_toward_the_longer_turn():
    v = (0.6, 0.8, 0.0)  # cosine 0.6 with A
    p = pool_turn_embeddings(
        [A, v], [100.0, 1.0], trim_fraction=0.0, min_coherent_turns=2, purity_floor=0.5
    )
    assert p is not None
    assert p.vector[0] > 0.99  # dominated by the 100 s turn


def test_mismatched_lengths_raise():
    with pytest.raises(ValueError):
        pool_turn_embeddings(
            [A, B], [1.0], trim_fraction=0.0, min_coherent_turns=1, purity_floor=0.0
        )


def test_two_turn_cluster_is_rejected_by_the_trim():
    """With the default trim the effective floor is 3 turns, not 2 — deliberately.

    The 0.25 quantile of two similarities sits between them, so the tail-drop keeps one turn
    and the cluster is rejected. Keeping it would mean skipping the trim, and at the production
    ``purity_floor=0`` a 2-turn cluster holding two DIFFERENT voices (a diarization error) would
    pool into a blended voiceprint. Pinned so the interaction is not "fixed" by accident.
    """
    near_a = (0.99, 0.141, 0.0)  # ~8 degrees off A: same voice, not identical
    assert (
        pool_turn_embeddings(
            [A, near_a], [10.0, 10.0], trim_fraction=0.25, min_coherent_turns=2, purity_floor=0.0
        )
        is None
    )


def test_three_turn_cluster_pools_without_the_trim_starving_it():
    """Three turns clear the trim — the smallest cluster the enrollment path can pool."""
    near_a = (0.99, 0.141, 0.0)
    p = pool_turn_embeddings(
        [A, near_a, A],
        [10.0, 10.0, 10.0],
        trim_fraction=0.25,
        min_coherent_turns=2,
        purity_floor=0.5,
    )
    assert p is not None and p.n_turns == 3
