"""Unit tests for Gate A label-quality predicates. Pure math, no GPU/DB."""

from __future__ import annotations

import pytest

from actalux.diarization.labelqa import coherent_core, collapse_suspects, mean_cosine_to_others

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
