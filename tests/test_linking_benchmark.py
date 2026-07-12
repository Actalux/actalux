"""Tests for the pure measurement harness (actalux.diarization.linking.benchmark)."""

from __future__ import annotations

import numpy as np

from actalux.diarization.linking.benchmark import (
    best_at_floors,
    candidate_thresholds,
    cannot_link_same_meeting,
    evaluate_point,
    label_stats,
    sweep_backend,
)
from actalux.diarization.linking.observations import VoiceObservation, embedding_matrix
from actalux.diarization.linking.scoring import cosine_matrix, diverse_cohort


def _obs(doc_id: int, label: str, vec: list[float], cond: str = "in_person") -> VoiceObservation:
    return VoiceObservation(
        document_id=doc_id,
        cluster_label=label,
        embedding=np.asarray(vec, dtype=np.float32),
        speech_seconds=30.0,
        acoustic_condition=cond,
        meeting_date="2021-01-01",
    )


def test_cannot_link_pairs_only_within_a_meeting() -> None:
    obs = [_obs(1, "S0", [1, 0]), _obs(1, "S1", [0, 1]), _obs(2, "S0", [1, 0])]
    # indices 0 and 1 share document 1; index 2 is a different meeting
    assert cannot_link_same_meeting(obs) == {frozenset((0, 1))}


def test_cannot_link_empty_when_all_meetings_distinct() -> None:
    obs = [_obs(1, "S0", [1, 0]), _obs(2, "S0", [1, 0]), _obs(3, "S0", [1, 0])]
    assert cannot_link_same_meeting(obs) == set()


def test_candidate_thresholds_scale_adaptive_and_sorted() -> None:
    scores = np.array([[1.0, 0.2, 0.8], [0.2, 1.0, 0.5], [0.8, 0.5, 1.0]])
    thr = candidate_thresholds(scores, n_thresholds=5)
    assert thr == sorted(thr)
    # only off-diagonal upper-triangle values {0.2, 0.8, 0.5} drive the percentiles
    assert min(thr) >= 0.2 - 1e-9
    assert max(thr) <= 0.8 + 1e-9


def test_candidate_thresholds_empty_for_singleton() -> None:
    assert candidate_thresholds(np.ones((1, 1))) == []


def test_evaluate_point_perfect_recovery() -> None:
    # two officials, two clusters each, all correctly grouped
    pred = [0, 0, 1, 1]
    true = [10, 10, 20, 20]
    meeting = ["1", "2", "3", "4"]  # every cluster a distinct meeting
    cond = ["zoom", "in_person", "zoom", "in_person"]
    m = evaluate_point(pred, true, meeting, cond)
    assert m["purity"] == 1.0
    assert m["coverage"] == 1.0
    assert m["pair_f1"] == 1.0
    assert m["across_meeting_f1"] == 1.0  # both same-person pairs are cross-meeting


def test_evaluate_point_none_labels_skipped() -> None:
    # a None label is "not in the benchmark" and contributes to no pair
    pred = [0, 0, 1]
    true = [10, 10, None]
    meeting = ["1", "2", "3"]
    cond = ["zoom", "zoom", "zoom"]
    m = evaluate_point(pred, true, meeting, cond)
    assert m["pair_f1"] == 1.0  # only the (0,1) same-person pair is scored


def test_label_stats_counts() -> None:
    true = [10, 10, 20, 30, None]
    cond = ["zoom", "in_person", "zoom", "in_person", "zoom"]
    stats = label_stats(true, cond)
    assert stats["clusters"] == 4
    assert stats["officials"] == 3
    assert stats["recurring_officials"] == 1  # only official 10 appears twice
    assert stats["cross_condition_officials"] == 1  # official 10 spans zoom + in_person


def test_sweep_backend_recovers_two_officials() -> None:
    # official A near [1,0] across docs 1,2; official B near [0,1] across docs 3,4
    obs = [
        _obs(1, "S0", [1.0, 0.0], "zoom"),
        _obs(2, "S0", [0.99, 0.141], "in_person"),
        _obs(3, "S0", [0.0, 1.0], "zoom"),
        _obs(4, "S0", [0.141, 0.99], "in_person"),
    ]
    true = [100, 100, 200, 200]
    meeting = [str(o.document_id) for o in obs]
    cond = [o.acoustic_condition for o in obs]
    scores = cosine_matrix(embedding_matrix(obs))
    best, sweep = sweep_backend(
        scores, cannot_link_same_meeting(obs), true, meeting, cond, purity_floor=0.95
    )
    assert best is not None
    assert best["n_nodes"] == 2
    assert best["across_meeting_f1"] == 1.0
    assert best["across_condition_f1"] == 1.0  # each official's pair is zoom<->in_person
    assert len(sweep) >= 2


def test_diverse_cohort_spreads_across_groups() -> None:
    # six clusters in three distinct directions; FPS should pick one per direction
    groups = [[1, 0], [0.99, 0.14], [0, 1], [0.14, 0.99], [-1, 0], [-0.99, 0.14]]
    emb = np.asarray(groups, dtype=float)
    cohort = diverse_cohort(emb, 3)
    assert cohort.shape[0] == 3
    off = cosine_matrix(cohort)[np.triu_indices(3, k=1)]
    assert off.max() < 0.9  # the three picks are mutually dissimilar (not same-group siblings)


def test_diverse_cohort_returns_all_when_k_ge_n() -> None:
    emb = np.asarray([[1, 0], [0, 1]], dtype=float)
    assert diverse_cohort(emb, 5).shape[0] == 2  # nothing to prune


def test_best_at_floors_selects_and_reports_misses() -> None:
    sweep = [
        {"purity": 0.99, "across_meeting_f1": 0.1},
        {"purity": 0.95, "across_meeting_f1": 0.5},
        {"purity": 0.90, "across_meeting_f1": 0.8},
    ]
    got = best_at_floors(sweep, [0.99, 0.92, 1.0])
    assert got[0.99]["across_meeting_f1"] == 0.1  # only the 0.99 point clears 0.99
    assert got[0.92]["across_meeting_f1"] == 0.5  # 0.99 + 0.95 clear 0.92 -> max F1 = 0.5
    assert got[1.0] is None  # nothing clears 1.0 -> honest miss


def test_sweep_backend_reports_miss_when_floor_unreachable() -> None:
    obs = [_obs(1, "S0", [1.0, 0.0]), _obs(2, "S0", [0.0, 1.0])]
    scores = cosine_matrix(embedding_matrix(obs))
    best, sweep = sweep_backend(
        scores, set(), [1, 2], ["1", "2"], ["zoom", "zoom"], purity_floor=1.01
    )
    assert best is None  # an impossible floor yields an honest None, not a low-purity point
    assert sweep  # the sweep still ran
