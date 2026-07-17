"""Tests for the pure measurement harness (actalux.diarization.linking.benchmark)."""

from __future__ import annotations

import numpy as np

from actalux.diarization.linking.benchmark import (
    _stratified_poison_pairs,
    best_at_floors,
    best_proposer_point,
    candidate_thresholds,
    cannot_link_audit,
    cannot_link_same_meeting,
    evaluate_point,
    label_stats,
    loo_operating_point,
    poison_blast_radius,
    proposer_outcome,
    proposer_outcomes,
    proposer_tradeoff,
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


def _two_officials_across_conditions() -> list[VoiceObservation]:
    # official 100 near [1,0] across docs 1,2; official 200 near [0,1] across docs 3,4
    return [
        _obs(1, "S0", [1.0, 0.0], "zoom"),
        _obs(2, "S0", [0.99, 0.141], "in_person"),
        _obs(3, "S0", [0.0, 1.0], "zoom"),
        _obs(4, "S0", [0.141, 0.99], "in_person"),
    ]


def test_evaluate_point_surfaces_bcubed_and_macro_recall() -> None:
    # perfect recovery -> B-cubed F1 and macro-per-official recall are both exactly 1.0
    m = evaluate_point([0, 0, 1, 1], [10, 10, 20, 20], ["1", "2", "3", "4"], ["z", "i", "z", "i"])
    assert m["bcubed_f1"] == 1.0
    assert m["macro_official_recall"] == 1.0


def test_poison_blast_radius_forced_merge_creates_false_enrollment() -> None:
    obs = _two_officials_across_conditions()
    scores = cosine_matrix(embedding_matrix(obs))
    # clean base assignment merges each official's own pair; a forced cross-official merge poisons
    result = poison_blast_radius(scores, set(), [100, 100, 200, 200], threshold=0.5)
    assert result["n_trials"] >= 1.0
    assert result["max_false_enrollments"] >= 1.0


def test_poison_blast_radius_no_cross_official_pairs_is_zero() -> None:
    # a single official -> no cross-official poison pair exists -> nothing to inject
    obs = [_obs(1, "S0", [1.0, 0.0]), _obs(2, "S0", [0.99, 0.141])]
    scores = cosine_matrix(embedding_matrix(obs))
    result = poison_blast_radius(scores, set(), [100, 100], threshold=0.5)
    assert result["n_trials"] == 0.0
    assert result["max_false_enrollments"] == 0.0


def test_poison_blast_radius_catches_ambiguity_on_anchored_fixture() -> None:
    obs = _two_officials_across_conditions()
    scores = cosine_matrix(embedding_matrix(obs))
    result = poison_blast_radius(scores, set(), [100, 100, 200, 200], threshold=0.5)
    # every sampled poison fuses two ANCHORED officials, so the proposer's ambiguity guard would
    # refuse all of them -> the false-enrollment count is an upper bound the guard never realizes
    assert result["ambiguity_caught"] == 1.0


def test_stratified_poison_pairs_spread_across_official_pairs() -> None:
    # 3 officials -> 3 unordered official-pairs (A-B, A-C, B-C), 4 candidate cluster pairs each
    true = [100, 100, 200, 200, 300, 300]
    pairs = _stratified_poison_pairs(list(range(6)), true, set(), max_trials=3)
    assert len(pairs) == 3
    official_pairs = {tuple(sorted(str(true[i]) for i in p)) for p in pairs}
    assert len(official_pairs) == 3  # one per official-pair...
    assert not all(0 in p for p in pairs)  # ...not three poisonings of cluster 0 (index-order bug)


def _three_officials() -> list[VoiceObservation]:
    # A near [1,0,0] (docs 1,2); B near [0,1,0] (docs 3,4); C near [0,0,1] (docs 5,6).
    # Within-official cosine ~0.99, cross-official <=0.28 -> a threshold exists that separates all
    # three, so holding one out still leaves purity able to punish a degenerate low threshold.
    return [
        _obs(1, "S0", [1.0, 0.0, 0.0], "zoom"),
        _obs(2, "S0", [0.99, 0.141, 0.0], "in_person"),
        _obs(3, "S0", [0.0, 1.0, 0.0], "zoom"),
        _obs(4, "S0", [0.141, 0.99, 0.0], "in_person"),
        _obs(5, "S0", [0.0, 0.0, 1.0], "zoom"),
        _obs(6, "S0", [0.0, 0.141, 0.99], "in_person"),
    ]


_THREE_TRUE = [100, 100, 200, 200, 300, 300]


def test_loo_operating_point_reports_heldout_recall_per_fold() -> None:
    obs = _three_officials()
    scores = cosine_matrix(embedding_matrix(obs))
    meeting = [str(o.document_id) for o in obs]
    cond = [o.acoustic_condition for o in obs]
    result = loo_operating_point(
        scores, cannot_link_same_meeting(obs), _THREE_TRUE, meeting, cond, purity_floor=0.95
    )
    assert result["n_folds"] == 3.0  # one fold per official, each resolving a point
    # each held-out official's own pair still merges at the threshold chosen without them
    assert result["mean_heldout_recall"] == 1.0
    assert result["n_false_merge_folds"] == 0.0
    assert result["threshold_spread_lo"] <= result["threshold_spread_hi"]
    assert all(f["heldout_recall"] == 1.0 for f in result["folds"])


def test_loo_operating_point_thresholds_match_sweep_backend_selection() -> None:
    # the precomputed-linkage-per-threshold path must select exactly what a full re-cluster would
    obs = _three_officials()
    scores = cosine_matrix(embedding_matrix(obs))
    meeting = [str(o.document_id) for o in obs]
    cond = [o.acoustic_condition for o in obs]
    cl = cannot_link_same_meeting(obs)
    result = loo_operating_point(scores, cl, _THREE_TRUE, meeting, cond, purity_floor=0.95)
    for fold in result["folds"]:
        reduced = [None if t == fold["official"] else t for t in _THREE_TRUE]
        best, _ = sweep_backend(scores, cl, reduced, meeting, cond, purity_floor=0.95)
        assert best is not None
        assert fold["threshold"] == best["threshold"]


def test_loo_operating_point_flags_false_merge_of_heldout_official() -> None:
    # A's second cluster sits on top of B's voice, so any threshold that merges B's own pair also
    # swallows A2 -> the held-out A fold must report the false merge and a broken recall
    obs = [
        _obs(1, "S0", [1.0, 0.0, 0.0], "zoom"),
        _obs(2, "S0", [0.02, 1.0, 0.0], "zoom"),  # ~B's direction
        _obs(3, "S0", [0.0, 1.0, 0.0], "zoom"),
        _obs(4, "S0", [0.01, 1.0, 0.0], "in_person"),
        _obs(5, "S0", [0.0, 0.0, 1.0], "zoom"),
        _obs(6, "S0", [0.0, 0.141, 0.99], "in_person"),
    ]
    scores = cosine_matrix(embedding_matrix(obs))
    meeting = [str(o.document_id) for o in obs]
    cond = [o.acoustic_condition for o in obs]
    result = loo_operating_point(
        scores, cannot_link_same_meeting(obs), _THREE_TRUE, meeting, cond, purity_floor=0.95
    )
    a_fold = next(f for f in result["folds"] if f["official"] == 100)
    assert a_fold["false_merge"] is True
    assert a_fold["heldout_recall"] == 0.0  # A1 and A2 never share a node
    assert result["n_false_merge_folds"] >= 1.0


def test_cannot_link_audit_flags_high_similarity_same_meeting_pair() -> None:
    # doc 1's two clusters look alike (a likely fragmented speaker); doc 2's are orthogonal
    obs = [
        _obs(1, "S0", [1.0, 0.0]),
        _obs(1, "S1", [0.99, 0.141]),
        _obs(2, "S0", [1.0, 0.0]),
        _obs(2, "S1", [0.0, 1.0]),
    ]
    scores = cosine_matrix(embedding_matrix(obs))
    flagged = cannot_link_audit(obs, scores, threshold=0.5)
    assert len(flagged) == 1  # only the suspicious same-meeting pair, cross-meeting pairs ignored
    assert flagged[0]["document_id"] == 1
    assert flagged[0]["score"] > 0.9


def test_proposer_outcome_four_ways() -> None:
    # nodes: {0,1} same official, {2,3} two officials, {4} singleton, {5,6} wrong-name inheritance
    pred = [0, 0, 1, 1, 2, 3, 3]
    true = ["a", "a", "b", "c", "d", "e", "f"]
    assert proposer_outcome(pred, true, 0) == "correct"
    assert proposer_outcome(pred, true, 2) == "wrong"  # its only node-mate is official c
    assert proposer_outcome(pred, true, 4) == "alone"
    assert proposer_outcome(pred, true, 5) == "wrong"


def test_proposer_outcome_ambiguous_node() -> None:
    pred = [0, 0, 0]
    true = ["a", "b", "c"]  # any held-out cluster sees TWO other officials
    assert proposer_outcome(pred, true, 0) == "ambiguous"


def test_proposer_outcomes_skips_unlabeled() -> None:
    pred = [0, 0, 1]
    true = ["a", None, "b"]
    outcomes = proposer_outcomes(pred, true)
    assert sum(outcomes.values()) == 2  # the unlabeled cluster is not judged
    assert outcomes["alone"] == 2  # each labeled cluster has no labeled node-mate


def test_proposer_tradeoff_and_best_point() -> None:
    obs = _three_officials()
    scores = cosine_matrix(embedding_matrix(obs))
    cannot_link = cannot_link_same_meeting(obs)
    rows = proposer_tradeoff(scores, cannot_link, _THREE_TRUE)
    assert rows, "tradeoff must produce at least one threshold row"
    for row in rows:
        assert {"threshold", "purity", "correct", "wrong", "ambiguous", "alone"} <= row.keys()
    best = best_proposer_point(rows, purity_floor=0.95)
    assert best is not None
    # the separable three-official geometry admits a threshold naming every cluster correctly
    assert best["correct"] == 6.0
    assert best["wrong"] == 0.0


def test_best_proposer_point_none_when_floor_unreachable() -> None:
    rows = [{"threshold": 0.1, "purity": 0.5, "correct": 9.0, "wrong": 3.0}]
    assert best_proposer_point(rows, purity_floor=0.95) is None
