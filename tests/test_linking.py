"""Tests for the cross-meeting speaker-linking prototype (pure numpy; no DB/Modal/torch)."""

from __future__ import annotations

import numpy as np
import pytest

from actalux.diarization.linking import (
    FAMILY_WITHIN_DISCOUNT,
    EvidenceLedger,
    EvidenceObservation,
    VoiceNode,
    VoiceObservation,
    asnorm_matrix,
    bcubed_prf,
    constrained_complete_linkage,
    cosine_matrix,
    coverage,
    embedding_matrix,
    load_observations,
    macro_recall_by_official,
    pairwise_prf,
    per_condition_pair_f1,
    purity,
    save_observations,
)

# --------------------------------------------------------------------------------------------
# observations: dataclasses + lossless cache round-trip
# --------------------------------------------------------------------------------------------


def _obs(doc: int, label: str, vec: list[float], cond: str, date: str | None) -> VoiceObservation:
    return VoiceObservation(
        document_id=doc,
        cluster_label=label,
        embedding=np.asarray(vec, dtype=np.float32),
        speech_seconds=float(len(vec)),
        acoustic_condition=cond,
        meeting_date=date,
    )


def test_voice_node_member_keys():
    node = VoiceNode(
        node_id=7,
        observations=[
            _obs(1, "SPEAKER_00", [1.0, 0.0], "in_person", "2026-01-01"),
            _obs(2, "SPEAKER_03", [0.0, 1.0], "zoom_gallery", None),
        ],
    )
    assert node.member_keys() == [(1, "SPEAKER_00"), (2, "SPEAKER_03")]


def test_embedding_matrix_stacks_in_order():
    obs = [
        _obs(1, "A", [1.0, 2.0, 3.0], "in_person", None),
        _obs(1, "B", [4.0, 5.0, 6.0], "phone", None),
    ]
    mat = embedding_matrix(obs)
    assert mat.shape == (2, 3)
    assert np.array_equal(mat[1], np.asarray([4.0, 5.0, 6.0], dtype=np.float32))


def test_embedding_matrix_empty():
    mat = embedding_matrix([])
    assert mat.shape == (0, 0)


def test_save_load_round_trip_lossless(tmp_path):
    obs = [
        _obs(11, "SPEAKER_01", [0.1, 0.2, 0.3, 0.4], "zoom_gallery", "2026-02-03"),
        _obs(12, "SPEAKER_02", [0.5, 0.6, 0.7, 0.8], "in_person", None),
    ]
    path = tmp_path / "obs.npz"
    save_observations(obs, path)
    loaded = load_observations(path)
    assert len(loaded) == 2
    for original, restored in zip(obs, loaded, strict=True):
        assert restored.document_id == original.document_id
        assert restored.cluster_label == original.cluster_label
        assert restored.speech_seconds == original.speech_seconds
        assert restored.acoustic_condition == original.acoustic_condition
        assert restored.meeting_date == original.meeting_date
        assert np.array_equal(restored.embedding, original.embedding)
    # embeddings survive the file being closed (copied out of the archive)
    assert loaded[1].meeting_date is None


def test_save_load_empty(tmp_path):
    path = tmp_path / "empty.npz"
    save_observations([], path)
    assert load_observations(path) == []


# --------------------------------------------------------------------------------------------
# scoring: cosine + AS-norm drift fix
# --------------------------------------------------------------------------------------------


def test_cosine_identical_is_one():
    mat = np.asarray([[1.0, 0.0], [2.0, 0.0]])  # same direction, different magnitude
    cos = cosine_matrix(mat)
    assert cos[0, 1] == pytest.approx(1.0)


def test_cosine_orthogonal_is_zero():
    mat = np.asarray([[1.0, 0.0], [0.0, 1.0]])
    cos = cosine_matrix(mat)
    assert cos[0, 1] == pytest.approx(0.0, abs=1e-12)


def test_cosine_symmetric_and_zero_norm_safe():
    mat = np.asarray([[1.0, 1.0], [0.0, 0.0], [1.0, -1.0]])  # row 1 is a zero vector
    cos = cosine_matrix(mat)
    assert np.allclose(cos, cos.T)
    assert np.all(np.isfinite(cos))
    assert cos[0, 1] == pytest.approx(0.0)  # zero-norm row -> 0, not NaN


def _asnorm_fixture() -> tuple[np.ndarray, tuple[int, int], tuple[int, int]]:
    """Dense background near direction g, an impostor pair in the dense region, and a
    distinctive true-same pair far from the background. Returns (embeddings, impostor, true)."""
    background = [
        [1.0, 1.0, 0.0, 0.0],
        [1.0, 0.9, 0.0, 0.0],
        [0.9, 1.0, 0.0, 0.0],
        [1.0, 1.1, 0.0, 0.0],
        [1.1, 1.0, 0.0, 0.0],
        [0.95, 1.05, 0.0, 0.0],
    ]
    impostor = [[1.0, 1.0, 0.02, 0.0], [1.0, 1.0, 0.0, 0.02]]  # near-identical, in dense region
    true_same = [[0.0, 0.0, 1.0, 1.0], [0.0, 0.05, 0.97, 1.0]]  # distinctive direction, less tight
    rows = background + impostor + true_same
    embeddings = np.asarray(rows, dtype=np.float64)
    impostor_pair = (len(background), len(background) + 1)
    true_pair = (len(background) + 2, len(background) + 3)
    return embeddings, impostor_pair, true_pair


def test_asnorm_reduces_impostor_relative_to_true_pair():
    embeddings, impostor, true_pair = _asnorm_fixture()
    cos = cosine_matrix(embeddings)
    # Setup sanity: the impostor pair's RAW cosine is at least as high as the true pair's.
    assert cos[impostor] >= cos[true_pair] - 1e-9
    asn = asnorm_matrix(embeddings)
    # Drift-fix property: after AS-norm the true-same pair outscores the impostor pair.
    assert asn[true_pair] > asn[impostor]


def test_asnorm_symmetric_and_finite():
    embeddings, _, _ = _asnorm_fixture()
    asn = asnorm_matrix(embeddings)
    assert np.allclose(asn, asn.T)
    assert np.all(np.isfinite(asn))


def test_asnorm_topk_and_cohort_arg_run():
    embeddings, _, _ = _asnorm_fixture()
    external = embeddings[:4]
    asn = asnorm_matrix(embeddings, cohort=external, topk=3)
    assert asn.shape == (embeddings.shape[0], embeddings.shape[0])
    assert np.all(np.isfinite(asn))


# --------------------------------------------------------------------------------------------
# cluster: constrained complete-linkage
# --------------------------------------------------------------------------------------------


def _two_block_scores() -> np.ndarray:
    return np.asarray(
        [
            [1.0, 0.9, 0.1, 0.1],
            [0.9, 1.0, 0.1, 0.1],
            [0.1, 0.1, 1.0, 0.9],
            [0.1, 0.1, 0.9, 1.0],
        ]
    )


def test_linkage_merges_two_clusters_below_threshold():
    labels = constrained_complete_linkage(_two_block_scores(), threshold=0.5)
    assert labels[0] == labels[1]
    assert labels[2] == labels[3]
    assert labels[0] != labels[2]


def test_linkage_no_merge_above_threshold():
    labels = constrained_complete_linkage(_two_block_scores(), threshold=0.95)
    assert len(set(labels)) == 4  # every index its own node


def test_linkage_cannot_link_never_co_clustered():
    scores = np.full((3, 3), 0.9)
    np.fill_diagonal(scores, 1.0)
    labels = constrained_complete_linkage(scores, threshold=0.0, cannot_link={frozenset({0, 1})})
    assert labels[0] != labels[1]  # forbidden pair split even at threshold 0


def test_linkage_must_link_co_clustered_below_threshold():
    scores = np.asarray([[1.0, 0.1, 0.05], [0.1, 1.0, 0.05], [0.05, 0.05, 1.0]])
    labels = constrained_complete_linkage(scores, threshold=0.5, must_link={frozenset({0, 1})})
    assert labels[0] == labels[1]  # seeded together despite score 0.1 < 0.5


def test_linkage_contradictory_constraints_raise():
    scores = np.eye(2)
    with pytest.raises(ValueError):
        constrained_complete_linkage(
            scores,
            threshold=0.5,
            cannot_link={frozenset({0, 1})},
            must_link={frozenset({0, 1})},
        )


def test_linkage_empty_matrix():
    assert constrained_complete_linkage(np.zeros((0, 0)), threshold=0.5) == []


# --------------------------------------------------------------------------------------------
# evaluate: purity / coverage / pairwise / per-condition
# --------------------------------------------------------------------------------------------

# Hand-built example (index 5 is unlabeled and must be ignored everywhere):
#   true = A A A B B None ; pred = 0 0 1 1 2 99
#   purity  = (2 + 1 + 1) / 5 = 0.8
#   coverage= (2 + 1)     / 5 = 0.6
#   pairs: TP=1 FP=1 FN=3 -> P=0.5 R=0.25 F1=1/3
_EVAL_PRED = [0, 0, 1, 1, 2, 99]
_EVAL_TRUE: list[str | None] = ["A", "A", "A", "B", "B", None]


def test_purity_known_value():
    assert purity(_EVAL_PRED, _EVAL_TRUE) == pytest.approx(0.8)


def test_coverage_known_value():
    assert coverage(_EVAL_PRED, _EVAL_TRUE) == pytest.approx(0.6)


def test_pairwise_prf_known_value():
    precision, recall, f1 = pairwise_prf(_EVAL_PRED, _EVAL_TRUE)
    assert precision == pytest.approx(0.5)
    assert recall == pytest.approx(0.25)
    assert f1 == pytest.approx(1.0 / 3.0)


def test_metrics_empty_when_no_labels():
    pred = [0, 1, 2]
    true: list[str | None] = [None, None, None]
    assert purity(pred, true) == 0.0
    assert coverage(pred, true) == 0.0
    assert pairwise_prf(pred, true) == (0.0, 0.0, 0.0)


def test_per_condition_across_lower_than_within_on_drift():
    # Two people (A, B), each split across zoom/in_person. Within-condition clusters are pure
    # but nobody links ACROSS conditions -> within F1 high, across F1 zero.
    pred = [0, 0, 1, 2, 3, 3]
    true: list[str | None] = ["A", "A", "A", "B", "B", "B"]
    conditions = ["zoom", "zoom", "in_person", "zoom", "in_person", "in_person"]
    result = per_condition_pair_f1(pred, true, conditions)
    assert result["within"] == pytest.approx(1.0)
    assert result["across"] == pytest.approx(0.0)
    assert result["across"] < result["within"]


def test_bcubed_prf_known_value():
    # same hand-built example: B-cubed P = 0.8, R = 8/15, F1 = 0.64 (each item counts itself)
    precision, recall, f1 = bcubed_prf(_EVAL_PRED, _EVAL_TRUE)
    assert precision == pytest.approx(0.8)
    assert recall == pytest.approx(8.0 / 15.0)
    assert f1 == pytest.approx(0.64)


def test_bcubed_prf_empty_when_no_labels():
    assert bcubed_prf([0, 1], [None, None]) == (0.0, 0.0, 0.0)


def test_macro_recall_by_official_weights_officials_equally():
    # official A (0,1,2) recall 1/3 (only 0,1 share a node); official B (3,4) recall 0 -> mean 1/6
    assert macro_recall_by_official(_EVAL_PRED, _EVAL_TRUE) == pytest.approx(1.0 / 6.0)


def test_macro_recall_excludes_singletons():
    # official A recurs (two clusters, grouped -> recall 1.0); B is a singleton and is excluded
    assert macro_recall_by_official([0, 0, 1], ["A", "A", "B"]) == pytest.approx(1.0)


# --------------------------------------------------------------------------------------------
# ledger: family-aware evidence scoring
# --------------------------------------------------------------------------------------------


def _ev(
    family: str, weight: float, slug: str, channel: str = "c", doc: int = 1
) -> EvidenceObservation:
    return EvidenceObservation(
        channel=channel,
        family=family,
        weight=weight,
        candidate_slug=slug,
        source_document_id=doc,
    )


def test_ledger_multi_family_beats_single_family_same_raw_weight():
    ledger = EvidenceLedger()
    # multi: two families, 0.5 + 0.5 (raw total 1.0) -> 0.5 + 0.5 = 1.0
    ledger.add(_ev("adjacency", 0.5, "multi"))
    ledger.add(_ev("vote", 0.5, "multi"))
    # single: one family, 0.5 + 0.5 (raw total 1.0) -> 0.5 + 0.3*0.5 = 0.65
    ledger.add(_ev("adjacency", 0.5, "single"))
    ledger.add(_ev("adjacency", 0.5, "single"))
    scores = ledger.score_by_candidate()
    assert scores["multi"] == pytest.approx(1.0)
    assert scores["single"] == pytest.approx(0.65)
    assert scores["multi"] > scores["single"]


def test_ledger_within_family_diminishing_returns():
    ledger = EvidenceLedger()
    for _ in range(3):
        ledger.add(_ev("adjacency", 1.0, "x"))
    # 1.0 + 0.3 * (1.0 + 1.0) = 1.6, not 3.0
    assert ledger.score_by_candidate()["x"] == pytest.approx(1.0 + FAMILY_WITHIN_DISCOUNT * 2.0)


def test_ledger_explain_sorted_by_weight_desc():
    ledger = EvidenceLedger()
    ledger.add(_ev("adjacency", 0.2, "x", channel="rollcall"))
    ledger.add(_ev("vote", 0.9, "x", channel="vote_anchor"))
    ledger.add(_ev("screen", 0.5, "x", channel="screen_name"))
    ledger.add(_ev("adjacency", 0.7, "other"))  # different candidate, excluded
    explained = ledger.explain("x")
    assert [o.weight for o in explained] == [0.9, 0.5, 0.2]
    assert all(o.candidate_slug == "x" for o in explained)
