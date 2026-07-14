"""Tests for the light condition-aware pair-score calibrator (pure numpy)."""

from __future__ import annotations

import numpy as np

from actalux.diarization.linking.calibration import (
    _fit_logistic,
    _sigmoid,
    calibrated_matrix,
    fit_calibrator,
    labeled_pair_targets,
    pair_features,
)
from actalux.diarization.linking.observations import VoiceObservation, embedding_matrix


def _obs(doc_id: int, vec: list[float], cond: str) -> VoiceObservation:
    return VoiceObservation(
        document_id=doc_id,
        cluster_label="S0",
        embedding=np.asarray(vec, dtype=np.float32),
        speech_seconds=30.0,
        acoustic_condition=cond,
        meeting_date="2021-01-01",
    )


def _two_officials() -> list[VoiceObservation]:
    return [
        _obs(1, [1.0, 0.0], "zoom"),
        _obs(2, [0.99, 0.141], "in_person"),
        _obs(3, [0.0, 1.0], "zoom"),
        _obs(4, [0.141, 0.99], "in_person"),
    ]


def test_sigmoid_is_stable_at_extremes() -> None:
    out = _sigmoid(np.array([1000.0, -1000.0, 0.0]))
    assert not np.isnan(out).any()
    assert out[0] == 1.0
    assert out[1] == 0.0
    assert out[2] == 0.5


def test_fit_logistic_learns_increasing_boundary() -> None:
    design = np.array([[1.0, -2.0], [1.0, -1.0], [1.0, 1.0], [1.0, 2.0]])
    w = _fit_logistic(design, np.array([0.0, 0.0, 1.0, 1.0]), l2=0.1, iters=25)
    assert w[1] > 0.0  # higher feature -> higher predicted probability


def test_pair_features_cross_condition_indicator() -> None:
    obs = _two_officials()
    feats, pairs = pair_features(
        embedding_matrix(obs),
        embedding_matrix(obs),
        [o.acoustic_condition for o in obs],
        [o.speech_seconds for o in obs],
    )
    assert feats.shape == (6, 4)
    assert len(pairs) == 6
    # combinations order (0,1)(0,2)(0,3)(1,2)(1,3)(2,3); cross=1 unless both same condition
    assert feats[:, 2].tolist() == [1.0, 0.0, 1.0, 1.0, 0.0, 1.0]


def test_labeled_pair_targets_skips_unlabeled_pairs() -> None:
    keep, y = labeled_pair_targets([10, 10, None], [(0, 1), (0, 2), (1, 2)])
    assert keep == [0]  # only (0,1) has both ends labeled
    assert y.tolist() == [1.0]


def test_calibrated_matrix_ranks_same_official_above_cross() -> None:
    obs = _two_officials()
    emb = embedding_matrix(obs)
    conds = [o.acoustic_condition for o in obs]
    secs = [o.speech_seconds for o in obs]
    feats, pairs = pair_features(emb, emb, conds, secs)
    true = [100, 100, 200, 200]
    keep, y = labeled_pair_targets(true, pairs)
    cal = fit_calibrator(feats[keep], y)
    mat = calibrated_matrix(emb, emb, conds, secs, cal)
    same = np.mean([mat[0, 1], mat[2, 3]])
    cross = np.mean([mat[0, 2], mat[0, 3], mat[1, 2], mat[1, 3]])
    assert same > cross
