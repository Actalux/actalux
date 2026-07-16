"""Tests for the light condition-aware pair-score calibrator (pure numpy)."""

from __future__ import annotations

import numpy as np

from actalux.diarization.linking.calibration import (
    FEATURE_NAMES,
    _fit_logistic,
    _sigmoid,
    balanced_sample_weight,
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


def test_labeled_pair_targets_excludes_cannot_link_pairs() -> None:
    # (0,2) are same-meeting -> a structural negative the linker already forbids; it must not be
    # fed to the fit as a free easy negative
    true = [10, 20, 30]
    pairs = [(0, 1), (0, 2), (1, 2)]
    keep, y = labeled_pair_targets(true, pairs, exclude={frozenset((0, 2))})
    assert keep == [0, 2]
    assert y.tolist() == [0.0, 0.0]


def test_pair_features_log_scales_duration() -> None:
    assert FEATURE_NAMES[3] == "log_min_seconds"
    obs = [_obs(1, [1.0, 0.0], "zoom"), _obs(2, [0.0, 1.0], "zoom")]
    emb = embedding_matrix(obs)
    feats, _ = pair_features(emb, emb, ["zoom", "zoom"], [30.0, 1200.0])
    # the pair's duration feature is log1p(min(30, 1200)), not the raw 30
    assert feats[0, 3] == np.log1p(30.0)


def test_balanced_sample_weight_equalizes_class_mass() -> None:
    y = np.array([1.0, 0.0, 0.0, 0.0])
    w = balanced_sample_weight(y)
    # the lone positive carries the same mass as the three negatives combined
    assert np.isclose(w[y == 1.0].sum(), w[y == 0.0].sum())
    assert np.isclose(w.sum(), y.size)  # total mass preserved


def test_balanced_sample_weight_single_class_is_uniform() -> None:
    assert balanced_sample_weight(np.array([1.0, 1.0])).tolist() == [1.0, 1.0]


def test_balanced_weighting_shifts_bias_not_the_score_direction() -> None:
    # one positive against many negatives: balancing must lift the intercept toward the rare class
    # without flipping what the score means (higher feature still -> more likely same-official)
    feats = np.array([[0.9], [0.1], [0.2], [0.15]])
    y = np.array([1.0, 0.0, 0.0, 0.0])
    plain = fit_calibrator(feats, y, balanced=False)
    weighted = fit_calibrator(feats, y, balanced=True)
    assert weighted.weights[0] > plain.weights[0]  # bias shifts toward the rare positive class
    assert plain.weights[1] > 0.0
    assert weighted.weights[1] > 0.0  # sign of the feature weight is unchanged


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
