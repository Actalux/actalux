"""Unit tests for voiceprint calibration math (no DB/GPU)."""

from __future__ import annotations

import scripts.voiceprint_calibrate as vc

A = (1.0, 0.0, 0.0)
B = (0.0, 1.0, 0.0)
MID = (0.7071, 0.7071, 0.0)  # equidistant from A and B


def _s(person, meeting, vec):
    return vc.Sample(person_id=person, meeting_key=meeting, embedding=vec)


def _gallery():
    # Two officials, two meetings each, voices cleanly separated.
    return [
        _s(1, "m1", A),
        _s(1, "m2", A),
        _s(2, "m1", B),
        _s(2, "m2", B),
    ]


def test_cosine_orthogonal_and_identical():
    assert vc.cosine(A, A) == 1.0
    assert vc.cosine(A, B) == 0.0


def test_person_scores_mean_and_max():
    q = _s(1, "mX", A)
    gallery = [_s(1, "m1", A), _s(1, "m2", MID), _s(2, "m3", B)]
    mean = vc.person_scores(q, gallery, aggregation="mean")
    assert mean[1] == (1.0 + 0.7071) / 2
    assert mean[2] == 0.0
    top = vc.person_scores(q, gallery, aggregation="max")
    assert top[1] == 1.0


def test_predict_clears_threshold_and_margin():
    q = _s(1, "mX", A)
    assert vc.predict(q, _gallery(), 0.5, 0.1, aggregation="mean") == 1


def test_predict_abstains_when_margin_too_small():
    # A negative equidistant from both officials -> zero margin -> abstain.
    q = _s(None, "mX", MID)
    assert vc.predict(q, _gallery(), 0.5, 0.1, aggregation="mean") is None


def test_predict_abstains_below_threshold():
    # Closer to A (0.8) than B (0.6) — margin is fine, but 0.8 < threshold 0.9.
    q = _s(1, "mX", (0.8, 0.6, 0.0))
    assert vc.predict(q, _gallery(), 0.9, 0.0, aggregation="mean") is None


def test_leave_one_meeting_out_excludes_own_meeting():
    # Give m1's A a distinctive vector; if its own meeting leaked it would self-match.
    samples = [_s(1, "m1", A), _s(1, "m2", A), _s(2, "m1", B), _s(2, "m2", B)]
    preds = vc.leave_one_meeting_out(samples, 0.5, 0.1, aggregation="mean")
    assert all(true == pred for true, pred in preds)  # cross-meeting still correct


def test_score_all_correct():
    preds = [(1, 1), (1, 1), (2, 2), (2, 2)]
    m = vc.score(preds)
    assert m.macro_precision == 1.0
    assert m.recall == 1.0
    assert m.predictions == 4


def test_score_negative_prediction_is_false_positive():
    # true=None predicted as person 1 -> FP, drags person 1's precision to 0.5.
    preds = [(1, 1), (None, 1)]
    m = vc.score(preds)
    assert m.per_person_precision[1] == 0.5
    assert m.macro_precision == 0.5
    assert m.recall == 1.0  # 1 of 1 real positive recalled
    assert m.confusions == [(None, 1)]


def test_best_operating_point_picks_highest_recall_meeting_bar():
    m_hi = vc.Metrics(macro_precision=1.0, recall=0.5, predictions=5)
    m_hi_recall = vc.Metrics(macro_precision=0.99, recall=0.9, predictions=9)
    m_low = vc.Metrics(macro_precision=0.80, recall=1.0, predictions=10)
    grid = [(0.7, 0.1, m_hi), (0.6, 0.1, m_hi_recall), (0.4, 0.0, m_low)]
    t, mg, chosen = vc.best_operating_point(grid, 0.98)
    assert chosen is m_hi_recall  # both clear 0.98; higher recall wins


def test_best_operating_point_none_when_bar_unmet():
    grid = [(0.4, 0.0, vc.Metrics(macro_precision=0.5, recall=1.0, predictions=10))]
    assert vc.best_operating_point(grid, 0.98) is None
