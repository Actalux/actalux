"""Unit tests for the voiceprint matcher math: allowed-gallery, Gate A, nested LOMO."""

from __future__ import annotations

from actalux.diarization.matching import (
    Metrics,
    Sample,
    best_operating_point,
    enabled_officials,
    nested_leave_one_meeting_out,
    person_scores,
    select_operating_point,
)

A = (1.0, 0.0, 0.0)
B = (0.0, 1.0, 0.0)
C = (0.0, 0.0, 1.0)


def _s(person, meeting, vec):
    return Sample(person_id=person, meeting_key=meeting, embedding=vec)


def test_person_scores_allowed_restricts_gallery():
    q = _s(1, "mX", A)
    gallery = [_s(1, "m1", A), _s(2, "m2", B)]
    scores = person_scores(q, gallery, aggregation="mean", allowed={1})
    assert set(scores) == {1}


def test_enabled_officials_requires_core_and_min_samples():
    # p1 has two agreeing meetings (core); p2 has one (no core at min_core=2).
    train = [_s(1, "m1", A), _s(1, "m2", A), _s(2, "m3", B)]
    assert enabled_officials(train, core_floor=0.5, min_core=2, collapse_bound=0.85) == {1}


def test_enabled_officials_drops_collapsed_voices():
    # p1 and p2 share one voice -> a roll-call caller labeled twice -> neither enabled.
    train = [_s(1, "m1", A), _s(1, "m2", A), _s(2, "m3", A), _s(2, "m4", A)]
    assert enabled_officials(train, core_floor=0.5, min_core=2, collapse_bound=0.85) == set()


def test_nested_lomo_clean_separation_rejects_negative():
    samples = [
        _s(1, "m1", A), _s(1, "m2", A), _s(1, "m3", A),
        _s(2, "m4", B), _s(2, "m5", B), _s(2, "m6", B),
        _s(None, "m7", C),  # a citizen distinct from both officials
    ]  # fmt: skip
    metrics, prov = nested_leave_one_meeting_out(samples, precision_bar=0.9)
    assert metrics.macro_precision == 1.0
    assert metrics.recall == 1.0
    assert prov["n_negatives"] == 1
    assert not any(true is None for true, _ in metrics.confusions)  # negative rejected


def test_nested_lomo_counts_negative_match_as_false_positive():
    # A "negative" that is actually official 1's voice must get matched -> FP -> macroP < 1.
    samples = [
        _s(1, "m1", A), _s(1, "m2", A), _s(1, "m3", A),
        _s(2, "m4", B), _s(2, "m5", B), _s(2, "m6", B),
        _s(None, "m7", A),
    ]  # fmt: skip
    metrics, _ = nested_leave_one_meeting_out(samples, precision_bar=0.5)
    assert any(true is None for true, _ in metrics.confusions)
    assert metrics.macro_precision < 1.0


def test_select_operating_point_none_without_a_core():
    # Two singleton officials -> no coherent core -> nothing enable-able -> None.
    assert select_operating_point([_s(1, "m1", A), _s(2, "m2", B)], precision_bar=0.9) is None


def test_best_operating_point_conservative_tiebreak():
    tied_lo = Metrics(macro_precision=1.0, recall=0.5, predictions=5)
    tied_hi = Metrics(macro_precision=1.0, recall=0.5, predictions=5)
    grid = [(0.5, 0.0, tied_lo), (0.7, 0.0, tied_hi)]
    t, _margin, _metrics = best_operating_point(grid, 0.98)
    assert t == 0.7  # equal recall -> prefer the higher (stricter) threshold
