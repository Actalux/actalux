"""Unit tests for the voiceprint matcher math: allowed-gallery, Gate A, nested LOMO."""

from __future__ import annotations

import math
from collections import defaultdict

import numpy as np

from actalux.diarization.labelqa import (
    coherent_core,
    coherent_core_asnorm,
    collapse_suspects,
)
from actalux.diarization.matching import (
    DEFAULT_AGGREGATIONS,
    DEFAULT_COLLAPSE_BOUNDS,
    DEFAULT_CORE_FLOORS,
    DEFAULT_MARGINS,
    DEFAULT_PURITY_FLOORS,
    DEFAULT_SCORE_NORMS,
    DEFAULT_THRESHOLDS,
    GATE_A_MIN_CORE,
    GridPoint,
    Metrics,
    Sample,
    best_operating_point,
    build_sim,
    enabled_officials,
    evaluate_grid,
    leave_one_meeting_out,
    nested_leave_one_meeting_out,
    nested_lomo_multi_bar,
    pareto_frontier,
    person_scores,
    score,
    select_operating_point,
)

A = (1.0, 0.0, 0.0)
B = (0.0, 1.0, 0.0)
C = (0.0, 0.0, 1.0)

# Threshold grid before the 0.92/0.95 extension — the identity oracle sweeps this so a "none"-mode
# run reproduces the pre-asnorm selection exactly.
LEGACY_THRESHOLDS = (0.40, 0.45, 0.50, 0.55, 0.60, 0.65, 0.70, 0.75, 0.80, 0.85, 0.90)


def _s(person, meeting, vec):
    return Sample(person_id=person, meeting_key=meeting, embedding=vec)


def test_person_scores_allowed_restricts_gallery():
    q = _s(1, "mX", A)
    gallery = [_s(1, "m1", A), _s(2, "m2", B)]
    scores = person_scores(q, gallery, aggregation="mean", allowed={1})
    assert set(scores) == {1}


def test_sim_fast_path_matches_cosine():
    samples = [_s(1, "m1", A), _s(1, "m2", B), _s(2, "m3", (0.6, 0.8, 0.0))]
    sim = build_sim(samples)  # assigns idx + precomputes the cosine matrix
    slow = person_scores(samples[0], samples[1:], aggregation="mean")
    fast = person_scores(samples[0], samples[1:], aggregation="mean", sim=sim)
    assert slow.keys() == fast.keys()
    assert all(abs(slow[k] - fast[k]) < 1e-9 for k in slow)


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


# --- AS-norm (Gate A E#1) --------------------------------------------------------------------
# Hand-computable fixture (R^5): person 1 has two samples cosine 0.4 apart (own-coherence 0.4);
# person 2 (the impostor cohort) sits near-orthogonal. Raw@0.5 enables neither, but person 1's
# coherence is 4σ (sample s1a) / 10σ (s1b) above the impostor cloud, so asnorm recovers it.
_R99 = math.sqrt(0.99)
_R84 = math.sqrt(0.84)
_S1A = (1.0, 0.0, 0.0, 0.0, 0.0)
_S1B = (0.4, _R84, 0.0, 0.0, 0.0)  # cosine 0.4 with _S1A
_S2X = (0.1, 0.0, _R99, 0.0, 0.0)  # cosine 0.1 with _S1A, 0.04 with _S1B
_S2Y = (-0.1, 0.0, 0.0, _R99, 0.0)  # cosine -0.1 with _S1A, -0.04 with _S1B


def _asnorm_train():
    return [_s(1, "m1", _S1A), _s(1, "m2", _S1B), _s(2, "m3", _S2X), _s(2, "m4", _S2Y)]


def test_asnorm_zfloor_pins_core_membership():
    # z(s1a) = (0.4-0)/0.1 = 4.0 ; z(s1b) = (0.4-0)/0.04 = 10.0 (population σ). min_core=1 exposes
    # the per-sample decision the enablement filter would otherwise hide. Floors bracket (not equal)
    # the true z-values so the assertions are robust to the √ round-off in the fixture.
    both, cohort = [_S1A, _S1B], [_S2X, _S2Y]
    kw = {"min_core": 1, "min_cohort": 2, "sigma_eps": 1e-6, "raw_fallback_floor": 0.99}
    assert coherent_core_asnorm(both, cohort, z_floor=3.9, **kw) == [0, 1]  # both above ~4σ / ~10σ
    assert coherent_core_asnorm(both, cohort, z_floor=4.1, **kw) == [1]  # s1a (z≈4) drops out
    assert coherent_core_asnorm(both, cohort, z_floor=9.9, **kw) == [1]  # s1b (z≈10) still in
    assert coherent_core_asnorm(both, cohort, z_floor=10.1, **kw) == []  # s1b drops out


def test_asnorm_flips_enablement_the_right_way():
    train = _asnorm_train()
    # Raw self-coherence 0.4 < 0.5 -> neither official has a core.
    assert enabled_officials(train, core_floor=0.5, min_core=2, collapse_bound=0.85) == set()
    # asnorm rescues the genuine, non-impostor official 1; the incoherent official 2 stays out.
    assert enabled_officials(
        train,
        core_floor=0.5,
        min_core=2,
        collapse_bound=0.85,
        score_norm="asnorm",
        z_floor=2.0,
        cohort_min=2,
    ) == {1}
    # A z-floor above s1b's 10σ leaves person 1 with fewer than min_core survivors -> not enabled.
    assert (
        enabled_officials(
            train,
            core_floor=0.5,
            min_core=2,
            collapse_bound=0.85,
            score_norm="asnorm",
            z_floor=5.0,
            cohort_min=2,
        )
        == set()
    )


def test_asnorm_degenerate_sigma_falls_back_to_raw():
    # Cohort cosines with zero spread give no z-scale: the sample is judged by the raw fallback.
    c1 = (0.1, 0.0, _R99, 0.0, 0.0)
    c2 = (0.1, 0.0, 0.0, _R99, 0.0)  # both cosine 0.1 with _S1A -> σ = 0
    train = [_s(1, "m1", _S1A), _s(1, "m2", _S1B), _s(2, "m3", c1), _s(2, "m4", c2)]
    common = {"min_core": 2, "collapse_bound": 0.85, "score_norm": "asnorm", "z_floor": 2.0}
    assert enabled_officials(train, core_floor=0.3, cohort_min=2, **common) == {1}  # 0.4 >= 0.3
    assert enabled_officials(train, core_floor=0.5, cohort_min=2, **common) == set()  # 0.4 < 0.5


def test_asnorm_small_cohort_falls_back_to_raw():
    # A single-sample cohort is below the default size floor -> raw fallback, not a divide-by-~0.
    train = [_s(1, "m1", _S1A), _s(1, "m2", _S1B), _s(2, "m3", _S2X)]
    common = {"min_core": 2, "collapse_bound": 0.85, "score_norm": "asnorm", "z_floor": 2.0}
    assert enabled_officials(train, core_floor=0.3, **common) == {1}
    assert enabled_officials(train, core_floor=0.5, **common) == set()


def test_collapse_stays_raw_under_asnorm():
    # One voice wearing two names: collapse_suspects (raw cosine) must still drop both, even though
    # each official's own-coherence is perfect and z_floor/core_floor are wide open.
    train = [_s(1, "m1", A), _s(1, "m2", A), _s(2, "m3", A), _s(2, "m4", A)]
    assert (
        enabled_officials(
            train,
            core_floor=0.0,
            min_core=2,
            collapse_bound=0.85,
            score_norm="asnorm",
            z_floor=0.0,
            cohort_min=2,
        )
        == set()
    )


# --- grid + curve ----------------------------------------------------------------------------


def test_grid_extension_present():
    assert 0.92 in DEFAULT_THRESHOLDS and 0.95 in DEFAULT_THRESHOLDS
    assert DEFAULT_COLLAPSE_BOUNDS == (0.80, 0.85, 0.90)
    assert "asnorm" in DEFAULT_SCORE_NORMS


def test_unknown_score_norm_raises():
    # A typo must fail loudly, not silently record a z_floor while scoring raw.
    import pytest

    with pytest.raises(ValueError, match="unknown score_norm"):
        evaluate_grid(_random_samples(1), score_norms=("none", "asnrom"))


def _gp(precision, recall, *, threshold=0.5, margin=0.0):
    return GridPoint(
        purity_floor=0.0,
        collapse_bound=0.85,
        score_norm="none",
        core_floor=0.4,
        z_floor=None,
        aggregation="mean",
        threshold=threshold,
        margin=margin,
        enabled=frozenset({1}),
        metrics=Metrics(macro_precision=precision, recall=recall, predictions=1),
    )


def _no_point_dominated(frontier):
    for a in frontier:
        for b in frontier:
            if a is b:
                continue
            if (
                b["precision"] >= a["precision"]
                and b["recall"] >= a["recall"]
                and (b["precision"] > a["precision"] or b["recall"] > a["recall"])
            ):
                return False
    return True


def test_pareto_frontier_hand_example():
    grid = [_gp(1.0, 0.2), _gp(0.9, 0.5), _gp(0.8, 0.5), _gp(0.9, 0.4), _gp(1.0, 0.1)]
    frontier = pareto_frontier(grid)
    got = {(d["precision"], d["recall"]) for d in frontier}
    assert got == {(1.0, 0.2), (0.9, 0.5)}
    assert _no_point_dominated(frontier)


def test_pareto_frontier_on_real_grid_has_no_dominated_points():
    grid = evaluate_grid(_random_samples(7))
    frontier = pareto_frontier(grid)
    assert frontier  # a non-trivial fixture produces at least one point
    assert _no_point_dominated(frontier)
    # every reported point carries the split-FP fields the operator's policy needs
    for d in frontier:
        assert "citizen_fp" in d and "official_confusion_count" in d


def test_multi_bar_matches_single_bar_per_bar():
    samples = _random_samples(11)
    bars = (0.80, 0.90, 0.98)
    multi = nested_lomo_multi_bar(samples, precision_bars=bars)
    for b in bars:
        single_m, single_prov = nested_leave_one_meeting_out(samples, precision_bar=b)
        multi_m, multi_prov = multi[b]
        assert multi_m.macro_precision == single_m.macro_precision
        assert multi_m.recall == single_m.recall
        assert multi_m.confusions == single_m.confusions
        assert multi_prov["abstained_folds"] == single_prov["abstained_folds"]


# --- regression identity: none-mode + legacy grid == pre-asnorm behavior ----------------------


def _legacy_enabled(train, *, core_floor, min_core, collapse_bound):
    """Pre-asnorm Gate A rebuilt from the untouched primitives (not via enabled_officials)."""
    by_person = defaultdict(list)
    for s in train:
        if s.person_id is not None:
            by_person[s.person_id].append(s.embedding)
    suspects = collapse_suspects(
        [(p, v) for p, vs in by_person.items() for v in vs], collapse_bound=collapse_bound
    )
    enabled = set()
    for person, vecs in by_person.items():
        if person in suspects:
            continue
        if coherent_core(vecs, core_floor=core_floor, min_core=min_core):
            enabled.add(person)
    return enabled


def _legacy_select(samples, *, precision_bar):
    """Faithful copy of select_operating_point as it was before the asnorm/collapse-sweep rework."""
    best = None
    best_key = None
    for pf in DEFAULT_PURITY_FLOORS:
        filtered = [s for s in samples if s.purity >= pf]
        if len(filtered) < GATE_A_MIN_CORE:
            continue
        sim = build_sim(filtered)
        below = [(s.person_id, None) for s in samples if s.person_id is not None and s.purity < pf]
        for core_floor in DEFAULT_CORE_FLOORS:
            enabled = _legacy_enabled(
                filtered, core_floor=core_floor, min_core=GATE_A_MIN_CORE, collapse_bound=0.85
            )
            if not enabled:
                continue
            for agg in DEFAULT_AGGREGATIONS:
                for t in LEGACY_THRESHOLDS:
                    for mgn in DEFAULT_MARGINS:
                        preds = leave_one_meeting_out(
                            filtered, t, mgn, aggregation=agg, allowed=enabled, sim=sim
                        )
                        mtr = score(preds + below)
                        if mtr.macro_precision >= precision_bar:
                            key = (mtr.recall, t, mgn)
                            if best_key is None or key > best_key:
                                best_key = key
                                best = (pf, core_floor, t, mgn, agg, frozenset(enabled), mtr)
    return best


def test_none_mode_with_legacy_grid_matches_pre_asnorm_selection():
    # With the added axes pinned to their single legacy values, the reworked selector must return
    # the byte-identical operating point the pre-asnorm code would have — no silent drift.
    selected_any = False
    for seed in range(8):
        samples = _random_samples(seed)
        for bar in (0.80, 0.90, 0.98):
            legacy = _legacy_select(samples, precision_bar=bar)
            new = select_operating_point(
                samples,
                thresholds=LEGACY_THRESHOLDS,
                core_floors=DEFAULT_CORE_FLOORS,
                purity_floors=DEFAULT_PURITY_FLOORS,
                margins=DEFAULT_MARGINS,
                aggregations=DEFAULT_AGGREGATIONS,
                collapse_bounds=(0.85,),
                score_norms=("none",),
                min_core=GATE_A_MIN_CORE,
                precision_bar=bar,
            )
            if legacy is None:
                assert new is None
                continue
            selected_any = True
            pf, core_floor, t, mgn, agg, enabled, mtr = legacy
            assert (new.purity_floor, new.core_floor, new.threshold, new.margin) == (
                pf,
                core_floor,
                t,
                mgn,
            )
            assert new.aggregation == agg
            assert frozenset(new.enabled) == enabled
            assert new.metrics.macro_precision == mtr.macro_precision
            assert new.metrics.recall == mtr.recall
    assert selected_any  # guard against a vacuous all-None comparison


# --- fixtures + performance -------------------------------------------------------------------


def _unit(vec):
    arr = np.asarray(vec, dtype=float)
    norm = np.linalg.norm(arr)
    return tuple((arr / (norm if norm else 1.0)).tolist())


def _random_samples(seed, *, n_officials=4, n_meetings=6, dim=16, noise=0.6, n_neg=4):
    """Deterministic messy gallery: officials with moderate coherence + near-orthogonal negatives.

    Moderate noise means some officials fail the coherent-core check at the higher floors, so the
    selection is non-trivial (recall < 1) and exercises the grid + tie-break.
    """
    rng = np.random.default_rng(seed)
    centers = [rng.standard_normal(dim) for _ in range(n_officials)]
    samples = []
    for o in range(n_officials):
        for mk in rng.choice(n_meetings, size=3, replace=False):
            samples.append(
                Sample(o + 1, f"m{int(mk)}", _unit(centers[o] + noise * rng.standard_normal(dim)))
            )
    for _ in range(n_neg):
        mk = int(rng.integers(0, n_meetings))
        samples.append(Sample(None, f"m{mk}", _unit(rng.standard_normal(dim))))
    return samples


def _scale_samples(seed=0, *, n_officials=12, meetings=21, dim=256, noise=0.7):
    """~47 positives / ~63 negatives across 21 meetings — the id=2-comparable full-scale shape."""
    rng = np.random.default_rng(seed)
    centers = [rng.standard_normal(dim) for _ in range(n_officials)]
    samples = []
    for m in range(meetings):
        # a random handful of officials speak at each meeting (~47 positives total)
        for o in rng.choice(n_officials, size=int(rng.integers(1, 4)), replace=False):
            samples.append(
                Sample(int(o) + 1, f"m{m}", _unit(centers[o] + noise * rng.standard_normal(dim)))
            )
        for _ in range(3):  # 3 negatives/meeting
            samples.append(Sample(None, f"m{m}", _unit(rng.standard_normal(dim))))
    return samples


def test_full_scale_sweep_stays_within_budget():
    import time

    from actalux.diarization.matching import CURVE_PRECISION_BARS

    samples = _scale_samples()
    n_pos = sum(1 for s in samples if s.person_id is not None)
    n_neg = sum(1 for s in samples if s.person_id is None)
    start = time.perf_counter()
    multi = nested_lomo_multi_bar(samples, precision_bars=CURVE_PRECISION_BARS)
    elapsed = time.perf_counter() - start
    print(f"\n[perf] {n_pos} pos / {n_neg} neg, 5-bar nested curve: {elapsed:.1f}s")
    assert set(multi) == set(CURVE_PRECISION_BARS)
    # The full-scale sweep must stay within the ~60s budget (design note perf guard).
    assert elapsed < 60.0
