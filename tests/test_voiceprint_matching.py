"""Unit tests for the voiceprint matcher math: allowed-gallery, Gate A, nested LOMO."""

from __future__ import annotations

import math

import numpy as np

from actalux.diarization.labelqa import (
    coherent_core_asnorm,
)
from actalux.diarization.matching import (
    CONSENSUS_MIN_CORE_MEETINGS,
    CONSENSUS_MIN_FAMILIES,
    DEFAULT_COLLAPSE_BOUNDS,
    DEFAULT_SCORE_NORMS,
    DEFAULT_THRESHOLDS,
    GATE_A_MIN_CORE,
    GridPoint,
    Metrics,
    Sample,
    best_operating_point,
    build_sim,
    enabled_officials,
    enablement_delta,
    evaluate_grid,
    gate_official,
    gate_officials,
    nested_leave_one_meeting_out,
    nested_lomo_multi_bar,
    pareto_frontier,
    person_scores,
    recall_by_confidence,
    select_operating_point,
    trusted_tier_recall,
)

A = (1.0, 0.0, 0.0)
B = (0.0, 1.0, 0.0)
C = (0.0, 0.0, 1.0)

# Two independent evidence families (adjacency vs vote) — the consensus gate needs ≥2 distinct
# families on one coherent voice, so multi-family fixtures use these bases.
ADJ, VOTE = "rollcall", "vote_anchor"


def _s(person, meeting, vec, basis=None):
    return Sample(person_id=person, meeting_key=meeting, embedding=vec, basis=basis)


def _two_family(person, vec, meetings=("m1", "m2", "m3")):
    """A consensus-eligible official: the SAME coherent voice across ``meetings`` with ≥2 families
    (adjacency + vote) landing on it — the minimum a non-confirmed official needs to enable."""
    bases = [ADJ, VOTE, ADJ, VOTE]
    return [_s(person, m, vec, basis=bases[i % len(bases)]) for i, m in enumerate(meetings)]


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


def test_enabled_officials_single_family_does_not_enable():
    # p1 agrees across three meetings but on ONE evidence family (all adjacency); consensus needs
    # ≥2 independent families on the coherent voice, so a single-family official is NOT enabled.
    train = [_s(1, "m1", A, ADJ), _s(1, "m2", A, ADJ), _s(1, "m3", A, ADJ)]
    assert enabled_officials(train, core_floor=0.5, min_core=2, collapse_bound=0.85) == set()


def test_enabled_officials_consensus_two_families_enables():
    # The same coherent voice, now anchored by two independent families (adjacency + vote) across
    # three meetings -> consensus enables. This is the sole non-confirmed enablement path.
    train = _two_family(1, A)
    assert enabled_officials(train, core_floor=0.5, min_core=2, collapse_bound=0.85) == {1}


def test_enabled_officials_consensus_needs_three_meetings():
    # Two families but only two meetings (below CONSENSUS_MIN_CORE_MEETINGS) -> not enabled.
    train = _two_family(1, A, meetings=("m1", "m2"))
    assert enabled_officials(train, core_floor=0.5, min_core=2, collapse_bound=0.85) == set()


def test_enabled_officials_drops_collapsed_voices():
    # p1 and p2 share one voice -> a roll-call caller labeled twice -> neither enabled.
    train = [_s(1, "m1", A), _s(1, "m2", A), _s(2, "m3", A), _s(2, "m4", A)]
    assert enabled_officials(train, core_floor=0.5, min_core=2, collapse_bound=0.85) == set()


def test_nested_lomo_clean_separation_rejects_negative():
    # Two consensus-eligible officials (each 2 families across FOUR meetings on one coherent voice,
    # so that when nested LOMO holds one out the remaining three still clear the 3-meeting consensus
    # floor), cleanly separated, plus a distinct citizen the matcher must reject.
    samples = [
        *_two_family(1, A, ("m1", "m2", "m3", "m4")),
        *_two_family(2, B, ("m5", "m6", "m7", "m8")),
        _s(None, "m9", C),  # a citizen distinct from both officials
    ]
    metrics, prov = nested_leave_one_meeting_out(samples, precision_bar=0.9)
    assert metrics.macro_precision == 1.0
    assert metrics.recall == 1.0
    assert prov["n_negatives"] == 1
    assert not any(true is None for true, _ in metrics.confusions)  # negative rejected


def test_nested_lomo_counts_negative_match_as_false_positive():
    # A "negative" that is actually official 1's voice must get matched -> FP -> macroP < 1.
    samples = [
        *_two_family(1, A, ("m1", "m2", "m3")),
        *_two_family(2, B, ("m4", "m5", "m6")),
        _s(None, "m7", A),
    ]
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


def test_consensus_holds_under_asnorm_mode():
    # asnorm changes only how the coherent core is selected (the z-scored radius); the consensus
    # family/meeting requirements still gate enablement. A clean 2-family official enables in either
    # mode; asnorm never lowers the bar to admit a single-family official.
    two_fam = _two_family(1, A)
    one_fam = [_s(2, "m4", B, ADJ), _s(2, "m5", B, ADJ), _s(2, "m6", B, ADJ)]
    kw = {"score_norm": "asnorm", "z_floor": 1.0, "cohort_min": 2}
    enabled = enabled_officials(
        two_fam + one_fam, core_floor=0.5, min_core=2, collapse_bound=0.85, **kw
    )
    assert enabled == {1}  # 2-family official enabled; 1-family official excluded even under asnorm


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


# --- consensus invariant: the refit's enabled set is reproducible from its own knobs -----------


def test_refit_enabled_set_reproducible_from_gate_officials():
    # The audit block re-derives each official's enable path via gate_officials at the refit's
    # (purity, core, collapse, score_norm, z) knobs; that re-derivation must reproduce EXACTLY the
    # enabled set the selected operating point carries, or the audit could misattribute an enable.
    reproduced_any = False
    for seed in range(8):
        samples = _random_samples(seed)
        op = select_operating_point(samples, precision_bar=0.90)
        if op is None:
            continue
        reproduced_any = True
        filtered = [s for s in samples if s.purity >= op.purity_floor]
        decisions = gate_officials(
            filtered,
            core_floor=op.core_floor,
            min_core=GATE_A_MIN_CORE,
            collapse_bound=op.collapse_bound,
            score_norm=op.score_norm,
            z_floor=op.z_floor,
        )
        assert {p for p, d in decisions.items() if d.enabled} == op.enabled
    assert reproduced_any  # guard against a vacuous all-None sweep


# --- fixtures + performance -------------------------------------------------------------------


def _unit(vec):
    arr = np.asarray(vec, dtype=float)
    norm = np.linalg.norm(arr)
    return tuple((arr / (norm if norm else 1.0)).tolist())


def _random_samples(seed, *, n_officials=4, n_meetings=6, dim=16, noise=0.45, n_neg=4):
    """Deterministic messy gallery: officials with moderate coherence + near-orthogonal negatives.

    Each official speaks across three distinct meetings anchored by two evidence families
    (adjacency + vote), so a coherent official can clear the consensus gate; the moderate noise
    means some fail the coherent-core check at higher floors, keeping the selection non-trivial.
    """
    rng = np.random.default_rng(seed)
    centers = [rng.standard_normal(dim) for _ in range(n_officials)]
    samples = []
    for o in range(n_officials):
        for i, mk in enumerate(rng.choice(n_meetings, size=3, replace=False)):
            basis = ADJ if i % 2 == 0 else VOTE  # two families across the three meetings
            vec = _unit(centers[o] + noise * rng.standard_normal(dim))
            samples.append(Sample(o + 1, f"m{int(mk)}", vec, basis=basis))
    for _ in range(n_neg):
        mk = int(rng.integers(0, n_meetings))
        samples.append(Sample(None, f"m{mk}", _unit(rng.standard_normal(dim))))
    return samples


def _scale_samples(seed=0, *, n_officials=12, meetings=21, dim=256, noise=0.55):
    """~47 positives / ~63 negatives across 21 meetings — the id=2-comparable full-scale shape.

    Bases alternate by meeting parity, so an official who speaks across even- and odd-indexed
    meetings carries both evidence families and can clear the consensus gate — exercising the real
    Gate-A path (not a trivially-empty gallery) under the perf budget.
    """
    rng = np.random.default_rng(seed)
    centers = [rng.standard_normal(dim) for _ in range(n_officials)]
    samples = []
    for m in range(meetings):
        basis = ADJ if m % 2 == 0 else VOTE
        # a random handful of officials speak at each meeting (~47 positives total)
        for o in rng.choice(n_officials, size=int(rng.integers(1, 4)), replace=False):
            vec = _unit(centers[o] + noise * rng.standard_normal(dim))
            samples.append(Sample(int(o) + 1, f"m{m}", vec, basis=basis))
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


# --- lever B: confirmed samples relax Gate A coherence (fold-safe) -----------------------------


def _conf(person, meeting, vec):
    """A human-confirmed sample (the trusted-core tier Gate A relaxes coherence for)."""
    return Sample(person_id=person, meeting_key=meeting, embedding=vec, confidence="confirmed")


def test_enabled_officials_default_tier_never_trips_confirmed_bypass():
    # An incoherent, UNCONFIRMED official must still be excluded — the default confidence tier
    # must not accidentally enable it, or a plain (no-confirmation) gallery would change behavior.
    train = [_s(1, "m1", A), _s(1, "m2", B), _s(1, "m3", C)]  # orthogonal -> no coherent core
    assert enabled_officials(train, core_floor=0.5, min_core=2, collapse_bound=0.85) == set()


def test_enabled_officials_confirmed_core_enables_despite_incoherence():
    # Same scattered official, now confirmed across three distinct meetings -> enabled on the
    # trusted core even though raw self-coherence fails at the 0.5 floor.
    train = [_conf(1, "m1", A), _conf(1, "m2", B), _conf(1, "m3", C)]
    assert enabled_officials(train, core_floor=0.5, min_core=2, collapse_bound=0.85) == {1}


def test_enabled_officials_confirmed_needs_two_distinct_meetings():
    # Three confirmed samples but all in ONE meeting: the bypass needs >=2 distinct meetings, so
    # it falls through to the coherence test (which fails on orthogonal vectors) -> not enabled.
    train = [_conf(1, "m1", A), _conf(1, "m1", B), _conf(1, "m1", C)]
    assert enabled_officials(train, core_floor=0.5, min_core=2, collapse_bound=0.85) == set()


def test_enabled_officials_collapse_still_excludes_confirmed_official():
    # p1 and p2 are the SAME voice (A) across two confirmed meetings each. The collapse guard runs
    # first and excludes both: a human confirming one name can't split one voice into two people.
    train = [
        _conf(1, "m1", A), _conf(1, "m2", A),
        _conf(2, "m3", A), _conf(2, "m4", A),
    ]  # fmt: skip
    assert enabled_officials(train, core_floor=0.5, min_core=2, collapse_bound=0.85) == set()


def test_confirmed_enablement_is_fold_safe():
    # An official confirmed in exactly two meetings. With both present the bypass enables it;
    # holding out one meeting (as each nested fold does) leaves a single confirmed meeting in
    # TRAIN, below the >=2 floor, so a held-out fold can never enable an official from its own
    # confirmations. (The lone remaining sample also has no coherent core.)
    both = [_conf(1, "m1", A), _conf(1, "m2", B)]
    assert enabled_officials(both, core_floor=0.5, min_core=2, collapse_bound=0.85) == {1}
    train_without_m1 = [s for s in both if s.meeting_key != "m1"]
    assert (
        enabled_officials(train_without_m1, core_floor=0.5, min_core=2, collapse_bound=0.85)
        == set()
    )


def test_recall_by_confidence_splits_tiers_and_ignores_negatives():
    records = [
        ("confirmed", 1, 1),  # confirmed positive, recalled
        ("confirmed", 2, None),  # confirmed positive, missed
        ("inferred_high", 3, 3),  # inferred_high positive, recalled
        ("inferred_medium", 4, 4),  # not a named tier -> "other" bucket, recalled
        ("inferred_high", None, 5),  # a negative (true None) -> never counts toward recall
    ]
    out = recall_by_confidence(records)
    assert out["confirmed"] == {"positives": 2, "recalled": 1, "recall": 0.5}
    assert out["inferred_high"] == {"positives": 1, "recalled": 1, "recall": 1.0}
    assert out["other"] == {"positives": 1, "recalled": 1, "recall": 1.0}


def test_recall_by_confidence_empty_tier_reports_none():
    out = recall_by_confidence([])
    assert set(out) == {"confirmed", "inferred_high", "other"}
    assert out["confirmed"] == {"positives": 0, "recalled": 0, "recall": None}


def test_nested_lomo_provenance_carries_recall_by_confidence():
    samples = [
        _s(1, "m1", A), _s(1, "m2", A), _s(1, "m3", A),
        _s(2, "m4", B), _s(2, "m5", B), _s(2, "m6", B),
        _s(None, "m7", C),
    ]  # fmt: skip
    _m, prov = nested_leave_one_meeting_out(samples, precision_bar=0.9)
    rbc = prov["recall_by_confidence"]
    assert set(rbc) == {"confirmed", "inferred_high", "other"}
    # every positive was drawn at the default inferred_high tier; the negative never counts
    assert rbc["inferred_high"]["positives"] == 6
    assert rbc["confirmed"]["positives"] == 0


# --- Phase C: Sample.family, consensus gate decisions, Hummell subset -------------------------


def test_sample_family_from_basis_and_confidence():
    assert Sample(1, "m", A, basis="rollcall").family == "adjacency"
    assert Sample(1, "m", A, basis="vote_anchor").family == "vote"
    assert Sample(1, "m", A, basis="discourse").family == "discourse"
    # confirmed collapses to the human family regardless of basis
    assert Sample(1, "m", A, basis="discourse", confidence="confirmed").family == "human"
    # an unknown basis becomes its own family (forward-compatible)
    assert Sample(1, "m", A, basis="new_signal").family == "new_signal"


def test_gate_official_consensus_path_and_fields():
    # Two families across three meetings on the coherent voice -> consensus enable, and the decision
    # exposes the audit substrate (families present + which agree on the coherent voice).
    d = gate_official(_two_family(1, A), collapsed=False, core_floor=0.5, min_core=2)
    assert d.enabled and d.path == "consensus"
    assert d.families == {"adjacency": 2, "vote": 1}
    assert set(d.core_families) == {"adjacency", "vote"}
    assert d.core_meetings == 3


def test_gate_official_single_family_reason():
    d = gate_official(
        [_s(1, "m1", A, ADJ), _s(1, "m2", A, ADJ), _s(1, "m3", A, ADJ)],
        collapsed=False,
        core_floor=0.5,
        min_core=2,
    )
    assert not d.enabled and d.path == "not_enabled"
    assert "single family" in d.reason


def test_gate_official_collapse_vetoes_before_consensus():
    # Even a would-be 2-family consensus official is vetoed when flagged collapsed (one voice, two
    # names): the collapse guard runs first and no consensus rescues it.
    d = gate_official(_two_family(1, A), collapsed=True, core_floor=0.5, min_core=2)
    assert not d.enabled and d.path == "not_enabled" and "collapse" in d.reason


def test_gate_official_hummell_subset_survives_scattered_minority():
    # The Hummell case: four coherent anchors (2 families across 3 meetings) + six scattered anchors
    # pointing at inconsistent voices. The coherent-subset selection keeps the four and discards the
    # six, so the noisy minority does NOT knock out the coherent majority -> still enabled.
    rng = np.random.default_rng(3)
    voice = _unit(rng.standard_normal(24))

    def near():
        return _unit(np.asarray(voice) + 0.12 * rng.standard_normal(24))

    coherent = [
        _s(1, "m1", near(), ADJ),
        _s(1, "m2", near(), ADJ),
        _s(1, "m2", near(), VOTE),
        _s(1, "m3", near(), VOTE),
    ]
    scattered = [_s(1, f"s{i}", _unit(rng.standard_normal(24)), "discourse") for i in range(6)]
    d = gate_official(coherent + scattered, collapsed=False, core_floor=0.35, min_core=2)
    assert d.enabled and d.path == "consensus"
    assert d.discarded_by_family.get("discourse") == 6  # the scattered minority was discarded
    assert d.core_meetings == 3


def test_gate_officials_maps_every_official():
    train = _two_family(1, A) + [_s(2, "m4", B, ADJ), _s(2, "m5", B, ADJ)]
    decisions = gate_officials(train, core_floor=0.5, min_core=2, collapse_bound=0.85)
    assert set(decisions) == {1, 2}
    assert decisions[1].enabled and not decisions[2].enabled  # 2-family vs single-family


# --- Phase C: trusted-tier recall + enablement delta -----------------------------------------


def test_trusted_tier_recall_counts_only_trusted_positives():
    records = [
        (True, 1, 1),  # trusted, recalled
        (True, 2, None),  # trusted, missed
        (False, 3, 3),  # UNTRUSTED positive -> excluded even though it was "recalled"
        (True, None, 4),  # a negative -> never counts
    ]
    out = trusted_tier_recall(records)
    assert out == {"positives": 2, "recalled": 1, "recall": 0.5}


def test_trusted_tier_recall_empty_is_none():
    assert trusted_tier_recall([]) == {"positives": 0, "recalled": 0, "recall": None}
    # a corpus of only untrusted positives yields no headline (not a misleading 0.0 over noise)
    assert trusted_tier_recall([(False, 1, None), (False, 2, 2)])["recall"] is None


def test_nested_lomo_provenance_carries_trusted_recall():
    samples = [
        *_two_family(1, A, ("m1", "m2", "m3", "m4")),
        *_two_family(2, B, ("m5", "m6", "m7", "m8")),
        _s(None, "m9", C),
    ]
    _m, prov = nested_leave_one_meeting_out(samples, precision_bar=0.9)
    tr = prov["trusted_recall"]
    assert set(tr) == {"positives", "recalled", "recall"}
    assert tr["positives"] > 0 and tr["recall"] == 1.0  # clean separation -> perfect trusted recall


def test_enablement_delta_gained_lost_with_reasons():
    delta = enablement_delta(
        previous_enabled={1, 2},
        current_enabled={2, 3},
        current_reasons={2: "consensus", 3: "consensus", 1: "single family (adjacency) ..."},
        names={1: "Alice", 2: "Bob", 3: "Cara"},
    )
    assert [g["person_id"] for g in delta["gained"]] == [3]
    assert delta["gained"][0]["name"] == "Cara" and delta["gained"][0]["reason"] == "consensus"
    assert [z["person_id"] for z in delta["lost"]] == [1]  # demotion is visible
    assert delta["lost"][0]["name"] == "Alice" and "single family" in delta["lost"][0]["reason"]


def test_enablement_delta_no_change():
    delta = enablement_delta({1, 2}, {1, 2}, current_reasons={1: "consensus", 2: "consensus"})
    assert delta == {"gained": [], "lost": []}


def test_consensus_constants_are_sane():
    # The consensus meeting floor is strictly above the confirmed-waiver's 2 (an unconfirmed
    # official clears a higher bar), and independence needs at least two families.
    assert CONSENSUS_MIN_CORE_MEETINGS >= 3
    assert CONSENSUS_MIN_FAMILIES >= 2
