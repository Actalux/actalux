"""Unit tests for sample hygiene: twin-negative quarantine + confirmed-positive vetting."""

from __future__ import annotations

import math

import pytest

from actalux.diarization.hygiene import (
    CONFIRMED_CORE_FLOOR,
    NEAR_BAND_FLOOR,
    QUARANTINE_BOUND,
    quarantine_twin_negatives,
    vet_confirmed_positives,
)
from actalux.diarization.matching import Sample

A = (1.0, 0.0, 0.0)
B = (0.0, 1.0, 0.0)
C = (0.0, 0.0, 1.0)


def _at_cosine(c: float) -> tuple[float, float, float]:
    """A unit vector at exactly cosine ``c`` to ``A`` (rotated in the A-B plane)."""
    return (c, math.sqrt(1.0 - c * c), 0.0)


def _pos(person, meeting, vec, *, confidence="inferred_medium", doc=None, label=None):
    return Sample(person, meeting, vec, confidence=confidence, document_id=doc, cluster_label=label)


def _neg(meeting, vec, *, doc=None, label=None):
    return Sample(None, meeting, vec, document_id=doc, cluster_label=label)


# --- quarantine_twin_negatives -----------------------------------------------------------------


def test_twin_negative_is_quarantined_with_receipt():
    # The cal-15 mechanism: a "citizen" negative that IS the official's voice (mean cosine
    # inside the official's own cross-meeting range) leaves the metric — with a receipt.
    samples = [
        _pos(1, "m1", A),
        _pos(1, "m2", A),
        _neg("m3", A, doc=30, label="SPEAKER_07"),
        _neg("m4", C),
    ]
    result = quarantine_twin_negatives(samples)
    assert [s.person_id for s in result.kept] == [1, 1, None]  # order preserved, twin gone
    assert len(result.quarantined) == 1
    q = result.quarantined[0]
    assert q.person_id == 1 and q.score == pytest.approx(1.0)
    assert (q.sample.document_id, q.sample.cluster_label) == (30, "SPEAKER_07")
    assert result.near_band == []


def test_near_band_negative_is_kept_but_reported():
    # Just under the bound: still a citizen in the metric, but surfaced so boundary drift
    # is visible instead of silently re-poisoning the FP count.
    mid = (NEAR_BAND_FLOOR + QUARANTINE_BOUND) / 2
    samples = [_pos(1, "m1", A), _pos(1, "m2", A), _neg("m3", _at_cosine(mid))]
    result = quarantine_twin_negatives(samples)
    assert len(result.kept) == 3 and result.quarantined == []
    assert len(result.near_band) == 1
    assert result.near_band[0].score == pytest.approx(mid, abs=1e-6)


def test_distinct_negative_is_untouched():
    samples = [_pos(1, "m1", A), _neg("m2", C)]
    result = quarantine_twin_negatives(samples)
    assert len(result.kept) == 2 and not result.quarantined and not result.near_band


def test_quarantine_scores_mean_per_person_not_pooled_across_officials():
    # The negative sits at 0.5 to EACH of two different officials; pooled across all positives
    # it would look official-like, but no single person's mean clears the bound -> kept.
    samples = [
        _pos(1, "m1", A),
        _pos(2, "m2", B),
        _neg("m3", (0.5, 0.5, math.sqrt(0.5))),  # cosine exactly 0.5 to A and to B
    ]
    result = quarantine_twin_negatives(samples)
    assert result.quarantined == []


def test_quarantine_receipt_names_the_best_matching_official():
    samples = [_pos(1, "m1", A), _pos(2, "m2", B), _neg("m3", A)]
    result = quarantine_twin_negatives(samples)
    assert len(result.quarantined) == 1 and result.quarantined[0].person_id == 1


def test_quarantine_noop_without_positives_or_negatives():
    negs_only = [_neg("m1", A)]
    assert quarantine_twin_negatives(negs_only).kept == negs_only
    pos_only = [_pos(1, "m1", A)]
    assert quarantine_twin_negatives(pos_only).kept == pos_only


# --- vet_confirmed_positives ---------------------------------------------------------------


def test_alien_positive_is_quarantined_against_confirmed_centroid():
    # The Patel-doc2549 shape: a person with a human-confirmed voice carries one inferred
    # anchor on a completely different voice -> vetted out, with a receipt.
    samples = [
        _pos(1, "m1", A, confidence="confirmed"),
        _pos(1, "m2", A, confidence="confirmed"),
        _pos(1, "m3", C, doc=2549, label="SPEAKER_03"),
        _neg("m4", B),
    ]
    kept, alien = vet_confirmed_positives(samples)
    assert [s.meeting_key for s in kept] == ["m1", "m2", "m4"]
    assert len(alien) == 1
    q = alien[0]
    assert q.person_id == 1 and q.score == pytest.approx(0.0)
    assert (q.sample.document_id, q.sample.cluster_label) == (2549, "SPEAKER_03")


def test_unconfirmed_person_is_not_vetted():
    # No confirmed sample -> no trusted voice to vet against; coherence is Gate A's job.
    samples = [_pos(1, "m1", A), _pos(1, "m2", C)]
    kept, alien = vet_confirmed_positives(samples)
    assert kept == samples and alien == []


def test_genuine_cross_meeting_variation_survives_the_floor():
    # Real same-voice variation (cross-doc cosine ~0.66-0.92 measured) sits far above the
    # floor; the vet only drops provably-alien voices.
    close = _at_cosine(0.66)
    samples = [_pos(1, "m1", A, confidence="confirmed"), _pos(1, "m2", close)]
    kept, alien = vet_confirmed_positives(samples)
    assert len(kept) == 2 and alien == []
    assert CONFIRMED_CORE_FLOOR < 0.66


def test_contradicted_confirmation_surfaces_with_enough_agreement():
    # Three agreeing confirmations + one confirmed sample on another voice: the centroid is
    # dominated by the majority (cos(C, centroid) ~ 0.32 < floor), so the contradicting
    # CONFIRMED sample is itself quarantined and surfaced for review.
    samples = [
        _pos(1, "m1", A, confidence="confirmed"),
        _pos(1, "m2", A, confidence="confirmed"),
        _pos(1, "m3", A, confidence="confirmed"),
        _pos(1, "m4", C, confidence="confirmed"),
    ]
    kept, alien = vet_confirmed_positives(samples)
    assert [s.meeting_key for s in kept] == ["m1", "m2", "m3"]
    assert alien[0].sample.meeting_key == "m4"


def test_two_conflicting_confirmations_survive_the_floor_by_design():
    # The DOCUMENTED limit: with only 1-vs-1 agreement the wrong confirmation drags the
    # centroid toward itself (each side scores ~0.71) — and 2-vs-1 still lands at ~0.45,
    # above the floor. Conflicting confirmations are the collapse guard's and the human
    # reviewer's territory; the floor only drops provably-alien voices.
    samples = [
        _pos(1, "m1", A, confidence="confirmed"),
        _pos(1, "m2", A, confidence="confirmed"),
        _pos(1, "m3", C, confidence="confirmed"),
    ]
    kept, alien = vet_confirmed_positives(samples)
    assert len(kept) == 3 and alien == []


def test_vetting_is_per_person():
    # Person 2's samples are never scored against person 1's confirmed voice.
    samples = [
        _pos(1, "m1", A, confidence="confirmed"),
        _pos(1, "m2", A, confidence="confirmed"),
        _pos(2, "m3", C),
    ]
    kept, alien = vet_confirmed_positives(samples)
    assert len(kept) == 3 and alien == []


def test_vet_then_quarantine_order_protects_citizens():
    # An alien positive left in the comparison set would quarantine citizens matching the
    # WRONG voice: person 1's alien anchor is voice C; a genuine citizen also near C must
    # remain a citizen once the alien is vetted out first.
    samples = [
        _pos(1, "m1", A, confidence="confirmed"),
        _pos(1, "m2", A, confidence="confirmed"),
        _pos(1, "m3", C),  # alien anchor (wrong voice)
        _neg("m4", C),  # genuine citizen who happens to sound like voice C
    ]
    vetted, alien = vet_confirmed_positives(samples)
    assert len(alien) == 1
    result = quarantine_twin_negatives(vetted)
    assert result.quarantined == []  # the citizen survives because the alien voice is gone
    assert any(s.person_id is None for s in result.kept)
