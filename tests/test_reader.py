"""Unit tests for the reader-side speaker overlay (pure, dict-based; no DB)."""

from __future__ import annotations

from actalux.diarization.reader import (
    build_meeting_speakers,
    clusters_in_window,
    resolve_speakers,
    speakers_in_window,
)


def _turns() -> list[dict]:
    return [
        {
            "cluster_label": "SPEAKER_00",
            "start_seconds": 0.0,
            "end_seconds": 10.0,
            "words": [{"word": "Hello", "start": 0.0, "end": 1.0}],
        },
        {
            "cluster_label": "SPEAKER_01",
            "start_seconds": 10.0,
            "end_seconds": 20.0,
            "words": [{"word": "Second", "start": 10.0, "end": 11.0}],
        },
        {
            "cluster_label": "SPEAKER_00",
            "start_seconds": 20.0,
            "end_seconds": 30.0,
            "words": [],
        },
    ]


def _identity_rows() -> list[dict]:
    return [
        {
            "cluster_label": "SPEAKER_00",
            "confidence": "confirmed",
            "basis": "rollcall",
            "subject_id": 7,
            "subject": {"slug": "jane-harris", "canonical_name": "Jane Harris"},
        },
        # A row with no subject (e.g. a service-key caller seeing an ungated row): dropped.
        {
            "cluster_label": "SPEAKER_01",
            "confidence": "inferred_low",
            "basis": None,
            "subject_id": None,
            "subject": None,
        },
    ]


def test_resolve_speakers_keeps_only_named_subjects():
    resolved = resolve_speakers(_identity_rows())
    assert set(resolved) == {"SPEAKER_00"}  # SPEAKER_01 has no subject -> dropped
    assert resolved["SPEAKER_00"] == {
        "name": "Jane Harris",
        "slug": "jane-harris",
        "confidence": "confirmed",
        "basis": "rollcall",
    }


def test_resolve_speakers_drops_ungated_even_with_a_subject():
    # Defense in depth: a low/medium row that DOES name a subject (a service-key caller
    # can see these) must NOT surface the name — only high/confirmed are public.
    rows = [
        {
            "cluster_label": "SPEAKER_02",
            "confidence": "inferred_medium",
            "basis": "self_intro",
            "subject_id": 9,
            "subject": {"slug": "bob-stevens", "canonical_name": "Bob Stevens"},
        }
    ]
    assert resolve_speakers(rows) == {}


def test_clusters_in_window_zero_width_returns_empty():
    assert clusters_in_window(_turns(), 5.0, 5.0) == []


def test_clusters_in_window_overlap_and_order():
    turns = _turns()
    assert clusters_in_window(turns, 1.0, 9.0) == ["SPEAKER_00"]
    assert clusters_in_window(turns, 5.0, 25.0) == ["SPEAKER_00", "SPEAKER_01"]
    # touching boundaries don't count (half-open window): exactly [10,20) is SPEAKER_01
    assert clusters_in_window(turns, 10.0, 20.0) == ["SPEAKER_01"]


def test_clusters_in_window_dedups_recurring_speaker():
    # SPEAKER_00 speaks twice (0-10 and 20-30); a window spanning both lists it once.
    assert clusters_in_window(_turns(), 0.0, 30.0) == ["SPEAKER_00", "SPEAKER_01"]


def test_speakers_in_window_attaches_gated_identity_or_none():
    identities = resolve_speakers(_identity_rows())
    spans = speakers_in_window(_turns(), identities, 5.0, 15.0)
    assert spans == [
        {"cluster_label": "SPEAKER_00", "speaker": identities["SPEAKER_00"]},
        {"cluster_label": "SPEAKER_01", "speaker": None},  # not gated -> anonymous
    ]


def test_build_meeting_speakers_shape():
    layer = build_meeting_speakers(_turns(), _identity_rows())
    assert set(layer["speakers"]) == {"SPEAKER_00"}
    assert len(layer["turns"]) == 3
    first, second = layer["turns"][0], layer["turns"][1]
    assert first["speaker"]["name"] == "Jane Harris"  # gated identity attached
    assert second["speaker"] is None  # anonymous cluster keeps its turn
    assert first["words"] == [{"word": "Hello", "start": 0.0, "end": 1.0}]
    assert layer["turns"][2]["words"] == []  # missing/empty words tolerated
