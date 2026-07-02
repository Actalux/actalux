"""Unit tests for voiceprint enrollment selection + row shaping (no DB/GPU)."""

from __future__ import annotations

import scripts.enroll_voiceprints as ev


def _subjects() -> dict[int, dict]:
    return {
        10: {"id": 10, "person_id": 100, "publishable": True, "canonical_name": "Kami Waldman"},
        11: {"id": 11, "person_id": 101, "publishable": True, "canonical_name": "Susan Buse"},
        12: {"id": 12, "person_id": 102, "publishable": False, "canonical_name": "Hidden Person"},
        13: {"id": 13, "person_id": None, "publishable": True, "canonical_name": "No Person Link"},
    }


def _identity(id_, doc, cluster, subject_id, confidence, basis):
    return {
        "id": id_,
        "document_id": doc,
        "cluster_label": cluster,
        "subject_id": subject_id,
        "confidence": confidence,
        "basis": basis,
    }


def test_select_enrollable_includes_confirmed_and_name_anchored_high():
    rows = [
        _identity(1, 5, "SPEAKER_00", 10, "inferred_high", "rollcall"),
        _identity(2, 6, "SPEAKER_01", 11, "confirmed", "manual"),
    ]
    out = ev.select_enrollable(rows, _subjects(), confirmed_only=False)
    assert {e.person_id for e in out} == {100, 101}
    assert {e.source_basis for e in out} == {"rollcall", "manual"}


def test_select_enrollable_confirmed_only_excludes_name_anchored():
    rows = [
        _identity(1, 5, "SPEAKER_00", 10, "inferred_high", "rollcall"),
        _identity(2, 6, "SPEAKER_01", 11, "confirmed", None),
    ]
    out = ev.select_enrollable(rows, _subjects(), confirmed_only=True)
    assert [e.person_id for e in out] == [101]
    assert out[0].source_basis == "manual"  # confirmed row with no basis -> 'manual'


def test_select_enrollable_never_enrolls_voiceprint_basis():
    # A biometric-derived high must never train the gallery (poison loop).
    rows = [_identity(1, 5, "SPEAKER_00", 10, "inferred_high", "voiceprint")]
    assert ev.select_enrollable(rows, _subjects(), confirmed_only=False) == []


def test_select_enrollable_excludes_low_unpublishable_and_unlinked():
    rows = [
        _identity(1, 5, "SPEAKER_00", 10, "inferred_low", "rollcall"),  # below high
        _identity(2, 5, "SPEAKER_01", 12, "inferred_high", "rollcall"),  # not publishable
        _identity(3, 5, "SPEAKER_02", 13, "inferred_high", "rollcall"),  # no person_id
        _identity(4, 5, "SPEAKER_03", None, "inferred_high", "rollcall"),  # no subject
        _identity(5, 5, "SPEAKER_04", 999, "inferred_high", "rollcall"),  # unknown subject
    ]
    assert ev.select_enrollable(rows, _subjects(), confirmed_only=False) == []


def test_cluster_spans_filters_and_sorts():
    turns = [
        {"cluster_label": "SPEAKER_00", "start_seconds": 9.0, "end_seconds": 12.0},
        {"cluster_label": "SPEAKER_01", "start_seconds": 0.0, "end_seconds": 4.0},
        {"cluster_label": "SPEAKER_00", "start_seconds": 0.0, "end_seconds": 4.0},
    ]
    assert ev.cluster_spans(turns, "SPEAKER_00") == [[0.0, 4.0], [9.0, 12.0]]


def test_span_seconds_sums_durations():
    assert ev.span_seconds([[0.0, 4.0], [9.0, 12.0]]) == 7.0


def test_voiceprint_row_shape():
    from actalux.diarization.pooling import Pooled

    ec = ev.EnrollableCluster(
        person_id=100,
        source_subject_id=10,
        source_identity_id=1,
        document_id=5,
        cluster_label="SPEAKER_00",
        source_basis="rollcall",
        canonical_name="Kami Waldman",
    )
    pooled = Pooled(vector=(0.1, 0.2, 0.3), purity=0.9, n_turns=5, coherent_turns=4, seconds=42.0)
    row = ev.voiceprint_row(ec, pooled, "wespeaker", calibration_id=7)
    assert row == {
        "person_id": 100,
        "source_subject_id": 10,
        "source_document_id": 5,
        "source_identity_id": 1,
        "cluster_label": "SPEAKER_00",
        "embedding": [0.1, 0.2, 0.3],
        "source_basis": "rollcall",
        "model": "wespeaker",
        "seconds": 42.0,
        "purity": 0.9,
        "n_turns": 5,
        "coherent_turns": 4,
        "calibration_id": 7,
    }


def test_superseded_doc_ids():
    docs = [
        {"id": 1, "replaces_id": None},
        {"id": 2, "replaces_id": 99},
        {"id": 3, "replaces_id": None},
    ]
    assert ev.superseded_doc_ids(docs) == {2}
