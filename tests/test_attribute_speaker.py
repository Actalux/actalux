"""Unit tests for the rejected-slot attribution tool's guard logic (no DB)."""

from __future__ import annotations

import pytest

import scripts.attribute_speaker as attr
from actalux.errors import ActaluxError

DOC = {"id": 2145, "entity_id": 7, "meeting_date": "2025-12-10"}
REJECTED = {"id": 99, "subject_id": 1468, "confidence": "rejected", "basis": "presenter_intro"}
SUBJECT = {"id": 20, "person_id": 20, "publishable": True, "canonical_name": "Kim Hurst"}
MEMBERSHIP = {
    "subject_id": 20,
    "entity_id": 7,
    "role": "Director",
    "start_date": "2020-06-10",
    "end_date": "2026-03-11",
}


def _plan(**overrides):
    kwargs = dict(
        document=DOC,
        entity_id=7,
        slot_rows=[REJECTED],
        subjects=[SUBJECT],
        membership=MEMBERSHIP,
        cluster_label="SPEAKER_13",
        name="Kim Hurst",
        subject_names={1468: "Gina Tarte"},
    )
    kwargs.update(overrides)
    return attr.plan_attribution(**kwargs)


def test_valid_plan():
    plan = _plan()
    assert plan.rejected_row_id == 99
    assert plan.rejected_subject_name == "Gina Tarte"
    assert plan.subject_id == 20
    assert plan.subject_name == "Kim Hurst"
    assert plan.warnings == ()


def test_wrong_body_document_refused():
    with pytest.raises(ActaluxError, match="belongs to entity"):
        _plan(entity_id=8)


def test_empty_slot_refused():
    with pytest.raises(ActaluxError, match="exactly one identity row"):
        _plan(slot_rows=[])


def test_non_rejected_slot_refused():
    confirmed = dict(REJECTED, confidence="confirmed")
    with pytest.raises(ActaluxError, match="not 'rejected'"):
        _plan(slot_rows=[confirmed])


def test_unknown_name_refused():
    with pytest.raises(ActaluxError, match="matched 0"):
        _plan(subjects=[])


def test_unpublishable_subject_refused():
    with pytest.raises(ActaluxError, match="publishable"):
        _plan(subjects=[dict(SUBJECT, publishable=False)])


def test_non_member_refused():
    with pytest.raises(ActaluxError, match="no roster membership"):
        _plan(membership=None)


def test_out_of_term_meeting_warns_but_plans():
    late = dict(MEMBERSHIP, end_date="2021-05-19")
    plan = _plan(membership=late)
    assert any("postdates" in w for w in plan.warnings)
