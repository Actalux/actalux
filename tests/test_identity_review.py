"""Unit tests for the speaker-identity review-queue shaping (pure; no DB)."""

from __future__ import annotations

from actalux.identity.review import shape_review_queue


def _docs() -> dict[int, dict]:
    return {
        10: {"meeting_date": "2026-06-10", "meeting_title": "Council June 10"},
        11: {"meeting_date": "2026-06-03", "meeting_title": "Council June 3"},
    }


def test_shape_review_queue_joins_and_sorts():
    rows = [
        {
            "document_id": 10,
            "cluster_label": "SPEAKER_02",
            "confidence": "inferred_low",
            "basis": "rollcall",
            "subject": {"slug": "jane-harris", "canonical_name": "Jane Harris"},
        },
        {
            "document_id": 11,
            "cluster_label": "SPEAKER_00",
            "confidence": "inferred_medium",
            "basis": "self_intro",
            "subject": {"slug": "bob-stevens", "canonical_name": "Bob Stevens"},
        },
    ]
    queue = shape_review_queue(rows, _docs())
    # sorted by meeting_date (June 3 before June 10)
    assert [r["document_id"] for r in queue] == [11, 10]
    assert queue[0]["candidate_subject"] == "Bob Stevens"
    assert queue[0]["meeting_title"] == "Council June 3"
    assert queue[1]["candidate_slug"] == "jane-harris"
    assert queue[1]["confidence"] == "inferred_low"


def test_shape_review_queue_tolerates_missing_doc_and_subject():
    rows = [
        {
            "document_id": 99,  # not in docs map
            "cluster_label": "SPEAKER_05",
            "confidence": "inferred_low",
            "basis": None,
            "subject": None,  # no candidate subject
        }
    ]
    queue = shape_review_queue(rows, _docs())
    assert len(queue) == 1
    assert queue[0]["meeting_date"] is None
    assert queue[0]["candidate_subject"] is None
    assert queue[0]["basis"] is None
