"""Tests for G4 vote linkage (scripts/build_land_use_cases.match_vote)."""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

_spec = importlib.util.spec_from_file_location(
    "build_land_use_cases", Path(__file__).parent.parent / "scripts" / "build_land_use_cases.py"
)
_mod = importlib.util.module_from_spec(_spec)
sys.modules["build_land_use_cases"] = _mod
_spec.loader.exec_module(_mod)
match_vote = _mod.match_vote

BODY = (
    "1. 42 North Central Avenue - Conditional Use Permit\n"
    "Ira Berkowitz made a motion to approve the conditional use permit\n"
    "for a restaurant. The motion carried unanimously."
)


def test_single_verbatim_motion_links() -> None:
    votes = [
        {
            "id": 7,
            "document_id": 1,
            "motion": "a motion to approve the conditional use permit for a restaurant",
        },
        {"id": 8, "document_id": 1, "motion": "a motion to adjourn the meeting"},
    ]
    assert match_vote(BODY, votes) == (7, False)


def test_motion_wrapped_across_lines_still_links() -> None:
    # The body wraps mid-motion; quote_in's whitespace tolerance covers it.
    votes = [
        {
            "id": 7,
            "document_id": 1,
            "motion": "motion to approve the conditional use permit for a restaurant",
        }
    ]
    assert match_vote(BODY, votes) == (7, False)


def test_multiple_candidates_go_to_review_not_guessed() -> None:
    votes = [
        {"id": 7, "document_id": 1, "motion": "motion to approve"},
        {"id": 8, "document_id": 1, "motion": "The motion carried unanimously"},
    ]
    assert match_vote(BODY, votes) == (None, True)


def test_no_match_stays_null() -> None:
    votes = [{"id": 7, "document_id": 1, "motion": "motion to approve the site plan at 1 Elm"}]
    assert match_vote(BODY, votes) == (None, False)


def test_empty_motion_never_matches() -> None:
    assert match_vote(BODY, [{"id": 7, "document_id": 1, "motion": None}]) == (None, False)
