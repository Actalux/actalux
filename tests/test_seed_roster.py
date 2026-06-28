"""Unit tests for the roster seeder's pure grouping logic (no DB).

build_people groups roster entries by slug into one person with one subject PER body
(Model B): the primary board keeps the clean slug = the person slug, every other board
gets an internal '{slug}--{body_slug}'. Aliases are unioned across the person's bodies
and copied onto each per-board subject (the clobber test pins the bug the union fixes:
a body that lists only a surname must not wipe another body's full-name alias). A slug
that maps to two different canonical names hard-fails (never a silent merge).
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))

from seed_roster import _alias_rows, build_people, slugify  # noqa: E402


def _norms(subject_plan: dict) -> set[str]:
    """Normalized aliases a per-board subject plan would write."""
    rows = _alias_rows(
        0, subject_plan["subject"]["canonical_name"], sorted(subject_plan["aliases"])
    )
    return {r["normalized_alias"] for r in rows}


def test_slugify_basic() -> None:
    assert slugify("Susan Buse") == "susan-buse"
    assert slugify("Alex Berger III") == "alex-berger-iii"


def test_single_body_member_one_subject() -> None:
    bodies = {
        "board-of-adjustment": [
            {"canonical_name": "Liza Streett", "aliases": [], "role": "Board member"}
        ]
    }
    plans = build_people(bodies, {"board-of-adjustment": {"id": 4}}, place_id=1)
    assert set(plans) == {"liza-streett"}
    plan = plans["liza-streett"]
    assert plan["person"]["slug"] == "liza-streett"
    assert len(plan["subjects"]) == 1
    sp = plan["subjects"][0]
    assert sp["subject"]["slug"] == "liza-streett"  # single board -> clean slug
    assert sp["subject"]["place_id"] == 1
    assert sp["subject"]["entity_id"] == 4
    assert sp["membership"]["entity_id"] == 4
    assert "liza streett" in _norms(sp)


def test_cross_body_member_one_person_per_board_subjects() -> None:
    bodies = {
        "council": [
            {
                "canonical_name": "Susan Buse",
                "aliases": ["Buse"],
                "role": "Councilmember",
                "term_start": "2020-06-23",
                "term_end": None,
            }
        ],
        "plan-commission": [
            {"canonical_name": "Susan Buse", "aliases": [], "role": "Commissioner"}
        ],
    }
    ebb = {"council": {"id": 2}, "plan-commission": {"id": 3}}
    plan = build_people(bodies, ebb, place_id=1)["susan-buse"]
    # one person, two per-board subjects
    by_eid = {sp["entity_id"]: sp for sp in plan["subjects"]}
    assert set(by_eid) == {2, 3}
    # primary board (lowest entity_id = council) keeps the clean slug; PC gets the suffix
    assert by_eid[2]["subject"]["slug"] == "susan-buse"
    assert by_eid[3]["subject"]["slug"] == "susan-buse--plan-commission"
    # each subject carries its own board's role on both the subject + the membership
    assert by_eid[2]["subject"]["metadata"]["role"] == "Councilmember"
    assert by_eid[3]["subject"]["metadata"]["role"] == "Commissioner"
    assert by_eid[2]["membership"]["role"] == "Councilmember"
    assert by_eid[3]["membership"]["role"] == "Commissioner"
    # the unioned aliases (surname + full name) are copied onto BOTH per-board subjects
    assert {"buse", "susan buse"} <= _norms(by_eid[2])
    assert {"buse", "susan buse"} <= _norms(by_eid[3])


def test_aliases_not_clobbered_across_bodies() -> None:
    # Richard Lintz sits on all three; only council lists the surname variants, only
    # PC/BoA list "Rich Lintz". The union must keep them all, on every per-board subject.
    bodies = {
        "council": [{"canonical_name": "Richard Lintz", "aliases": ["Lintz", "Linz"]}],
        "plan-commission": [{"canonical_name": "Richard Lintz", "aliases": ["Rich Lintz"]}],
        "board-of-adjustment": [{"canonical_name": "Richard Lintz", "aliases": ["Rich Lintz"]}],
    }
    ebb = {"council": {"id": 2}, "plan-commission": {"id": 3}, "board-of-adjustment": {"id": 4}}
    plan = build_people(bodies, ebb, place_id=1)["richard-lintz"]
    assert len(plan["subjects"]) == 3
    by_eid = {sp["entity_id"]: sp for sp in plan["subjects"]}
    assert by_eid[2]["subject"]["slug"] == "richard-lintz"
    assert by_eid[3]["subject"]["slug"] == "richard-lintz--plan-commission"
    assert by_eid[4]["subject"]["slug"] == "richard-lintz--board-of-adjustment"
    for sp in plan["subjects"]:
        assert {"lintz", "linz", "rich lintz", "richard lintz"} <= _norms(sp)


def test_same_slug_different_name_hard_fails() -> None:
    # Two entries collide on the slugified name but disagree on canonical_name: a roster
    # error (typo or accidental merge). The seeder must refuse, never silently merge.
    bodies = {
        "council": [{"canonical_name": "Sam Smith", "aliases": []}],
        "plan-commission": [{"canonical_name": "Samuel Smith", "slug": "sam-smith", "aliases": []}],
    }
    ebb = {"council": {"id": 2}, "plan-commission": {"id": 3}}
    with pytest.raises(SystemExit, match="two names"):
        build_people(bodies, ebb, place_id=1)
