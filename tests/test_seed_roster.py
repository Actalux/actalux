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
    assert sp["memberships"] == [
        {"entity_id": 4, "role": "Board member", "start_date": None, "end_date": None}
    ]
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
    assert by_eid[2]["memberships"][0]["role"] == "Councilmember"
    assert by_eid[3]["memberships"][0]["role"] == "Commissioner"
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


class TestInterruptedService:
    """A member whose service was interrupted needs one window per stretch.

    Michelle Harris sat on the Clayton council through April 2015, was term-limited
    out, and returned via the April 2017 election. A single window spanning both
    would cover the two years she was not seated, which is exactly what the
    resolver's tenure check exists to prevent — so the roster carries a `terms`
    list and the seeder writes one membership row per entry.
    """

    _HARRIS = {
        "canonical_name": "Michelle Harris",
        "aliases": ["Harris"],
        "role": "Mayor",
        "terms": [
            {"start": None, "end": "2015-04-28", "role": "Alderman"},
            {"start": "2017-05-09", "end": "2025-04-22", "role": "Mayor"},
        ],
    }

    def _plan(self):
        plans = build_people({"council": [self._HARRIS]}, {"council": {"id": 2}}, place_id=1)
        return plans["michelle-harris"]["subjects"][0]

    def test_each_window_becomes_its_own_membership(self) -> None:
        rows = self._plan()["memberships"]
        assert [(r["start_date"], r["end_date"]) for r in rows] == [
            (None, "2015-04-28"),
            ("2017-05-09", "2025-04-22"),
        ]

    def test_per_window_role_wins_over_the_member_role(self) -> None:
        # She sat as Alderman in the first stretch and Mayor in the second.
        assert [r["role"] for r in self._plan()["memberships"]] == ["Alderman", "Mayor"]

    def test_single_window_members_are_unaffected(self) -> None:
        member = {
            "canonical_name": "Susan Buse",
            "aliases": ["Buse"],
            "role": "Councilmember",
            "term_start": "2019-08-13",
            "term_end": None,
        }
        plans = build_people({"council": [member]}, {"council": {"id": 2}}, place_id=1)
        rows = plans["susan-buse"]["subjects"][0]["memberships"]
        assert rows == [
            {"entity_id": 2, "role": "Councilmember", "start_date": "2019-08-13", "end_date": None}
        ]

    def test_the_gap_is_not_covered(self) -> None:
        # The point of the split: a 2016 date falls in neither window.
        from datetime import date

        from actalux.graph.resolve import Membership, RosterSubject

        rows = self._plan()["memberships"]
        subject = RosterSubject(
            subject_id=3,
            aliases=frozenset({"harris"}),
            memberships=tuple(
                Membership(
                    entity_id=r["entity_id"],
                    start_date=date.fromisoformat(r["start_date"]) if r["start_date"] else None,
                    end_date=date.fromisoformat(r["end_date"]) if r["end_date"] else None,
                )
                for r in rows
            ),
        )
        assert subject.seated_on(2, date(2015, 1, 13))  # first stretch (open start)
        assert subject.seated_on(2, date(2019, 5, 14))  # second stretch
        assert not subject.seated_on(2, date(2016, 6, 1))  # the gap
