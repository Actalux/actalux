"""Unit tests for the roster seeder's pure grouping logic (no DB).

build_subjects groups roster entries by slug across bodies so one person who sits
on more than one body becomes a single subject with one membership per body and a
unioned alias set. The clobber test pins the bug the grouping fixes: a body that
lists only a surname must not wipe another body's full-name alias for that person.
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))

from seed_roster import _alias_rows, build_subjects, slugify  # noqa: E402


def _norms(plan: dict) -> set[str]:
    rows = _alias_rows(0, plan["subject"]["canonical_name"], sorted(plan["aliases"]))
    return {r["normalized_alias"] for r in rows}


def test_slugify_basic() -> None:
    assert slugify("Susan Buse") == "susan-buse"
    assert slugify("Alex Berger III") == "alex-berger-iii"


def test_single_body_member_one_membership() -> None:
    bodies = {
        "board-of-adjustment": [
            {"canonical_name": "Liza Streett", "aliases": [], "role": "Board member"}
        ]
    }
    groups = build_subjects(bodies, {"board-of-adjustment": {"id": 4}}, place_id=1)
    assert set(groups) == {"liza-streett"}
    plan = groups["liza-streett"]
    assert plan["subject"]["place_id"] == 1
    assert [m["entity_id"] for m in plan["memberships"]] == [4]
    assert "liza streett" in _norms(plan)


def test_cross_body_member_one_subject_two_memberships() -> None:
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
    groups = build_subjects(bodies, ebb, place_id=1)
    plan = groups["susan-buse"]
    # one subject, a membership per body, each carrying its own role
    roles = {(m["entity_id"], m["role"]) for m in plan["memberships"]}
    assert roles == {(2, "Councilmember"), (3, "Commissioner")}
    # the council surname AND the full name both resolve
    assert {"buse", "susan buse"} <= _norms(plan)


def test_aliases_not_clobbered_across_bodies() -> None:
    # Richard Lintz sits on all three; only council lists the surname variants, only
    # PC/BoA list "Rich Lintz". The union must keep them all.
    bodies = {
        "council": [{"canonical_name": "Richard Lintz", "aliases": ["Lintz", "Linz"]}],
        "plan-commission": [{"canonical_name": "Richard Lintz", "aliases": ["Rich Lintz"]}],
        "board-of-adjustment": [{"canonical_name": "Richard Lintz", "aliases": ["Rich Lintz"]}],
    }
    ebb = {"council": {"id": 2}, "plan-commission": {"id": 3}, "board-of-adjustment": {"id": 4}}
    plan = build_subjects(bodies, ebb, place_id=1)["richard-lintz"]
    assert len(plan["memberships"]) == 3
    assert {"lintz", "linz", "rich lintz", "richard lintz"} <= _norms(plan)
