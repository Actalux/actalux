"""Roster name resolution — connections-graph §4/§7 (Phase 1 members)."""

from __future__ import annotations

from datetime import date

from actalux.graph.resolve import (
    Membership,
    Resolution,
    Roster,
    RosterSubject,
    normalize_name,
)

# --- normalize_name: systematic variants collapse, OCR garble stays distinct ----


def test_normalize_strips_school_honorifics():
    assert normalize_name("Mr. Ben Beinfeld") == "ben beinfeld"
    assert normalize_name("Ms. Stacy Siwak") == "stacy siwak"
    assert normalize_name("Dr. Pamela Lyss-Lerman") == "pamela lyss-lerman"


def test_normalize_strips_council_titles_to_surname():
    # One person, three titles -> one key.
    for raw in ("Alderman Harris", "Mayor Harris", "Mayor Pro Tempore Harris"):
        assert normalize_name(raw) == "harris"
    assert normalize_name("Councilmember Buse") == "buse"
    assert normalize_name("Alderman Buse") == "buse"


def test_normalize_folds_dash_glyphs_and_spacing():
    # OCR em-dash + stray space around the hyphen all fold to one surname.
    assert normalize_name("Dr Pamela Lyss— Lerman") == "pamela lyss-lerman"
    assert normalize_name("Dr Pamela Lyss- Lerman") == "pamela lyss-lerman"
    assert normalize_name("Pamela Lyss - Lerman") == "pamela lyss-lerman"


def test_normalize_is_idempotent():
    once = normalize_name("Mayor Pro Tempore Winings")
    assert once == "winings"
    assert normalize_name(once) == once


def test_normalize_leaves_ocr_garble_distinct():
    # No rule recovers these; they must NOT collide with the real key, so the
    # resolver leaves them for an explicit alias or the queue.
    assert normalize_name("lVls.Kim Hurst") != normalize_name("Ms. Kim Hurst")
    assert normalize_name("Alderman Garhnolz") != normalize_name("Alderman Garnholz")


def test_normalize_handles_blank():
    assert normalize_name("") == ""
    assert normalize_name("   ") == ""


# --- Roster.resolve --------------------------------------------------------------

COUNCIL = 2
SCHOOLS = 1


def _subject(sid: int, aliases: list[str], entity: int, **window) -> RosterSubject:
    return RosterSubject(
        subject_id=sid,
        aliases=frozenset(normalize_name(a) for a in aliases),
        memberships=(Membership(entity_id=entity, **window),),
    )


def test_resolve_single_match():
    roster = Roster([_subject(10, ["Alderman Harris", "Mayor Harris"], COUNCIL)])
    res = roster.resolve("Mayor Pro Tempore Harris", COUNCIL, date(2018, 4, 10))
    assert res == Resolution("resolved", subject_id=10)


def test_resolve_nickname_alias_maps_to_same_subject():
    # "Pam" and "Pamela" both registered as aliases of one subject.
    subj = _subject(20, ["Pamela Lyss-Lerman", "Pam Lyss-Lerman"], SCHOOLS)
    roster = Roster([subj])
    for raw in ("Dr. Pamela Lyss-Lerman", "Dr. Pam Lyss-Lerman"):
        assert roster.resolve(raw, SCHOOLS, date(2025, 1, 1)).subject_id == 20


def test_resolve_no_match_is_unresolved():
    roster = Roster([_subject(10, ["Alderman Harris"], COUNCIL)])
    res = roster.resolve("Alderman Nobody", COUNCIL, date(2020, 1, 1))
    assert res.status == "unresolved"
    assert res.reason == "no_roster_match"


def test_resolve_wrong_body_is_unresolved():
    # Same alias, but the member sits on a different body.
    roster = Roster([_subject(10, ["Harris"], SCHOOLS)])
    res = roster.resolve("Alderman Harris", COUNCIL, date(2020, 1, 1))
    assert res.status == "unresolved"


def test_resolve_date_breaks_same_surname_tie():
    # Two members ever share a surname; the meeting date picks the seated one.
    early = _subject(
        1, ["Smith"], COUNCIL, start_date=date(2015, 1, 1), end_date=date(2018, 12, 31)
    )
    late = _subject(2, ["Smith"], COUNCIL, start_date=date(2019, 1, 1))
    roster = Roster([early, late])
    assert roster.resolve("Alderman Smith", COUNCIL, date(2016, 6, 1)).subject_id == 1
    assert roster.resolve("Alderman Smith", COUNCIL, date(2020, 6, 1)).subject_id == 2


def test_resolve_unbroken_tie_is_ambiguous():
    a = _subject(1, ["Smith"], COUNCIL)  # NULL window -> always seated
    b = _subject(2, ["Smith"], COUNCIL)
    roster = Roster([a, b])
    res = roster.resolve("Alderman Smith", COUNCIL, date(2020, 1, 1))
    assert res.status == "ambiguous"
    assert set(res.candidates) == {1, 2}


def test_resolve_empty_name():
    roster = Roster([_subject(10, ["Harris"], COUNCIL)])
    assert roster.resolve("", COUNCIL, date(2020, 1, 1)).reason == "empty_name"
