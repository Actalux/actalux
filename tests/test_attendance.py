"""Roll-call attendance vs the attendance the minutes record."""

from __future__ import annotations

from actalux.identity.attendance import (
    compare_attendance,
    parse_minutes_attendance,
    roll_call_from_text,
    unambiguous_surnames,
)
from actalux.identity.resolve import RosterMember
from actalux.identity.vote_align import RollCallAttendance

MINUTES_WITH_ABSENCES = """
Mayor McAndrew called the meeting to order and requested a roll call. The following individuals
were in attendance:

In person: Susan Buse, Gary Feder, Jeff Yorg, Betsy Meyland-Smith, and Mayor Bridget McAndrew.

Staff: City Manager Gipson, City Attorney O'Keefe

Absent: Becky Patel and Kami Waldman

OPEN FORUM
"""

ROSTER = [
    RosterMember(1, "susan-buse", "Susan Buse", frozenset({"buse"})),
    RosterMember(2, "becky-patel", "Becky Patel", frozenset({"patel"})),
    RosterMember(3, "kami-waldman", "Kami Waldman", frozenset({"waldman"})),
    RosterMember(4, "gary-feder", "Gary Feder", frozenset({"feder"})),
]


def _votes(*surnames: str) -> list[dict]:
    return [
        {"details": {"members": [{"name": f"Councilmember {s}", "vote": "aye"} for s in surnames]}}
    ]


def test_parses_present_and_absent_lists():
    att = parse_minutes_attendance(MINUTES_WITH_ABSENCES)
    assert att is not None
    assert {"buse", "feder", "yorg", "meyland-smith", "mcandrew"} <= att.present
    assert att.absent == frozenset({"patel", "waldman"})
    # The staff block must not leak into the seated-member list.
    assert "gipson" not in att.present


def test_no_attendance_block_is_distinct_from_an_empty_one():
    assert parse_minutes_attendance("MINUTES\n\nOPEN FORUM\n") is None


def test_silence_matching_the_absent_list_is_not_a_finding():
    # Patel and Waldman were silent at the roll AND the minutes list them absent: records agree.
    roll = RollCallAttendance(frozenset({1, 2, 3, 4}), frozenset({1, 4}), regions=1)
    att = parse_minutes_attendance(MINUTES_WITH_ABSENCES)
    assert compare_attendance(roll, att, ROSTER, _votes("Buse", "Feder")) == []


def test_silence_contradicted_by_the_minutes_is_reported():
    minutes = "In person: Susan Buse, Gary Feder, and Becky Patel.\n\nOPEN FORUM\n"
    roll = RollCallAttendance(frozenset({1, 2, 4}), frozenset({1, 4}), regions=1)
    readings = compare_attendance(
        roll, parse_minutes_attendance(minutes), ROSTER, _votes("Buse", "Feder")
    )
    assert [r.canonical_name for r in readings] == ["Becky Patel"]
    assert readings[0].corroborating_signals == (
        "named in none of the 1 votes these minutes record by member",
    )


def test_a_noted_late_arrival_is_not_a_discrepancy():
    minutes = (
        "In person: Susan Buse, Gary Feder, and Becky Patel.\n\n"
        "Patel arrived at 7:15 p.m.\n\nOPEN FORUM\n"
    )
    roll = RollCallAttendance(frozenset({1, 2, 4}), frozenset({1, 4}), regions=1)
    assert (
        compare_attendance(roll, parse_minutes_attendance(minutes), ROSTER, _votes("Buse", "Feder"))
        == []
    )


def test_silence_without_corroboration_is_withheld():
    # Patel is silent and seated by the minutes, but she DID vote - the diarizer most likely
    # swallowed her "here", so this must not be reported.
    minutes = "In person: Susan Buse, Gary Feder, and Becky Patel.\n\nOPEN FORUM\n"
    roll = RollCallAttendance(frozenset({1, 2, 4}), frozenset({1, 4}), regions=1)
    votes = _votes("Buse", "Feder", "Patel")
    assert compare_attendance(roll, parse_minutes_attendance(minutes), ROSTER, votes) == []


def test_missing_side_yields_nothing():
    att = parse_minutes_attendance(MINUTES_WITH_ABSENCES)
    assert compare_attendance(None, att, ROSTER, []) == []
    assert (
        compare_attendance(RollCallAttendance(frozenset({1}), frozenset(), 1), None, ROSTER, [])
        == []
    )


def test_a_prose_list_of_members_is_not_a_roll_call():
    # Three titled names in a sentence: proximity without anyone answering. Reading this as a
    # roll would report every one of them as silent.
    prose = (
        "The committee includes Councilmember Buse, Councilmember Patel, and Councilmember Feder."
    )
    assert roll_call_from_text(prose, ROSTER) is None


def test_a_real_roll_call_is_read():
    spoken = (
        "Council Member Buse. Here. Council Member Patel. Council Member Feder. Here. "
        "Council Member Waldman. Here."
    )
    roll = roll_call_from_text(spoken, ROSTER)
    assert roll is not None
    assert roll.called == frozenset({1, 2, 3, 4})
    assert roll.unanswered == frozenset({2})  # Patel called, no answer


def test_an_interjection_before_the_answer_still_counts():
    # The 2026-03-10 case: "Oh, here" must not read as silence.
    spoken = "Council Member Buse. Here. Council Member Patel. Oh, here. Council Member Feder. Yes."
    roll = roll_call_from_text(spoken, ROSTER)
    assert roll is not None and roll.unanswered == frozenset()


def test_shared_surnames_are_dropped_not_guessed():
    roster = [
        RosterMember(1, "ann-smith", "Ann Smith", frozenset()),
        RosterMember(2, "bob-smith", "Bob Smith", frozenset()),
        RosterMember(3, "gary-feder", "Gary Feder", frozenset()),
    ]
    assert unambiguous_surnames(roster) == {"feder": 3}


def test_a_shared_surname_never_produces_a_reading():
    roster = [
        RosterMember(1, "ann-smith", "Ann Smith", frozenset()),
        RosterMember(2, "bob-smith", "Bob Smith", frozenset()),
        RosterMember(3, "gary-feder", "Gary Feder", frozenset()),
    ]
    minutes = parse_minutes_attendance("In person: Ann Smith, Bob Smith, and Gary Feder.\n\nOPEN\n")
    roll = RollCallAttendance(frozenset({1, 2, 3}), frozenset({3}), regions=1)
    votes = [{"details": {"members": [{"name": "Councilmember Feder", "vote": "aye"}]}}]
    assert compare_attendance(roll, minutes, roster, votes) == []


def test_tally_only_minutes_cannot_corroborate_silence():
    # 76% of parsed votes carry a tally but no member list. Absence from a meeting recorded that
    # way is missing data, not evidence, so it must not publish a reading.
    minutes = parse_minutes_attendance(
        "In person: Susan Buse, Gary Feder, and Becky Patel.\n\nOPEN FORUM\n"
    )
    roll = RollCallAttendance(frozenset({1, 2, 4}), frozenset({1, 4}), regions=1)
    tally_only = [{"details": {"moved_by": "Councilmember Buse", "seconded_by": "Feder"}}]
    assert compare_attendance(roll, minutes, ROSTER, tally_only) == []
