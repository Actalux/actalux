"""Roll-call attendance vs the attendance the minutes record."""

from __future__ import annotations

from actalux.identity.attendance import (
    RollCallAttendance,
    compare_attendance,
    merge_rolls,
    parse_minutes_attendance,
    reads_as_attendance_roll,
    roll_call_from_text,
    unambiguous_surnames,
)
from actalux.identity.resolve import RosterMember

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


_NAMES = {m.subject_id: m.canonical_name for m in ROSTER}


def _votes(*surnames: str) -> list[dict]:
    return [
        {"details": {"members": [{"name": f"Councilmember {s}", "vote": "aye"} for s in surnames]}}
    ]


def _roll(
    called: set[int],
    answered: set[int],
    quoted: set[int] | None = None,
) -> RollCallAttendance:
    """A roll whose calls are quotable, so a test exercises the citation gate rather than trip it.

    ``quoted`` narrows which calls carry a quote, for the test that a call with no quotable
    transcript span yields no reading.
    """
    quotes = {
        sid: f"Council Member {_NAMES.get(sid, sid)}."
        for sid in (called if quoted is None else quoted)
    }
    return RollCallAttendance(frozenset(called), frozenset(answered), quotes)


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
    roll = _roll({1, 2, 3, 4}, {1, 4})
    att = parse_minutes_attendance(MINUTES_WITH_ABSENCES)
    assert compare_attendance(roll, att, ROSTER, _votes("Buse", "Feder")) == []


def test_silence_contradicted_by_the_minutes_is_reported():
    minutes = "In person: Susan Buse, Gary Feder, and Becky Patel.\n\nOPEN FORUM\n"
    roll = _roll({1, 2, 4}, {1, 4})
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
    roll = _roll({1, 2, 4}, {1, 4})
    assert (
        compare_attendance(roll, parse_minutes_attendance(minutes), ROSTER, _votes("Buse", "Feder"))
        == []
    )


def test_silence_without_corroboration_is_withheld():
    # Patel is silent and seated by the minutes, but she DID vote - the diarizer most likely
    # swallowed her "here", so this must not be reported.
    minutes = "In person: Susan Buse, Gary Feder, and Becky Patel.\n\nOPEN FORUM\n"
    roll = _roll({1, 2, 4}, {1, 4})
    votes = _votes("Buse", "Feder", "Patel")
    assert compare_attendance(roll, parse_minutes_attendance(minutes), ROSTER, votes) == []


def test_missing_side_yields_nothing():
    att = parse_minutes_attendance(MINUTES_WITH_ABSENCES)
    assert compare_attendance(None, att, ROSTER, []) == []
    assert compare_attendance(_roll({1}, set()), None, ROSTER, []) == []


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
    roll = _roll({1, 2, 3}, {3})
    votes = [{"details": {"members": [{"name": "Councilmember Feder", "vote": "aye"}]}}]
    assert compare_attendance(roll, minutes, roster, votes) == []


def test_tally_only_minutes_cannot_corroborate_silence():
    # 76% of parsed votes carry a tally but no member list. Absence from a meeting recorded that
    # way is missing data, not evidence, so it must not publish a reading.
    minutes = parse_minutes_attendance(
        "In person: Susan Buse, Gary Feder, and Becky Patel.\n\nOPEN FORUM\n"
    )
    roll = _roll({1, 2, 4}, {1, 4})
    tally_only = [{"details": {"moved_by": "Councilmember Buse", "seconded_by": "Feder"}}]
    assert compare_attendance(roll, minutes, ROSTER, tally_only) == []


def test_a_reading_quotes_the_transcript_not_only_the_minutes():
    # The claim is "called, did not answer", so the span that claim was read from has to travel
    # with it. Minutes-only evidence would leave the transcript side of the reading unsourced.
    minutes = "In person: Susan Buse, Gary Feder, and Becky Patel.\n\nOPEN FORUM\n"
    roll = _roll({1, 2, 4}, {1, 4})
    readings = compare_attendance(
        roll, parse_minutes_attendance(minutes), ROSTER, _votes("Buse", "Feder")
    )
    assert readings[0].transcript_quote == "Council Member Becky Patel."
    assert readings[0].minutes_quote


def test_an_unquotable_call_yields_no_reading():
    # Everything else about Patel lines up, but nothing can be quoted for her call. A reading no
    # reader could check against the record is withheld rather than published unsourced.
    minutes = "In person: Susan Buse, Gary Feder, and Becky Patel.\n\nOPEN FORUM\n"
    roll = _roll({1, 2, 4}, {1, 4}, quoted={1, 4})
    assert (
        compare_attendance(roll, parse_minutes_attendance(minutes), ROSTER, _votes("Buse", "Feder"))
        == []
    )


def test_a_per_motion_vote_roll_is_not_read_as_attendance():
    # Same clerk-calls-each-name shape as the opening roll, but these are votes. Read as
    # attendance it would report Patel - who voted no - as absent from the meeting.
    vote_roll = (
        "Council Member Buse. Aye. Council Member Patel. No. "
        "Council Member Feder. Aye. Council Member Waldman. Aye."
    )
    assert roll_call_from_text(vote_roll, ROSTER) is None


def test_a_roll_deep_into_the_meeting_is_not_the_opening_roll():
    # The attendance roll opens the meeting. A qualifying run this far in is some later roll.
    spoken = "Council Member Buse. Here. Council Member Patel. Here. Council Member Feder. Here."
    assert roll_call_from_text("discussion follows. " * 400 + spoken, ROSTER) is None


def test_merging_readers_keeps_every_answer_either_one_heard():
    # The turn reader heard Buse answer; the text reader heard Patel. Neither alone clears both,
    # and keeping only the first would leave the other's answer looking like silence.
    from_turns = _roll({1, 2}, {1})
    from_text = _roll({1, 2}, {2})
    merged = merge_rolls(from_turns, from_text)
    assert merged is not None
    assert merged.answered == frozenset({1, 2})
    assert merged.unanswered == frozenset()
    assert merged.call_quotes[2] == "Council Member Becky Patel."


def test_merging_nothing_is_still_nothing():
    assert merge_rolls(None, None) is None


def test_a_roll_nobody_answered_is_not_a_roll():
    # The failure that matters: a region where the diarizer bound no response at all. Accepting it
    # would mark every member called as silent, and the merge would carry that into the audit.
    assert not reads_as_attendance_roll(called=5, answered=0, stating_presence=0)


def test_answers_that_cast_votes_do_not_make_an_attendance_roll():
    assert not reads_as_attendance_roll(called=5, answered=5, stating_presence=0)


def test_a_roll_most_of_whom_stated_presence_is_an_attendance_roll():
    assert reads_as_attendance_roll(called=5, answered=4, stating_presence=3)


def test_a_single_answer_is_too_thin_to_be_a_roll():
    assert not reads_as_attendance_roll(called=5, answered=1, stating_presence=1)
