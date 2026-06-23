"""Tests for the Plan Commission (PC-ARB) CivicPlus vote parser.

Fixtures are synthetic minutes text modeled on the real Clayton PC-ARB minutes
across both eras: name-before (~2021-2026) "<Name> made a motion ... <Name>
seconded the motion. The motion carried ..." and name-after (~2016-2021) "<Name>
made a motion ... The motion was seconded by <Name> and unanimously approved by
the Board." The parser reads only text (no DB), so these run without local data.
"""

from __future__ import annotations

from actalux.ingest.votes_parser_civicplus import (
    count_lead_ins_pc,
    find_citing_chunk_pc,
    parse_votes_pc,
)


def _one(text: str):
    votes = parse_votes_pc(text)
    assert len(votes) == 1, f"expected one vote, got {len(votes)}"
    return votes[0]


class TestNameBeforeEra:
    """Modern form: '<Name> seconded the motion. The motion carried ...'."""

    def test_unanimous_no_count(self) -> None:
        v = _one(
            "Helen DiFate made a motion to approve the minutes as submitted. "
            "Jim Arsenault seconded the motion. The motion carried unanimously."
        )
        assert v.result == "passed"
        assert v.result_basis == "stated"
        assert (v.vote_count_yes, v.vote_count_no) == (
            None,
            None,
        )  # not inferred from "unanimously"
        assert v.moved_by == "Helen DiFate"
        assert v.seconded_by == "Jim Arsenault"

    def test_word_number_count(self) -> None:
        v = _one(
            "Ron Reim made a motion to approve the site plan. Bob Denlow seconded the motion. "
            "The motion carried with five votes in favor and one vote opposed."
        )
        assert v.result == "passed"
        assert (v.vote_count_yes, v.vote_count_no) == (5, 1)

    def test_recommendation_motion(self) -> None:
        # PC recommends to the City Council; the outcome word, not the action, sets result.
        v = _one(
            "Helen DiFate made a motion to recommend approval of the Conditional Use Permit "
            "to the City Council as submitted. Jim Arsenault seconded the motion. "
            "The motion carried unanimously."
        )
        assert v.result == "passed"
        assert v.motion.startswith("Helen DiFate made a motion to recommend approval")


class TestNameAfterEra:
    """Older form: 'The motion was seconded by <Name> and unanimously approved ...'."""

    def test_appended_outcome(self) -> None:
        v = _one(
            "Carolyn Gaidis made a motion to approve the rezoning as submitted. "
            "The motion was seconded by Joanne Boulton and unanimously approved by the members."
        )
        assert v.result == "passed"
        assert v.result_basis == "stated"
        assert v.moved_by == "Carolyn Gaidis"
        assert v.seconded_by == "Joanne Boulton"

    def test_moved_lead_in(self) -> None:
        v = _one(
            "William Liebermann moved to approve the final plat. "
            "The motion was seconded by Ron Reim and unanimously approved by the Board."
        )
        assert v.result == "passed"
        assert v.moved_by == "William Liebermann"


class TestOutcomesAndCounts:
    """Failed/denied outcomes, named-dissent counts, and reconciliation."""

    def test_failed(self) -> None:
        v = _one(
            "Bob Denlow made a motion to approve the variance. Scott Wilson seconded the motion. "
            "The motion failed unanimously."
        )
        assert v.result == "failed"

    def test_denied_is_failed(self) -> None:
        v = _one(
            "Bob Denlow made a motion to approve the application. "
            "Scott Wilson seconded the motion. The motion was denied unanimously."
        )
        assert v.result == "failed"

    def test_named_dissent_has_no_count(self) -> None:
        v = _one(
            "Ron Reim made a motion to approve the plan. Helen DiFate seconded the motion. "
            "The motion was approved with Ira Berkowitz opposing."
        )
        assert v.result == "passed"
        # A named dissent with no number -> no inferred tally.
        assert (v.vote_count_yes, v.vote_count_no) == (None, None)

    def test_two_motions_each_get_their_own_outcome(self) -> None:
        text = (
            "Helen DiFate made a motion to approve item A. Jim Arsenault seconded the motion. "
            "The motion carried unanimously. "
            "Ron Reim made a motion to approve item B. Bob Denlow seconded the motion. "
            "The motion carried with four votes in favor and two votes opposed."
        )
        votes = parse_votes_pc(text)
        assert len(votes) == 2
        assert votes[0].vote_count_yes is None
        assert (votes[1].vote_count_yes, votes[1].vote_count_no) == (4, 2)


class TestCitation:
    """find_citing_chunk_pc: full match, then a *unique* opening/closing fallback."""

    def test_full_motion_match(self) -> None:
        v = _one(
            "Helen DiFate made a motion to approve the minutes as submitted. "
            "Jim Arsenault seconded the motion. The motion carried unanimously."
        )
        chunks = [
            {"id": 1, "content": "Open forum. None.", "citation_id": "aaaaaaaa"},
            {
                "id": 2,
                "content": "Helen DiFate made a motion to approve the minutes as submitted. "
                "Jim Arsenault seconded the motion.",
                "citation_id": "bbbbbbbb",
            },
        ]
        chunk = find_citing_chunk_pc(v.anchors, chunks)
        assert chunk is not None and chunk["id"] == 2

    def test_conditional_motion_split_across_chunks_uses_unique_opening(self) -> None:
        # The full motion spans two chunks; the opening sits in exactly one chunk,
        # so the uniqueness-gated fallback cites it.
        v = _one(
            "Helen DiFate made a motion to approve with the following conditions: "
            "1. Deck materials shall not consist of vinyl. "
            "2. The deck shall be administratively approved. "
            "Jim Arsenault seconded the motion. The motion carried unanimously."
        )
        chunks = [
            {
                "id": 7,
                "content": "The board discussed the deck. Helen DiFate made a motion to approve "
                "with the following conditions:",
                "citation_id": "cccccccc",
            },
            {
                "id": 8,
                "content": "1. Deck materials shall not consist of vinyl. 2. The deck shall be "
                "administratively approved. Jim Arsenault seconded the motion.",
                "citation_id": "dddddddd",
            },
        ]
        chunk = find_citing_chunk_pc(v.anchors, chunks)
        assert chunk is not None and chunk["id"] in (7, 8)  # a real, single passage

    def test_ambiguous_opening_is_not_miscited(self) -> None:
        # Two motions share the generic opening, split across two chunks; the opening
        # is non-unique, so it is NOT used to cite (skip beats mis-attribution).
        v = _one(
            "Helen DiFate made a motion to approve with the following conditions: "
            "1. The applicant shall submit revised plans for staff review and approval "
            "prior to the issuance of any building permit for the project. "
            "Jim Arsenault seconded the motion. The motion carried unanimously."
        )
        chunks = [
            {
                "id": 1,
                "content": "Helen DiFate made a motion to approve with the following "
                "conditions: 1. unrelated condition A.",
                "citation_id": "11111111",
            },
            {
                "id": 2,
                "content": "Helen DiFate made a motion to approve with the following "
                "conditions: 1. unrelated condition B.",
                "citation_id": "22222222",
            },
        ]
        assert find_citing_chunk_pc(v.anchors, chunks) is None


class TestAudit:
    def test_count_lead_ins_pc(self) -> None:
        text = (
            "Helen DiFate made a motion to approve item A. Jim Arsenault seconded the motion. "
            "The motion carried unanimously. "
            "Ron Reim moved to approve item B. Bob Denlow seconded the motion. "
            "The motion carried unanimously."
        )
        assert count_lead_ins_pc(text) == 2

    def test_free_prose_yields_nothing(self) -> None:
        assert parse_votes_pc("The commission discussed the proposal. No action was taken.") == []
