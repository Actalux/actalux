"""Tests for the deterministic CivicPlus (City Council / Board of Aldermen) vote parser.

Fixtures are synthetic minutes text modeled on the real Clayton city-body minutes
(2015-2026): the three motion lead-ins ("Motion made by" / "<title> moved" /
"introduced Bill No."), the optional second, the two result forms (R1 "the motion
passed ..." and R2 a bare roll call + disposition), and the count/skip rules. The
parser reads only text (no DB), so these run without any local data.
"""

from __future__ import annotations

from actalux.ingest.votes_parser import build_details
from actalux.ingest.votes_parser_civicplus import (
    count_lead_ins,
    find_citing_chunk,
    parse_votes,
)


def _one(text: str):
    votes = parse_votes(text)
    assert len(votes) == 1, f"expected one vote, got {len(votes)}"
    return votes[0]


class TestVoiceVotes:
    """The dominant modern form: a 'Motion made by ...' motion and a voice result."""

    def test_bare_unanimous_has_no_count(self) -> None:
        v = _one(
            "Motion made by Councilmember Buse to approve the Consent Agenda. "
            "Councilmember Patel seconded. "
            "The motion passed unanimously on a voice vote."
        )
        assert v.result == "passed"
        assert v.result_basis == "stated"
        # "unanimously" with no number is NOT turned into a count.
        assert (v.vote_count_yes, v.vote_count_no, v.vote_count_abstain) == (None, None, None)
        assert v.moved_by == "Councilmember Buse"
        assert v.seconded_by == "Councilmember Patel"

    def test_inline_count(self) -> None:
        v = _one(
            "Motion made by Councilmember Buse to postpone Bill No. 7156 until June. "
            "Councilmember Patel seconded. "
            "The motion passed 7-0 on a voice vote."
        )
        assert (v.vote_count_yes, v.vote_count_no) == (7, 0)

    def test_unanimously_with_parenthetical_count(self) -> None:
        v = _one(
            "Councilmember Buse introduced Bill No. 7157, approving an agreement, "
            "to be read for the first time by title only. Councilmember Patel seconded. "
            "City Attorney O'Keefe reads Bill No. 7157, first reading, by title only. "
            "The motion passed unanimously (7-0) on a voice vote."
        )
        assert (v.vote_count_yes, v.vote_count_no) == (7, 0)
        assert v.motion.startswith("Councilmember Buse introduced Bill No. 7157")


class TestRollCall:
    """R1 roll-call results: counts come only from a fully-clean per-member list."""

    def test_clean_roll_call_counts_and_members(self) -> None:
        v = _one(
            "Motion made by Councilmember Buse to approve the Consent Agenda. "
            "Councilmember Patel seconded. "
            "The motion passed on a roll call vote: Councilmember Buse - Aye; "
            "Councilmember Patel - Aye; Councilmember Feder - Aye; Councilmember Yorg - Aye; "
            "Councilmember Waldman - Nay; Councilmember Meyland-Smith - Aye; "
            "and Mayor McAndrew - Aye."
        )
        assert v.result == "passed"
        assert (v.vote_count_yes, v.vote_count_no, v.vote_count_abstain) == (6, 1, 0)
        members = {m["name"]: m["vote"] for m in v.members}
        assert members["Councilmember Meyland-Smith"] == "aye"  # hyphenated surname
        assert members["Councilmember Waldman"] == "no"

    def test_garbled_roll_call_keeps_result_drops_count(self) -> None:
        # OCR garble ("Yorg - A Nay ye") makes one segment unparseable; the stated
        # result still stands but no partial count is stored.
        v = _one(
            "Motion made by Alderman Hummell to terminate Bill No. 7045. "
            "Alderman Buse seconded. "
            "The motion failed on a roll call vote: Alderman McAndrew - Nay; "
            "Alderman Buse - Nay; Alderman Patel - Nay; Alderman Feder - Nay; "
            "Alderman Hummell - Aye; Alderman Yorg - A Nay ye; and Mayor Harris - Nay."
        )
        assert v.result == "failed"
        assert v.result_basis == "stated"
        assert (v.vote_count_yes, v.vote_count_no, v.vote_count_abstain) == (None, None, None)
        assert v.members == ()


class TestBareRollCallR2:
    """Older second readings: no 'The motion' line, just a roll call + disposition."""

    def test_adopted_disposition_is_stated(self) -> None:
        v = _one(
            "Alderman Garnholz introduced Bill No. 6478, an ordinance on outdoor dining, "
            "to be read for the second time by title only. Alderman Winings seconded. "
            "City Attorney O'Keefe reads Bill No. 6478 for the second time by title only. "
            "Alderman Garnholz - Aye; Alderman Winings - Aye; Alderman Boulton - Aye; "
            "Alderman Berger - Aye; Alderman Lintz - Aye; and Mayor Pro Tempore Harris - Aye. "
            "The bill was adopted and became Ordinance No. 6352 of the City of Clayton."
        )
        assert v.result == "passed"
        assert v.result_basis == "stated"  # "adopted" is a stated outcome
        assert (v.vote_count_yes, v.vote_count_no) == (6, 0)

    def test_no_disposition_word_derives_from_tally(self) -> None:
        v = _one(
            "Alderman Garnholz introduced Bill No. 6500, to be read for the second time "
            "by title only. Alderman Winings seconded. "
            "City Attorney O'Keefe reads Bill No. 6500 for the second time by title only. "
            "Alderman Garnholz - Aye; Alderman Winings - Aye; Alderman Boulton - Nay."
        )
        assert v.result == "passed"
        assert v.result_basis == "derived"  # no disposition word; majority of the roll call
        assert (v.vote_count_yes, v.vote_count_no) == (2, 1)


class TestMotionLeadIns:
    """All three motion lead-ins, plus the nameless 'Mayor Pro Tem' mover."""

    def test_moved_form(self) -> None:
        v = _one(
            "Alderman Boulton moved to approve the December 23, 2014 minutes. "
            "Alderman Winings seconded. "
            "The motion to approve the minutes passed unanimously on a voice vote."
        )
        assert v.result == "passed"
        assert v.moved_by == "Alderman Boulton"
        assert v.motion.startswith("Alderman Boulton moved to approve")

    def test_nameless_mayor_pro_tem(self) -> None:
        v = _one(
            "Mayor Pro Tem Harris moved that the Board give unanimous consent to "
            "consideration for adoption of Bill No. 6480 on the day of its introduction. "
            "Alderman Garnholz seconded. "
            "The motion passed unanimously by a voice vote."
        )
        assert v.result == "passed"
        assert v.moved_by == "Mayor Pro Tem Harris"

    def test_result_without_leading_the(self) -> None:
        # "Motion to approve the minutes passed ..." (no "The") is a real result form.
        v = _one(
            "Motion made by Alderman Lintz to approve the October 23, 2018 minutes. "
            "Alderman Boulton seconded. "
            "Motion to approve the minutes passed unanimously on a voice vote."
        )
        assert v.result == "passed"


class TestSkipsAndCounts:
    """Skip rules and the unanimous/count reconciliation."""

    def test_closed_session_with_no_result_is_skipped(self) -> None:
        # A motion to adjourn to closed session with a closed vote/record has no
        # public result, so no vote is recorded.
        text = (
            "Motion was made by Alderman McAndrew that the Board adjourn to a closed "
            "meeting, with a closed vote and record, as authorized by Section 610.021. "
            "Alderman Buse seconded."
        )
        assert parse_votes(text) == []

    def test_inline_count_contradicting_unanimous_is_dropped(self) -> None:
        v = _one(
            "Motion made by Alderman Winings to approve the schedule. "
            "Alderman Boulton seconded. "
            "The motion passed unanimously 5-2 on a voice vote."
        )
        assert v.result == "passed"
        # 5-2 contradicts "unanimously" -> the count is dropped rather than trusted.
        assert (v.vote_count_yes, v.vote_count_no) == (None, None)

    def test_each_motion_gets_its_own_result(self) -> None:
        text = (
            "Motion made by Alderman Winings to approve the minutes. "
            "Alderman Boulton seconded. The motion passed unanimously on a voice vote. "
            "Motion made by Alderman Boulton to adjourn. Alderman Winings seconded. "
            "The motion passed 7-0 on a voice vote."
        )
        votes = parse_votes(text)
        assert len(votes) == 2
        assert votes[0].vote_count_yes is None  # voice, no count
        assert votes[1].vote_count_yes == 7  # its own counted result


class TestCitationAndFooter:
    """find_citing_chunk anchors on the motion, normalizing the interleaved footer."""

    def test_anchors_to_containing_chunk(self) -> None:
        v = _one(
            "Motion made by Councilmember Buse to approve the Consent Agenda. "
            "Councilmember Patel seconded. The motion passed unanimously on a voice vote."
        )
        chunks = [
            {"id": 1, "content": "Open forum. None.", "citation_id": "aaaaaaaa"},
            {
                "id": 2,
                "content": "Motion made by Councilmember Buse to approve the Consent Agenda. "
                "Councilmember Patel seconded.",
                "citation_id": "bbbbbbbb",
            },
        ]
        chunk = find_citing_chunk(v.anchors, chunks)
        assert chunk is not None and chunk["id"] == 2

    def test_match_survives_interleaved_running_footer(self) -> None:
        # The chunker preserves the PDF's running footer verbatim; both anchor and
        # chunk are footer-normalized before matching.
        v = _one(
            "Councilmember Buse introduced Bill No. 7158, authorizing funds, "
            "to be read for the second time by title only. Councilmember Patel seconded. "
            "The motion passed unanimously on a voice vote."
        )
        chunk = {
            "id": 9,
            "content": (
                "Councilmember Buse introduced Bill No. 7158, authorizing funds, to be "
                "05-26-2026 BOA Minutes May 26, 2026 Page 3 of 5 "
                "read for the second time by title only."
            ),
            "citation_id": "cccccccc",
        }
        assert find_citing_chunk(v.anchors, [chunk]) is not None

    def test_uncitable_motion_returns_none(self) -> None:
        v = _one(
            "Motion made by Alderman Winings to approve the budget. "
            "Alderman Boulton seconded. The motion passed unanimously on a voice vote."
        )
        assert find_citing_chunk(v.anchors, [{"id": 1, "content": "unrelated text"}]) is None


class TestDetailsAndAudit:
    """Shared build_details works on a civicplus ParsedVote; lead-in audit count."""

    def test_build_details(self) -> None:
        v = _one(
            "Motion made by Councilmember Buse to approve the Consent Agenda. "
            "Councilmember Patel seconded. "
            "The motion passed on a roll call vote: Councilmember Buse - Aye; "
            "and Councilmember Patel - Aye."
        )
        details = build_details(v)
        assert details["moved_by"] == "Councilmember Buse"
        assert details["seconded_by"] == "Councilmember Patel"
        assert len(details["members"]) == 2

    def test_count_lead_ins(self) -> None:
        text = (
            "Motion made by Alderman Winings to approve the minutes. Alderman Boulton seconded. "
            "The motion passed unanimously on a voice vote. "
            "Alderman Boulton moved to adjourn. Alderman Winings seconded. "
            "The motion passed unanimously on a voice vote."
        )
        assert count_lead_ins(text) == 2

    def test_free_prose_yields_nothing(self) -> None:
        assert parse_votes("The Board discussed the matter at length. No action was taken.") == []
