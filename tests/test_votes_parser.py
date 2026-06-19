"""Tests for the deterministic minutes vote parser.

All fixtures are synthetic minutes text exercising the three Diligent layouts,
the result-derivation policy, the skip rules, and citation anchoring. The parser
reads only text (no DB), so these run without any local data.
"""

from __future__ import annotations

from actalux.ingest.votes_parser import (
    build_details,
    find_citing_chunk,
    parse_votes,
)


def _one(text: str):
    votes = parse_votes(text)
    assert len(votes) == 1, f"expected one vote, got {len(votes)}"
    return votes[0]


class TestLayoutA:
    """Newest layout: per-member 'Name-aye' roll call, bare result word."""

    TEXT = (
        "1.2\n"
        "Adoption of Agenda\n"
        "Move to approve the agenda as posted.\n"
        "Moved by: Ms. Chris Win; seconded by: Mr. Leo Human\n"
        "Votes: Ben Beinfeld-aye, Leo Human-aye, Pam Lyss-Lerman-aye, Chris Win-nay\n"
        "Carried\n"
    )

    def test_fields(self) -> None:
        v = _one(self.TEXT)
        assert v.motion == "Move to approve the agenda as posted."
        assert v.result == "passed"
        assert v.result_basis == "stated"
        assert (v.vote_count_yes, v.vote_count_no, v.vote_count_abstain) == (3, 1, 0)
        assert v.moved_by == "Ms. Chris Win"
        assert v.seconded_by == "Mr. Leo Human"

    def test_members_carry_hyphenated_surnames(self) -> None:
        v = _one(self.TEXT)
        members = {m["name"]: m["vote"] for m in v.members}
        assert members["Pam Lyss-Lerman"] == "aye"  # internal hyphen survives
        assert members["Chris Win"] == "no"

    def test_details(self) -> None:
        v = _one(self.TEXT)
        details = build_details(v)
        assert details["moved_by"] == "Ms. Chris Win"
        assert details["seconded_by"] == "Mr. Leo Human"
        assert len(details["members"]) == 4


class TestLayoutB:
    """'Aye' header + name list + 'Motion Carries N-N' explicit total."""

    TEXT = (
        "1.2\n"
        "Adoption of Agenda\n"
        "Motion to approve the agenda as posted.\n"
        "Moved by: Ms. Chris Win\n"
        "Seconded by: Mr. Jason Growe\n"
        "Aye\n"
        "Ms. Stacy Siwak, Ms. Kim Hurst, Ms. Chris Win, Mr. Jason\n"
        "Growe, Mr. Leo Human, Dr Pamela Lyss-Lerman, and Mr. Ben Beinfeld\n"
        "Motion Carries 7-0\n"
    )

    def test_header_members_and_reconciled_counts(self) -> None:
        v = _one(self.TEXT)
        assert v.result == "passed"
        assert v.result_basis == "stated"
        # roll call (7 aye) reconciles with the explicit 7-0 -> full tally incl. abstain
        assert (v.vote_count_yes, v.vote_count_no, v.vote_count_abstain) == (7, 0, 0)
        assert len(v.members) == 7
        # the wrapped "Mr. Jason\nGrowe" name rejoins cleanly
        assert {"name": "Mr. Jason Growe", "vote": "aye"} in [dict(m) for m in v.members]


class TestLayoutC:
    """'Yes:' header prefix + names, bare 'Carried'."""

    TEXT = (
        "1.2\n"
        "Adoption of Agenda\n"
        "Approve the agenda as posted.\n"
        "Moved by: Ms. Chris Win\n"
        "Seconded by: Ms. Kim Hurst\n"
        "Yes: Mr. Ben Beinfeld, Mr. Leo Human, Dr. Pam Lyss-Lerman\n"
        "Carried\n"
    )

    def test_yes_prefix_members(self) -> None:
        v = _one(self.TEXT)
        assert v.motion == "Approve the agenda as posted."  # bare imperative, title dropped
        assert v.result == "passed"
        assert (v.vote_count_yes, v.vote_count_no, v.vote_count_abstain) == (3, 0, 0)


class TestCountsAndResults:
    def test_explicit_total_when_members_unparseable(self) -> None:
        text = "Approve the contract.\nMoved by: A\nSeconded by: B\nMotion Carries 5-2\n"
        v = _one(text)
        assert (v.vote_count_yes, v.vote_count_no) == (5, 2)
        assert v.vote_count_abstain is None  # 2-part total: abstain not stated

    def test_split_vote_with_nay_header(self) -> None:
        text = (
            "Approve the policy.\n"
            "Moved by: A\n"
            "Seconded by: B\n"
            "Aye\n"
            "Alice, Bob, Carol\n"
            "Nay\n"
            "Dave\n"
            "Motion Carries 3-1\n"
        )
        v = _one(text)
        assert (v.vote_count_yes, v.vote_count_no, v.vote_count_abstain) == (3, 1, 0)

    def test_failed_result(self) -> None:
        text = "Approve the measure.\nMoved by: A\nSeconded by: B\nMotion Fails 3-4\n"
        v = _one(text)
        assert v.result == "failed"
        assert (v.vote_count_yes, v.vote_count_no) == (3, 4)

    def test_abstain_counted(self) -> None:
        text = (
            "Approve the item.\n"
            "Moved by: A\n"
            "Seconded by: B\n"
            "Alice-aye, Bob-aye, Carol-abstain\n"
            "Carried\n"
        )
        v = _one(text)
        assert (v.vote_count_yes, v.vote_count_no, v.vote_count_abstain) == (2, 0, 1)

    def test_non_oxford_roster_counts_all_members(self) -> None:
        # "A, B and C" (no comma before 'and') must count three voters, not two.
        text = "Approve the agenda.\nMoved by: A\nSeconded by: B\nAye\nAlice, Bob and Carol\n2.\n"
        v = _one(text)
        assert (v.vote_count_yes, v.vote_count_no, v.vote_count_abstain) == (3, 0, 0)

    def test_count_conflict_drops_count_keeps_result(self) -> None:
        # Roll call (2 aye) disagrees with the stated total (7-0): the count is a
        # parse conflict, so it is dropped to None — but the stated result stands.
        text = (
            "Approve the measure.\n"
            "Moved by: A\n"
            "Seconded by: B\n"
            "Alice-aye, Bob-aye\n"
            "Motion Carries 7-0\n"
        )
        v = _one(text)
        assert v.result == "passed"
        assert (v.vote_count_yes, v.vote_count_no, v.vote_count_abstain) == (None, None, None)


class TestDerivedResults:
    def test_derive_passed_from_rollcall(self) -> None:
        text = (
            "Approve the agenda as posted.\n"
            "Moved by: A\n"
            "Seconded by: B\n"
            "Aye\n"
            "Alice, Bob, Carol\n"
            "2.\n"  # next section: no result word was printed
        )
        v = _one(text)
        assert v.result == "passed"
        assert v.result_basis == "derived"
        assert (v.vote_count_yes, v.vote_count_no, v.vote_count_abstain) == (3, 0, 0)

    def test_derive_failed_from_rollcall(self) -> None:
        text = (
            "Approve the measure.\nMoved by: A\nSeconded by: B\nAlice-nay, Bob-nay, Carol-aye\n2.\n"
        )
        v = _one(text)
        assert v.result == "failed"
        assert v.result_basis == "derived"

    def test_tie_is_not_derivable(self) -> None:
        text = "Approve the measure.\nMoved by: A\nSeconded by: B\nAlice-aye, Bob-nay\n2.\n"
        assert parse_votes(text) == []  # tie -> no derivable result -> skipped

    def test_unanimous_prose_without_count(self) -> None:
        text = "Approve the agenda.\nMoved by: A\nSeconded by: B\nAll aye\n2.\n"
        v = _one(text)
        assert v.result == "passed"
        assert v.result_basis == "derived"
        assert v.vote_count_yes is None  # unanimous, but no countable roll call


class TestSkips:
    def test_no_motion_cue_is_skipped(self) -> None:
        # An adjournment recorded only as a narrative line — no motion to record.
        text = (
            "9.1\n"
            "Adjournment\n"
            "The meeting adjourned at 8:14 p.m.\n"
            "Moved by: A\n"
            "Seconded by: B\n"
            "Carried\n"
        )
        assert parse_votes(text) == []

    def test_no_result_and_no_rollcall_is_skipped(self) -> None:
        text = (
            "Approve the agenda.\n"
            "Moved by: A\n"
            "Seconded by: B\n"
            "2.\n"  # no result, no roll call
        )
        assert parse_votes(text) == []

    def test_declarative_motion_recovered(self) -> None:
        # No imperative verb; the resolution text is itself the motion.
        text = (
            "8.1\n"
            "Receipt of the financial audit report is hereby acknowledged and\n"
            "accepted from the District's independent auditor.\n"
            "Moved by: Ms. Chris Win\n"
            "Seconded by: Mr. Jason Growe\n"
            "All aye\n"
            "Carried\n"
        )
        v = _one(text)
        assert v.motion.startswith("Receipt of the financial audit report")
        assert v.result == "passed"
        assert v.result_basis == "stated"

    def test_title_merged_motion_recovered_midline(self) -> None:
        text = (
            "3.1\n"
            "Adjournment Adjourn the meeting.\n"
            "Page 1 of 2\n"
            "Moved by: A\n"
            "Seconded by: B\n"
            "Carried\n"
        )
        v = _one(text)
        assert v.motion == "Adjourn the meeting."  # title + page footer stripped


class TestMultipleBlocks:
    def test_two_motions(self) -> None:
        text = (
            "Move to approve the agenda.\n"
            "Moved by: A; seconded by: B\n"
            "Alice-aye, Bob-aye\n"
            "Carried\n"
            "2.\n"
            "Move that the meeting be adjourned.\n"
            "Moved by: A; seconded by: B\n"
            "Carried\n"
        )
        votes = parse_votes(text)
        assert len(votes) == 2
        assert votes[0].vote_count_yes == 2
        assert votes[1].vote_count_yes is None  # second has no roll call


class TestCitationAnchoring:
    CHUNKS = [
        {"id": 10, "citation_id": "aaa", "content": "1. Call to Order. The meeting began."},
        {
            "id": 11,
            "citation_id": "bbb",
            "content": "Move to approve the agenda as posted.\nMoved by: A\nCarried",
        },
    ]

    def test_motion_anchor_finds_chunk(self) -> None:
        v = _one("Move to approve the agenda as posted.\nMoved by: A\nSeconded by: B\nCarried\n")
        chunk = find_citing_chunk(v.anchors, self.CHUNKS)
        assert chunk is not None and chunk["id"] == 11

    def test_no_matching_chunk_returns_none(self) -> None:
        v = _one(
            "Approve a completely different motion text.\nMoved by: A\nSeconded by: B\nCarried\n"
        )
        assert find_citing_chunk(v.anchors, self.CHUNKS) is None

    def test_anchor_matches_across_pdf_linewraps(self) -> None:
        # The chunk stores the roll call wrapped; the anchor normalizes to match.
        chunks = [
            {
                "id": 12,
                "citation_id": "ccc",
                "content": "Approve the budget item.\nMoved by: A\nAlice-aye, Bob-\naye\nCarried",
            }
        ]
        v = _one(
            "Approve the budget item.\nMoved by: A\nSeconded by: B\nAlice-aye, Bob-aye\nCarried\n"
        )
        # the motion anchor alone is enough to find the chunk
        assert find_citing_chunk(v.anchors, chunks)["id"] == 12
