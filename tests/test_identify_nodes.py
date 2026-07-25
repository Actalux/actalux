"""Unit tests for the node-identification evidence helpers (no DB, no cache)."""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

# identify_nodes lives in scripts/linking/ (not an installed package); load by path.
_path = Path(__file__).resolve().parent.parent / "scripts" / "linking" / "identify_nodes.py"
_spec = importlib.util.spec_from_file_location("identify_nodes", _path)
idn = importlib.util.module_from_spec(_spec)
sys.modules[_spec.name] = idn
_spec.loader.exec_module(idn)


class TestSpokenMotions:
    def test_extracts_motion_sentence_from_cue_onward(self) -> None:
        texts = [
            "Okay thank you everyone. I move that the Board of Education approve "
            "the consent agenda items 7.1 through 7.8. Any second?"
        ]
        out = idn.spoken_motions(texts)
        assert len(out) == 1
        assert out[0].startswith("I move that the Board of Education approve")

    def test_short_fragments_and_plain_talk_ignored(self) -> None:
        assert idn.spoken_motions(["I move.", "We discussed the budget at length."]) == []

    def test_moved_that_variant(self) -> None:
        out = idn.spoken_motions(["Moved that the Board of Education adopt the agenda as posted."])
        assert len(out) == 1


class TestMatchMotion:
    VOTES = [
        {
            "motion": "That the Board of Education approve the consent agenda items 7.1 "
            "through 7.8.",
            "details": {"moved_by": "Gary Pierson", "seconded_by": "Amy Rubin"},
        },
        {
            "motion": "That the Board of Education adjourn.",
            "details": {"moved_by": "Stacy Siwak"},
        },
        # An unattributed vote can never identify anyone, however well it matches.
        {"motion": "Approve the agenda as posted.", "details": None},
    ]

    def test_matches_paraphrased_spoken_motion_to_mover(self) -> None:
        spoken = "I move that the Board of Education approve the consent agenda items 7.1 to 7.8"
        hit = idn.match_motion(spoken, self.VOTES)
        assert hit is not None
        vote, score = hit
        assert vote["details"]["moved_by"] == "Gary Pierson"
        assert score >= idn._MOTION_MATCH_THRESHOLD

    def test_unrelated_motion_below_threshold(self) -> None:
        assert idn.match_motion("I move that we rename the stadium after my dog", self.VOTES) is (
            None
        )

    def test_unattributed_votes_are_never_candidates(self) -> None:
        assert idn.match_motion("Move to approve the agenda as posted", self.VOTES) is None


class TestAuditVerdict:
    ROSTER = ["Susan Buse", "Susan Harris", "Gary Pierson", "Mark Winings"]

    def test_unique_surname_agrees(self) -> None:
        assert idn.audit_verdict("Gary Pierson", "Motion by Gary Pierson", self.ROSTER) == "agree"

    def test_shared_first_name_alone_is_unclear(self) -> None:
        # Two Susans: a bare "Susan" supports neither of them.
        assert idn.audit_verdict("Susan Buse", "Susan", self.ROSTER) == "unclear"

    def test_unique_token_of_another_member_contradicts(self) -> None:
        assert idn.audit_verdict("Susan Buse", "Alderman Harris", self.ROSTER) == "contradict"

    def test_no_overlap_is_unclear(self) -> None:
        assert idn.audit_verdict("Gary Pierson", "the City Manager", self.ROSTER) == "unclear"


class TestChairRequestExclusion:
    def test_chair_soliciting_a_motion_is_not_a_motion(self) -> None:
        # First live run: this question fuzzy-matched the agenda motion, whose minutes
        # attribution names whoever ANSWERED the chair -- a misidentification shape.
        texts = ["May I have a motion to approve the agenda, please?"]
        assert idn.spoken_motions(texts) == []

    def test_bare_motion_to_without_first_person_ignored(self) -> None:
        assert idn.spoken_motions(["Motion to approve the agenda carries unanimously."]) == []

    def test_first_person_motion_still_extracted(self) -> None:
        out = idn.spoken_motions(["I move that we approve the agenda as posted today."])
        assert len(out) == 1
