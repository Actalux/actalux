"""Unit tests for the vote-sequence alignment labeler (synthetic roll calls)."""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any

from actalux.identity.resolve import (
    IdentityProposal,
    ResolverTurn,
    RosterMember,
    _name_index,
)
from actalux.identity.vote_align import (
    VOTE_ANCHOR_BASIS,
    VoteReference,
    _detect_regions,
    _Region,
    align_votes,
    merge_vote_anchor,
    vote_reference_for_document,
)


def _regions(turns: list[ResolverTurn]) -> list[_Region]:
    """Detect regions the way the aligner does (build the name index first)."""
    strong, surname = _name_index(_members())
    return _detect_regions(turns, strong, surname)


def _member(subject_id: int, name: str) -> RosterMember:
    """A council-style member keyed by full name + bare surname (what a clerk reads)."""
    surname = name.split()[-1].lower()
    return RosterMember(
        subject_id,
        name.lower().replace(" ", "-"),
        name,
        frozenset({name.lower(), surname}),
        "Councilmember",
    )


def _members() -> list[RosterMember]:
    return [
        _member(1, "Al Smith"),
        _member(2, "Bea Jones"),
        _member(3, "Cy Diaz"),
        _member(4, "Dan Yorg"),
        _member(5, "Ed Hummell"),
    ]


def _t(cluster: str, text: str) -> ResolverTurn:
    return ResolverTurn(cluster, text)


def _clean_rollcall() -> list[ResolverTurn]:
    """Clerk SPEAKER_09 reads five names; five distinct members answer, one turn each."""
    return [
        _t("SPEAKER_09", "Smith"),
        _t("SPEAKER_00", "Here"),
        _t("SPEAKER_09", "Jones"),
        _t("SPEAKER_01", "Present"),
        _t("SPEAKER_09", "Diaz"),
        _t("SPEAKER_02", "Aye"),
        _t("SPEAKER_09", "Yorg"),
        _t("SPEAKER_03", "Here"),
        _t("SPEAKER_09", "Hummell"),
        _t("SPEAKER_04", "Here"),
    ]


def _ref() -> VoteReference:
    return VoteReference(frozenset({1, 2, 3, 4, 5}))


# --- region detection --------------------------------------------------------------------


def test_region_detected_for_clean_rollcall():
    regions = _regions(_clean_rollcall())
    assert len(regions) == 1
    region = regions[0]
    assert region.clerk_clusters == frozenset({"SPEAKER_09"})
    assert len(region.calls) == 5
    assert {r.cluster_label for r in region.responses} == {
        "SPEAKER_00",
        "SPEAKER_01",
        "SPEAKER_02",
        "SPEAKER_03",
        "SPEAKER_04",
    }


def test_too_few_members_is_not_a_region():
    # Only three distinct names called -> below the roll-call floor -> no region.
    turns = [
        _t("SPEAKER_09", "Smith"),
        _t("SPEAKER_00", "Here"),
        _t("SPEAKER_09", "Jones"),
        _t("SPEAKER_01", "Here"),
        _t("SPEAKER_09", "Diaz"),
        _t("SPEAKER_02", "Here"),
    ]
    assert _regions(turns) == []


def test_no_dominant_caller_is_not_a_region():
    # Five names, but each "call" comes from a different cluster -> nobody calls two -> no clerk.
    turns = [
        _t("SPEAKER_00", "Smith"),
        _t("SPEAKER_01", "Jones"),
        _t("SPEAKER_02", "Diaz"),
        _t("SPEAKER_03", "Yorg"),
        _t("SPEAKER_04", "Hummell"),
    ]
    assert _regions(turns) == []


# --- alignment + acceptance --------------------------------------------------------------


def test_clean_correlated_rollcall_binds_all_at_medium():
    # A clean five-member roll call binds every responder to the right member, at inferred_medium:
    # text alone never publishes (the publishable tier is gated on the Phase C acoustic check).
    props = align_votes(_clean_rollcall(), _members(), _ref())
    by_cluster = {p.cluster_label: p for p in props}
    assert by_cluster["SPEAKER_00"].subject_id == 1
    assert all(p.confidence == "inferred_medium" for p in props)
    assert all(p.basis == VOTE_ANCHOR_BASIS for p in props)
    assert "SPEAKER_09" not in by_cluster  # the clerk is never a responder


def test_no_anchor_ever_publishes_on_text_alone():
    # The publishable (inferred_high) tier is withheld regardless of correlation or margin: even a
    # clean, correlated, gap-free 5-member roll call stays inferred_medium.
    for ref in (_ref(), None):
        props = align_votes(_clean_rollcall(), _members(), ref)
        assert props and all(p.confidence == "inferred_medium" for p in props)


def test_glued_response_is_a_gap_never_a_mislabel():
    # Jones's answer is glued into the clerk's turn ("Jones present"), so that clerk turn is not a
    # clean name-only call and no separable response exists for Jones -> Jones is a gap. The four
    # separable members are still bound (but the gap drops the tier below high).
    turns = [
        _t("SPEAKER_09", "Smith"),
        _t("SPEAKER_00", "Here"),
        _t("SPEAKER_09", "Jones present"),
        _t("SPEAKER_09", "Diaz"),
        _t("SPEAKER_02", "Aye"),
        _t("SPEAKER_09", "Yorg"),
        _t("SPEAKER_03", "Here"),
        _t("SPEAKER_09", "Hummell"),
        _t("SPEAKER_04", "Here"),
    ]
    props = align_votes(turns, _members(), _ref())
    slugs = {p.subject_id for p in props}
    assert 2 not in slugs  # Bea Jones (glued) is never attributed to a cluster
    assert slugs == {1, 3, 4, 5}
    assert all(p.confidence == "inferred_medium" for p in props)  # a gap forbids the high tier


def test_absent_member_is_a_gap():
    # The clerk calls Jones but nobody answers (absent). Jones is unbound; the rest bind.
    turns = [
        _t("SPEAKER_09", "Smith"),
        _t("SPEAKER_00", "Here"),
        _t("SPEAKER_09", "Jones"),
        _t("SPEAKER_09", "Diaz"),
        _t("SPEAKER_02", "Aye"),
        _t("SPEAKER_09", "Yorg"),
        _t("SPEAKER_03", "Here"),
        _t("SPEAKER_09", "Hummell"),
        _t("SPEAKER_04", "Here"),
    ]
    props = align_votes(turns, _members(), _ref())
    assert {p.subject_id for p in props} == {1, 3, 4, 5}


def test_voice_vote_chorus_rejected_by_count():
    # Names are read, but far more distinct voices answer than the four-member vote record allows.
    turns = [
        _t("SPEAKER_09", "Smith"),
        _t("SPEAKER_00", "Here"),
        _t("SPEAKER_09", "Jones"),
        _t("SPEAKER_01", "Aye"),
        _t("SPEAKER_09", "Diaz"),
        _t("SPEAKER_02", "Aye"),
        _t("SPEAKER_09", "Yorg"),
        _t("SPEAKER_03", "Aye"),
        _t("SPEAKER_10", "Aye"),
        _t("SPEAKER_11", "Aye"),
        _t("SPEAKER_12", "Aye"),
        _t("SPEAKER_13", "Aye"),
    ]
    assert align_votes(turns, _members(), VoteReference(frozenset({1, 2, 3, 4}))) == []


def test_cluster_answering_two_names_rejects_region():
    # SPEAKER_00 answers for both Smith and Jones -> a cluster maps to two members -> reject.
    turns = [
        _t("SPEAKER_09", "Smith"),
        _t("SPEAKER_00", "Here"),
        _t("SPEAKER_09", "Jones"),
        _t("SPEAKER_00", "Here"),
        _t("SPEAKER_09", "Diaz"),
        _t("SPEAKER_02", "Aye"),
        _t("SPEAKER_09", "Yorg"),
        _t("SPEAKER_03", "Here"),
    ]
    assert align_votes(turns, _members(), _ref()) == []


def test_four_member_rollcall_accepted_at_medium():
    # The minimum real roll call — four called members, all answering — is accepted (>=3 bindings).
    turns = [
        _t("SPEAKER_09", "Smith"),
        _t("SPEAKER_00", "Here"),
        _t("SPEAKER_09", "Jones"),
        _t("SPEAKER_01", "Present"),
        _t("SPEAKER_09", "Diaz"),
        _t("SPEAKER_02", "Aye"),
        _t("SPEAKER_09", "Yorg"),
        _t("SPEAKER_03", "Here"),
    ]
    props = align_votes(turns, _members(), VoteReference(frozenset({1, 2, 3, 4})))
    assert len(props) == 4 and all(p.confidence == "inferred_medium" for p in props)


def test_dissent_and_absence_are_gaps_not_mislabels():
    # A member voting "No" and an absent member are both gaps (their tokens aren't responses), so
    # they are never bound; only the clean affirmatives bind. Dropping "no" keeps precision high.
    turns = [
        _t("SPEAKER_09", "Smith"),
        _t("SPEAKER_00", "Here"),
        _t("SPEAKER_09", "Jones"),
        _t("SPEAKER_01", "No"),  # dissent -> not an affirmative -> gap
        _t("SPEAKER_09", "Diaz"),
        _t("SPEAKER_02", "Aye"),
        _t("SPEAKER_09", "Yorg"),
        _t("SPEAKER_03", "Here"),
        _t("SPEAKER_09", "Hummell"),
        _t("SPEAKER_04", "Aye"),
    ]
    props = align_votes(turns, _members(), _ref())
    assert {p.subject_id for p in props} == {1, 3, 4, 5}  # Jones (voted no) is a gap, never bound


def test_non_roster_voice_is_never_labeled():
    # A citizen (SPEAKER_20) says "here" in the middle of the roll; they are never called by name,
    # so no call binds them -> Option B holds (only roster members are labeled).
    turns = [
        _t("SPEAKER_09", "Smith"),
        _t("SPEAKER_00", "Here"),
        _t("SPEAKER_20", "Here"),
        _t("SPEAKER_09", "Jones"),
        _t("SPEAKER_01", "Present"),
        _t("SPEAKER_09", "Diaz"),
        _t("SPEAKER_02", "Aye"),
        _t("SPEAKER_09", "Yorg"),
        _t("SPEAKER_03", "Here"),
        _t("SPEAKER_09", "Hummell"),
        _t("SPEAKER_04", "Here"),
    ]
    props = align_votes(turns, _members(), _ref())
    assert "SPEAKER_20" not in {p.cluster_label for p in props}


def test_two_regions_same_cluster_different_members_drops_that_cluster():
    # An attendance roll and a later motion roll both fire; SPEAKER_00 answers as Smith in the
    # first and (implausibly) as Jones in the second -> meeting-level conflict -> SPEAKER_00 drops.
    first = _clean_rollcall()
    second = [
        _t("SPEAKER_09", "Smith"),
        _t("SPEAKER_05", "Here"),
        _t("SPEAKER_09", "Jones"),
        _t("SPEAKER_00", "Here"),  # SPEAKER_00 now answers to Jones
        _t("SPEAKER_09", "Diaz"),
        _t("SPEAKER_02", "Aye"),
        _t("SPEAKER_09", "Yorg"),
        _t("SPEAKER_03", "Here"),
        _t("SPEAKER_09", "Hummell"),
        _t("SPEAKER_04", "Here"),
    ]
    # A long debate stretch separates the two regions so they don't merge.
    filler = [_t("SPEAKER_06", "budget " * 20) for _ in range(3)]
    props = align_votes(first + filler + second, _members(), _ref())
    assert "SPEAKER_00" not in {p.cluster_label for p in props}


def test_no_members_or_no_turns_is_empty():
    assert align_votes([], _members(), _ref()) == []
    assert align_votes(_clean_rollcall(), [], _ref()) == []


# --- merge precedence with the deterministic resolver ------------------------------------


def _prop(cluster: str, sid: int, slug: str, conf: str, basis: str) -> IdentityProposal:
    return IdentityProposal(cluster, sid, slug, conf, basis)


def test_merge_agreement_records_vote_anchor_at_higher_confidence():
    resolver = [_prop("C0", 1, "smith", "inferred_high", "rollcall")]
    vote = [_prop("C0", 1, "smith", "inferred_medium", "vote_anchor")]
    merged = {p.cluster_label: p for p in merge_vote_anchor(resolver, vote)}
    assert merged["C0"].basis == "vote_anchor"  # the poisoned rollcall label is retired
    assert merged["C0"].confidence == "inferred_high"  # keeps the higher tier


def test_merge_high_vote_anchor_overrides_conflicting_rollcall():
    # Unit test of the merge contract with a hand-built high proposal (the aligner currently caps
    # at medium, so this exercises the precedence mechanism the Phase C tier will use).
    resolver = [_prop("C0", 1, "smith", "inferred_high", "rollcall")]
    vote = [_prop("C0", 2, "jones", "inferred_high", "vote_anchor")]
    merged = {p.cluster_label: p for p in merge_vote_anchor(resolver, vote)}
    assert merged["C0"].subject_id == 2 and merged["C0"].basis == "vote_anchor"


def test_merge_high_vote_anchor_does_not_override_self_intro():
    # A spoken self-declaration ("I'm X") outranks an inferred alignment: even a high vote_anchor
    # does not displace a conflicting self_intro on the same cluster.
    resolver = [_prop("C0", 1, "smith", "inferred_high", "self_intro")]
    vote = [_prop("C0", 2, "jones", "inferred_high", "vote_anchor")]
    merged = {p.cluster_label: p for p in merge_vote_anchor(resolver, vote)}
    assert merged["C0"].subject_id == 1 and merged["C0"].basis == "self_intro"


def test_merge_medium_vote_anchor_does_not_override_rollcall():
    resolver = [_prop("C0", 1, "smith", "inferred_high", "rollcall")]
    vote = [_prop("C0", 2, "jones", "inferred_medium", "vote_anchor")]
    merged = {p.cluster_label: p for p in merge_vote_anchor(resolver, vote)}
    assert merged["C0"].subject_id == 1 and merged["C0"].basis == "rollcall"


def test_merge_adds_vote_anchor_on_a_new_cluster():
    resolver = [_prop("C0", 1, "smith", "inferred_high", "rollcall")]
    vote = [_prop("C1", 2, "jones", "inferred_medium", "vote_anchor")]
    merged = {p.cluster_label: p.basis for p in merge_vote_anchor(resolver, vote)}
    assert merged == {"C0": "rollcall", "C1": "vote_anchor"}


def test_merge_cross_cluster_member_contest_demotes_to_review():
    # Resolver put Smith on C0, vote_anchor put Smith on C1 -> a member on two clusters -> both low.
    resolver = [_prop("C0", 1, "smith", "inferred_high", "rollcall")]
    vote = [_prop("C1", 1, "smith", "inferred_high", "vote_anchor")]
    merged = merge_vote_anchor(resolver, vote)
    assert {p.confidence for p in merged} == {"inferred_low"}


# --- DB correlation ----------------------------------------------------------------------


class _CorrTable:
    """A minimal PostgREST double for one (documents | member_vote_records) query chain."""

    def __init__(self, name: str, doc_row: dict[str, Any] | None, vote_rows: list[dict[str, Any]]):
        self._name = name
        self._doc_row = doc_row
        self._vote_rows = vote_rows

    def select(self, _cols: str) -> _CorrTable:
        return self

    def eq(self, _col: str, _val: Any) -> _CorrTable:
        return self

    def limit(self, _n: int) -> _CorrTable:
        return self

    def range(self, _lo: int, _hi: int) -> _CorrTable:
        return self

    def order(self, *_a: Any, **_k: Any) -> _CorrTable:
        return self

    def execute(self) -> SimpleNamespace:
        if self._name == "documents":
            return SimpleNamespace(data=[self._doc_row] if self._doc_row else [])
        return SimpleNamespace(data=self._vote_rows)


class _CorrClient:
    def __init__(self, doc_row: dict[str, Any] | None, vote_rows: list[dict[str, Any]]):
        self._doc_row = doc_row
        self._vote_rows = vote_rows

    def table(self, name: str) -> _CorrTable:
        return _CorrTable(name, self._doc_row, self._vote_rows)


def test_vote_reference_builds_member_set_from_correlated_votes():
    client = _CorrClient(
        {"meeting_date": "2025-03-04"},
        [{"subject_id": 1}, {"subject_id": 2}, {"subject_id": 2}, {"subject_id": 5}],
    )
    ref = vote_reference_for_document(client, 7, 42)
    assert ref is not None and ref.member_ids == frozenset({1, 2, 5})


def test_vote_reference_is_none_without_meeting_date():
    assert vote_reference_for_document(_CorrClient({"meeting_date": None}, []), 7, 42) is None
    assert vote_reference_for_document(_CorrClient(None, []), 7, 42) is None


def test_vote_reference_is_none_without_votes():
    client = _CorrClient({"meeting_date": "2025-03-04"}, [])
    assert vote_reference_for_document(client, 7, 42) is None
