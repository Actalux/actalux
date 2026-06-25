"""Vote -> edge projection — connections-graph Phase 1 (graph.project)."""

from __future__ import annotations

from actalux.graph.project import derive_document_edges, quote_hash
from actalux.graph.resolve import Membership, Roster, RosterSubject, normalize_name

COUNCIL = 2


def _roster() -> Roster:
    def sub(sid, alias):
        return RosterSubject(sid, frozenset({normalize_name(alias)}), (Membership(COUNCIL),))

    return Roster([sub(1, "Harris"), sub(2, "Buse")])


def _vote(**over) -> dict:
    base = {
        "document_id": 100,
        "vote_ref": "ref-1",
        "citation_id": "cit1",
        "source_quote": "The motion carried.",
        "chunk_id": 5,
        "meeting_date": "2020-06-01",
        "entity_id": COUNCIL,
        "details": {},
    }
    base.update(over)
    return base


def test_quote_hash_normalizes():
    assert quote_hash("  Hello   World ") == quote_hash("hello world")
    assert quote_hash("x") != quote_hash("y")


def test_rollcall_and_roles_produce_cited_edges():
    vote = _vote(
        details={
            "members": [
                {"name": "Alderman Harris", "vote": "aye"},
                {"name": "Alderman Buse", "vote": "no"},
            ],
            "moved_by": "Alderman Harris",
            "seconded_by": "Alderman Buse",
        }
    )
    edges, queue = derive_document_edges([vote], _roster())
    assert queue == []
    kinds = {(e["from_subject"], e["type"]) for e in edges}
    assert kinds == {(1, "voted_aye_on"), (2, "voted_no_on"), (1, "moved"), (2, "seconded")}
    # every edge is a cited, complete, durably-anchored fact
    for e in edges:
        assert e["status"] == "cited"
        assert e["projection_complete"] is True
        assert e["vote_document_id"] == 100
        assert e["source_document_id"] == 100
        assert e["vote_ref"] == "ref-1"
        assert e["citation_id"] == "cit1"
        assert e["as_of_date"] == "2020-06-01"


def test_abstain_maps_to_its_own_edge():
    vote = _vote(details={"members": [{"name": "Harris", "vote": "abstain"}]})
    edges, _ = derive_document_edges([vote], _roster())
    assert [e["type"] for e in edges] == ["voted_abstain_on"]


def test_absent_present_produce_no_edge_and_no_queue():
    vote = _vote(
        details={
            "members": [{"name": "Harris", "vote": "absent"}, {"name": "Buse", "vote": "present"}]
        }
    )
    edges, queue = derive_document_edges([vote], _roster())
    assert edges == []
    assert queue == []  # a non-vote is never attributed, never queued


def test_unresolved_name_is_queued_not_dropped():
    vote = _vote(details={"members": [{"name": "Alderman Nobody", "vote": "aye"}]})
    edges, queue = derive_document_edges([vote], _roster())
    assert edges == []
    assert len(queue) == 1
    assert queue[0]["reason"] == "no_roster_match"
    assert queue[0]["normalized_alias"] == "nobody"
    assert queue[0]["document_id"] == 100


def test_duplicate_member_in_one_vote_collapses():
    # Same person, two name forms, same vote -> one outcome edge (mirrors the
    # partial unique index, so the insert can't trip it).
    vote = _vote(
        details={
            "members": [
                {"name": "Alderman Harris", "vote": "aye"},
                {"name": "Mayor Harris", "vote": "aye"},
            ]
        }
    )
    edges, _ = derive_document_edges([vote], _roster())
    assert len(edges) == 1
    assert edges[0]["type"] == "voted_aye_on"


def test_mover_and_voter_coexist_for_same_member():
    vote = _vote(details={"members": [{"name": "Harris", "vote": "aye"}], "moved_by": "Harris"})
    edges, _ = derive_document_edges([vote], _roster())
    assert {e["type"] for e in edges} == {"voted_aye_on", "moved"}


def test_vote_without_vote_ref_is_skipped():
    vote = _vote(vote_ref=None, details={"members": [{"name": "Harris", "vote": "aye"}]})
    edges, queue = derive_document_edges([vote], _roster())
    assert edges == [] and queue == []
