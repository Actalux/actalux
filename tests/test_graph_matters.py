"""Unit tests for council matter extraction + edge derivation (pure, no DB)."""

from __future__ import annotations

from actalux.graph.matters import (
    collect_matters,
    derive_matter_edges,
    extract_matter_refs,
)


def test_extract_bill_with_title() -> None:
    motion = (
        "Motion made by Councilmember Buse to postpone Bill No. 7156, an Ordinance "
        "Amending Chapter 405 of the Clayton City Code to Add New Definitions."
    )
    refs = extract_matter_refs(motion)
    assert len(refs) == 1
    r = refs[0]
    assert (r.kind, r.number, r.slug, r.canonical) == (
        "bill",
        "7156",
        "bill-7156",
        "Bill No. 7156",
    )
    assert r.title is not None and r.title.startswith("an Ordinance Amending Chapter 405")


def test_extract_resolution() -> None:
    refs = extract_matter_refs("Motion to adopt Resolution No. 2024-19 approving the plan.")
    assert len(refs) == 1
    assert refs[0].kind == "resolution"
    assert refs[0].number == "2024-19"
    assert refs[0].slug == "resolution-2024-19"


def test_procedural_motion_has_no_matter() -> None:
    for motion in ("Approve the agenda as posted.", "Motion to approve the consent agenda", ""):
        assert extract_matter_refs(motion) == []


def test_amendment_suffix_collapses_to_base_bill() -> None:
    # 'Bill No. 6734.1' / '6734.2' are amended versions of the same bill.
    assert extract_matter_refs("table Bill No. 6734.1")[0].slug == "bill-6734"
    assert extract_matter_refs("introduce Bill No. 6734.2")[0].slug == "bill-6734"


def test_multiple_distinct_bills_in_one_motion() -> None:
    refs = extract_matter_refs("first reading of Bill No. 7156 and Bill No. 7157")
    assert {r.slug for r in refs} == {"bill-7156", "bill-7157"}


def test_collect_matters_keeps_richest_title() -> None:
    votes = [
        {"motion": "postpone Bill No. 7156"},  # no title
        {"motion": "introduce Bill No. 7156, an Ordinance Amending Chapter 405 of the Code"},
        {"motion": "Approve the minutes"},  # no matter
    ]
    matters = collect_matters(votes)
    assert set(matters) == {"bill-7156"}
    assert matters["bill-7156"].title.startswith("an Ordinance Amending")


def _vote(ref: str, motion: str, doc: int = 10) -> dict:
    return {
        "document_id": doc,
        "vote_ref": ref,
        "citation_id": "abc123",
        "source_quote": motion,
        "chunk_id": 5,
        "meeting_date": "2024-05-14",
        "motion": motion,
    }


def test_derive_matter_edges_links_known_matters() -> None:
    votes = [_vote("r1", "pass Bill No. 7156, an Ordinance ...")]
    edges = derive_matter_edges(votes, {"bill-7156": 42})
    assert len(edges) == 1
    e = edges[0]
    assert e["from_subject"] == 42
    assert e["type"] == "considered"
    assert e["status"] == "cited"
    assert (e["vote_document_id"], e["vote_ref"]) == (10, "r1")
    assert e["projection_complete"] is True


def test_derive_matter_edges_skips_unknown_and_unanchored() -> None:
    # matter not in the index -> no edge; vote with no vote_ref -> no edge.
    assert derive_matter_edges([_vote("r1", "pass Bill No. 9999")], {"bill-7156": 42}) == []
    no_ref = _vote("", "pass Bill No. 7156")
    assert derive_matter_edges([no_ref], {"bill-7156": 42}) == []


def test_derive_matter_edges_dedups_per_matter_vote() -> None:
    # same bill named twice in one motion -> one edge.
    votes = [_vote("r1", "amend Bill No. 7156 and re-refer Bill No. 7156")]
    edges = derive_matter_edges(votes, {"bill-7156": 42})
    assert len(edges) == 1
