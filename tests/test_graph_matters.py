"""Unit tests for council matter extraction + edge derivation (pure, no DB)."""

from __future__ import annotations

from actalux.graph.matters import (
    MatterRef,
    collect_matter_refs,
    collect_matters,
    derive_document_matter_mentions,
    derive_matter_edges,
    extract_matter_refs,
    select_mintable_matters,
)


def _bill(num: str, title: str | None = None) -> MatterRef:
    return MatterRef("bill", num, f"Bill No. {num}", f"bill-{num}", title)


def _res(num: str) -> MatterRef:
    return MatterRef("resolution", num, f"Resolution No. {num}", f"resolution-{num.lower()}", None)


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


def test_collect_matter_refs_over_arbitrary_texts() -> None:
    # collect_matters is now a thin wrapper; the general helper reads raw strings.
    texts = ["agenda item: Bill No. 7156, an Ordinance ...", "no matter here", "Resolution 24-3"]
    refs = collect_matter_refs(texts)
    assert set(refs) == {"bill-7156", "resolution-24-3"}


def _chunk(cid: str, content: str, chunk_id: int = 1) -> dict:
    return {"id": chunk_id, "citation_id": cid, "content": content}


def test_derive_matter_mentions_links_known_matters() -> None:
    chunks = [
        _chunk("q1", "City Attorney reads Bill No. 7156, first reading.", 1),
        _chunk("q2", "In response to a question about Bill No. 7156, staff explained ...", 2),
        _chunk("q3", "Approval of the minutes.", 3),  # no matter
    ]
    mentions = derive_document_matter_mentions(853, chunks, {"bill-7156": 42})
    assert len(mentions) == 2  # one per referencing chunk (distinct citation_ids)
    m = mentions[0]
    assert m["subject_id"] == 42
    assert m["document_id"] == 853
    assert (m["citation_id"], m["chunk_id"]) == ("q1", 1)
    assert m["source_quote"].startswith("City Attorney reads Bill No. 7156")
    assert m["quote_hash"] and m["projection_complete"] is True


def test_derive_matter_mentions_drops_unknown_and_uncited() -> None:
    chunks = [
        _chunk("q1", "discussion of Bill No. 9999"),  # not a minted matter
        {"id": 2, "citation_id": None, "content": "Bill No. 7156 mentioned"},  # no citation_id
    ]
    assert derive_document_matter_mentions(853, chunks, {"bill-7156": 42}) == []


def test_derive_matter_mentions_dedups_matter_per_chunk() -> None:
    # a bill named twice in one chunk -> one mention (keyed on subject + citation_id).
    chunks = [_chunk("q1", "Bill No. 7156 introduced; later, Bill No. 7156 postponed.")]
    assert len(derive_document_matter_mentions(853, chunks, {"bill-7156": 42})) == 1


# Voted bills calibrate the number family: 4-digit, 6478..7161 (mirrors Clayton's real
# distribution used to design the guard).
_VOTED = {"bill-6478": _bill("6478"), "bill-7161": _bill("7161"), "resolution-22-04": _res("22-04")}


def test_mintable_keeps_voted_and_in_family_new_bills() -> None:
    candidates = {
        "bill-6600": _bill("6600"),  # in range -> keep
        "bill-7162": _bill("7162"),  # just above max, within margin -> keep (recent bill)
    }
    out = select_mintable_matters(_VOTED, candidates)
    assert set(out) == {"bill-6478", "bill-7161", "resolution-22-04", "bill-6600", "bill-7162"}


def test_mintable_drops_out_of_family_junk_bills() -> None:
    junk = {
        "bill-190": _bill("190"),  # 3-digit, wrong length
        "bill-881": _bill("881"),  # 3-digit
        "bill-1413": _bill("1413"),  # 4-digit but below min
        "bill-2026": _bill("2026"),  # a year
        "bill-2761": _bill("2761"),  # below min
        "bill-67455": _bill("67455"),  # 5-digit OCR mash
    }
    out = select_mintable_matters(_VOTED, junk)
    assert set(out) == set(_VOTED)  # nothing new minted


def test_mintable_resolution_requires_hyphenated_form() -> None:
    out = select_mintable_matters(
        _VOTED, {"resolution-2023-12": _res("2023-12"), "resolution-45": _res("45")}
    )
    assert "resolution-2023-12" in out  # distinctive YY-NN form
    assert "resolution-45" not in out  # bare number -> stray hit, dropped


def test_mintable_no_voted_bills_mints_no_new_bills() -> None:
    # Without a voted baseline the family can't be calibrated -> mint no new bills, but a
    # distinctively-formatted resolution is still safe.
    candidates = {"bill-7000": _bill("7000"), "resolution-24-01": _res("24-01")}
    out = select_mintable_matters({}, candidates)
    assert set(out) == {"resolution-24-01"}


def test_mintable_merges_richer_title_onto_voted_matter() -> None:
    voted = {"bill-6600": _bill("6600", None)}
    candidates = {"bill-6600": _bill("6600", "an Ordinance amending the zoning code")}
    out = select_mintable_matters(voted, candidates)
    assert out["bill-6600"].title == "an Ordinance amending the zoning code"
