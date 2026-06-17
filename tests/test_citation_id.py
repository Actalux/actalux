"""Tests for stable, content-addressed citation ids (ingest.hashing)."""

from __future__ import annotations

import re

from actalux.ingest.hashing import (
    CITATION_ID_LEN,
    assign_citation_ids,
    compute_citation_id,
    doc_stable_key,
)

_HEX = re.compile(r"^[0-9a-f]+$")


class TestComputeCitationId:
    def test_length_and_hex(self) -> None:
        cid = compute_citation_id("doc-key", "The board approved the budget.")
        assert len(cid) == CITATION_ID_LEN
        assert _HEX.match(cid)

    def test_deterministic(self) -> None:
        a = compute_citation_id("doc-key", "Some passage of text.")
        b = compute_citation_id("doc-key", "Some passage of text.")
        assert a == b

    def test_whitespace_and_case_insensitive(self) -> None:
        # Cosmetic reflow / case differences between a PDF and HTML twin must not
        # change the id (matches resolve_canonical_chunk normalization).
        a = compute_citation_id("doc-key", "The Board Approved   the\nBudget.")
        b = compute_citation_id("doc-key", "the board approved the budget.")
        assert a == b

    def test_different_content_differs(self) -> None:
        a = compute_citation_id("doc-key", "Passage one.")
        b = compute_citation_id("doc-key", "Passage two.")
        assert a != b

    def test_different_doc_key_differs(self) -> None:
        a = compute_citation_id("doc-a", "Same passage.")
        b = compute_citation_id("doc-b", "Same passage.")
        assert a != b

    def test_dup_ordinal_differs(self) -> None:
        base = compute_citation_id("doc-key", "Repeated passage.", 0)
        second = compute_citation_id("doc-key", "Repeated passage.", 1)
        assert base != second


class TestAssignCitationIds:
    def test_unique_passages_match_compute(self) -> None:
        contents = ["First passage.", "Second passage."]
        out = assign_citation_ids("doc-key", contents)
        assert out == [
            compute_citation_id("doc-key", "First passage."),
            compute_citation_id("doc-key", "Second passage."),
        ]

    def test_repeated_passage_gets_distinct_ids(self) -> None:
        contents = ["Motion carried.", "Other text.", "Motion carried."]
        out = assign_citation_ids("doc-key", contents)
        # The two identical passages get different ids (dup ordinal 0 and 1).
        assert out[0] != out[2]
        # First occurrence equals the plain (ordinal-0) hash.
        assert out[0] == compute_citation_id("doc-key", "Motion carried.", 0)
        assert out[2] == compute_citation_id("doc-key", "Motion carried.", 1)

    def test_order_stable(self) -> None:
        contents = ["a", "b", "c"]
        assert assign_citation_ids("k", contents) == assign_citation_ids("k", contents)

    def test_empty(self) -> None:
        assert assign_citation_ids("k", []) == []


class TestDocStableKey:
    def test_prefers_source_ref(self) -> None:
        assert doc_stable_key("ref", "hash", "file.pdf") == "ref"

    def test_falls_back_to_content_hash(self) -> None:
        assert doc_stable_key("", "hash", "file.pdf") == "hash"

    def test_falls_back_to_source_file(self) -> None:
        assert doc_stable_key("", "", "file.pdf") == "file.pdf"

    def test_empty_when_all_empty(self) -> None:
        assert doc_stable_key("", "", "") == ""
