"""Tests for the supersession resolver (db.resolve_canonical_*).

Covers: chain following to the current version, the no-op when a row is already
current, cycle/dangling-pointer safety, and the chunk re-anchoring that NEVER
sends a citation to a different passage (content match only).
"""

from __future__ import annotations

from typing import Any

from actalux.db import (
    resolve_canonical_chunk,
    resolve_canonical_document,
)


class _Result:
    def __init__(self, data: list[dict[str, Any]]) -> None:
        self.data = data


class _DocQuery:
    """Mimics client.table('documents').select('*').eq('id', X).execute()."""

    def __init__(self, docs: dict[int, dict[str, Any]]) -> None:
        self._docs = docs
        self._wanted_id: int | None = None

    def select(self, _cols: str) -> _DocQuery:
        return self

    def eq(self, column: str, value: Any) -> _DocQuery:
        if column == "id":
            self._wanted_id = value
        return self

    def execute(self) -> _Result:
        row = self._docs.get(self._wanted_id)
        return _Result([row] if row is not None else [])


class _ChunkQuery:
    """Mimics client.table('chunks').select('*').eq('document_id', X).order(...).execute()."""

    def __init__(self, chunks_by_doc: dict[int, list[dict[str, Any]]]) -> None:
        self._chunks_by_doc = chunks_by_doc
        self._doc_id: int | None = None

    def select(self, _cols: str) -> _ChunkQuery:
        return self

    def eq(self, column: str, value: Any) -> _ChunkQuery:
        if column == "document_id":
            self._doc_id = value
        return self

    def order(self, _column: str) -> _ChunkQuery:
        return self

    def execute(self) -> _Result:
        return _Result(list(self._chunks_by_doc.get(self._doc_id, [])))


class _FakeClient:
    """A minimal Supabase-shaped client over in-memory documents + chunks."""

    def __init__(
        self,
        docs: dict[int, dict[str, Any]],
        chunks_by_doc: dict[int, list[dict[str, Any]]] | None = None,
    ) -> None:
        self._docs = docs
        self._chunks_by_doc = chunks_by_doc or {}

    def table(self, name: str) -> Any:
        if name == "documents":
            return _DocQuery(self._docs)
        if name == "chunks":
            return _ChunkQuery(self._chunks_by_doc)
        raise AssertionError(f"unexpected table {name!r}")


def _doc(doc_id: int, replaces_id: int | None = None, **extra: Any) -> dict[str, Any]:
    return {"id": doc_id, "replaces_id": replaces_id, **extra}


class TestResolveCanonicalDocument:
    def test_current_document_is_noop(self) -> None:
        client = _FakeClient({10: _doc(10, None)})
        result = resolve_canonical_document(client, 10)
        assert result.document is not None
        assert result.document["id"] == 10
        assert result.superseded is False
        assert result.requested_id == 10

    def test_one_hop_chain_resolves_to_canonical(self) -> None:
        # 10 superseded by 11 (11 is current).
        client = _FakeClient({10: _doc(10, 11), 11: _doc(11, None)})
        result = resolve_canonical_document(client, 10)
        assert result.document["id"] == 11
        assert result.superseded is True

    def test_multi_hop_chain_follows_to_current(self) -> None:
        # 10 -> 11 -> 12 (current).
        client = _FakeClient({10: _doc(10, 11), 11: _doc(11, 12), 12: _doc(12, None)})
        result = resolve_canonical_document(client, 10)
        assert result.document["id"] == 12
        assert result.superseded is True

    def test_missing_document_returns_none(self) -> None:
        client = _FakeClient({})
        result = resolve_canonical_document(client, 999)
        assert result.document is None
        assert result.superseded is False

    def test_cycle_returns_requested_row_not_superseded(self) -> None:
        # Malformed mutual reference: 10 -> 11 -> 10. Resolver must terminate,
        # report not-superseded, AND return the REQUESTED row (10) — not the
        # mid-cycle row (11) — so /document/10 renders document 10 in place
        # rather than silently swapping in 11 or bouncing a 301 loop.
        client = _FakeClient({10: _doc(10, 11), 11: _doc(11, 10)})
        result = resolve_canonical_document(client, 10)
        assert result.document["id"] == 10
        assert result.superseded is False

    def test_hop_bound_returns_requested_row_not_superseded(self) -> None:
        # A pathological long chain that never reaches a true canonical within the
        # hop bound must not redirect, and must return the requested row in place.
        from actalux.db import _MAX_SUPERSESSION_HOPS

        # Build a chain 0 -> 1 -> 2 -> ... longer than the bound, none canonical.
        n = _MAX_SUPERSESSION_HOPS + 5
        docs = {i: _doc(i, i + 1) for i in range(n)}
        client = _FakeClient(docs)
        result = resolve_canonical_document(client, 0)
        assert result.document["id"] == 0
        assert result.superseded is False

    def test_dangling_replaces_id_stops_at_last_good_row(self) -> None:
        # 10 points at a deleted canonical 11; keep the last good row (10).
        client = _FakeClient({10: _doc(10, 11)})
        result = resolve_canonical_document(client, 10)
        assert result.document["id"] == 10
        assert result.superseded is False  # no successful hop was taken


class TestResolveCanonicalChunk:
    def test_exact_content_match_reanchors(self) -> None:
        # Old chunk in superseded doc 10; canonical doc 11 has the same passage.
        old_chunk = {"id": 1, "document_id": 10, "chunk_index": 2, "content": "The board approved."}
        chunks_by_doc = {
            11: [
                {
                    "id": 50,
                    "document_id": 11,
                    "chunk_index": 0,
                    "content": "Meeting called to order.",
                },
                {"id": 51, "document_id": 11, "chunk_index": 1, "content": "The board approved."},
            ]
        }
        client = _FakeClient({}, chunks_by_doc)
        match = resolve_canonical_chunk(client, old_chunk, canonical_doc_id=11)
        assert match is not None
        assert match["id"] == 51

    def test_whitespace_and_case_insensitive_match(self) -> None:
        old_chunk = {
            "id": 1,
            "document_id": 10,
            "chunk_index": 0,
            "content": "The Board   Approved.",
        }
        chunks_by_doc = {
            11: [{"id": 60, "document_id": 11, "chunk_index": 0, "content": "the board approved."}]
        }
        client = _FakeClient({}, chunks_by_doc)
        match = resolve_canonical_chunk(client, old_chunk, canonical_doc_id=11)
        assert match is not None
        assert match["id"] == 60

    def test_no_content_match_returns_none(self) -> None:
        # Position 0 exists in canonical, but its content differs — never jump.
        old_chunk = {"id": 1, "document_id": 10, "chunk_index": 0, "content": "Resolution 24-01."}
        chunks_by_doc = {
            11: [
                {
                    "id": 70,
                    "document_id": 11,
                    "chunk_index": 0,
                    "content": "Entirely different text.",
                }
            ]
        }
        client = _FakeClient({}, chunks_by_doc)
        assert resolve_canonical_chunk(client, old_chunk, canonical_doc_id=11) is None

    def test_duplicate_passage_disambiguated_by_index(self) -> None:
        old_chunk = {"id": 1, "document_id": 10, "chunk_index": 1, "content": "Approved."}
        chunks_by_doc = {
            11: [
                {"id": 80, "document_id": 11, "chunk_index": 0, "content": "Approved."},
                {"id": 81, "document_id": 11, "chunk_index": 1, "content": "Approved."},
            ]
        }
        client = _FakeClient({}, chunks_by_doc)
        match = resolve_canonical_chunk(client, old_chunk, canonical_doc_id=11)
        assert match is not None
        assert match["id"] == 81

    def test_duplicate_passage_no_index_match_returns_none(self) -> None:
        # Ambiguous repeated passage and no index lines up -> refuse to guess.
        old_chunk = {"id": 1, "document_id": 10, "chunk_index": 9, "content": "Approved."}
        chunks_by_doc = {
            11: [
                {"id": 80, "document_id": 11, "chunk_index": 0, "content": "Approved."},
                {"id": 81, "document_id": 11, "chunk_index": 1, "content": "Approved."},
            ]
        }
        client = _FakeClient({}, chunks_by_doc)
        assert resolve_canonical_chunk(client, old_chunk, canonical_doc_id=11) is None

    def test_empty_content_returns_none(self) -> None:
        client = _FakeClient({}, {11: [{"id": 1, "chunk_index": 0, "content": "x"}]})
        old_chunk = {"id": 1, "document_id": 10, "chunk_index": 0, "content": ""}
        assert resolve_canonical_chunk(client, old_chunk, canonical_doc_id=11) is None
