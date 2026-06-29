"""Tests for database query construction."""

from dataclasses import replace
from datetime import date

import pytest
from postgrest.exceptions import APIError

from actalux.db import (
    _CHUNK_INSERT_BATCH,
    _SUPERSEDE_RETRIES,
    backfill_document_source_ref,
    delete_chunks_for_document,
    delete_document,
    document_has_chunks,
    fetch_all_rows,
    get_chunk_citation_ids,
    get_chunk_with_context,
    insert_chunks,
    insert_document,
    resolve_chunk_ref,
    resolve_source_anchor,
    supersede_document,
)
from actalux.models import Chunk, Document


class _Result:
    def __init__(self, data: list[dict]) -> None:
        self.data = data


class _Query:
    def __init__(self, data: list[dict], calls: list[tuple]) -> None:
        self.data = data
        self.calls = calls

    def select(self, value: str) -> "_Query":
        self.calls.append(("select", value))
        return self

    def eq(self, column: str, value: object) -> "_Query":
        self.calls.append(("eq", column, value))
        return self

    def gte(self, column: str, value: object) -> "_Query":
        self.calls.append(("gte", column, value))
        return self

    def lte(self, column: str, value: object) -> "_Query":
        self.calls.append(("lte", column, value))
        return self

    def in_(self, column: str, values: object) -> "_Query":
        self.calls.append(("in_", column, values))
        return self

    def order(self, column: str) -> "_Query":
        self.calls.append(("order", column))
        return self

    def execute(self) -> _Result:
        self.calls.append(("execute",))
        return _Result(self.data)


class _Client:
    def __init__(self, responses: list[list[dict]]) -> None:
        self.responses = responses
        self.queries: list[list[tuple]] = []

    def table(self, name: str) -> _Query:
        calls: list[tuple] = [("table", name)]
        self.queries.append(calls)
        return _Query(self.responses.pop(0), calls)


class _InsertResult:
    def __init__(self, data: list[dict]) -> None:
        self.data = data


class _InsertTable:
    """Captures the row dict passed to .insert(...) for assertion."""

    def __init__(self, captured: dict) -> None:
        self._captured = captured

    def insert(self, data: dict) -> "_InsertTable":
        self._captured.update(data)
        return self

    def execute(self) -> _InsertResult:
        return _InsertResult([{"id": 1}])


class _InsertClient:
    def __init__(self) -> None:
        self.captured: dict = {}

    def table(self, _name: str) -> _InsertTable:
        return _InsertTable(self.captured)


_BASE_DOC = Document(
    meeting_date=date(2025, 2, 19),
    meeting_title="February 19, 2025 BOE Meeting Minutes",
    document_type="minutes",
    source_url="https://example.test/doc.pdf",
    source_file="February 19, 2025 BOE Meeting Minutes.pdf",
    content="body",
    content_hash="abc",
)


def _doc(**overrides) -> Document:
    return replace(_BASE_DOC, **overrides)


class TestInsertDocumentEntity:
    """A doc must persist its entity_id, or it is invisible to entity-scoped views."""

    def test_entity_id_written_when_set(self) -> None:
        client = _InsertClient()
        insert_document(client, _doc(entity_id=1))
        assert client.captured["entity_id"] == 1

    def test_entity_id_omitted_when_none(self) -> None:
        # Omitted (not NULL) so the column default/whatever is preserved; ingest is
        # expected to always pass one, but the writer must not force a NULL.
        client = _InsertClient()
        insert_document(client, _doc(entity_id=None))
        assert "entity_id" not in client.captured


class TestInsertDocumentSourceRef:
    """The stable external id must persist, or dedup can't key on it next time."""

    def test_source_ref_written(self) -> None:
        client = _InsertClient()
        insert_document(client, _doc(source_ref="https://example.test/document/abc"))
        assert client.captured["source_ref"] == "https://example.test/document/abc"

    def test_empty_source_ref_written_as_empty(self) -> None:
        # Always written (matches source_portal/video_id) so the row is explicit;
        # the column default and "" agree, so legacy/origin-less docs are fine.
        client = _InsertClient()
        insert_document(client, _doc(source_ref=""))
        assert client.captured["source_ref"] == ""


class TestInsertDocumentDateSource:
    """date_source provenance must be persisted; without it the column is stuck at 'unknown'."""

    def test_filename_provenance_written(self) -> None:
        client = _InsertClient()
        insert_document(client, _doc(date_source="filename"))
        assert client.captured["date_source"] == "filename"

    def test_default_provenance_written(self) -> None:
        # 'default' means ingest fell back to date.today() — a suspect date that
        # needs human review.  It must persist so auditors can surface these rows.
        client = _InsertClient()
        insert_document(client, _doc(date_source="default"))
        assert client.captured["date_source"] == "default"

    def test_unknown_provenance_written(self) -> None:
        # 'unknown' is the Document default — legacy rows ingested before A3.
        client = _InsertClient()
        insert_document(client, _doc(date_source="unknown"))
        assert client.captured["date_source"] == "unknown"


class _BatchInsertTable:
    """Records each .insert(batch) and returns ascending ids across batches."""

    def __init__(self, recorder: "_BatchInsertClient") -> None:
        self._recorder = recorder
        self._batch: list[dict] = []

    def insert(self, rows: list[dict]) -> "_BatchInsertTable":
        self._batch = rows
        return self

    def execute(self) -> _InsertResult:
        self._recorder.batch_sizes.append(len(self._batch))
        start = self._recorder.next_id
        ids = list(range(start, start + len(self._batch)))
        self._recorder.next_id = start + len(self._batch)
        return _InsertResult([{"id": i} for i in ids])


class _BatchInsertClient:
    def __init__(self) -> None:
        self.batch_sizes: list[int] = []
        self.next_id = 1

    def table(self, _name: str) -> _BatchInsertTable:
        return _BatchInsertTable(self)


class _TimeoutInsertTable:
    """Raises the free-tier statement timeout (57014) for batches larger than
    ``max_ok``; with a non-timeout ``error_code`` it always raises that instead."""

    def __init__(self, recorder: "_TimeoutInsertClient") -> None:
        self._recorder = recorder
        self._batch: list[dict] = []

    def insert(self, rows: list[dict]) -> "_TimeoutInsertTable":
        self._batch = rows
        return self

    def execute(self) -> _InsertResult:
        rec = self._recorder
        if rec.error_code != "57014":
            raise APIError({"message": "boom", "code": rec.error_code})
        if len(self._batch) > rec.max_ok:
            raise APIError(
                {"message": "canceling statement due to statement timeout", "code": "57014"}
            )
        rec.ok_sizes.append(len(self._batch))
        start = rec.next_id
        ids = list(range(start, start + len(self._batch)))
        rec.next_id = start + len(self._batch)
        return _InsertResult([{"id": i} for i in ids])


class _TimeoutInsertClient:
    def __init__(self, max_ok: int, error_code: str = "57014") -> None:
        self.max_ok = max_ok
        self.error_code = error_code
        self.ok_sizes: list[int] = []
        self.next_id = 1

    def table(self, _name: str) -> _TimeoutInsertTable:
        return _TimeoutInsertTable(self)


class TestInsertChunksBatching:
    """A long transcript's chunks must insert in batches, or one statement's HNSW
    index work trips the free-tier statement timeout (the backfill failure)."""

    def _chunks(self, n: int) -> list[Chunk]:
        return [Chunk(document_id=7, content=f"c{i}", chunk_index=i) for i in range(n)]

    def test_splits_into_batches_and_returns_ids_in_order(self) -> None:
        n = 162  # the doc that failed the backfill; at batch 25 -> 6x25 + 12
        client = _BatchInsertClient()
        ids = insert_chunks(client, self._chunks(n))
        assert client.batch_sizes == [25, 25, 25, 25, 25, 25, 12]
        assert max(client.batch_sizes) <= _CHUNK_INSERT_BATCH
        assert len(ids) == n
        assert ids == sorted(ids)  # ids returned in chunk order

    def test_single_batch_when_small(self) -> None:
        client = _BatchInsertClient()
        ids = insert_chunks(client, self._chunks(3))
        assert client.batch_sizes == [3]
        assert len(ids) == 3

    def test_empty_is_noop(self) -> None:
        client = _BatchInsertClient()
        assert insert_chunks(client, []) == []
        assert client.batch_sizes == []

    def test_halves_on_statement_timeout_preserving_order(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # A 25-row batch that still trips the free-tier statement timeout must be
        # halved and retried until each piece fits — never crashing the backfill.
        monkeypatch.setattr("actalux.db.time.sleep", lambda *_: None)
        client = _TimeoutInsertClient(max_ok=7)  # anything > 7 rows "times out"
        n = 60
        ids = insert_chunks(client, self._chunks(n))
        assert len(ids) == n
        assert ids == sorted(ids)  # order preserved across the recursive halving
        assert client.ok_sizes  # at least one successful (small) insert
        assert all(size <= 7 for size in client.ok_sizes)  # every committed insert fit
        assert sum(client.ok_sizes) == n  # all rows landed exactly once

    def test_single_row_timeout_propagates(self, monkeypatch: pytest.MonkeyPatch) -> None:
        # If even one row times out, there's nothing left to halve — surface it.
        monkeypatch.setattr("actalux.db.time.sleep", lambda *_: None)
        client = _TimeoutInsertClient(max_ok=0)
        with pytest.raises(APIError):
            insert_chunks(client, self._chunks(2))

    def test_non_timeout_error_is_not_halved(self, monkeypatch: pytest.MonkeyPatch) -> None:
        # A non-timeout APIError (e.g. a constraint violation) must propagate
        # immediately, not get retried via halving.
        monkeypatch.setattr("actalux.db.time.sleep", lambda *_: None)
        client = _TimeoutInsertClient(max_ok=100, error_code="23505")
        with pytest.raises(APIError):
            insert_chunks(client, self._chunks(3))
        assert client.ok_sizes == []  # never retried


class _UpdateTable:
    """Captures the update payload and the eq() predicate chain."""

    def __init__(self, calls: list[tuple]) -> None:
        self.calls = calls

    def update(self, data: dict) -> "_UpdateTable":
        self.calls.append(("update", data))
        return self

    def eq(self, column: str, value: object) -> "_UpdateTable":
        self.calls.append(("eq", column, value))
        return self

    def execute(self) -> _Result:
        self.calls.append(("execute",))
        return _Result([])


class _UpdateClient:
    def __init__(self) -> None:
        self.calls: list[tuple] = []

    def table(self, _name: str) -> _UpdateTable:
        return _UpdateTable(self.calls)


class TestBackfillDocumentSourceRef:
    """Backfill must be non-overwriting at the DB predicate, not just the caller."""

    def test_guards_on_empty_source_ref_and_id(self) -> None:
        client = _UpdateClient()
        backfill_document_source_ref(client, 5, "https://example.test/document/abc")

        assert ("update", {"source_ref": "https://example.test/document/abc"}) in client.calls
        # The id targets the row; the source_ref="" predicate refuses to clobber a
        # row that already carries a stable id (stale-read / reuse safety).
        assert ("eq", "id", 5) in client.calls
        assert ("eq", "source_ref", "") in client.calls


class TestChunkContext:
    def test_context_uses_document_local_chunk_index(self) -> None:
        client = _Client(
            [
                [{"id": 10, "document_id": 5, "chunk_index": 3, "content": "target"}],
                [{"id": 9}, {"id": 10}, {"id": 11}],
            ]
        )

        result = get_chunk_with_context(client, chunk_id=10, context_count=1)

        assert result["context"] == [{"id": 9}, {"id": 10}, {"id": 11}]
        assert ("gte", "chunk_index", 2) in client.queries[1]
        assert ("lte", "chunk_index", 4) in client.queries[1]
        assert ("order", "chunk_index") in client.queries[1]


# Chunks of one document, as resolve_source_anchor would read them. The anchor it
# resolves is a verbatim fragment that should match exactly one of these.
_ANCHOR_CHUNKS = [
    {"id": 100, "chunk_index": 0, "content": "The plan opens with an executive summary."},
    {
        "id": 101,
        "chunk_index": 1,
        "content": "It identifies approximately $23.5 million dollars in immediate needs.",
    },
    {
        "id": 102,
        "chunk_index": 2,
        "content": "Future-development options are presented separately.",
    },
]


class TestResolveSourceAnchor:
    """Anchor-to-chunk resolution: exact, whitespace-normalised, missing, ambiguous."""

    def test_exact_substring_match_returns_chunk_id(self) -> None:
        client = _Client([list(_ANCHOR_CHUNKS)])
        chunk_id = resolve_source_anchor(
            client, 87, "approximately $23.5 million dollars in immediate needs"
        )
        assert chunk_id == 101
        # It scoped the chunk read to the document.
        assert ("eq", "document_id", 87) in client.queries[0]

    def test_whitespace_and_case_normalised_match(self) -> None:
        # The anchor differs only by line wraps, double spaces, and casing — the
        # normalisation must still land it on the same chunk.
        client = _Client([list(_ANCHOR_CHUNKS)])
        chunk_id = resolve_source_anchor(
            client,
            87,
            "Approximately  $23.5 MILLION\n  dollars in immediate\tneeds",
        )
        assert chunk_id == 101

    def test_missing_anchor_returns_none(self) -> None:
        client = _Client([list(_ANCHOR_CHUNKS)])
        chunk_id = resolve_source_anchor(client, 87, "a phrase that is not in any chunk")
        assert chunk_id is None

    def test_empty_anchor_returns_none_without_query(self) -> None:
        client = _Client([])  # no responses: an empty anchor must short-circuit
        chunk_id = resolve_source_anchor(client, 87, "   ")
        assert chunk_id is None
        assert client.queries == []  # never hit the database

    def test_ambiguous_anchor_returns_none(self) -> None:
        # Two chunks contain the anchor: it cannot vouch for a single citation, so
        # the resolver refuses rather than picking one arbitrarily.
        dup_chunks = [
            {"id": 200, "chunk_index": 0, "content": "Roofing repairs are needed district-wide."},
            {"id": 201, "chunk_index": 1, "content": "Additional roofing repairs are scheduled."},
        ]
        client = _Client([dup_chunks])
        chunk_id = resolve_source_anchor(client, 87, "roofing repairs")
        assert chunk_id is None

    def test_cache_hit_avoids_second_query(self) -> None:
        cache: dict[tuple[int, str], int | None] = {}
        client = _Client([list(_ANCHOR_CHUNKS)])
        first = resolve_source_anchor(
            client, 87, "approximately $23.5 million dollars in immediate needs", cache=cache
        )
        assert first == 101
        assert len(client.queries) == 1
        # The second resolution of the same (doc, anchor) is served from the cache:
        # no further query is issued (the client has no more queued responses).
        second = resolve_source_anchor(
            client, 87, "approximately $23.5 million dollars in immediate needs", cache=cache
        )
        assert second == 101
        assert len(client.queries) == 1

    def test_anchor_extended_past_overlap_disambiguates(self) -> None:
        # The facilities-plan anchors rely on this: a phrase duplicated across two
        # overlapping chunks is ambiguous, but extending it into text unique to one
        # chunk lands a single match. (The real fix for the delivery-date and
        # options-table anchors, which the chunker's overlap duplicated.)
        overlap = [
            {"id": 300, "chunk_index": 0, "content": "Delivered to District on: 02.19.2025"},
            {
                "id": 301,
                "chunk_index": 1,
                "content": "Delivered to District on: 02.19.2025 THE IMPORTANCE OF planning.",
            },
        ]
        # Bare date line is in both chunks -> ambiguous -> None.
        assert (
            resolve_source_anchor(_Client([list(overlap)]), 87, "Delivered to District on") is None
        )
        # Extended into the unique "THE IMPORTANCE OF" run -> exactly one match.
        extended = resolve_source_anchor(
            _Client([list(overlap)]), 87, "02.19.2025 THE IMPORTANCE OF"
        )
        assert extended == 301


class TestResolveChunkRef:
    """A chunk ref resolves a stable citation_id (8 hex) or a legacy numeric id."""

    def test_citation_id_single_match(self) -> None:
        client = _Client([[{"id": 976, "document_id": 5}]])
        assert resolve_chunk_ref(client, "0f7e408e") == 976

    def test_citation_id_prefers_current_version(self) -> None:
        # Two chunks share the citation_id (a passage across versions); the chunk
        # in the current (replaces_id IS NULL) document wins.
        client = _Client(
            [
                [{"id": 47, "document_id": 10}, {"id": 8017, "document_id": 487}],
                [{"id": 10, "replaces_id": 99}, {"id": 487, "replaces_id": None}],
            ]
        )
        assert resolve_chunk_ref(client, "9f0a7e9e") == 8017

    def test_legacy_numeric_id_no_db(self) -> None:
        # A short numeric ref is interpreted directly, without a citation lookup.
        client = _Client([])  # table() must never be called
        assert resolve_chunk_ref(client, "976") == 976

    def test_unknown_citation_id_falls_through_to_none(self) -> None:
        client = _Client([[]])
        assert resolve_chunk_ref(client, "deadbeef") is None

    def test_eight_digit_ref_falls_back_to_numeric(self) -> None:
        # "00123456" matches the 8-hex shape but no chunk has it; falls back to
        # the numeric interpretation rather than 404ing.
        client = _Client([[]])
        assert resolve_chunk_ref(client, "00123456") == 123456


class TestGetChunkCitationIds:
    def test_maps_ids_to_citation_ids(self) -> None:
        client = _Client([[{"id": 1, "citation_id": "aaaa1111"}, {"id": 2, "citation_id": None}]])
        assert get_chunk_citation_ids(client, [1, 2]) == {1: "aaaa1111", 2: ""}

    def test_empty_input(self) -> None:
        assert get_chunk_citation_ids(_Client([]), []) == {}


class _PagedBuilder:
    """A fake PostgREST builder that pages a backing list via .range() like the
    real server (a single response is capped, so a full read must page)."""

    def __init__(self, rows: list[dict]) -> None:
        self._rows = rows
        self._order: str | None = None
        self._desc = False
        self._lo = 0
        self._hi: int | None = None

    def select(self, *_a) -> "_PagedBuilder":
        return self

    def eq(self, *_a) -> "_PagedBuilder":
        return self

    def order(self, column: str, desc: bool = False) -> "_PagedBuilder":
        self._order, self._desc = column, desc
        return self

    def range(self, lo: int, hi: int) -> "_PagedBuilder":
        self._lo, self._hi = lo, hi
        return self

    def execute(self) -> _Result:
        rows = self._rows
        if self._order:
            rows = sorted(rows, key=lambda r: r[self._order], reverse=self._desc)
        return _Result(rows[self._lo : (self._hi + 1 if self._hi is not None else None)])


class TestFetchAllRows:
    def test_pages_past_the_row_cap_in_order(self) -> None:
        backing = [{"id": i} for i in range(2300)]  # > 2 pages of 1000
        out = fetch_all_rows(lambda: _PagedBuilder(backing))
        assert [r["id"] for r in out] == list(range(2300))  # all rows, no gaps/dupes

    def test_exact_multiple_of_page_size_terminates(self) -> None:
        # Exactly 2 full pages: must read a 3rd (empty) page to detect the end.
        backing = [{"id": i} for i in range(2000)]
        assert len(fetch_all_rows(lambda: _PagedBuilder(backing))) == 2000

    def test_single_short_page(self) -> None:
        backing = [{"id": i} for i in range(5)]
        assert len(fetch_all_rows(lambda: _PagedBuilder(backing))) == 5

    def test_respects_desc_order(self) -> None:
        backing = [{"id": i} for i in range(3)]
        out = fetch_all_rows(lambda: _PagedBuilder(backing), order="id", desc=True)
        assert [r["id"] for r in out] == [2, 1, 0]


class _CountResult:
    def __init__(self, count: int | None) -> None:
        self.count = count
        self.data: list = []


class _CountTable:
    def __init__(self, count: int | None) -> None:
        self._count = count

    def select(self, *_a, **_k) -> "_CountTable":
        return self

    def eq(self, *_a, **_k) -> "_CountTable":
        return self

    def limit(self, *_a, **_k) -> "_CountTable":
        return self

    def execute(self) -> _CountResult:
        return _CountResult(self._count)


class _CountClient:
    def __init__(self, count: int | None) -> None:
        self._count = count

    def table(self, _name: str) -> _CountTable:
        return _CountTable(self._count)


class _DeleteTable:
    def __init__(self, calls: list) -> None:
        self._calls = calls

    def delete(self) -> "_DeleteTable":
        self._calls.append("delete")
        return self

    def eq(self, col: str, val: object) -> "_DeleteTable":
        self._calls.append((col, val))
        return self

    def execute(self) -> _Result:
        return _Result([])


class _DeleteClient:
    def __init__(self) -> None:
        self.calls: list = []

    def table(self, name: str) -> _DeleteTable:
        self.calls.append(name)
        return _DeleteTable(self.calls)


class TestDocumentHasChunks:
    def test_true_when_count_positive(self) -> None:
        assert document_has_chunks(_CountClient(3), 7) is True

    def test_false_when_zero(self) -> None:
        assert document_has_chunks(_CountClient(0), 7) is False

    def test_false_when_count_none(self) -> None:
        assert document_has_chunks(_CountClient(None), 7) is False


class TestDeleteDocument:
    def test_deletes_documents_row_by_id(self) -> None:
        client = _DeleteClient()
        delete_document(client, 42)
        assert client.calls[0] == "documents"
        assert "delete" in client.calls
        assert ("id", 42) in client.calls


class _SupersedeTable:
    def __init__(self, rec: dict) -> None:
        self._rec = rec

    def update(self, payload: dict) -> "_SupersedeTable":
        self._rec["payload"] = payload
        return self

    def eq(self, col: str, val: object) -> "_SupersedeTable":
        self._rec.setdefault("eqs", []).append((col, val))
        return self

    def execute(self) -> _Result:
        rec = self._rec
        rec["attempts"] += 1
        if rec["attempts"] <= rec["fail_timeouts"]:
            raise APIError(
                {"message": "canceling statement due to statement timeout", "code": "57014"}
            )
        if rec.get("hard_error"):
            raise APIError({"message": "boom", "code": "23505"})
        return _Result([])


class _SupersedeClient:
    def __init__(self, fail_timeouts: int = 0, hard_error: bool = False) -> None:
        self.rec = {"attempts": 0, "fail_timeouts": fail_timeouts, "hard_error": hard_error}

    def table(self, _name: str) -> _SupersedeTable:
        return _SupersedeTable(self.rec)


class TestSupersedeDocument:
    def test_succeeds_after_timeout_retries(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr("actalux.db.time.sleep", lambda *_: None)
        client = _SupersedeClient(fail_timeouts=2)  # times out twice, then succeeds
        supersede_document(client, old_id=10, new_id=20)
        assert client.rec["attempts"] == 3
        assert client.rec["payload"] == {"replaces_id": 20}
        assert ("id", 10) in client.rec["eqs"]

    def test_raises_after_max_timeouts(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr("actalux.db.time.sleep", lambda *_: None)
        client = _SupersedeClient(fail_timeouts=99)
        with pytest.raises(APIError):
            supersede_document(client, old_id=10, new_id=20)
        assert client.rec["attempts"] == _SUPERSEDE_RETRIES

    def test_non_timeout_propagates_immediately(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr("actalux.db.time.sleep", lambda *_: None)
        client = _SupersedeClient(hard_error=True)
        with pytest.raises(APIError):
            supersede_document(client, old_id=10, new_id=20)
        assert client.rec["attempts"] == 1  # not retried


class TestDeleteChunksForDocument:
    def test_deletes_chunks_by_document_id(self) -> None:
        client = _DeleteClient()
        delete_chunks_for_document(client, 42)
        assert client.calls[0] == "chunks"
        assert "delete" in client.calls
        assert ("document_id", 42) in client.calls
