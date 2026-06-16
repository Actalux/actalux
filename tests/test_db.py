"""Tests for database query construction."""

from dataclasses import replace
from datetime import date

from actalux.db import (
    backfill_document_source_ref,
    get_chunk_with_context,
    insert_document,
)
from actalux.models import Document


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
