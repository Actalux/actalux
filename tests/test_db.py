"""Tests for database query construction."""

from actalux.db import get_chunk_with_context


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
