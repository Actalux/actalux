"""Tests for domain model helpers."""

from actalux.models import Chunk, chunk_hash_id


class TestChunkHashId:
    def test_formats_small_ids(self) -> None:
        assert chunk_hash_id(63) == "#q003f"

    def test_keeps_hash_prefix_for_large_ids(self) -> None:
        assert chunk_hash_id(65536) == "#q10000"

    def test_unknown_id(self) -> None:
        assert Chunk(document_id=1, content="x").hash_id == "#unknown"
