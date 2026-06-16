"""Tests for domain model helpers."""

from datetime import date

from actalux.models import Chunk, Document, chunk_hash_id


class TestChunkHashId:
    def test_formats_small_ids(self) -> None:
        assert chunk_hash_id(63) == "#q003f"

    def test_keeps_hash_prefix_for_large_ids(self) -> None:
        assert chunk_hash_id(65536) == "#q10000"

    def test_unknown_id(self) -> None:
        assert Chunk(document_id=1, content="x").hash_id == "#unknown"


class TestDocumentSourceRef:
    """source_ref carries the stable external identity used for dedup."""

    def test_defaults_to_empty(self) -> None:
        # Backward-compatible: docs constructed without source_ref get "".
        doc = Document(
            meeting_date=date(2024, 8, 4),
            meeting_title="Resolution",
            document_type="resolution",
            source_url="",
            source_file="resolution.pdf",
            content="body",
        )
        assert doc.source_ref == ""

    def test_round_trips_value(self) -> None:
        doc = Document(
            meeting_date=date(2024, 8, 4),
            meeting_title="Resolution",
            document_type="resolution",
            source_url="https://example.test/document/abc",
            source_file="resolution.pdf",
            content="body",
            source_ref="https://example.test/document/abc",
        )
        assert doc.source_ref == "https://example.test/document/abc"
