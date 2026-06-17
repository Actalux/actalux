"""Tests for domain model helpers."""

from datetime import date

from actalux.models import Chunk, Document, chunk_hash_id


class TestChunkHashId:
    def test_formats_small_ids(self) -> None:
        # Legacy numeric branch: row id rendered as hex (transition shim).
        assert chunk_hash_id(63) == "#q003f"

    def test_keeps_hash_prefix_for_large_ids(self) -> None:
        assert chunk_hash_id(65536) == "#q10000"

    def test_stable_citation_id_string(self) -> None:
        # The primary form: a content-addressed citation_id renders verbatim.
        assert chunk_hash_id("a3f91c08") == "#qa3f91c08"

    def test_none_and_empty_are_unknown(self) -> None:
        assert chunk_hash_id(None) == "#unknown"
        assert chunk_hash_id("") == "#unknown"

    def test_unknown_id(self) -> None:
        assert Chunk(document_id=1, content="x").hash_id == "#unknown"

    def test_hash_id_prefers_citation_id(self) -> None:
        chunk = Chunk(document_id=1, content="x", citation_id="a3f91c08", id=976)
        assert chunk.hash_id == "#qa3f91c08"


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
