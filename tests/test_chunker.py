"""Tests for the chunking module."""

from actalux.ingest.chunker import (
    _extract_speaker,
    _split_into_sections,
    chunk_document,
    validate_chunks,
)
from actalux.models import Chunk


class TestSplitIntoSections:
    def test_no_headings(self) -> None:
        text = "Just a paragraph of text."
        sections = _split_into_sections(text)
        assert len(sections) == 1
        assert sections[0][0] == ""
        assert sections[0][1] == text

    def test_with_headings(self) -> None:
        text = "## Budget\n\nBudget content here.\n\n## Personnel\n\nPersonnel content."
        sections = _split_into_sections(text)
        assert len(sections) == 2
        assert sections[0][0] == "Budget"
        assert "Budget content" in sections[0][1]
        assert sections[1][0] == "Personnel"
        assert "Personnel content" in sections[1][1]

    def test_preamble_before_first_heading(self) -> None:
        text = "Opening remarks.\n\n## Item 1\n\nFirst item content."
        sections = _split_into_sections(text)
        assert len(sections) == 2
        assert sections[0][0] == ""
        assert "Opening remarks" in sections[0][1]


class TestExtractSpeaker:
    def test_bold_speaker(self) -> None:
        text = "**Board Member Johnson** (00:14:32)\nWe propose a 3% reduction."
        assert _extract_speaker(text) == "Board Member Johnson"

    def test_no_speaker(self) -> None:
        text = "Just plain text with no speaker attribution."
        assert _extract_speaker(text) == ""


class TestChunkDocument:
    def test_basic_chunking(self) -> None:
        text = "## Section A\n\n" + ("word " * 150) + "\n\n" + ("word " * 150)
        chunks = chunk_document(document_id=1, text=text, target_words=200)
        assert len(chunks) >= 1
        for index, chunk in enumerate(chunks):
            assert chunk.document_id == 1
            assert chunk.chunk_index == index
            assert len(chunk.content) > 0

    def test_empty_text_raises(self) -> None:
        import pytest

        with pytest.raises(Exception):
            chunk_document(document_id=1, text="")

    def test_short_text_single_chunk(self) -> None:
        text = "## Budget\n\nThe budget was approved unanimously."
        chunks = chunk_document(document_id=1, text=text, target_words=200)
        assert len(chunks) == 1
        assert chunks[0].section == "Budget"

    def test_chunk_preserves_content(self) -> None:
        content = "The board voted 5-2 to approve the operational budget for FY2027."
        text = f"## Vote\n\n{content}"
        chunks = chunk_document(document_id=1, text=text, target_words=200)
        assert any(content in c.content for c in chunks)


class TestValidateChunks:
    def test_valid_chunks_pass(self) -> None:
        source = "The full document text with all the content."
        chunks = [
            Chunk(document_id=1, content="The full document text"),
            Chunk(document_id=1, content="all the content"),
        ]
        valid = validate_chunks(chunks, source)
        assert len(valid) == 2

    def test_invalid_chunks_rejected(self) -> None:
        source = "The full document text."
        chunks = [
            Chunk(document_id=1, content="This text is NOT in the source"),
        ]
        valid = validate_chunks(chunks, source)
        assert len(valid) == 0

    def test_mixed_valid_invalid(self) -> None:
        source = "Real content from the document."
        chunks = [
            Chunk(document_id=1, content="Real content"),
            Chunk(document_id=1, content="Fabricated content"),
        ]
        valid = validate_chunks(chunks, source)
        assert len(valid) == 1
        assert "Real content" in valid[0].content
