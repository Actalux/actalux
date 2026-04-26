"""Chunk documents into searchable passages with metadata.

Chunking rules (from DESIGN.md):
- Chunk by section/agenda item
- Target ~200 words per chunk
- Split long sections at paragraph boundaries
- 2-sentence overlap between consecutive chunks
- Never split mid-sentence
- Each chunk is verbatim document text
"""

from __future__ import annotations

import logging
import re

from actalux.errors import ChunkError
from actalux.models import Chunk

logger = logging.getLogger(__name__)

# Sentence boundary: period/question/exclamation followed by space and uppercase letter
_SENTENCE_RE = re.compile(r"(?<=[.!?])\s+(?=[A-Z])")


def chunk_document(
    document_id: int,
    text: str,
    target_words: int = 200,
    overlap_sentences: int = 2,
) -> list[Chunk]:
    """Split document text into chunks with section metadata.

    Sections are detected by markdown headings (## or ###).
    Within a section, text is split into ~target_words chunks at paragraph
    boundaries, with overlap_sentences of overlap.
    """
    if not text.strip():
        raise ChunkError("Cannot chunk empty document")

    sections = _split_into_sections(text)
    chunks: list[Chunk] = []

    chunk_index = 0
    for section_title, section_text in sections:
        section_chunks = _chunk_section(
            section_text,
            target_words=target_words,
            overlap_sentences=overlap_sentences,
        )
        for chunk_text in section_chunks:
            speaker = _extract_speaker(chunk_text)
            chunks.append(
                Chunk(
                    document_id=document_id,
                    content=chunk_text,
                    section=section_title,
                    speaker=speaker,
                    chunk_index=chunk_index,
                )
            )
            chunk_index += 1

    if not chunks:
        raise ChunkError("Document produced zero chunks")

    logger.info(
        "Chunked document %d into %d chunks (avg %d words)",
        document_id,
        len(chunks),
        sum(len(c.content.split()) for c in chunks) // len(chunks),
    )
    return chunks


def validate_chunks(chunks: list[Chunk], source_text: str) -> list[Chunk]:
    """Verify each chunk is an exact substring of the source document.

    Returns only valid chunks. Logs warnings for invalid ones.
    This is the ingest-time citation integrity guarantee.
    """
    valid: list[Chunk] = []
    for chunk in chunks:
        # Normalize whitespace for comparison
        normalized_chunk = _normalize_whitespace(chunk.content)
        normalized_source = _normalize_whitespace(source_text)
        if normalized_chunk in normalized_source:
            valid.append(chunk)
        else:
            logger.warning(
                "Chunk failed validation (not a substring of source): %.80s...",
                chunk.content,
            )
    return valid


def _split_into_sections(text: str) -> list[tuple[str, str]]:
    """Split text at section headings into (title, body) pairs.

    Detects both:
    - Markdown headings: ## Title or ### Title
    - Numbered sections from official minutes: "1. Business Meeting" or
      "Action: 6.01 Cooperation Agreement..."
    - Underlined headings: Title followed by a line of === or ---

    If no headings found, the entire text is one section with title "".
    """
    # Try markdown headings first
    heading_re = re.compile(r"^(#{2,3})\s+(.+)$", re.MULTILINE)
    matches = list(heading_re.finditer(text))

    # If no markdown headings, try numbered section patterns from official minutes
    if not matches:
        # Match patterns like:
        # "1. Business Meeting – 7:00 p.m."
        # "Action: 6.01 Cooperation Agreement..."
        # "Information: 4.01 Superintendent Communications"
        # "First Reading: 5.01 Policy BDC..."
        section_re = re.compile(
            r"^(?:"
            r"\d+\.\s+[A-Z][^\n]+"  # "1. Business Meeting"
            r"|(?:Action|Information|First Reading|Discussion)"  # labeled items
            r"(?:\s*\(?\w+\)?)?\s*:\s*\d+\.\d+\s+[^\n]+"
            r")",
            re.MULTILINE,
        )
        matches = list(section_re.finditer(text))

    if not matches:
        return [("", text)]

    sections: list[tuple[str, str]] = []

    # Content before first heading
    preamble = text[: matches[0].start()].strip()
    if preamble:
        sections.append(("", preamble))

    for i, match in enumerate(matches):
        # For markdown headings, title is in group 2. For section patterns, it's group 0.
        try:
            title = match.group(2).strip()
        except IndexError:
            title = match.group(0).strip()
        start = match.end()
        end = matches[i + 1].start() if i + 1 < len(matches) else len(text)
        body = text[start:end].strip()
        if body:
            sections.append((title, body))

    return sections


def _chunk_section(
    text: str,
    target_words: int,
    overlap_sentences: int,
) -> list[str]:
    """Split a section into chunks at paragraph boundaries.

    If a paragraph is under target_words, accumulate paragraphs until
    the target is reached. If a single paragraph exceeds the target,
    split it at sentence boundaries.
    """
    paragraphs = [p.strip() for p in text.split("\n\n") if p.strip()]

    if not paragraphs:
        return []

    chunks: list[str] = []
    current_paragraphs: list[str] = []
    current_words = 0

    for para in paragraphs:
        para_words = len(para.split())

        # Single paragraph exceeds target: split at sentence boundaries
        if para_words > target_words * 1.5 and not current_paragraphs:
            sentence_chunks = _split_at_sentences(para, target_words, overlap_sentences)
            chunks.extend(sentence_chunks)
            continue

        # Adding this paragraph would exceed target
        if current_words + para_words > target_words * 1.3 and current_paragraphs:
            chunks.append("\n\n".join(current_paragraphs))
            # Overlap: keep last overlap_sentences from the end
            overlap_text = _get_trailing_sentences(current_paragraphs[-1], overlap_sentences)
            current_paragraphs = [overlap_text] if overlap_text else []
            current_words = len(overlap_text.split()) if overlap_text else 0

        current_paragraphs.append(para)
        current_words += para_words

    # Flush remaining
    if current_paragraphs:
        chunks.append("\n\n".join(current_paragraphs))

    return chunks


def _split_at_sentences(
    text: str,
    target_words: int,
    overlap_sentences: int,
) -> list[str]:
    """Split a long paragraph at sentence boundaries."""
    sentences = _SENTENCE_RE.split(text)
    if len(sentences) <= 1:
        return [text]

    chunks: list[str] = []
    current: list[str] = []
    current_words = 0

    for sentence in sentences:
        s_words = len(sentence.split())
        if current_words + s_words > target_words * 1.3 and current:
            chunks.append(" ".join(current))
            # Overlap
            overlap = current[-overlap_sentences:] if overlap_sentences else []
            current = list(overlap)
            current_words = sum(len(s.split()) for s in current)

        current.append(sentence)
        current_words += s_words

    if current:
        chunks.append(" ".join(current))

    return chunks


def _get_trailing_sentences(text: str, n: int) -> str:
    """Get the last n sentences from text."""
    if n <= 0:
        return ""
    sentences = _SENTENCE_RE.split(text)
    trailing = sentences[-n:] if len(sentences) >= n else sentences
    return " ".join(trailing)


def _extract_speaker(text: str) -> str:
    """Try to extract a speaker name from bold markdown pattern.

    Matches: **Speaker Name** (HH:MM:SS) or **Speaker Name**:
    Returns empty string if no speaker found.
    """
    speaker_re = re.compile(r"\*\*([^*]+)\*\*")
    match = speaker_re.search(text)
    if match:
        return match.group(1).strip()
    return ""


def _normalize_whitespace(text: str) -> str:
    """Collapse all whitespace to single spaces for comparison."""
    return " ".join(text.split())
