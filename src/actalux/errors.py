"""Typed exceptions for Actalux. No bare except Exception."""


class ActaluxError(Exception):
    """Base exception for all Actalux errors."""


class ParseError(ActaluxError):
    """Document could not be parsed (corrupted, unreadable, or unsupported format)."""


class MetadataError(ActaluxError):
    """Required metadata (speaker, timestamp, section) is missing or malformed."""


class ChunkError(ActaluxError):
    """Chunking failed (empty content, validation mismatch)."""


class EmbeddingError(ActaluxError):
    """Embedding model failed to load or produce vectors."""


class SearchError(ActaluxError):
    """Search query failed (DB timeout, connection error)."""


class SummaryError(ActaluxError):
    """LLM summary generation failed (API error, citation verification failure)."""


class IngestError(ActaluxError):
    """Document ingestion pipeline failed at any stage."""
