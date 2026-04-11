"""Domain models as immutable dataclasses."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime


@dataclass(frozen=True)
class Document:
    """An official document (agenda, minutes, packet, resolution)."""

    meeting_date: date
    meeting_title: str
    document_type: str  # "agenda", "minutes", "packet", "resolution"
    source_url: str
    source_file: str
    content: str
    id: int | None = None
    created_at: datetime | None = None


@dataclass(frozen=True)
class Chunk:
    """A searchable passage extracted verbatim from a document."""

    document_id: int
    content: str
    section: str = ""
    speaker: str = ""
    embedding: list[float] = field(default_factory=list)
    id: int | None = None

    @property
    def hash_id(self) -> str:
        """Short deterministic hash for display (e.g., #q3f8a)."""
        if self.id is None:
            return "#unknown"
        # 5-char hex from chunk id, stable and deterministic
        return f"#q{self.id:04x}"[-6:]


@dataclass(frozen=True)
class Vote:
    """A structured vote record extracted from official minutes."""

    document_id: int
    meeting_date: date
    motion: str
    result: str  # "passed", "failed", "tabled"
    vote_count_yes: int = 0
    vote_count_no: int = 0
    vote_count_abstain: int = 0
    details: dict | None = None  # per-member votes if available
    id: int | None = None


@dataclass(frozen=True)
class Speaker:
    """A board member or official appearing in records."""

    name: str
    role: str = ""  # "Board Member", "Superintendent", etc.
    active: bool = True
    id: int | None = None


@dataclass(frozen=True)
class Correction:
    """An error report submitted by a user."""

    chunk_id: int
    description: str
    reporter_email: str = ""
    status: str = "open"  # "open", "fixed", "dismissed"
    id: int | None = None
    created_at: datetime | None = None


@dataclass(frozen=True)
class IngestRun:
    """Tracks the result of an ingestion run for a single meeting."""

    meeting_date: date
    meeting_title: str
    docs_found: int
    docs_ingested: int
    docs_failed: int
    errors: list[str] = field(default_factory=list)
    id: int | None = None
    created_at: datetime | None = None
