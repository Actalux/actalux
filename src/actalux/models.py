"""Domain models as immutable dataclasses."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime
from decimal import Decimal


def chunk_hash_id(chunk_id: int | None) -> str:
    """Return the display hash for a chunk ID."""
    if chunk_id is None:
        return "#unknown"
    return f"#q{chunk_id:04x}"


@dataclass(frozen=True)
class Document:
    """An official document (agenda, minutes, packet, resolution)."""

    meeting_date: date
    meeting_title: str
    document_type: str  # "agenda", "minutes", "packet", "resolution", "transcript", etc.
    source_url: str
    source_file: str
    content: str
    content_hash: str = ""
    source_portal: str = ""  # "diligent", "claytonschools", "youtube", "manual"
    video_id: str = ""  # YouTube video id for board-meeting docs; "" for non-video docs
    version: int = 1
    replaces_id: int | None = None
    last_checked_at: datetime | None = None
    updated_at: datetime | None = None
    id: int | None = None
    created_at: datetime | None = None


@dataclass(frozen=True)
class Chunk:
    """A searchable passage extracted verbatim from a document."""

    document_id: int
    content: str
    section: str = ""
    speaker: str = ""
    chunk_index: int = 0
    embedding: list[float] = field(default_factory=list)
    start_seconds: int | None = None  # video offset for YouTube transcript chunks
    id: int | None = None

    @property
    def hash_id(self) -> str:
        """Short deterministic hash for display (e.g., #q3f8a)."""
        return chunk_hash_id(self.id)


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
class BudgetLineItem:
    """A structured budget figure with a verbatim citation.

    Every figure traces to a document (``document_id``) and, where
    available, the exact passage (``chunk_id``) plus the ``source_quote``
    it was read from. ``amount`` is a Decimal to preserve cents.
    """

    fiscal_year: str  # e.g. "2023-2024"
    category: str  # "revenue", "expenditure", "fund_balance"
    amount: Decimal
    document_id: int
    dimension: str = "fund"  # breakdown: "fund", "source", "function", "budget"
    fund: str = ""
    subcategory: str = ""
    basis: str | None = None  # budget-vs-actual only: "original", "final", "actual"
    chunk_id: int | None = None
    source_quote: str = ""
    note: str = ""
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
