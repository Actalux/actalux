"""Domain models as immutable dataclasses."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime
from decimal import Decimal


def chunk_hash_id(ref: int | str | None) -> str:
    """Return the display hash for a chunk.

    Accepts either a stable ``citation_id`` (the content-addressed string, e.g.
    ``"a3f91c08"`` -> ``"#qa3f91c08"``) or a legacy numeric row id (rendered as
    hex, ``8140`` -> ``"#q1fcc"``). The numeric branch is a transition shim for
    call sites that still pass a row id (budget figures, facilities) until they
    carry ``citation_id``; it is removed once every caller passes the stable id.
    """
    if ref is None or ref == "":
        return "#unknown"
    if isinstance(ref, int):
        return f"#q{ref:04x}"
    return f"#q{ref}"


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
    # Stable external identity (normalized canonical origin URL). Dedup keys on
    # this first so PDF/HTML twins of the same record collapse to one document;
    # "" for legacy/hand-added docs with no known origin (falls back to filename).
    source_ref: str = ""
    # How meeting_date was derived: 'filename' | 'content' | 'manual' | 'default' | 'unknown'.
    # 'default' means ingest fell back to date.today() — a suspect value that needs
    # human review. 'unknown' is the column default for rows ingested before A3.
    date_source: str = "unknown"
    video_id: str = ""  # YouTube video id for board-meeting docs; "" for non-video docs
    # Owning public body (entities.id). Entity-scoped browse/search filter on it,
    # so a doc with entity_id=None is invisible to those views — ingest must set it.
    entity_id: int | None = None
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
    # Stable, content-addressed citation id (see ingest.hashing.compute_citation_id):
    # survives re-ingest's SERIAL-id reassignment, so it is what citations render
    # and route on. "" only for a chunk ingested before the column existed (the
    # backfill fills those; render falls back to the row id while empty).
    citation_id: str = ""
    id: int | None = None

    @property
    def hash_id(self) -> str:
        """Display hash, from the stable citation_id when set, else the row id."""
        return chunk_hash_id(self.citation_id or self.id)


@dataclass(frozen=True)
class Vote:
    """A structured vote record extracted from official minutes.

    Every record cites the verbatim minutes passage it was parsed from: ``chunk_id``
    is the best-effort numeric link (nulled when the source doc is re-ingested) and
    ``citation_id`` the stable, content-addressed reference that survives re-ingest;
    ``source_quote`` is the motion/tally/result text itself. Tallies are
    ``int | None`` — ``None`` means the minutes recorded a result with no
    per-member count, which is distinct from a recorded 0.
    """

    document_id: int
    meeting_date: date
    motion: str
    result: str  # normalized: "passed", "failed", "tabled", "withdrawn"
    # "stated" when the minutes printed a result word; "derived" when passed/failed
    # was computed from the verbatim roll call because no result line was printed.
    result_basis: str = "stated"
    vote_count_yes: int | None = None
    vote_count_no: int | None = None
    vote_count_abstain: int | None = None
    details: dict | None = None  # mover/seconder + per-member votes when recorded
    chunk_id: int | None = None
    citation_id: str = ""
    source_quote: str = ""
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
    # Stable citation reference (chunks.citation_id) so the figure keeps citing the
    # same passage when its source document is re-ingested and chunk_id is nulled.
    citation_id: str = ""
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
