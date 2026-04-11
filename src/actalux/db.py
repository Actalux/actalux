"""Supabase database operations.

All DB access goes through this module. Uses the Supabase Python client
for data operations and raw SQL (via RPC) for pgvector queries.
"""

from __future__ import annotations

import logging
from typing import Any

from supabase import Client, create_client

from actalux.models import Chunk, Correction, Document, IngestRun, Vote

logger = logging.getLogger(__name__)

_client: Client | None = None


def get_client(url: str, key: str) -> Client:
    """Get or create the Supabase client. Cached after first call."""
    global _client
    if _client is not None:
        return _client
    _client = create_client(url, key)
    return _client


# --- Documents ---


def insert_document(client: Client, doc: Document) -> int:
    """Insert a document and return its ID."""
    data = {
        "meeting_date": doc.meeting_date.isoformat(),
        "meeting_title": doc.meeting_title,
        "document_type": doc.document_type,
        "source_url": doc.source_url,
        "source_file": doc.source_file,
        "content": doc.content,
    }
    result = client.table("documents").insert(data).execute()
    doc_id: int = result.data[0]["id"]
    logger.info("Inserted document %d: %s (%s)", doc_id, doc.meeting_title, doc.document_type)
    return doc_id


def get_document(client: Client, doc_id: int) -> dict[str, Any] | None:
    """Fetch a document by ID."""
    result = client.table("documents").select("*").eq("id", doc_id).execute()
    return result.data[0] if result.data else None


# --- Chunks ---


def insert_chunks(client: Client, chunks: list[Chunk]) -> list[int]:
    """Bulk insert chunks and return their IDs."""
    if not chunks:
        return []

    rows = []
    for chunk in chunks:
        row: dict[str, Any] = {
            "document_id": chunk.document_id,
            "content": chunk.content,
            "section": chunk.section,
            "speaker": chunk.speaker,
        }
        if chunk.embedding:
            row["embedding"] = chunk.embedding
        rows.append(row)

    result = client.table("chunks").insert(rows).execute()
    ids = [r["id"] for r in result.data]
    logger.info("Inserted %d chunks for document %d", len(ids), chunks[0].document_id)
    return ids


def get_chunk_with_context(client: Client, chunk_id: int, context_count: int = 2) -> dict[str, Any]:
    """Get a chunk plus surrounding chunks from the same document."""
    # Get the target chunk
    target = client.table("chunks").select("*").eq("id", chunk_id).execute()
    if not target.data:
        return {"chunk": None, "context": []}

    chunk_data = target.data[0]
    doc_id = chunk_data["document_id"]

    # Get surrounding chunks (by ID ordering, which matches document order)
    context = (
        client.table("chunks")
        .select("*")
        .eq("document_id", doc_id)
        .gte("id", chunk_id - context_count)
        .lte("id", chunk_id + context_count)
        .order("id")
        .execute()
    )

    return {"chunk": chunk_data, "context": context.data}


# --- Votes ---


def insert_vote(client: Client, vote: Vote) -> int:
    """Insert a vote record and return its ID."""
    data = {
        "document_id": vote.document_id,
        "meeting_date": vote.meeting_date.isoformat(),
        "motion": vote.motion,
        "result": vote.result,
        "vote_count_yes": vote.vote_count_yes,
        "vote_count_no": vote.vote_count_no,
        "vote_count_abstain": vote.vote_count_abstain,
    }
    if vote.details:
        data["details"] = vote.details
    result = client.table("votes").insert(data).execute()
    return result.data[0]["id"]


# --- Speakers ---


def upsert_speaker(client: Client, name: str, role: str = "") -> int:
    """Insert or update a speaker. Returns the speaker ID."""
    data = {"name": name, "role": role, "active": True}
    result = client.table("speakers").upsert(data, on_conflict="name").execute()
    return result.data[0]["id"]


# --- Corrections ---


def insert_correction(client: Client, correction: Correction) -> int:
    """Insert an error report."""
    data = {
        "chunk_id": correction.chunk_id,
        "description": correction.description,
        "reporter_email": correction.reporter_email,
        "status": "open",
    }
    result = client.table("corrections").insert(data).execute()
    return result.data[0]["id"]


# --- Ingest Runs ---


def insert_ingest_run(client: Client, run: IngestRun) -> int:
    """Log an ingestion run result."""
    data = {
        "meeting_date": run.meeting_date.isoformat(),
        "meeting_title": run.meeting_title,
        "docs_found": run.docs_found,
        "docs_ingested": run.docs_ingested,
        "docs_failed": run.docs_failed,
        "errors": run.errors,
    }
    result = client.table("ingest_runs").insert(data).execute()
    return result.data[0]["id"]
