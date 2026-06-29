"""Supabase database operations.

All DB access goes through this module. Uses the Supabase Python client
for data operations and raw SQL (via RPC) for pgvector queries.
"""

from __future__ import annotations

import logging
import re
import time
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from postgrest.exceptions import APIError
from supabase import Client, create_client

from actalux.models import BudgetLineItem, Chunk, Correction, Document, IngestRun, Vote

logger = logging.getLogger(__name__)

_clients: dict[tuple[str, str], Client] = {}


def get_client(url: str, key: str) -> Client:
    """Get or create a Supabase client, cached per (url, key).

    Cached by key, not as a single global, so the publishable-key (web) and
    service-key (ingest) clients can coexist without one silently overwriting
    the other.
    """
    cache_key = (url, key)
    client = _clients.get(cache_key)
    if client is None:
        client = create_client(url, key)
        _clients[cache_key] = client
    return client


# PostgREST caps a single response (Supabase default ~1000 rows), so a bare
# .select().execute() silently truncates once a table grows past that. Page in
# windows of this size to read every row.
_PAGE_SIZE = 1000


def fetch_all_rows(
    make_query: Callable[[], Any], *, order: str = "id", desc: bool = False
) -> list[dict[str, Any]]:
    """Return every row of a PostgREST query, paging past the server row cap.

    ``make_query`` returns a FRESH query builder each call (e.g.
    ``lambda: client.table("documents").select("id,summary")``); it is ordered by
    ``order`` for a stable page sequence and read in ``_PAGE_SIZE`` windows via
    ``.range()`` until a short page signals the end. Without this, a bare
    ``.select().execute()`` returns only the first ~1000 rows and silently drops
    the newest documents once the corpus exceeds that — the failure that left the
    latest transcripts un-timestamped (see notes 2026-06-23).
    """
    rows: list[dict[str, Any]] = []
    start = 0
    while True:
        page = (
            make_query().order(order, desc=desc).range(start, start + _PAGE_SIZE - 1).execute().data
        )
        rows.extend(page)
        if len(page) < _PAGE_SIZE:
            return rows
        start += _PAGE_SIZE


# --- Documents ---


def insert_document(client: Client, doc: Document) -> int:
    """Insert a document and return its ID."""
    data: dict[str, Any] = {
        "meeting_date": doc.meeting_date.isoformat(),
        "meeting_title": doc.meeting_title,
        "document_type": doc.document_type,
        "source_url": doc.source_url,
        "source_file": doc.source_file,
        "content": doc.content,
        "content_hash": doc.content_hash,
        "source_portal": doc.source_portal,
        "source_ref": doc.source_ref,
        "video_id": doc.video_id,
        "version": doc.version,
        "date_source": doc.date_source,
    }
    if doc.entity_id is not None:
        data["entity_id"] = doc.entity_id
    if doc.replaces_id is not None:
        data["replaces_id"] = doc.replaces_id
    result = client.table("documents").insert(data).execute()
    doc_id: int = result.data[0]["id"]
    logger.info(
        "Inserted document %d: %s (%s) v%d",
        doc_id,
        doc.meeting_title,
        doc.document_type,
        doc.version,
    )
    return doc_id


def get_document(client: Client, doc_id: int) -> dict[str, Any] | None:
    """Fetch a document by ID."""
    result = client.table("documents").select("*").eq("id", doc_id).execute()
    return result.data[0] if result.data else None


def get_documents(client: Client, doc_ids: list[int]) -> dict[int, dict[str, Any]]:
    """Fetch many documents by ID in one round-trip, keyed by ID.

    Used to enrich search results without an N+1 query per result. Returns
    only the documents that exist; missing IDs are simply absent from the map.
    """
    if not doc_ids:
        return {}
    unique_ids = list(dict.fromkeys(doc_ids))
    result = client.table("documents").select("*").in_("id", unique_ids).execute()
    return {row["id"]: row for row in (result.data or [])}


def get_entity_by_path(
    client: Client, state: str, place_slug: str, body_slug: str
) -> dict[str, Any] | None:
    """Resolve a public body from its URL parts, e.g. ('mo','clayton','schools').

    Returns the entity row with its place embedded under ``place``, or None if
    no such place/body exists. The two-step lookup avoids PostgREST embedded-
    filter quirks and keeps the query intent obvious.
    """
    places = (
        client.table("places")
        .select("*")
        .eq("state", state)
        .eq("slug", place_slug)
        .limit(1)
        .execute()
    )
    if not places.data:
        return None
    place = places.data[0]
    entities = (
        client.table("entities")
        .select("*")
        .eq("place_id", place["id"])
        .eq("body_slug", body_slug)
        .limit(1)
        .execute()
    )
    if not entities.data:
        return None
    entity = entities.data[0]
    entity["place"] = place
    return entity


def get_place_by_path(client: Client, state: str, place_slug: str) -> dict[str, Any] | None:
    """Resolve a place from its URL parts, e.g. ('mo','clayton'), or None.

    The place-level seam (the lexicon spans every body in a place, so a person on
    two bodies is one entry) — distinct from get_entity_by_path, which resolves a
    single body.
    """
    places = (
        client.table("places")
        .select("*")
        .eq("state", state)
        .eq("slug", place_slug)
        .limit(1)
        .execute()
    )
    return places.data[0] if places.data else None


def get_name_corrections(client: Client, place_id: int) -> list[dict[str, Any]]:
    """Active name-corrections (mangling -> canonical) for a place.

    The place-scoped spelling lexicon served by the corrections endpoint and consumed
    by the downstream newsletter. Place-scoped because the same string can be a
    mangling in one town and a real name in another.
    """
    return fetch_all_rows(
        lambda: (
            client.table("name_corrections")
            .select("mangled,canonical,category,provenance")
            .eq("place_id", place_id)
            .eq("active", True)
        )
    )


def get_diarization_turns(client: Client, document_id: int) -> list[dict[str, Any]]:
    """Word-level speaker turns for a transcript, in time order (the attribution layer).

    Anonymous clusters + their word timings; identity is layered separately and gated
    (see ``get_speaker_identities``). A long meeting has thousands of turns, so this
    pages past the server row cap.
    """
    return fetch_all_rows(
        lambda: (
            client.table("diarization_turns")
            .select("cluster_label,start_seconds,end_seconds,words,source_model")
            .eq("document_id", document_id)
        ),
        order="start_seconds",
    )


def get_speaker_identities(client: Client, document_id: int) -> list[dict[str, Any]]:
    """Cluster -> official identity for a transcript, with the subject embedded.

    Through the anon RLS path this returns ONLY publicly-displayable
    (``inferred_high`` / ``confirmed``) rows — the display gate lives in the database,
    not here. A service-key caller sees all rows (for the review queue).
    """
    return (
        client.table("speaker_identities")
        .select("cluster_label,confidence,basis,subject_id,subject:subjects(slug,canonical_name)")
        .eq("document_id", document_id)
        .execute()
        .data
        or []
    )


def get_identity_review_queue(service_client: Client, entity_id: int) -> list[dict[str, Any]]:
    """Below-gate speaker-identity proposals for a body's transcripts, for human review.

    SERVICE client only: these rows (``inferred_low`` / ``inferred_medium``) are below
    the public display gate, so RLS hides them from the anon path. This is an operator
    tool, never a public surface. Returns one shaped row per proposal with its meeting
    context, sorted by date then cluster.
    """
    from actalux.identity.review import REVIEW_CONFIDENCE, shape_review_queue

    docs = fetch_all_rows(
        lambda: (
            service_client.table("documents")
            .select("id,meeting_date,meeting_title,video_id")
            .eq("entity_id", entity_id)
            .eq("document_type", "transcript")
        )
    )
    docs_by_id = {d["id"]: d for d in docs}
    if not docs_by_id:
        return []
    rows = (
        service_client.table("speaker_identities")
        .select("document_id,cluster_label,confidence,basis,subject:subjects(slug,canonical_name)")
        .in_("document_id", list(docs_by_id))
        .in_("confidence", list(REVIEW_CONFIDENCE))
        .execute()
        .data
        or []
    )
    return shape_review_queue(rows, docs_by_id)


def get_entity(client: Client, entity_id: int) -> dict[str, Any] | None:
    """Fetch one public body by id with its place embedded under ``place``."""
    result = (
        client.table("entities").select("*, place:places(*)").eq("id", entity_id).limit(1).execute()
    )
    return result.data[0] if result.data else None


def list_entities(client: Client) -> list[dict[str, Any]]:
    """All public bodies with their places embedded, for the landing/directory."""
    result = client.table("entities").select("*, place:places(*)").execute()
    return result.data or []


def list_documents(
    client: Client,
    entity_id: int | None = None,
    *,
    document_type: str | None = None,
    source_file_like: str | None = None,
    limit: int = 500,
) -> list[dict[str, Any]]:
    """List current documents for browse-by-type, newest meeting first.

    Filters are ANDed; ``source_file_like`` is an ILIKE pattern (used for
    curriculum maps, which share ``document_type='other'`` and are identified
    by filename instead). Superseded versions (``replaces_id`` set) are excluded.
    """
    query = (
        client.table("documents")
        .select("id, meeting_title, document_type, meeting_date, summary")
        .is_("replaces_id", "null")
    )
    if entity_id is not None:
        query = query.eq("entity_id", entity_id)
    if document_type is not None:
        query = query.eq("document_type", document_type)
    if source_file_like is not None:
        query = query.ilike("source_file", source_file_like)
    result = query.order("meeting_date", desc=True).limit(limit).execute()
    return result.data or []


# Columns the JSON API exposes per document (everything it needs to build a
# citation + source link; deliberately not "*").
_API_DOC_COLUMNS = (
    "id, meeting_title, document_type, meeting_date, summary, source_url, source_portal, video_id"
)

# Columns the change-digest drafter reads per document: the API columns plus the
# versioning fields it uses to label a row new (version 1) vs. updated (version >1).
_DIGEST_DOC_COLUMNS = _API_DOC_COLUMNS + ", version, created_at, source_file"


def list_documents_changed_since(
    client: Client,
    since: str,
    *,
    entity_id: int | None = None,
    limit: int = 500,
) -> list[dict[str, Any]]:
    """Current documents inserted at or after ``since``, newest first.

    Backs the change-digest. A current row (``replaces_id IS NULL``) created since
    the last digest is either brand new (``version == 1``) or a new version of an
    existing document (``version > 1`` — its prior row now carries ``replaces_id``).
    The ``ingest_runs`` log stores only aggregate counts, so "what changed" is
    derived from ``documents.created_at`` here rather than from the run log.
    ``since`` is an inclusive lower bound on ``created_at`` (ISO 8601, e.g.
    ``2026-06-18T09:00:00+00:00``).
    """
    query = (
        client.table("documents")
        .select(_DIGEST_DOC_COLUMNS)
        .is_("replaces_id", "null")
        .gte("created_at", since)
    )
    if entity_id is not None:
        query = query.eq("entity_id", entity_id)
    result = query.order("created_at", desc=True).limit(limit).execute()
    return result.data or []


def get_document_chunks(
    client: Client,
    doc_id: int,
    *,
    limit: int | None = None,
) -> list[dict[str, Any]]:
    """A document's chunks in document order, for building citeable evidence rows.

    Returns ``id, content, section, speaker, chunk_index, citation_id`` ordered by
    ``chunk_index`` so the leading chunks (a document's opening/overview) come
    first. ``limit`` caps how many are returned so a long document does not blow
    the summary prompt. Lets the digest drafter summarize a specific document from
    its own passages without a search round-trip.
    """
    query = (
        client.table("chunks")
        .select("id, content, section, speaker, chunk_index, citation_id")
        .eq("document_id", doc_id)
        .order("chunk_index")
    )
    if limit is not None:
        query = query.limit(limit)
    return query.execute().data or []


def get_meeting_documents(
    client: Client,
    entity_id: int,
    meeting_date: str,
    document_types: list[str],
) -> list[dict[str, Any]]:
    """Current documents for one body on one meeting date, of the given types.

    Backs the JSON API's single-meeting bundle. Ordered by document_type so the
    bundle is stable (agendas, minutes, resolutions, transcripts grouped).
    """
    result = (
        client.table("documents")
        .select(_API_DOC_COLUMNS)
        .is_("replaces_id", "null")
        .eq("entity_id", entity_id)
        .eq("meeting_date", meeting_date)
        .in_("document_type", document_types)
        .order("document_type")
        .execute()
    )
    return result.data or []


def get_meeting_records(
    client: Client,
    entity_id: int,
    meeting_date: str,
    document_types: list[str],
) -> list[dict[str, Any]]:
    """Full document rows (incl. ``content``) for one body on one meeting date.

    Backs the web meeting page, which renders the transcript and minutes text
    inline, so it needs ``content`` (unlike ``get_meeting_documents``, which serves
    the lightweight JSON bundle). Current versions only; ordered by document_type.
    """
    result = (
        client.table("documents")
        .select("*")
        .is_("replaces_id", "null")
        .eq("entity_id", entity_id)
        .eq("meeting_date", meeting_date)
        .in_("document_type", document_types)
        .order("document_type")
        .execute()
    )
    return result.data or []


def list_recent_meeting_documents(
    client: Client,
    entity_id: int,
    document_types: list[str],
    *,
    since: str | None = None,
    limit: int = 20,
) -> list[dict[str, Any]]:
    """Recent meeting-type documents for one body, newest meeting first.

    Backs the JSON API's recent-meetings feed. Restricted to ``document_types``
    (the meeting records, which carry real meeting dates) so the feed is not
    polluted by documents whose date is the ingest day. ``since`` is an inclusive
    lower bound on meeting_date (YYYY-MM-DD).
    """
    query = (
        client.table("documents")
        .select(_API_DOC_COLUMNS)
        .is_("replaces_id", "null")
        .eq("entity_id", entity_id)
        .in_("document_type", document_types)
    )
    if since is not None:
        query = query.gte("meeting_date", since)
    result = query.order("meeting_date", desc=True).limit(limit).execute()
    return result.data or []


def find_document_by_source(
    client: Client, source_file: str, source_portal: str = "", entity_id: int | None = None
) -> dict[str, Any] | None:
    """Find the latest version of a document by source_file and portal.

    When ``entity_id`` is given the match is scoped to that body, so a record is only
    ever deduped against a prior version of the *same* body's record.
    """
    query = (
        client.table("documents")
        .select("*")
        .eq("source_file", source_file)
        .is_("replaces_id", "null")
    )
    if source_portal:
        query = query.eq("source_portal", source_portal)
    if entity_id is not None:
        query = query.eq("entity_id", entity_id)
    result = query.execute()
    return result.data[0] if result.data else None


def find_document_by_source_ref(
    client: Client, source_portal: str, source_ref: str, entity_id: int | None = None
) -> dict[str, Any] | None:
    """Find the current document for a stable external id within a portal.

    ``source_ref`` is the normalized canonical origin URL (see
    ``Document.source_ref``); it is the dedup identity that survives a
    PDF/HTML-twin or a filename change. An empty ``source_ref`` has no stable
    identity to match on, so the lookup short-circuits to None and the caller
    falls back to content-hash / filename dedup.

    Parameters
    ----------
    client
        Supabase client.
    source_portal
        Portal the document belongs to (``"diligent"``, ``"claytonschools"``,
        ``"youtube"``, ``"manual"``); scopes the match so the same id under
        different portals cannot collide.
    source_ref
        Normalized canonical origin URL.

    Returns
    -------
    dict or None
        The current (``replaces_id IS NULL``) document row, or None if no
        match exists or ``source_ref`` is empty.
    """
    if not source_ref:
        return None
    query = (
        client.table("documents")
        .select("*")
        .eq("source_portal", source_portal)
        .eq("source_ref", source_ref)
        .is_("replaces_id", "null")
    )
    if entity_id is not None:
        query = query.eq("entity_id", entity_id)
    result = query.execute()
    return result.data[0] if result.data else None


def find_document_by_content_hash(
    client: Client, content_hash: str, source_portal: str = "", entity_id: int | None = None
) -> dict[str, Any] | None:
    """Find the current document whose stored content matches ``content_hash``.

    Used as the second dedup tier after ``source_ref``: identical bytes are the
    same document even when the filename or origin URL has changed. An empty
    hash short-circuits to None. Optionally scoped to a portal and/or body.

    Parameters
    ----------
    client
        Supabase client.
    content_hash
        SHA-256 of the parsed content.
    source_portal
        Optional portal scope; empty matches any portal.
    entity_id
        Optional body scope; when set, identical bytes under a *different* body are
        not treated as the same document (a meeting belongs to one body).

    Returns
    -------
    dict or None
        The current (``replaces_id IS NULL``) document row, or None.
    """
    if not content_hash:
        return None
    query = (
        client.table("documents")
        .select("*")
        .eq("content_hash", content_hash)
        .is_("replaces_id", "null")
    )
    if source_portal:
        query = query.eq("source_portal", source_portal)
    if entity_id is not None:
        query = query.eq("entity_id", entity_id)
    result = query.execute()
    return result.data[0] if result.data else None


def replace_document(client: Client, old_doc_id: int, new_doc: Document) -> int:
    """Create a new version, mark the old one as replaced.

    Returns the new document's ID.
    """
    new_id = insert_document(client, new_doc)
    # Mark old document as replaced
    client.table("documents").update({"replaces_id": new_id}).eq("id", old_doc_id).execute()
    logger.info("Document %d replaced by %d (v%d)", old_doc_id, new_id, new_doc.version)
    return new_id


def update_document_checked(client: Client, doc_id: int) -> None:
    """Update last_checked_at to now."""
    from datetime import UTC, datetime

    now = datetime.now(UTC).isoformat()
    client.table("documents").update({"last_checked_at": now}).eq("id", doc_id).execute()


def delete_document(client: Client, doc_id: int) -> None:
    """Delete a document row. Its chunks/votes cascade (``ON DELETE CASCADE``).

    Used to roll back a half-ingested document: a doc row goes ``current``
    (``replaces_id IS NULL``) the instant it's inserted, so if chunking/embedding
    or the chunk insert then fails, deleting the row prevents a current but
    chunkless (unsearchable) document from being left behind.
    """
    client.table("documents").delete().eq("id", doc_id).execute()


def delete_chunks_for_document(client: Client, doc_id: int) -> None:
    """Delete all chunks of a document (keeping the document row).

    Used by in-place repair of a chunkless doc: clearing first (and again on a
    failed rebuild) keeps the "0 chunks == needs repair" invariant true, so a
    repair that fails partway leaves the doc at zero chunks to be retried rather
    than stranded as a partial that future re-ingests skip.
    """
    client.table("chunks").delete().eq("document_id", doc_id).execute()


# Retries for the version cutover (demote old -> point at new). It runs right after
# a possibly-slow chunk insert and can hit the same free-tier statement timeout; if
# lost, both versions stay current. Retry on timeout before surfacing.
_SUPERSEDE_RETRIES = 3


def supersede_document(client: Client, old_id: int, new_id: int) -> None:
    """Mark ``old_id`` as replaced by ``new_id`` (demote the old version).

    Retries on the free-tier statement timeout so a transient slow update doesn't
    leave two current versions of the same meeting.
    """
    for attempt in range(_SUPERSEDE_RETRIES):
        try:
            client.table("documents").update({"replaces_id": new_id}).eq("id", old_id).execute()
            return
        except APIError as err:
            if not _is_statement_timeout(err) or attempt == _SUPERSEDE_RETRIES - 1:
                raise
            time.sleep(0.5)


# --- Supersession resolution -----------------------------------------------
# When a document is replaced (a new version, or a duplicate twin canonicalised
# by A2 dedup), the old row gets ``replaces_id`` set to the surviving canonical
# id, and the canonical row keeps ``replaces_id IS NULL``. Listings already hide
# superseded rows, but raw-id entrypoints (/document/{id}, /chunk/{id}/source,
# and their panes) can still be deep-linked to a superseded row — these helpers
# resolve such a deep-link to its current version without ever silently sending a
# citation to a different passage.

# Bound on chain length so a malformed ``replaces_id`` cycle (a → b → a) cannot
# loop forever. Real chains are at most a few versions deep.
_MAX_SUPERSESSION_HOPS = 16


@dataclass(frozen=True)
class CanonicalDocument:
    """Result of resolving a (possibly superseded) document to its current version.

    Attributes
    ----------
    document
        The current document row (``replaces_id IS NULL``), or None if the
        requested id does not exist.
    superseded
        True when the requested id was an older version that pointed forward
        through ``replaces_id`` to ``document``. False when the requested id was
        already current (or did not exist).
    requested_id
        The id originally requested, so callers can report "you asked for X,
        here is its current version".
    """

    document: dict[str, Any] | None
    superseded: bool
    requested_id: int


def resolve_canonical_document(client: Client, doc_id: int) -> CanonicalDocument:
    """Follow the ``replaces_id`` chain from ``doc_id`` to its current version.

    A row whose ``replaces_id`` is set has been superseded; the value points at
    the newer (canonical) row. This walks that chain until it reaches a row with
    ``replaces_id IS NULL`` (the current version) and reports whether any hop was
    taken.

    The walk is bounded by ``_MAX_SUPERSESSION_HOPS`` and tracks visited ids, so
    a malformed self- or mutual-reference cannot loop forever.

    ``superseded`` is True ONLY when the walk reached a true current row
    (``replaces_id IS NULL``) via at least one hop. If the chain breaks
    abnormally — a cycle, a dangling ``replaces_id``, or the hop bound — the
    returned row is still superseded, so ``superseded`` is reported as False:
    callers must then render the row in place rather than redirect, otherwise a
    cycle (10 -> 11 -> 10) would produce an HTTP redirect loop.

    Parameters
    ----------
    client
        Supabase client.
    doc_id
        The (possibly superseded) document id from the URL.

    Returns
    -------
    CanonicalDocument
        ``document`` is the resolved row (or None if ``doc_id`` does not exist);
        ``superseded`` is True only when a clean, hop-followed canonical was
        reached (safe to redirect to). On any abnormal termination (cycle,
        dangling pointer, hop bound) ``document`` is the ORIGINALLY REQUESTED row
        and ``superseded`` is False, so the caller renders the requested record
        in place rather than swapping in a mid-chain row or redirecting.
    """
    requested = get_document(client, doc_id)
    if requested is None:
        return CanonicalDocument(document=None, superseded=False, requested_id=doc_id)

    current = requested
    seen: set[int] = {doc_id}
    hops = 0
    while current.get("replaces_id") is not None and hops < _MAX_SUPERSESSION_HOPS:
        next_id = current["replaces_id"]
        if next_id in seen:
            # Cycle in the chain — abnormal. Return the requested row, not the
            # mid-cycle row, so the reader sees what they asked for (and no 301 loop).
            logger.warning("Supersession cycle at document %d -> %d; stopping.", doc_id, next_id)
            return CanonicalDocument(document=requested, superseded=False, requested_id=doc_id)
        nxt = get_document(client, next_id)
        if nxt is None:
            # Dangling replaces_id (canonical row deleted): no clean canonical to
            # offer; render the requested row in place.
            logger.warning("Document %d replaces_id -> missing %d; stopping.", doc_id, next_id)
            return CanonicalDocument(document=requested, superseded=False, requested_id=doc_id)
        seen.add(next_id)
        current = nxt
        hops += 1

    if current.get("replaces_id") is not None:
        # Hit the hop bound without reaching a true canonical — unresolved; render
        # the requested row in place rather than a still-superseded mid-chain row.
        logger.warning("Supersession chain from document %d exceeded hop bound.", doc_id)
        return CanonicalDocument(document=requested, superseded=False, requested_id=doc_id)

    # Clean termination at a current row; superseded iff we actually moved.
    return CanonicalDocument(document=current, superseded=hops > 0, requested_id=doc_id)


@dataclass(frozen=True)
class CanonicalChunk:
    """Result of mapping a superseded-document chunk to the canonical document.

    Chunks have no stored canonical mapping (only ``document_id`` +
    ``chunk_index``), so a citation deep-link into a superseded document is
    resolved by best-effort content match, never a blind redirect.

    Attributes
    ----------
    chunk
        The matched chunk row in the canonical document, or None when no
        confident match was found (caller then shows the old passage under a
        "superseded version" notice rather than jumping to a different one).
    superseded
        True when the original chunk's document was superseded.
    """

    chunk: dict[str, Any] | None
    superseded: bool


def _normalize_chunk_text(text: str) -> str:
    """Whitespace-normalised, case-folded chunk text for content matching.

    Two chunks are "the same passage" when their verbatim content matches after
    collapsing whitespace and case — robust to the cosmetic reflow differences
    between a PDF twin and an HTML twin without changing any words.
    """
    return " ".join((text or "").split()).casefold()


def resolve_canonical_chunk(
    client: Client, chunk: dict[str, Any], canonical_doc_id: int
) -> dict[str, Any] | None:
    """Best-effort map a superseded-document chunk to the canonical document's chunk.

    Resolution order (never a blind positional redirect):
      1. Exact normalised-content match within the canonical document. A unique
         content match is the only fully safe mapping — it is the same passage.
      2. Fallback to the same ``chunk_index`` ONLY when its normalised content
         also matches the original chunk's content. Same position with different
         text is treated as no match (returns None) so a citation is never sent
         to a different passage.

    Parameters
    ----------
    client
        Supabase client.
    chunk
        The original chunk row (from the superseded document).
    canonical_doc_id
        The current document's id (from ``resolve_canonical_document``).

    Returns
    -------
    dict or None
        The matching chunk row in the canonical document, or None when no
        confident match exists.
    """
    target_text = _normalize_chunk_text(chunk.get("content") or "")
    if not target_text:
        return None

    rows = (
        client.table("chunks")
        .select("*")
        .eq("document_id", canonical_doc_id)
        .order("chunk_index")
        .execute()
    ).data or []

    # 1. Unique exact content match.
    matches = [r for r in rows if _normalize_chunk_text(r.get("content") or "") == target_text]
    if len(matches) == 1:
        return matches[0]
    if len(matches) > 1:
        # Ambiguous (repeated passage) — disambiguate by chunk_index when it lines up.
        idx = chunk.get("chunk_index")
        for r in matches:
            if idx is not None and r.get("chunk_index") == idx:
                return r
        return None

    # 2. Position fallback, gated on content agreement so we never jump passages.
    idx = chunk.get("chunk_index")
    if idx is not None:
        for r in rows:
            if r.get("chunk_index") == idx and (
                _normalize_chunk_text(r.get("content") or "") == target_text
            ):
                return r

    return None


def set_document_video_id(client: Client, doc_id: int, video_id: str) -> None:
    """Set a document's YouTube video_id (writer -- needs the service key under RLS)."""
    client.table("documents").update({"video_id": video_id}).eq("id", doc_id).execute()


def backfill_document_source_ref(client: Client, doc_id: int, source_ref: str) -> None:
    """Set a legacy row's stable external id, only if it has none.

    Rows ingested before ``source_ref`` existed carry the column default ``''``.
    Re-ingesting an unchanged file reaches them only via the content-hash /
    filename dedup tiers and would otherwise never gain a ``source_ref`` --
    leaving a future PDF/HTML twin (different content and filename) unable to
    match. Persisting the value on the unchanged-skip path makes re-ingest
    self-healing.

    The ``source_ref = ''`` predicate enforces non-overwriting at the database
    level, not just at the caller: a row that already has a stable id is never
    clobbered, even under a stale read or if this helper is reused. Writer --
    needs the service key under RLS.
    """
    (
        client.table("documents")
        .update({"source_ref": source_ref})
        .eq("id", doc_id)
        .eq("source_ref", "")
        .execute()
    )


def set_chunk_start_seconds(client: Client, chunk_id: int, start_seconds: int) -> None:
    """Set a chunk's video start offset (writer -- needs the service key under RLS)."""
    client.table("chunks").update({"start_seconds": start_seconds}).eq("id", chunk_id).execute()


# --- Chunks ---

# Rows per chunk INSERT. Each row carries a 384-dim embedding, so the insert does
# HNSW index maintenance whose cost scales with the batch size; a large batch trips
# the Supabase free-tier statement timeout (postgrest 57014). 50 was still too large
# under sustained backfill load (a single 50-row batch timed out mid-run), so the
# default is 25 and ``_insert_chunk_rows`` halves further on any timeout it still hits.
_CHUNK_INSERT_BATCH = 25


def _is_statement_timeout(err: Exception) -> bool:
    """True if a PostgREST error is the free-tier statement timeout (57014)."""
    return getattr(err, "code", None) == "57014" or "statement timeout" in str(err).lower()


def _insert_chunk_rows(client: Client, rows: list[dict[str, Any]]) -> list[int]:
    """Insert chunk rows, halving and retrying on a free-tier statement timeout.

    The 384-dim-embedding insert's HNSW index work can exceed the Supabase
    free-tier statement timeout under load. On a timeout we split the batch and
    retry each half (down to a single row, which always fits under the timeout),
    so a one-time backfill completes regardless of how slow the instance is.
    Order is preserved (left half before right); a single row that still times
    out — or any non-timeout error — propagates.
    """
    try:
        result = client.table("chunks").insert(rows).execute()
        return [r["id"] for r in result.data]
    except APIError as err:
        if not _is_statement_timeout(err) or len(rows) <= 1:
            raise
    # Reached only on a timeout with >1 row: let the instance breathe, then halve.
    time.sleep(0.5)
    mid = len(rows) // 2
    return _insert_chunk_rows(client, rows[:mid]) + _insert_chunk_rows(client, rows[mid:])


def insert_chunks(client: Client, chunks: list[Chunk]) -> list[int]:
    """Bulk insert chunks (in batches) and return their IDs in chunk order.

    Inserted in ``_CHUNK_INSERT_BATCH``-row batches, not one statement: see the
    constant's note for why a single large insert times out on the free tier.
    Batching keeps each statement under the timeout while preserving insertion order.
    """
    if not chunks:
        return []

    rows = []
    for chunk in chunks:
        row: dict[str, Any] = {
            "document_id": chunk.document_id,
            "content": chunk.content,
            "section": chunk.section,
            "speaker": chunk.speaker,
            "chunk_index": chunk.chunk_index,
        }
        if chunk.embedding:
            row["embedding"] = chunk.embedding
        if chunk.citation_id:
            row["citation_id"] = chunk.citation_id
        rows.append(row)

    ids: list[int] = []
    for start in range(0, len(rows), _CHUNK_INSERT_BATCH):
        batch = rows[start : start + _CHUNK_INSERT_BATCH]
        ids.extend(_insert_chunk_rows(client, batch))
    logger.info("Inserted %d chunks for document %d", len(ids), chunks[0].document_id)
    return ids


def document_has_chunks(client: Client, doc_id: int) -> bool:
    """True if a document has at least one chunk.

    Lets ingest distinguish a genuinely-unchanged document (skip) from a current
    but chunkless one (a prior ingest died after the doc row but before its
    chunks) so the latter is repaired rather than skipped forever.
    """
    result = (
        client.table("chunks")
        .select("id", count="exact")
        .eq("document_id", doc_id)
        .limit(1)
        .execute()
    )
    return bool(result.count)


# A stable citation id is exactly CITATION_ID_LEN (8) lowercase-hex chars; a
# legacy deep-link is a short decimal row id. The pattern tells the two apart at
# the route boundary so /chunk/{ref} serves both.
_CITATION_ID_RE = re.compile(r"^[0-9a-f]{8}$")


# Cap on ids per ``in_`` lookup so a large id set can't overrun the PostgREST
# URL/row limits (which would silently drop rows). 300 keeps the query string well
# within limits while staying one round-trip for the search/answer path's handful.
_CITATION_LOOKUP_BATCH = 300


def get_chunk_citation_ids(client: Client, chunk_ids: list[int]) -> dict[int, str]:
    """Map chunk id -> stable citation_id (empty string if unset).

    Lets the answer/search path render and link citations on the stable id without
    threading citation_id through the search RPCs. Batched so a large id set (e.g.
    a corpus-wide backfill) can't exceed the API's URL/row caps; missing ids are
    simply absent.
    """
    if not chunk_ids:
        return {}
    unique = list(dict.fromkeys(chunk_ids))
    result: dict[int, str] = {}
    for start in range(0, len(unique), _CITATION_LOOKUP_BATCH):
        batch = unique[start : start + _CITATION_LOOKUP_BATCH]
        rows = (
            client.table("chunks").select("id, citation_id").in_("id", batch).execute().data or []
        )
        for r in rows:
            result[r["id"]] = r.get("citation_id") or ""
    return result


def _prefer_current_chunk(client: Client, rows: list[dict[str, Any]]) -> int:
    """Pick one chunk id from citation_id candidates, preferring a current version.

    A citation_id can be shared by a current chunk and its superseded twins (the
    same passage across versions). Routing prefers the chunk whose document is
    current (``replaces_id IS NULL``); ties and all-superseded sets fall back to
    the lowest chunk id (deterministic), logging any genuine current-vs-current
    ambiguity so it is visible rather than silent.
    """
    if len(rows) == 1:
        return rows[0]["id"]
    doc_ids = list({r["document_id"] for r in rows})
    docs = client.table("documents").select("id, replaces_id").in_("id", doc_ids).execute().data
    current_docs = {d["id"] for d in (docs or []) if d.get("replaces_id") is None}
    current_chunks = sorted(r["id"] for r in rows if r["document_id"] in current_docs)
    if current_chunks:
        if len(current_chunks) > 1:
            logger.warning(
                "citation_id resolves to %d current chunks; using lowest id %d",
                len(current_chunks),
                current_chunks[0],
            )
        return current_chunks[0]
    return sorted(r["id"] for r in rows)[0]


def resolve_chunk_ref(client: Client, ref: str) -> int | None:
    """Resolve a chunk reference (stable citation_id or legacy numeric id) to a row id.

    New citations route on the 8-hex ``citation_id``; links published before the
    stable id existed route on the numeric SERIAL id. An 8-hex ref is looked up as
    a citation_id first (preferring a current-version chunk); if nothing matches it
    falls through to a numeric interpretation, so an all-digit ref still resolves.
    Returns the numeric chunk id, or None when the ref names nothing.
    """
    if _CITATION_ID_RE.match(ref):
        rows = (
            client.table("chunks").select("id, document_id").eq("citation_id", ref).execute().data
        ) or []
        if rows:
            return _prefer_current_chunk(client, rows)
    if ref.isdigit():
        return int(ref)
    return None


def get_chunk_with_context(client: Client, chunk_id: int, context_count: int = 2) -> dict[str, Any]:
    """Get a chunk plus surrounding chunks from the same document."""
    # Get the target chunk
    target = client.table("chunks").select("*").eq("id", chunk_id).execute()
    if not target.data:
        return {"chunk": None, "context": []}

    chunk_data = target.data[0]
    doc_id = chunk_data["document_id"]

    chunk_index = chunk_data.get("chunk_index")
    if chunk_index is None:
        # Backward-compatible fallback for databases not migrated yet.
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

    # Get surrounding chunks by document-local order.
    context = (
        client.table("chunks")
        .select("*")
        .eq("document_id", doc_id)
        .gte("chunk_index", chunk_index - context_count)
        .lte("chunk_index", chunk_index + context_count)
        .order("chunk_index")
        .execute()
    )

    return {"chunk": chunk_data, "context": context.data}


def _normalize_anchor_text(text: str) -> str:
    """Whitespace-collapsed, case-folded text for anchor containment matching.

    The same normalisation is applied to both the anchor and each chunk's content
    so a verbatim anchor matches its passage regardless of how line wraps, double
    spaces, or casing differ between the curated string and the stored chunk.
    """
    return " ".join((text or "").split()).casefold()


def resolve_source_anchor(
    client: Client,
    doc_id: int,
    anchor: str,
    *,
    cache: dict[tuple[int, str], int | None] | None = None,
) -> int | None:
    """Resolve a curated citation anchor to the chunk id that contains it.

    A curated figure carries a ``Source(doc_id, anchor)`` where ``anchor`` is a
    short verbatim fragment of the source passage. Unlike
    ``get_chunk_with_context`` (which takes a known ``chunk_id``), this maps the
    anchor *text* to a chunk by scanning the document's chunks and finding the one
    whose content contains the anchor after whitespace/case normalisation. The
    result deep-links to ``/chunk/{id}/source``.

    A match must be unambiguous: exactly one chunk must contain the anchor. Zero
    matches (the passage was re-chunked away, or the anchor was mistyped) and
    multiple matches (the anchor is too generic to identify one passage) are both
    treated as failures — they are logged and return ``None`` so the caller shows
    the figure without a (wrong or arbitrary) citation link rather than sending a
    citation to a passage it cannot vouch for.

    Parameters
    ----------
    client
        Supabase client.
    doc_id
        The document the anchor is expected to live in.
    anchor
        A verbatim fragment of the cited passage.
    cache
        Optional per-request memo, keyed by ``(doc_id, normalized_anchor)``. A
        single page can cite the same passage several times; passing one dict
        across calls avoids re-querying the document's chunks each time.

    Returns
    -------
    int or None
        The unique chunk id that contains the anchor, or ``None`` on a logged
        failure (empty anchor, no match, or an ambiguous multi-chunk match).
    """
    target = _normalize_anchor_text(anchor)
    if not target:
        logger.warning("Empty source anchor for document %d; no citation link.", doc_id)
        return None

    cache_key = (doc_id, target)
    if cache is not None and cache_key in cache:
        return cache[cache_key]

    rows = (
        client.table("chunks")
        .select("id, content, chunk_index")
        .eq("document_id", doc_id)
        .order("chunk_index")
        .execute()
    ).data or []

    matches = [r["id"] for r in rows if target in _normalize_anchor_text(r.get("content") or "")]

    resolved: int | None
    if len(matches) == 1:
        resolved = matches[0]
    elif not matches:
        logger.warning("Source anchor not found in document %d: %r", doc_id, anchor)
        resolved = None
    else:
        # Ambiguous: the anchor identifies more than one passage, so it cannot
        # vouch for a single citation. Surface it rather than picking arbitrarily.
        logger.warning(
            "Source anchor matched %d chunks in document %d (ambiguous): %r",
            len(matches),
            doc_id,
            anchor,
        )
        resolved = None

    if cache is not None:
        cache[cache_key] = resolved
    return resolved


# --- Budget line items ---


def insert_budget_line_items(client: Client, items: list[BudgetLineItem]) -> list[int]:
    """Bulk insert budget line items and return their IDs."""
    if not items:
        return []

    rows: list[dict[str, Any]] = []
    for item in items:
        row: dict[str, Any] = {
            "fiscal_year": item.fiscal_year,
            "dimension": item.dimension,
            "fund": item.fund,
            "category": item.category,
            "subcategory": item.subcategory,
            "amount": str(item.amount),  # send as string so Postgres parses exact NUMERIC
            "document_id": item.document_id,
            "source_quote": item.source_quote,
            "note": item.note,
        }
        if item.basis is not None:
            row["basis"] = item.basis
        if item.chunk_id is not None:
            row["chunk_id"] = item.chunk_id
        if item.citation_id:
            row["citation_id"] = item.citation_id
        rows.append(row)

    result = client.table("budget_line_items").insert(rows).execute()
    ids = [r["id"] for r in result.data]
    logger.info("Inserted %d budget line items", len(ids))
    return ids


def get_budget_line_items(
    client: Client,
    category: str | None = None,
    dimension: str | None = None,
    basis: str | None = None,
    entity_id: int | None = None,
) -> list[dict[str, Any]]:
    """Fetch budget line items, oldest fiscal year first.

    Optionally filter by category ('revenue', 'expenditure', 'fund_balance'),
    dimension ('fund', 'source', 'function', 'budget'), and/or basis
    ('original', 'final', 'actual' for the budget-vs-actual rows).

    ``entity_id`` scopes to one public body. budget_line_items has no entity_id
    of its own; documents carries it (migrate_012), so an inner embed on the
    document FK drops cross-body rows — without it a finance ask on a body that
    has no budget data (e.g. City Council) would return another body's figures.
    """
    select = "*, documents!inner(entity_id)" if entity_id is not None else "*"
    query = client.table("budget_line_items").select(select)
    if entity_id is not None:
        query = query.eq("documents.entity_id", entity_id)
    if category:
        query = query.eq("category", category)
    if dimension:
        query = query.eq("dimension", dimension)
    if basis:
        query = query.eq("basis", basis)
    result = query.order("fiscal_year").order("fund").execute()
    return result.data


def get_proposed_budget_line_items(
    client: Client, fiscal_year: str, dimension: str, entity_id: int | None = None
) -> list[dict[str, Any]]:
    """Fetch one fiscal year's proposed-budget rows for a namespaced dimension.

    The proposed (planned) budget figures live under their own ``proposed_*``
    dimensions ('proposed_fund', 'proposed_source', 'proposed_object',
    'proposed_function') with ``basis='proposed'``, deliberately disjoint from
    the GAAP/budgetary actuals (dimensions 'fund'/'source'/'function'/'budget',
    basis NULL/original/final/actual) so the proposed figures never leak into the
    actuals charts or the finance router. This helper is the only reader of those
    rows; ``get_budget_line_items`` and its existing callers never see them.

    ``basis='proposed'`` is required in addition to the dimension as a second,
    independent guard against a stray non-proposed row sharing a dimension name.
    ``entity_id`` scopes to one body via the document FK (see
    ``get_budget_line_items``).
    """
    select = "*, documents!inner(entity_id)" if entity_id is not None else "*"
    query = (
        client.table("budget_line_items")
        .select(select)
        .eq("fiscal_year", fiscal_year)
        .eq("dimension", dimension)
        .eq("basis", "proposed")
    )
    if entity_id is not None:
        query = query.eq("documents.entity_id", entity_id)
    result = query.order("amount", desc=True).execute()
    return result.data or []


def get_dese_line_items(
    client: Client, dimension: str, entity_id: int | None = None
) -> list[dict[str, Any]]:
    """Fetch the DESE state-filing actuals for one namespaced dimension, oldest year first.

    The DESE multi-year actuals (ASBR object-level and per-fund, Per-Pupil
    building-level) are loaded under their own namespaced dimensions
    ('asbr_object', 'asbr_fund', 'perpupil_building') with ``basis='actual'``,
    deliberately disjoint from both the GAAP/budgetary actuals (dimensions
    'fund'/'source'/'function'/'budget') and the proposed figures (basis
    'proposed'). This helper is the only reader of those rows;
    ``get_budget_line_items`` and the finance router never see them, so the DESE
    data surfaces solely in its own Budget-page section.

    ``basis='actual'`` is required alongside the dimension as a second,
    independent guard against a stray non-actual row sharing a dimension name.
    ``entity_id`` scopes to one body via the document FK (see
    ``get_budget_line_items``); DESE data is school-only, so a city body returns [].
    """
    select = "*, documents!inner(entity_id)" if entity_id is not None else "*"
    query = (
        client.table("budget_line_items")
        .select(select)
        .eq("dimension", dimension)
        .eq("basis", "actual")
    )
    if entity_id is not None:
        query = query.eq("documents.entity_id", entity_id)
    result = query.order("fiscal_year").execute()
    return result.data or []


# --- Votes ---

# Columns the JSON API exposes per vote (everything to render a cited record;
# deliberately not "*").
_VOTE_COLUMNS = (
    "id, document_id, meeting_date, motion, result, result_basis, "
    "vote_count_yes, vote_count_no, vote_count_abstain, "
    "details, chunk_id, citation_id, source_quote"
)


def _vote_row(vote: Vote) -> dict[str, Any]:
    """The DB row for a vote. Tallies are sent verbatim — a None stays NULL (no
    per-member count recorded), never silently coerced to 0."""
    row: dict[str, Any] = {
        "document_id": vote.document_id,
        "meeting_date": vote.meeting_date.isoformat(),
        "motion": vote.motion,
        "result": vote.result,
        "result_basis": vote.result_basis,
        "vote_count_yes": vote.vote_count_yes,
        "vote_count_no": vote.vote_count_no,
        "vote_count_abstain": vote.vote_count_abstain,
        "source_quote": vote.source_quote,
    }
    if vote.details:
        row["details"] = vote.details
    if vote.chunk_id is not None:
        row["chunk_id"] = vote.chunk_id
    if vote.citation_id:
        row["citation_id"] = vote.citation_id
    if vote.vote_ref:
        row["vote_ref"] = vote.vote_ref
    return row


def insert_vote(client: Client, vote: Vote) -> int:
    """Insert a single vote record and return its ID."""
    result = client.table("votes").insert(_vote_row(vote)).execute()
    return result.data[0]["id"]


def insert_votes(client: Client, votes: list[Vote]) -> list[int]:
    """Bulk insert vote records and return their IDs."""
    if not votes:
        return []
    result = client.table("votes").insert([_vote_row(v) for v in votes]).execute()
    ids = [r["id"] for r in result.data]
    logger.info("Inserted %d vote records", len(ids))
    return ids


def get_document_vote_ids(client: Client, document_id: int) -> list[int]:
    """Existing vote ids for one document. Used to re-derive votes idempotently
    (insert the freshly parsed votes, then delete these prior rows)."""
    result = client.table("votes").select("id").eq("document_id", document_id).execute()
    return [r["id"] for r in (result.data or [])]


def delete_votes(client: Client, vote_ids: list[int]) -> None:
    """Delete vote rows by id (no-op on an empty list)."""
    if not vote_ids:
        return
    client.table("votes").delete().in_("id", vote_ids).execute()


def get_entity_votes(
    client: Client,
    entity_id: int,
    *,
    since: str | None = None,
    date_to: str | None = None,
    limit: int = 50,
) -> list[dict[str, Any]]:
    """Vote records for one public body, newest meeting first.

    Backs the JSON API's votes feed. Votes hang off documents, so this resolves
    the body's current documents first and fetches votes for them; votes on a
    superseded document (re-ingested minutes) are excluded with it. ``since`` is
    an inclusive lower bound and ``date_to`` an inclusive upper bound on
    ``meeting_date`` (YYYY-MM-DD).
    """
    docs = (
        client.table("documents")
        .select("id")
        .eq("entity_id", entity_id)
        .is_("replaces_id", "null")
        .execute()
    ).data or []
    doc_ids = [d["id"] for d in docs]
    if not doc_ids:
        return []
    query = client.table("votes").select(_VOTE_COLUMNS).in_("document_id", doc_ids)
    if since:
        query = query.gte("meeting_date", since)
    if date_to:
        query = query.lte("meeting_date", date_to)
    result = query.order("meeting_date", desc=True).order("id").limit(limit).execute()
    return result.data or []


# --- Speakers ---


def upsert_speaker(client: Client, name: str, role: str = "") -> int:
    """Insert or update a speaker. Returns the speaker ID."""
    data = {"name": name, "role": role, "active": True}
    result = client.table("speakers").upsert(data, on_conflict="name").execute()
    return result.data[0]["id"]


# --- Corrections ---


def insert_correction(client: Client, correction: Correction) -> None:
    """Insert a user-submitted error report.

    Uses returning="minimal" so the insert needs only the anon INSERT policy on
    corrections, not SELECT: the public must be able to file a report without
    being able to read others' reports (which carry reporter emails). The caller
    only needs the insert to succeed, so nothing is returned.
    """
    data = {
        "chunk_id": correction.chunk_id,
        "description": correction.description,
        "reporter_email": correction.reporter_email,
        "status": "open",
    }
    client.table("corrections").insert(data, returning="minimal").execute()


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
