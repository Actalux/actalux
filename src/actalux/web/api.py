"""Read-only JSON API (v1) for the Actalux corpus.

Built for downstream products (e.g. a Substack newsletter) that want to query
the archive as grounding. Everything here is read-only over already-public
records and mirrors the HTML site's retrieval, so the API can never expose more
than the site does.

Surface (all entity-scoped, mirroring the site's /{state}/{place}/{body} paths):
  GET /api/v1/{state}/{place}/{body}/search          ranked verbatim passages
  GET /api/v1/{state}/{place}/{body}/meetings/{date} one meeting's documents
  GET /api/v1/{state}/{place}/{body}/recent          recent meetings feed

Auth is key-optional: with ACTALUX_API_KEY unset the API is open; set it and a
valid X-API-Key (or Authorization: Bearer) header becomes required. Every route
is per-IP rate-limited regardless.
"""

from __future__ import annotations

import hmac
import time
from datetime import date

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from pydantic import BaseModel

from actalux.db import (
    get_documents,
    get_entity_by_path,
    get_meeting_documents,
    list_recent_meeting_documents,
)
from actalux.errors import SearchError
from actalux.search.answer import enrich_results
from actalux.search.hybrid import SearchFilters, hybrid_search
from actalux.web.display import display_title
from actalux.web.retrieval import build_reranker, embed_query, get_config, get_db
from actalux.web.text_snippets import normalize_whitespace

# The document types that are "meetings" — they carry real meeting dates, so a
# date-keyed bundle and a recency feed are meaningful for them. Budgets, plans,
# and curriculum docs are reached via search, not these endpoints.
MEETING_TYPES = ("minutes", "transcript", "resolution")

# Hits/results are capped so a single call can't ask the reranker for an
# unbounded pool.
_SEARCH_LIMIT_MAX = 50
_RECENT_LIMIT_MAX = 100


# --- Response models ---------------------------------------------------------


class SearchHit(BaseModel):
    chunk_id: int
    hash_id: str
    document_id: int
    document_type: str
    title: str
    meeting_date: str | None
    section: str
    text: str  # verbatim passage (whitespace normalized, characters unchanged)
    rrf_score: float
    source_url: str | None  # original artifact (PDF or YouTube video); may be null
    source_portal: str
    citation: str
    html_url: str  # site-relative deep link to the passage in context


class SearchResponse(BaseModel):
    entity: str
    query: str
    count: int
    results: list[SearchHit]


class DocumentRef(BaseModel):
    document_id: int
    document_type: str
    title: str
    meeting_date: str | None
    summary: str
    source_url: str | None
    source_portal: str
    citation: str
    html_url: str


class MeetingBundle(BaseModel):
    entity: str
    date: str
    count: int
    documents: list[DocumentRef]


class RecentResponse(BaseModel):
    entity: str
    since: str | None
    count: int
    items: list[DocumentRef]


# --- Auth --------------------------------------------------------------------


def _presented_key(request: Request) -> str:
    """The API key a request offers, via X-API-Key or a Bearer token."""
    header = request.headers.get("x-api-key")
    if header:
        return header
    auth = request.headers.get("authorization", "")
    if auth.lower().startswith("bearer "):
        return auth[7:].strip()
    return ""


def require_api_key(request: Request) -> None:
    """Key-optional gate: enforce a key only when ACTALUX_API_KEY is configured."""
    expected = get_config().api_key
    if not expected:
        return  # open mode
    if not hmac.compare_digest(_presented_key(request), expected):
        raise HTTPException(status_code=401, detail="Invalid or missing API key")


# --- Rate limiting -----------------------------------------------------------
# In-process fixed-window counter, per (client IP, scope). Adequate for the
# single-instance deploy; a multi-instance deploy would need a shared store
# (the limit then applies per instance). The bucket map is bounded only by the
# number of distinct IPs seen within a window, which is fine at this scale.

_RATE_WINDOW_SECONDS = 60.0
_rate_buckets: dict[str, tuple[float, int]] = {}


def _client_ip(request: Request) -> str:
    """Best-effort client IP, trusting Fly's edge header ahead of the socket."""
    fly = request.headers.get("fly-client-ip")
    if fly:
        return fly
    forwarded = request.headers.get("x-forwarded-for", "")
    if forwarded:
        return forwarded.split(",")[0].strip()
    return request.client.host if request.client else "unknown"


def _enforce_rate(request: Request, scope: str, limit_per_minute: int) -> None:
    key = f"{_client_ip(request)}:{scope}"
    now = time.monotonic()
    start, count = _rate_buckets.get(key, (now, 0))
    if now - start >= _RATE_WINDOW_SECONDS:
        start, count = now, 0
    count += 1
    _rate_buckets[key] = (start, count)
    if count > limit_per_minute:
        retry_after = int(_RATE_WINDOW_SECONDS - (now - start)) + 1
        raise HTTPException(
            status_code=429,
            detail="Rate limit exceeded",
            headers={"Retry-After": str(retry_after)},
        )


def rate_limit_search(request: Request) -> None:
    """Tighter limit for the search endpoint (it runs the paid reranker)."""
    _enforce_rate(request, "search", get_config().rate_limit_search_per_minute)


def rate_limit_general(request: Request) -> None:
    """Default limit for the cheap (DB-only) endpoints."""
    _enforce_rate(request, "general", get_config().rate_limit_api_per_minute)


def _reset_rate_limits() -> None:
    """Clear all rate-limit state. For tests only."""
    _rate_buckets.clear()


# --- Entity resolution -------------------------------------------------------


def resolve_api_entity(state: str, place: str, body: str) -> dict:
    """Resolve a public body from its URL parts, or 404 (as JSON)."""
    entity = get_entity_by_path(get_db(), state, place, body)
    if not entity:
        raise HTTPException(status_code=404, detail="Unknown jurisdiction")
    return entity


def _entity_path(entity: dict) -> str:
    """The canonical 'state/place/body' path string for an entity, echoed back."""
    place = entity.get("place") or {}
    return f"{place.get('state', '')}/{place.get('slug', '')}/{entity.get('body_slug', '')}"


def _parse_date(value: str) -> str:
    """Validate a YYYY-MM-DD string and return it normalized, else 400."""
    try:
        return date.fromisoformat(value).isoformat()
    except ValueError:
        raise HTTPException(status_code=400, detail="date must be YYYY-MM-DD") from None


def _to_date(value: str | None) -> date | None:
    """Parse an optional YYYY-MM-DD filter into a date, or None; 400 on garbage."""
    if not value:
        return None
    try:
        return date.fromisoformat(value)
    except ValueError:
        raise HTTPException(status_code=400, detail="date must be YYYY-MM-DD") from None


# --- Link / citation builders ------------------------------------------------


def _source_url(doc: dict) -> str | None:
    """The original artifact's URL: a video's watch page, else the stored URL.

    A transcript's source is its YouTube video, never the derived .txt; a
    transcript with no public video has no external source URL.
    """
    if doc.get("video_id"):
        return f"https://www.youtube.com/watch?v={doc['video_id']}"
    if doc.get("source_portal") == "youtube":
        return None
    return doc.get("source_url") or None


def _build_hit(row: dict, doc: dict) -> SearchHit:
    title = display_title(row)
    hash_id = row["hash_id"]
    return SearchHit(
        chunk_id=row["chunk_id"],
        hash_id=hash_id,
        document_id=row["document_id"],
        document_type=row.get("document_type") or "",
        title=title,
        meeting_date=row.get("meeting_date") or None,
        section=row.get("section") or "",
        text=normalize_whitespace(row.get("content") or ""),
        rrf_score=row.get("rrf_score", 0.0),
        source_url=_source_url(doc),
        source_portal=doc.get("source_portal", ""),
        citation=f"{title} [{hash_id}]",
        html_url=f"/chunk/{row['chunk_id']}/source",
    )


def _build_docref(row: dict) -> DocumentRef:
    title = display_title(row)
    return DocumentRef(
        document_id=row["id"],
        document_type=row.get("document_type") or "",
        title=title,
        meeting_date=row.get("meeting_date") or None,
        summary=row.get("summary") or "",
        source_url=_source_url(row),
        source_portal=row.get("source_portal", ""),
        citation=title,
        html_url=f"/document/{row['id']}",
    )


# --- Routes ------------------------------------------------------------------

api_router = APIRouter(prefix="/api/v1", dependencies=[Depends(require_api_key)])


@api_router.get(
    "/{state}/{place}/{body}/search",
    response_model=SearchResponse,
    dependencies=[Depends(rate_limit_search)],
)
def api_search(
    q: str = Query(..., min_length=1, description="Search query"),
    limit: int = Query(20, ge=1, le=_SEARCH_LIMIT_MAX),
    date_from: str | None = None,
    date_to: str | None = None,
    type: str | None = Query(None, description="Filter to one document_type"),
    entity: dict = Depends(resolve_api_entity),
) -> SearchResponse:
    """Hybrid search: ranked verbatim passages with citations and source links."""
    filters = SearchFilters(
        date_from=_to_date(date_from),
        date_to=_to_date(date_to),
        document_type=type or None,
        entity_id=entity["id"],
    )
    client = get_db()
    try:
        embedding = embed_query(q)
        results = hybrid_search(
            client, q, embedding, filters, max_results=limit, reranker=build_reranker()
        )
    except SearchError:
        results = []
    enriched = enrich_results(client, results)
    docs = get_documents(client, [r["document_id"] for r in enriched])
    hits = [_build_hit(r, docs.get(r["document_id"], {})) for r in enriched]
    return SearchResponse(entity=_entity_path(entity), query=q, count=len(hits), results=hits)


@api_router.get(
    "/{state}/{place}/{body}/meetings/{meeting_date}",
    response_model=MeetingBundle,
    dependencies=[Depends(rate_limit_general)],
)
def api_meeting_bundle(
    meeting_date: str,
    entity: dict = Depends(resolve_api_entity),
) -> MeetingBundle:
    """Every minutes / transcript / resolution document for one meeting date."""
    iso = _parse_date(meeting_date)
    rows = get_meeting_documents(get_db(), entity["id"], iso, list(MEETING_TYPES))
    documents = [_build_docref(r) for r in rows]
    return MeetingBundle(
        entity=_entity_path(entity), date=iso, count=len(documents), documents=documents
    )


@api_router.get(
    "/{state}/{place}/{body}/recent",
    response_model=RecentResponse,
    dependencies=[Depends(rate_limit_general)],
)
def api_recent(
    since: str | None = Query(None, description="Inclusive lower bound, YYYY-MM-DD"),
    limit: int = Query(20, ge=1, le=_RECENT_LIMIT_MAX),
    entity: dict = Depends(resolve_api_entity),
) -> RecentResponse:
    """Recent meeting documents, newest meeting first ('what's new since a date')."""
    iso_since = _parse_date(since) if since else None
    rows = list_recent_meeting_documents(
        get_db(), entity["id"], list(MEETING_TYPES), since=iso_since, limit=limit
    )
    items = [_build_docref(r) for r in rows]
    return RecentResponse(
        entity=_entity_path(entity), since=iso_since, count=len(items), items=items
    )
