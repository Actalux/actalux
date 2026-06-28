"""Read-only JSON API (v1) for the Actalux corpus.

Built for downstream products (e.g. a Substack newsletter) that want to query
the archive as grounding. Everything here is read-only over already-public
records and mirrors the HTML site's retrieval, so the API can never expose more
than the site does.

Surface (mostly entity-scoped, mirroring the site's /{state}/{place}/{body} paths;
the lexicon is place-scoped because a person can sit on more than one body):
  GET /api/v1/{state}/{place}/{body}/search          ranked verbatim passages
  GET /api/v1/{state}/{place}/{body}/meetings/{date} one meeting's documents
  GET /api/v1/{state}/{place}/{body}/recent          recent meetings feed
  GET /api/v1/{state}/{place}/{body}/votes           structured cited vote records
  GET /api/v1/{state}/{place}/{body}/transcripts/{id}/speakers  speaker turns + identities
  GET /api/v1/{state}/{place}/{body}/members         the body's roster
  GET /api/v1/{state}/{place}/{body}/members/{slug}  a member's cited voting record
  GET /api/v1/{state}/{place}/lexicon                canonical official names + variants
  GET /api/v1/{state}/{place}/corrections            proper-noun spelling corrections

Auth is key-optional and tier-aware. With no key presented a request runs as the
``anonymous`` tier (today's open path: open access at the historical rate limits,
unless ACTALUX_API_KEY is set, in which case no-key still 401s as before). A
presented key is either the operator's global ACTALUX_API_KEY (the ``admin`` tier)
or an issued per-holder key, looked up by sha256 hash via the api_key_authorize
RPC, which maps it to a tier (rate limits + monthly quota) and meters the call.
An invalid presented key 401s; a valid key over its monthly quota 429s. The
resolved tier drives the per-IP, per-minute rate limits. The keyed path is dormant
until a key is issued, so the open path is unchanged in production.
"""

from __future__ import annotations

import hashlib
import hmac
import time
from collections import Counter
from datetime import date

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from pydantic import BaseModel

from actalux.db import (
    get_diarization_turns,
    get_documents,
    get_entity_by_path,
    get_entity_votes,
    get_meeting_documents,
    get_name_corrections,
    get_place_by_path,
    get_speaker_identities,
    list_recent_meeting_documents,
)
from actalux.diarization.reader import build_meeting_speakers
from actalux.errors import SearchError
from actalux.graph.store import (
    body_matters,
    body_members,
    matter_by_slug,
    matter_records,
    member_by_slug,
    member_records,
    place_lexicon,
)
from actalux.models import chunk_hash_id
from actalux.search.answer import enrich_results
from actalux.search.hybrid import SearchFilters, hybrid_search
from actalux.web.display import display_title
from actalux.web.retrieval import (
    build_reranker,
    embed_query,
    get_config,
    get_db,
    search_expansions,
)
from actalux.web.text_snippets import normalize_whitespace

# The document types that are "meetings" — they carry real meeting dates, so a
# date-keyed bundle and a recency feed are meaningful for them. Budgets, plans,
# and curriculum docs are reached via search, not these endpoints.
MEETING_TYPES = ("minutes", "transcript", "resolution", "agenda")

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


class VoteRecord(BaseModel):
    vote_id: int
    document_id: int
    title: str
    meeting_date: str | None
    motion: str
    result: str  # normalized: passed / failed / tabled / withdrawn
    # "stated" if the minutes printed a result word; "derived" if passed/failed was
    # computed from the verbatim roll call (no result line was printed).
    result_basis: str
    vote_count_yes: int | None  # null = no per-member tally was recorded (not a 0)
    vote_count_no: int | None
    vote_count_abstain: int | None
    source_quote: str  # the verbatim motion / tally / result text
    citation: str
    source_url: str | None  # original artifact (PDF or YouTube video); may be null
    html_url: str  # site-relative deep link to the cited passage in context


class VotesResponse(BaseModel):
    entity: str
    since: str | None
    count: int
    votes: list[VoteRecord]


class SpeakerIdentity(BaseModel):
    name: str  # the official's canonical name (only present when gated high/confirmed)
    slug: str | None
    confidence: str | None  # inferred_high | confirmed (the only publicly-shown levels)
    basis: str | None  # rollcall | vote_anchor | self_intro | manual


class SpeakerWord(BaseModel):
    word: str
    start: float
    end: float


class SpeakerTurn(BaseModel):
    cluster_label: str  # anonymous per-document cluster, e.g. "SPEAKER_00"
    start_seconds: float
    end_seconds: float
    speaker: SpeakerIdentity | None  # the resolved official, when gated; else null
    words: list[SpeakerWord]  # word-level timings (for clip cutting / the podcast)


class MeetingSpeakersResponse(BaseModel):
    entity: str
    document_id: int
    turn_count: int
    speakers: dict[str, SpeakerIdentity]  # cluster_label -> identity (gated only)
    turns: list[SpeakerTurn]


class MemberVote(BaseModel):
    edge_type: str  # voted_aye_on | voted_no_on | voted_abstain_on | moved | seconded
    document_id: int
    meeting_date: str | None
    title: str
    motion: str
    result: str
    result_basis: str
    vote_count_yes: int | None
    vote_count_no: int | None
    vote_count_abstain: int | None
    source_quote: str  # the verbatim motion / tally / result text the edge cites
    citation: str
    source_url: str | None  # original artifact (PDF or YouTube); may be null
    html_url: str  # site-relative deep link to the cited passage in context


class MemberSummary(BaseModel):
    slug: str
    name: str
    role: str | None
    ward: int | None
    term_start: str | None
    term_end: str | None  # null = currently seated


class MembersResponse(BaseModel):
    entity: str
    count: int
    members: list[MemberSummary]


class MemberRecord(BaseModel):
    entity: str
    slug: str
    name: str
    role: str | None
    ward: int | None
    term_start: str | None
    term_end: str | None
    counts: dict[str, int]  # edges by type: voted_aye_on / .../ moved / seconded
    record: list[MemberVote]


class MatterAction(BaseModel):
    document_id: int
    meeting_date: str | None
    title: str  # display title of the meeting the action happened at
    motion: str
    result: str
    result_basis: str
    vote_count_yes: int | None
    vote_count_no: int | None
    vote_count_abstain: int | None
    source_quote: str  # the verbatim motion text the edge cites
    citation: str
    source_url: str | None  # original artifact (PDF); may be null
    html_url: str  # site-relative deep link to the cited passage in context


class MatterSummary(BaseModel):
    slug: str
    name: str  # 'Bill No. 7156'
    kind: str | None  # bill | resolution
    number: str | None
    title: str | None  # the ordinance/resolution title, when stated in a motion
    actions: int
    latest_date: str | None


class MattersResponse(BaseModel):
    entity: str
    count: int
    matters: list[MatterSummary]


class MatterRecord(BaseModel):
    entity: str
    slug: str
    name: str
    kind: str | None
    number: str | None
    title: str | None
    actions: int
    timeline: list[MatterAction]  # cited council actions, oldest first


class LexiconBody(BaseModel):
    body_slug: str
    role: str | None
    start_date: str | None
    end_date: str | None  # null = still seated on this body


class LexiconAlias(BaseModel):
    raw: str | None  # the variant as printed in the record (may be an OCR form)
    normalized: str  # the resolver's normalized key
    source: str | None  # provenance: roster | ocr | asr | reviewed


class LexiconEntry(BaseModel):
    slug: str
    canonical_name: str
    kind: str  # 'person' (orgs/places arrive with connections-graph Phase 3)
    role: str | None
    current: bool  # still seated on at least one body
    bodies: list[LexiconBody]
    aliases: list[LexiconAlias]


class LexiconResponse(BaseModel):
    place: str  # 'mo/clayton'
    count: int
    entries: list[LexiconEntry]


class CorrectionEntry(BaseModel):
    mangled: str  # the wrong form (match case-insensitively, word-boundaried)
    canonical: str  # the correct spelling
    category: str | None  # person | staff | street | business | school | place | org | other
    provenance: str | None  # asr | ocr | reviewed


class CorrectionsResponse(BaseModel):
    place: str  # 'mo/clayton'
    count: int
    corrections: list[CorrectionEntry]


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


def _resolve_tier(request: Request) -> str:
    """Resolve the request's tier, stash it on ``request.state``, and gate access.

    The single source of truth for "is this request allowed, and at what tier":

    - No key presented: if ACTALUX_API_KEY is set the API is locked (401, as
      before); otherwise the request is ``anonymous``. This branch makes NO DB
      call, so the open path is exactly as cheap as it has always been.
    - The global ACTALUX_API_KEY: the ``admin`` tier (operator's own key).
    - Any other presented key: only the keyed-DB path. It is gated by
      ``api_keys_enabled`` — off in prod today, so a non-global key 401s WITHOUT
      any DB call until per-holder keys are turned on. When enabled, a cheap per-IP
      attempt limiter fires BEFORE the RPC (so bogus-key floods can't hammer the
      DB), then api_key_authorize maps the key to a tier and meters the call: a
      non-matching key 401s, a valid key over its monthly quota 429s, otherwise its
      tier is used.

    The resolved tier name is stored on ``request.state.tier`` so the rate-limit
    dependencies can read it without re-doing the lookup.
    """
    presented = _presented_key(request)
    expected = get_config().api_key

    if not presented:
        if expected:
            raise HTTPException(status_code=401, detail="Invalid or missing API key")
        request.state.tier = "anonymous"
        return "anonymous"

    if expected and hmac.compare_digest(presented, expected):
        request.state.tier = "admin"
        return "admin"

    tier = _authorize_keyed(request, presented)
    request.state.tier = tier
    return tier


def _authorize_keyed(request: Request, presented: str) -> str:
    """Authorize a presented (non-global) key via the RPC; return its tier.

    Dormant unless ``api_keys_enabled``: a non-global key is rejected (401) with NO
    DB call until the keyed path is turned on. Once enabled, a per-IP minute limiter
    on auth attempts runs first (so a flood of bogus keys can't hammer the DB), then
    the raw key is hashed to sha256 hex (only the hash is ever stored) and passed to
    api_key_authorize, which returns one row ``{valid, tier, over_quota}`` and has
    already counted this call. Raises 401 for an unknown/inactive/expired key and
    429 for a valid key over its monthly quota.
    """
    config = get_config()
    if not config.api_keys_enabled:
        raise HTTPException(status_code=401, detail="Invalid or missing API key")

    _enforce_rate(request, "auth", config.rate_limit_auth_attempts_per_minute)

    key_hash = hashlib.sha256(presented.encode("utf-8")).hexdigest()
    result = get_db().rpc("api_key_authorize", {"p_key_hash": key_hash}).execute()
    rows = result.data or []
    row = rows[0] if rows else {}
    if not row.get("valid"):
        raise HTTPException(status_code=401, detail="Invalid or missing API key")
    if row.get("over_quota"):
        raise HTTPException(status_code=429, detail="Monthly quota exceeded")
    return row.get("tier") or "developer"


def require_api_key(request: Request) -> None:
    """Route dependency: resolve + enforce the request's tier (see ``_resolve_tier``)."""
    _resolve_tier(request)


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


def _request_tier(request: Request) -> str:
    """The tier resolved by ``require_api_key`` for this request.

    Always set in the normal flow (the auth dependency runs first), but defaults
    to ``anonymous`` so a limiter invoked without it still applies the open-path
    numbers rather than erroring.
    """
    return getattr(request.state, "tier", "anonymous")


def rate_limit_search(request: Request) -> None:
    """Tighter limit for the search endpoint (it runs the paid reranker).

    The per-minute cap is the resolved tier's ``search_per_min``; for anonymous
    that reads back the historical ``rate_limit_search_per_minute`` config, so the
    open path is unchanged.
    """
    limit = get_config().tier(_request_tier(request)).search_per_min
    _enforce_rate(request, "search", limit)


def rate_limit_general(request: Request) -> None:
    """Default limit for the cheap (DB-only) endpoints, per the resolved tier."""
    limit = get_config().tier(_request_tier(request)).general_per_min
    _enforce_rate(request, "general", limit)


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


def resolve_api_place(state: str, place: str) -> dict:
    """Resolve a place (state + slug) for the place-scoped lexicon, or 404 (as JSON)."""
    row = get_place_by_path(get_db(), state, place)
    if not row:
        raise HTTPException(status_code=404, detail="Unknown jurisdiction")
    return row


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
    """The real origin URL for a document, suitable for "Open original ↗" links.

    Returns the YouTube watch page for board-meeting video docs; returns None
    for transcripts without a public video (source is derived .txt, not an
    embeddable origin); returns ``documents.source_url`` as-is for everything
    else — that column holds the Diligent/school-site origin, not a storage URL.

    Never returns a Supabase Storage URL. PDF embeds are built separately via
    ``stored_file_url(source_file)`` at the template/call site.
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


def _build_vote_record(vote: dict, doc: dict) -> VoteRecord:
    """Assemble a cited vote record from a votes row and its document.

    The citation routes on the stable ``citation_id`` when present, falling back to
    the numeric ``chunk_id``; both resolve to the verbatim minutes passage.
    """
    title = display_title(doc) if doc else ""
    cite_ref = vote.get("citation_id") or vote.get("chunk_id")
    hash_id = chunk_hash_id(cite_ref)
    return VoteRecord(
        vote_id=vote["id"],
        document_id=vote["document_id"],
        title=title,
        meeting_date=vote.get("meeting_date") or None,
        motion=vote.get("motion") or "",
        result=vote.get("result") or "",
        result_basis=vote.get("result_basis") or "stated",
        vote_count_yes=vote.get("vote_count_yes"),
        vote_count_no=vote.get("vote_count_no"),
        vote_count_abstain=vote.get("vote_count_abstain"),
        source_quote=vote.get("source_quote") or "",
        citation=f"{title} [{hash_id}]",
        source_url=_source_url(doc) if doc else None,
        html_url=f"/chunk/{cite_ref}/source" if cite_ref else "",
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
    summary="Search a body's records for cited verbatim passages",
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
            client,
            q,
            embedding,
            filters,
            max_results=limit,
            reranker=build_reranker(),
            expansions=search_expansions(q, entity.get("place_id")),
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
    summary="All documents for one meeting date",
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
    summary="Recent meeting documents ('what's new since a date')",
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


@api_router.get(
    "/{state}/{place}/{body}/votes",
    response_model=VotesResponse,
    summary="Structured, cited board-vote records",
    dependencies=[Depends(rate_limit_general)],
)
def api_votes(
    since: str | None = Query(None, description="Inclusive meeting_date lower bound, YYYY-MM-DD"),
    limit: int = Query(50, ge=1, le=_RECENT_LIMIT_MAX),
    entity: dict = Depends(resolve_api_entity),
) -> VotesResponse:
    """Structured board-vote records for one body, newest meeting first.

    Each record carries the motion, normalized result, per-member tally (null when
    the minutes recorded no count), and a citation to the verbatim minutes passage.
    ``result_basis`` flags whether the result was stated in the minutes or derived
    from the roll call.
    """
    client = get_db()
    iso_since = _parse_date(since) if since else None
    rows = get_entity_votes(client, entity["id"], since=iso_since, limit=limit)
    docs = get_documents(client, [r["document_id"] for r in rows])
    records = [_build_vote_record(r, docs.get(r["document_id"], {})) for r in rows]
    return VotesResponse(
        entity=_entity_path(entity), since=iso_since, count=len(records), votes=records
    )


@api_router.get(
    "/{state}/{place}/{body}/transcripts/{document_id}/speakers",
    response_model=MeetingSpeakersResponse,
    summary="Word-level speaker turns + (gated) speaker identities for a transcript",
    dependencies=[Depends(rate_limit_general)],
)
def api_meeting_speakers(
    document_id: int,
    entity: dict = Depends(resolve_api_entity),
) -> MeetingSpeakersResponse:
    """The speaker-attribution layer for one meeting transcript.

    Anonymous word-level speaker turns plus, where a cluster is resolved to the public
    bar (high/confirmed), the official it maps to. The display gate is enforced in the
    database, so a not-yet-confirmed cluster simply appears as turns with no name. Word
    timings are included for downstream clip cutting / the podcast.
    """
    client = get_db()
    # Scope the document to the requested body so one body's transcript can't be served
    # under another body's path.
    owner = (
        client.table("documents").select("entity_id").eq("id", document_id).limit(1).execute().data
    )
    if not owner or owner[0].get("entity_id") != entity["id"]:
        raise HTTPException(status_code=404, detail="transcript not found for this body")
    layer = build_meeting_speakers(
        get_diarization_turns(client, document_id),
        get_speaker_identities(client, document_id),
    )
    return MeetingSpeakersResponse(
        entity=_entity_path(entity),
        document_id=document_id,
        turn_count=len(layer["turns"]),
        speakers={c: SpeakerIdentity(**ident) for c, ident in layer["speakers"].items()},
        turns=[
            SpeakerTurn(
                cluster_label=t["cluster_label"],
                start_seconds=t["start_seconds"],
                end_seconds=t["end_seconds"],
                speaker=SpeakerIdentity(**t["speaker"]) if t["speaker"] else None,
                words=[SpeakerWord(**w) for w in t["words"]],
            )
            for t in layer["turns"]
        ],
    )


def _member_doc(row: dict) -> dict:
    """A document-shaped dict from a member_vote_records row, for the shared
    display_title / _source_url builders (the view flattens the document fields)."""
    return {
        "id": row["document_id"],
        "meeting_title": row.get("meeting_title"),
        "meeting_date": row.get("meeting_date"),
        "document_type": "minutes",
        "source_portal": row.get("source_portal"),
        "video_id": row.get("video_id"),
        "source_url": row.get("source_url"),
        "source_file": row.get("source_file"),
    }


def _build_member_vote(row: dict) -> MemberVote:
    """One cited record from a member_vote_records row (citation routes on citation_id)."""
    doc = _member_doc(row)
    title = display_title(doc)
    cite_ref = row.get("citation_id")
    hash_id = chunk_hash_id(cite_ref)
    return MemberVote(
        edge_type=row["edge_type"],
        document_id=row["document_id"],
        meeting_date=row.get("meeting_date") or None,
        title=title,
        motion=row.get("motion") or "",
        result=row.get("result") or "",
        result_basis=row.get("result_basis") or "stated",
        vote_count_yes=row.get("vote_count_yes"),
        vote_count_no=row.get("vote_count_no"),
        vote_count_abstain=row.get("vote_count_abstain"),
        source_quote=row.get("source_quote") or "",
        citation=f"{title} [{hash_id}]",
        source_url=_source_url(doc),
        html_url=f"/chunk/{cite_ref}/source" if cite_ref else "",
    )


def _member_summary(member: dict) -> MemberSummary:
    meta = member.get("metadata") or {}
    return MemberSummary(
        slug=member["slug"],
        name=member["canonical_name"],
        role=meta.get("role"),
        ward=meta.get("ward"),
        term_start=member.get("start_date"),
        term_end=member.get("end_date"),
    )


def _build_matter_action(row: dict) -> MatterAction:
    """One cited action from a matter_vote_records row (same document fields as a
    member record, so the shared doc/title/url builders apply)."""
    doc = _member_doc(row)
    title = display_title(doc)
    cite_ref = row.get("citation_id")
    hash_id = chunk_hash_id(cite_ref)
    return MatterAction(
        document_id=row["document_id"],
        meeting_date=row.get("meeting_date") or None,
        title=title,
        motion=row.get("motion") or "",
        result=row.get("result") or "",
        result_basis=row.get("result_basis") or "stated",
        vote_count_yes=row.get("vote_count_yes"),
        vote_count_no=row.get("vote_count_no"),
        vote_count_abstain=row.get("vote_count_abstain"),
        source_quote=row.get("source_quote") or "",
        citation=f"{title} [{hash_id}]",
        source_url=_source_url(doc),
        html_url=f"/chunk/{cite_ref}/source" if cite_ref else "",
    )


@api_router.get(
    "/{state}/{place}/{body}/members",
    response_model=MembersResponse,
    summary="The body's roster (publishable members)",
    dependencies=[Depends(rate_limit_general)],
)
def api_members(entity: dict = Depends(resolve_api_entity)) -> MembersResponse:
    """The body's roster: publishable members with role, ward, and term window."""
    members = body_members(get_db(), entity["id"])
    summaries = sorted((_member_summary(m) for m in members), key=lambda s: s.name)
    return MembersResponse(entity=_entity_path(entity), count=len(summaries), members=summaries)


@api_router.get(
    "/{state}/{place}/{body}/matters",
    response_model=MattersResponse,
    summary="Bills & resolutions the body acted on",
    dependencies=[Depends(rate_limit_general)],
)
def api_matters(entity: dict = Depends(resolve_api_entity)) -> MattersResponse:
    """Every bill/resolution the body acted on, with its action count and latest date."""
    matters = body_matters(get_db(), entity["id"])
    summaries = [
        MatterSummary(
            slug=m["slug"],
            name=m["canonical_name"],
            kind=(m.get("metadata") or {}).get("kind"),
            number=(m.get("metadata") or {}).get("number"),
            title=(m.get("metadata") or {}).get("title"),
            actions=m["actions"],
            latest_date=m.get("latest_date"),
        )
        for m in matters
    ]
    return MattersResponse(entity=_entity_path(entity), count=len(summaries), matters=summaries)


@api_router.get(
    "/{state}/{place}/{body}/matters/{slug}",
    response_model=MatterRecord,
    summary="A bill/resolution's complete cited timeline",
    dependencies=[Depends(rate_limit_general)],
)
def api_matter(slug: str, entity: dict = Depends(resolve_api_entity)) -> MatterRecord:
    """One matter's complete cited timeline (every council action), oldest first.

    Every action cites the verbatim minutes passage it was read from. 404 if the slug
    is not a publishable matter, or has no action in this body.
    """
    client = get_db()
    matter = matter_by_slug(client, entity["place_id"], slug)
    if not matter:
        raise HTTPException(status_code=404, detail="Unknown matter")
    rows = matter_records(client, matter["id"], entity["id"])
    if not rows:
        raise HTTPException(status_code=404, detail="Unknown matter")
    rows.sort(key=lambda r: r.get("meeting_date") or "")
    meta = matter.get("metadata") or {}
    return MatterRecord(
        entity=_entity_path(entity),
        slug=matter["slug"],
        name=matter["canonical_name"],
        kind=meta.get("kind"),
        number=meta.get("number"),
        title=meta.get("title"),
        actions=len(rows),
        timeline=[_build_matter_action(r) for r in rows],
    )


@api_router.get(
    "/{state}/{place}/{body}/members/{slug}",
    response_model=MemberRecord,
    summary="A member's complete cited voting record",
    dependencies=[Depends(rate_limit_general)],
)
def api_member(slug: str, entity: dict = Depends(resolve_api_entity)) -> MemberRecord:
    """One member's complete cited voting record (aye/no/abstain + moved/seconded).

    Every entry cites the verbatim minutes passage it was read from. 404 if the slug
    is not a publishable member of this body.
    """
    client = get_db()
    member = member_by_slug(client, entity["place_id"], slug, entity["id"])
    if not member:
        raise HTTPException(status_code=404, detail="Unknown member")
    rows = member_records(client, member["id"], entity["id"])
    meta = member.get("metadata") or {}
    return MemberRecord(
        entity=_entity_path(entity),
        slug=member["slug"],
        name=member["canonical_name"],
        role=meta.get("role"),
        ward=meta.get("ward"),
        term_start=member.get("start_date"),
        term_end=member.get("end_date"),
        counts=dict(Counter(r["edge_type"] for r in rows)),
        record=[_build_member_vote(r) for r in rows],
    )


@api_router.get(
    "/{state}/{place}/lexicon",
    response_model=LexiconResponse,
    summary="Canonical proper-name lexicon for a place (officials + name variants)",
    dependencies=[Depends(rate_limit_general)],
)
def api_lexicon(place_row: dict = Depends(resolve_api_place)) -> LexiconResponse:
    """Every public official in the place: canonical name, name variants, memberships.

    The authority a downstream product uses to spell official names consistently, so
    they are maintained in one place. Place-scoped (not body-scoped): a person on two
    bodies is one entry carrying both memberships. Each variant reports its provenance
    (``source``). Only publishable subjects appear (the same gate as the dossiers).
    """
    entries = place_lexicon(get_db(), place_row["id"])
    items = [
        LexiconEntry(
            slug=e["slug"],
            canonical_name=e["canonical_name"],
            kind=e["kind"],
            role=e["role"],
            current=e["current"],
            bodies=[LexiconBody(**b) for b in e["bodies"]],
            aliases=[LexiconAlias(**a) for a in e["aliases"]],
        )
        for e in entries
    ]
    return LexiconResponse(
        place=f"{place_row['state']}/{place_row['slug']}", count=len(items), entries=items
    )


@api_router.get(
    "/{state}/{place}/corrections",
    response_model=CorrectionsResponse,
    summary="Proper-noun spelling corrections for a place (mangling -> canonical)",
    dependencies=[Depends(rate_limit_general)],
)
def api_corrections(place_row: dict = Depends(resolve_api_place)) -> CorrectionsResponse:
    """The place's name-correction lexicon: known manglings and their canonical form.

    The single home for proper-noun spelling fixes (officials, staff, streets,
    businesses, schools), so a downstream product maintains them in one place rather
    than its own list. Place-scoped: a mangling valid in one town can be a real name
    in another. Each row carries its category and provenance (asr/ocr/reviewed).
    """
    rows = get_name_corrections(get_db(), place_row["id"])
    corrections = [
        CorrectionEntry(
            mangled=r["mangled"],
            canonical=r["canonical"],
            category=r.get("category"),
            provenance=r.get("provenance"),
        )
        for r in sorted(rows, key=lambda r: r["mangled"])
    ]
    return CorrectionsResponse(
        place=f"{place_row['state']}/{place_row['slug']}",
        count=len(corrections),
        corrections=corrections,
    )
