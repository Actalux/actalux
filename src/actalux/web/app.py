"""FastAPI application for Actalux.

Endpoints:
  POST /search          — hybrid search with RRF
  GET  /topic/budget    — preset budget topic page
  GET  /document/{id}   — full document view
  GET  /chunk/{id}/source — citation context (chunk + neighbors)
  GET  /methodology     — how the system works
  POST /report-error    — submit a correction
  POST /summarize       — citation-backed LLM summary
"""

from __future__ import annotations

import logging
import re
import time
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any
from urllib.parse import quote

from fastapi import APIRouter, Depends, FastAPI, Form, HTTPException, Query, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from markupsafe import Markup, escape
from supabase import Client

from actalux.db import (
    get_budget_line_items,
    get_chunk_with_context,
    get_document,
    get_entity,
    get_entity_by_path,
    insert_correction,
    list_documents,
    list_entities,
)
from actalux.errors import SearchError, SummaryError
from actalux.models import Correction, chunk_hash_id
from actalux.search.answer import assemble_evidence, enrich_results
from actalux.search.hybrid import SearchFilters, hybrid_search
from actalux.search.summarize import generate_summary
from actalux.web.api import api_router
from actalux.web.charts import (
    aggregate_by_year,
    budget_vs_actual,
    component_trend,
    cross_split,
    function_breakdown,
    fund_breakdown,
    revenue_expenditure_svg,
    source_breakdown,
    trend_svg,
    usd,
)
from actalux.web.display import display_title, first_sentence, source_label
from actalux.web.retrieval import build_reranker, embed_query, get_config, get_db
from actalux.web.storage import stored_file_url
from actalux.web.text_snippets import (
    TRANSCRIPT_CAPTION_LABEL,
    clean_text_light,
    content_paragraphs,
    extractive_snippet,
    normalize_whitespace,
    reflow_transcript,
    split_for_highlight,
)

logger = logging.getLogger(__name__)

TEMPLATE_DIR = Path(__file__).parent / "templates"
STATIC_DIR = Path(__file__).parent / "static"

app = FastAPI(title="Actalux", version="0.1.0")
app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")
templates = Jinja2Templates(directory=str(TEMPLATE_DIR))
templates.env.filters["chunk_hash_id"] = chunk_hash_id

# Apex (actalux.org) is the canonical host; www redirects to it so links and
# search indexing don't fork across two hostnames.
CANONICAL_HOST = "actalux.org"
WWW_HOST = "www.actalux.org"


@app.middleware("http")
async def redirect_www_to_apex(request: Request, call_next: Any) -> Any:
    """301 www.actalux.org -> actalux.org, preserving path and query string."""
    if request.url.hostname == WWW_HOST:
        target = request.url.replace(scheme="https", netloc=CANONICAL_HOST)
        return RedirectResponse(str(target), status_code=301)
    return await call_next(request)


# --- Jurisdiction (place/entity) routing -------------------------------------
# Each public body is served under /{state}/{place}/{body}, e.g.
# /mo/clayton/schools. While there is one body, the apex and place hub redirect
# to it (a directory landing arrives with the second body). See
# docs/architecture/multi-tenancy.md.
DEFAULT_ENTITY_PATH = "/mo/clayton/schools"


@dataclass(frozen=True)
class EntityView:
    """A resolved public body plus the presentational bits every page needs."""

    entity: dict[str, Any]
    base: str  # URL prefix for this body, e.g. "/mo/clayton/schools"
    tag: str  # short top-bar label, e.g. "Clayton MO"


def _entity_view(entity: dict[str, Any]) -> EntityView:
    place = entity.get("place") or {}
    state = place.get("state", "")
    base = f"/{state}/{place.get('slug', '')}/{entity['body_slug']}"
    tag = f"{place.get('display_name', '')} {state.upper()}".strip()
    return EntityView(entity=entity, base=base, tag=tag)


def resolve_entity(state: str, place: str, body: str) -> EntityView:
    """FastAPI dependency: resolve a body from its URL parts or 404."""
    entity = get_entity_by_path(_get_db(), state, place, body)
    if not entity:
        raise HTTPException(status_code=404, detail="Unknown jurisdiction")
    return _entity_view(entity)


def _entity_view_for_document(client: Client, doc: dict[str, Any] | None) -> EntityView | None:
    """The EntityView a flat (doc/chunk) page renders its chrome under."""
    if not doc or doc.get("entity_id") is None:
        return None
    entity = get_entity(client, doc["entity_id"])
    return _entity_view(entity) if entity else None


def _page(ev: EntityView | None, **extra: Any) -> dict[str, Any]:
    """Template context with the entity chrome (entity, base, tag) merged in.

    Parameter is named ``ev`` (not ``view``) so callers can spread a breakdown
    context that carries its own ``view`` key without a keyword collision.
    base defaults to the canonical body so flat pages and the few entity-less
    contexts still render valid links while there is one body.
    """
    if ev is None:
        return {"entity": None, "base": DEFAULT_ENTITY_PATH, "entity_tag": "", **extra}
    return {"entity": ev.entity, "base": ev.base, "entity_tag": ev.tag, **extra}


def _match_snippet(content: str, query: str, width: int = 220) -> Markup:
    """Best-sentence match snippet with query terms marked, HTML-safe.

    Thin Markup wrapper over ``extractive_snippet`` (the pure, tested logic):
    picks the sentence that best covers the query rather than windowing around
    the first keyword hit, which routinely landed on boilerplate.
    """
    return Markup(extractive_snippet(content, query, max_chars=width))


def _cited_html(content: str, query: str) -> Markup:
    """Render a cited chunk with only its most query-relevant sentence highlighted.

    Keeps the archival-yellow ``.cited`` motif on the relevant clause instead of
    a 200-word solid-yellow block; the rest of the chunk reads as context.
    """
    before, key, after = split_for_highlight(content, query)
    parts = []
    if before:
        parts.append(str(escape(before)) + " ")
    parts.append('<span class="cited">' + str(escape(key)) + "</span>")
    if after:
        parts.append(" " + str(escape(after)))
    return Markup("".join(parts))


def _safe_url(value: str) -> str:
    """Percent-encode a stored source URL for safe use in href/src attributes.

    Many stored ``source_url`` values contain raw spaces and commas (e.g.
    ``.../April 24, 2024 Meeting Minutes.pdf``). Encode the unsafe characters
    while preserving URL structure (scheme, host, slashes, query). ``%`` is in
    the safe set so already-encoded sequences are not double-encoded.
    """
    if not value:
        return ""
    return quote(value, safe="%/:?#[]@!$&'()*+,;=~")


templates.env.filters["match_snippet"] = _match_snippet
templates.env.filters["cited_html"] = _cited_html
templates.env.filters["clean_text"] = normalize_whitespace
templates.env.filters["content_paragraphs"] = content_paragraphs
templates.env.filters["display_title"] = display_title
templates.env.filters["source_label"] = source_label
templates.env.filters["first_sentence"] = first_sentence
templates.env.filters["usd"] = usd
templates.env.filters["safe_url"] = _safe_url
# Transcript-specific reflow (YouTube portal only): strips standalone timestamps
# and paragraph-groups the result.  Rolling-caption dedup is NOT applied (verbatim
# safety — see reflow_transcript docstring).
templates.env.filters["reflow_transcript"] = reflow_transcript
# Light whitespace normalizer for non-transcript chunk text in the reader pane.
# Collapses whitespace without blank-line splits that would wreck tabular content.
templates.env.filters["clean_text_light"] = clean_text_light
# Public bucket URL for embedding/downloading a stored file (PDF only at the
# call sites). Lazy config load, so it costs nothing unless a template uses it.
templates.env.filters["stored_file_url"] = stored_file_url
templates.env.globals["stored_file_url"] = stored_file_url
# Caption label constant: used in reader_pane.html to label auto-generated captions.
# Exposed as a global so the template and the Python module share one source.
templates.env.globals["transcript_caption_label"] = TRANSCRIPT_CAPTION_LABEL

# In-process cache for topic page queries (1-hour TTL)
_topic_cache: dict[str, tuple[float, list[Any]]] = {}
TOPIC_CACHE_TTL = 3600  # seconds


# Retrieval primitives live in retrieval.py so the JSON API can share them
# without importing this module (which would be a cycle). The underscore aliases
# keep the existing call sites — and the tests that patch them — unchanged.
_get_config = get_config
_get_db = get_db
_embed_query = embed_query
_reranker = build_reranker


def _get_cached_topic(topic: str) -> list[Any] | None:
    """Return cached results if still valid, else None."""
    if topic not in _topic_cache:
        return None
    cached_time, results = _topic_cache[topic]
    if time.monotonic() - cached_time > TOPIC_CACHE_TTL:
        del _topic_cache[topic]
        return None
    return results


def _set_cached_topic(topic: str, results: list[Any]) -> None:
    """Cache topic results with current timestamp."""
    _topic_cache[topic] = (time.monotonic(), results)


# --- Topic preset queries ---
# Each entry is (heading, query): the heading is shown to readers, the query
# drives retrieval. Queries stay keyword-dense for recall; headings stay readable.

BUDGET_QUERIES = [
    ("Budget approval & spending", "budget approval spending fiscal year"),
    ("Tax levy & revenue", "tax levy revenue property tax"),
    ("Salaries & benefits", "salary compensation benefits"),
    ("Capital improvements & facilities", "capital improvement facilities construction"),
    ("Fund balance & reserves", "fund balance reserves financial"),
]

# The first query is phrased to surface the plan's own one-sentence definition of
# itself (verbatim), so the page leads with a sourced overview, not an editorial one.
FACILITIES_QUERIES = [
    (
        "What the plan is",
        "comprehensive strategic document future development maintenance "
        "management school facilities 15 years",
    ),
    (
        "Building conditions & deferred maintenance",
        "building conditions maintenance deferred repairs facility appraisal",
    ),
    (
        "Enrollment & building capacity",
        "enrollment demographic projections building capacity utilization",
    ),
    (
        "Cost estimates & priorities",
        "capital improvement construction cost estimates priorities ranking",
    ),
    ("HVAC & infrastructure", "HVAC mechanical systems infrastructure upgrades"),
]


# --- Routes ---

# Entity-scoped pages live on this router under /{state}/{place}/{body}. It is
# included last so the specific flat routes (/document, /chunk, redirects) win
# the match over its greedy path prefix.
jurisdiction = APIRouter(prefix="/{state}/{place}/{body}")


@app.get("/healthz")
def healthz() -> dict[str, str]:
    """Liveness probe for the platform health check.

    Deliberately DB- and config-free so a paused Supabase free tier can't mark
    the app unhealthy and trigger restarts. It only reports that the process is
    up and serving.
    """
    return {"status": "ok"}


def _redirect_to_default(suffix: str, request: Request) -> RedirectResponse:
    """301 an old flat path to its canonical entity-scoped path, keeping query."""
    target = DEFAULT_ENTITY_PATH + suffix
    if request.url.query:
        target = f"{target}?{request.url.query}"
    return RedirectResponse(target, status_code=301)


@app.get("/")
async def apex() -> RedirectResponse:
    """Apex -> the one body for now; becomes a directory landing in Phase C."""
    return RedirectResponse(DEFAULT_ENTITY_PATH, status_code=307)


@jurisdiction.get("", response_class=HTMLResponse)
async def home(request: Request, view: EntityView = Depends(resolve_entity)) -> HTMLResponse:
    """A body's home page with the search box."""
    return templates.TemplateResponse(request, "home.html", _page(view))


def _run_search(
    request: Request,
    view: EntityView,
    q: str,
    date_from: str,
    date_to: str,
    doc_type: str,
) -> HTMLResponse:
    """Shared search handler for GET and POST routes, scoped to one body."""
    is_htmx = bool(request.headers.get("HX-Request"))
    if not q.strip():
        template = "partials/search_results.html" if is_htmx else "search.html"
        return templates.TemplateResponse(request, template, _page(view, results=[], query=""))

    filters = SearchFilters(
        date_from=date.fromisoformat(date_from) if date_from else None,
        date_to=date.fromisoformat(date_to) if date_to else None,
        document_type=doc_type or None,
        entity_id=view.entity["id"],
    )

    client = _get_db()
    try:
        query_embedding = _embed_query(q)
        results = hybrid_search(client, q, query_embedding, filters, reranker=_reranker())
    except SearchError:
        logger.exception("Search failed for query: %s", q)
        results = []

    enriched = enrich_results(client, results)
    template = "partials/search_results.html" if is_htmx else "search.html"

    return templates.TemplateResponse(request, template, _page(view, results=enriched, query=q))


@jurisdiction.get("/search", response_class=HTMLResponse)
def search_get(
    request: Request,
    view: EntityView = Depends(resolve_entity),
    q: str = "",
    date_from: str = "",
    date_to: str = "",
    doc_type: str = "",
) -> HTMLResponse:
    """GET variant for linkable / restorable search URLs.

    Defined sync (not async) so Starlette runs the CPU-bound query embedding
    and blocking Supabase RPCs in a threadpool, keeping the event loop free
    for other requests on this single-instance server.
    """
    return _run_search(request, view, q, date_from, date_to, doc_type)


@jurisdiction.post("/search", response_class=HTMLResponse)
def search_post(
    request: Request,
    view: EntityView = Depends(resolve_entity),
    q: str = Form(""),
    date_from: str = Form(""),
    date_to: str = Form(""),
    doc_type: str = Form(""),
) -> HTMLResponse:
    """POST from the search form (works with or without HTMX). Sync for the same
    threadpool reason as ``search_get``."""
    return _run_search(request, view, q, date_from, date_to, doc_type)


@app.get("/search", response_class=HTMLResponse)
async def search_redirect(request: Request) -> RedirectResponse:
    """Legacy flat /search -> the canonical body's search (keeps query)."""
    return _redirect_to_default("/search", request)


# Browse-by-type: the sidebar "Documents" links list documents of one kind
# chronologically (newest first) rather than running a keyword search. Most
# kinds map to a document_type; curriculum maps share document_type='other' and
# are identified by their filename instead.
@dataclass(frozen=True)
class BrowseKind:
    slug: str
    label: str
    document_type: str | None = None
    source_file_like: str | None = None


BROWSE_KINDS: dict[str, BrowseKind] = {
    "minutes": BrowseKind("minutes", "Minutes", document_type="minutes"),
    "budgets": BrowseKind("budgets", "Budgets", document_type="budget"),
    "resolutions": BrowseKind("resolutions", "Resolutions", document_type="resolution"),
    "transcripts": BrowseKind("transcripts", "Transcripts", document_type="transcript"),
    "curriculum-maps": BrowseKind(
        "curriculum-maps", "Curriculum maps", source_file_like="%curriculum%map%"
    ),
    "facilities-plan": BrowseKind(
        "facilities-plan", "Facilities plan", document_type="facilities_plan"
    ),
}


@jurisdiction.get("/browse/{kind}", response_class=HTMLResponse)
def browse(request: Request, kind: str, view: EntityView = Depends(resolve_entity)) -> HTMLResponse:
    """List one document kind for this body, newest first — a browse, not a search.

    Sync (not async) so the blocking Supabase query runs in Starlette's
    threadpool, matching ``search_get``.
    """
    spec = BROWSE_KINDS.get(kind)
    if spec is None:
        raise HTTPException(status_code=404, detail="Unknown document type")
    docs = list_documents(
        _get_db(),
        view.entity["id"],
        document_type=spec.document_type,
        source_file_like=spec.source_file_like,
    )
    return templates.TemplateResponse(
        request,
        "browse.html",
        _page(view, documents=docs, kind=spec, active=f"browse-{spec.slug}"),
    )


def _topic_quote_sections(
    entity_id: int, queries: list[tuple[str, str]], cache_prefix: str
) -> list[dict[str, Any]]:
    """Run preset (heading, query) pairs into cited-quote sections (cached 1h).

    Each section carries the reader-facing ``label`` (heading) and the ``query``
    that drove retrieval (kept so the template can highlight the query terms in
    each snippet). Shared by the budget and facilities topic pages.
    """
    cache_key = f"{cache_prefix}:{entity_id}"
    cached = _get_cached_topic(cache_key)
    if cached is not None:
        return cached

    client = _get_db()
    filters = SearchFilters(entity_id=entity_id)
    sections: list[dict[str, Any]] = []
    for heading, query_text in queries:
        try:
            query_embedding = _embed_query(query_text)
            results = hybrid_search(
                client, query_text, query_embedding, filters, max_results=5, reranker=_reranker()
            )
            enriched = enrich_results(client, results)
            sections.append({"label": heading, "query": query_text, "results": enriched})
        except SearchError:
            logger.exception("%s topic query failed: %s", cache_prefix, query_text)
            sections.append({"label": heading, "query": query_text, "results": []})

    _set_cached_topic(cache_key, sections)
    return sections


def _budget_quote_sections(entity_id: int) -> list[dict[str, Any]]:
    """Cited-quote sections for the Budget topic page."""
    return _topic_quote_sections(entity_id, BUDGET_QUERIES, "budget")


def _facilities_quote_sections(entity_id: int) -> list[dict[str, Any]]:
    """Cited-quote sections for the Facilities Plan topic page."""
    return _topic_quote_sections(entity_id, FACILITIES_QUERIES, "facilities")


# Breakdown views switchable on the Budget page. "fund"/"function" break down
# expenditure; "source" breaks down revenue. Order = tab order.
_BREAKDOWN_VIEWS = ("function", "fund", "source")
_DEFAULT_BREAKDOWN_VIEW = "function"


def _breakdown_context(client: Client, view: str, fiscal_year: str | None) -> dict[str, Any]:
    """Shares + heading + citation anchor for one breakdown view of one year."""
    if view == "source":
        items = get_budget_line_items(client, dimension="source")
        shares = source_breakdown(items, fiscal_year) if fiscal_year else []
        measure = "Revenue by source"
    elif view == "fund":
        items = get_budget_line_items(client, dimension="fund")
        shares = fund_breakdown(items, fiscal_year) if fiscal_year else []
        measure = "Expenditure by fund"
    else:  # function
        items = get_budget_line_items(client, dimension="function")
        shares = function_breakdown(items, fiscal_year) if fiscal_year else []
        measure = "Expenditure by function"
    return {"view": view, "year": fiscal_year, "shares": shares, "measure": measure}


def _latest_budget_year(client: Client) -> str | None:
    """Most recent fiscal year present in the by-fund figures, or None."""
    year_totals = aggregate_by_year(get_budget_line_items(client, dimension="fund"))
    return year_totals[-1].fiscal_year if year_totals else None


# Human label per view, for the detail back-link and split heading.
_VIEW_LABEL = {"function": "by function", "fund": "by fund", "source": "by source"}


def _detail_context(client: Client, view: str, key: str) -> dict[str, Any]:
    """A single component's trend over all years + its fund<->function cross-split.

    The split bars drill the *other* direction: a function detail splits by fund
    (and each fund drills to its own detail), a fund detail splits by function.
    """
    latest = _latest_budget_year(client)
    if view == "fund":
        trend = component_trend(
            get_budget_line_items(client, dimension="fund"),
            category="expenditure",
            key="fund",
            value=key,
        )
        split = cross_split(
            get_budget_line_items(client, dimension="function"),
            latest or "",
            match={"category": "expenditure", "fund": key},
            group_key="subcategory",
        )
        split_label, split_view, measure = "by function", "function", "Expenditure"
    elif view == "source":
        trend = component_trend(
            get_budget_line_items(client, dimension="source"),
            category="revenue",
            key="subcategory",
            value=key,
        )
        split, split_label, split_view, measure = [], "", "", "Revenue"
    else:  # function
        function_items = get_budget_line_items(client, dimension="function")
        trend = component_trend(
            function_items, category="expenditure", key="subcategory", value=key
        )
        split = cross_split(
            function_items,
            latest or "",
            match={"category": "expenditure", "subcategory": key},
            group_key="fund",
        )
        split_label, split_view, measure = "by fund", "fund", "Expenditure"

    span = f"{trend[0].fiscal_year} to {trend[-1].fiscal_year}" if trend else ""
    return {
        "view": view,
        "key": key,
        "measure": measure,
        "back_label": _VIEW_LABEL[view],
        "trend_svg": trend_svg(trend),
        "rows": trend,
        "split": split,
        "split_label": split_label,
        "split_view": split_view,
        "split_year": latest,
        "span": span,
    }


@jurisdiction.get("/budget", response_class=HTMLResponse)
async def budget(request: Request, view: EntityView = Depends(resolve_entity)) -> HTMLResponse:
    """First-class Budget page: charts from budget_line_items plus cited quotes.

    The budget_line_items figures are not yet entity-scoped (one body has budget
    data today); the cited-quote sections are. Scope the figures in Phase C when
    a second body carries budget data.
    """
    client = _get_db()
    # By-fund rows drive the time-series chart and the figures table; the
    # breakdown panel below the chart switches between fund/function/source.
    line_items = get_budget_line_items(client, dimension="fund")
    year_totals = aggregate_by_year(line_items)
    latest_year = year_totals[-1].fiscal_year if year_totals else None
    breakdown = _breakdown_context(client, _DEFAULT_BREAKDOWN_VIEW, latest_year)
    budget_items = get_budget_line_items(client, dimension="budget")
    budget_actual = budget_vs_actual(budget_items, latest_year) if latest_year else []

    return templates.TemplateResponse(
        request,
        "budget.html",
        _page(
            view,
            active="topic-budget",
            line_items=line_items,
            year_totals=year_totals,
            latest_year=latest_year,
            chart_svg=revenue_expenditure_svg(year_totals),
            budget_actual=budget_actual,
            sections=_budget_quote_sections(view.entity["id"]),
            **breakdown,
        ),
    )


@jurisdiction.get("/budget/breakdown", response_class=HTMLResponse)
async def budget_breakdown(
    request: Request,
    view: EntityView = Depends(resolve_entity),
    breakdown_view: str = Query(_DEFAULT_BREAKDOWN_VIEW, alias="view"),
) -> HTMLResponse:
    """HTMX partial: the breakdown bars for one view of the latest fiscal year."""
    if breakdown_view not in _BREAKDOWN_VIEWS:
        breakdown_view = _DEFAULT_BREAKDOWN_VIEW
    client = _get_db()
    breakdown = _breakdown_context(client, breakdown_view, _latest_budget_year(client))
    return templates.TemplateResponse(request, "_budget_breakdown.html", _page(view, **breakdown))


@jurisdiction.get("/budget/detail", response_class=HTMLResponse)
async def budget_detail(
    request: Request,
    view: EntityView = Depends(resolve_entity),
    breakdown_view: str = Query(_DEFAULT_BREAKDOWN_VIEW, alias="view"),
    key: str = "",
) -> HTMLResponse:
    """HTMX partial: one component's multi-year trend + fund<->function cross-split."""
    if breakdown_view not in _BREAKDOWN_VIEWS:
        breakdown_view = _DEFAULT_BREAKDOWN_VIEW
    client = _get_db()
    return templates.TemplateResponse(
        request, "_budget_detail.html", _page(view, **_detail_context(client, breakdown_view, key))
    )


@jurisdiction.get("/facilities-plan", response_class=HTMLResponse)
async def facilities_plan(
    request: Request, view: EntityView = Depends(resolve_entity)
) -> HTMLResponse:
    """Long-Range Facilities Master Plan topic: the plan's source documents plus
    cited quotes drawn from the record.

    No charts — the LRFMP is a narrative plan, not a figures table — so the page
    is sourced quotes (led by the plan's own self-definition) plus the plan's
    documents.
    """
    client = _get_db()
    entity_id = view.entity["id"]
    # Curated source set: the master-plan volumes (by type) plus the board
    # presentation, which classifies as 'presentation' and so is caught by its
    # filename instead. Merge, keeping volumes first and dropping duplicates.
    volumes = list_documents(client, entity_id, document_type="facilities_plan")
    presentations = list_documents(client, entity_id, source_file_like="%LRFMP%")
    seen = {d["id"] for d in volumes}
    documents = volumes + [d for d in presentations if d["id"] not in seen]

    return templates.TemplateResponse(
        request,
        "facilities_plan.html",
        _page(
            view,
            active="topic-facilities",
            documents=documents,
            sections=_facilities_quote_sections(entity_id),
        ),
    )


@app.get("/budget", response_class=HTMLResponse)
async def budget_redirect(request: Request) -> RedirectResponse:
    """Legacy flat /budget (and old /budget/* partials) -> canonical body."""
    return _redirect_to_default("/budget", request)


@app.get("/topic/budget")
async def topic_budget_redirect(request: Request) -> RedirectResponse:
    """Legacy path -> canonical body's budget page."""
    return _redirect_to_default("/budget", request)


@app.get("/facilities-plan", response_class=HTMLResponse)
async def facilities_plan_redirect(request: Request) -> RedirectResponse:
    """Legacy flat /facilities-plan -> canonical body."""
    return _redirect_to_default("/facilities-plan", request)


@app.get("/topic/facilities-plan")
async def topic_facilities_redirect(request: Request) -> RedirectResponse:
    """Legacy path -> canonical body's facilities-plan page."""
    return _redirect_to_default("/facilities-plan", request)


# Document and chunk pages stay flat (IDs are globally unique, reached only via
# their body's results). They resolve their body from the document so the page
# chrome (sidebar, top-bar) renders under the right jurisdiction.
@app.get("/document/{doc_id}", response_class=HTMLResponse)
async def document_view(request: Request, doc_id: int) -> HTMLResponse:
    """Full document view."""
    client = _get_db()
    doc = get_document(client, doc_id)
    if not doc:
        raise HTTPException(status_code=404, detail="Document not found")

    view = _entity_view_for_document(client, doc)
    return templates.TemplateResponse(request, "document.html", _page(view, document=doc))


@app.get("/document/{doc_id}/pane", response_class=HTMLResponse)
async def document_pane(request: Request, doc_id: int) -> HTMLResponse:
    """Document-level reader pane (summary + original embed) for browse clicks.

    Unlike the chunk source pane, there is no cited passage — browse opens a
    whole document, not a search hit.
    """
    client = _get_db()
    doc = get_document(client, doc_id)
    if not doc:
        raise HTTPException(status_code=404, detail="Document not found")
    view = _entity_view_for_document(client, doc)
    return templates.TemplateResponse(request, "partials/doc_pane.html", _page(view, document=doc))


@app.get("/chunk/{chunk_id}/source", response_class=HTMLResponse)
async def chunk_source(request: Request, chunk_id: int, embed: int = 0) -> HTMLResponse:
    """Citation context: the chunk plus surrounding chunks.

    If `embed=1`, returns a partial suitable for injection into the search
    reader pane via HTMX. Otherwise returns the full page.
    """
    client = _get_db()
    context = get_chunk_with_context(client, chunk_id, context_count=2)
    if not context["chunk"]:
        raise HTTPException(status_code=404, detail="Chunk not found")

    doc = get_document(client, context["chunk"]["document_id"])
    view = _entity_view_for_document(client, doc)
    template = "partials/reader_pane.html" if embed else "chunk_source.html"

    return templates.TemplateResponse(
        request,
        template,
        _page(view, chunk=context["chunk"], context=context["context"], document=doc),
    )


@app.get("/chunk/{chunk_id}/source-pane", response_class=HTMLResponse)
async def chunk_source_pane(request: Request, chunk_id: int, q: str = "") -> HTMLResponse:
    """Return the source pane only (native-format document view) for a chunk.

    Called by HTMX when the user clicks a search result. Fetches the
    chunk and two neighbors for in-context display. Per-card AI
    summaries (doc summary + match summary) are generated separately
    on the result cards, not here.
    """
    client = _get_db()
    ctx = get_chunk_with_context(client, chunk_id, context_count=2)
    if not ctx["chunk"]:
        raise HTTPException(status_code=404, detail="Chunk not found")

    doc = get_document(client, ctx["chunk"]["document_id"])
    view = _entity_view_for_document(client, doc)
    target_hash = chunk_hash_id(chunk_id)

    return templates.TemplateResponse(
        request,
        "partials/source_pane.html",
        _page(
            view,
            chunk=ctx["chunk"],
            context=ctx["context"],
            document=doc,
            query=q,
            target_hash=target_hash,
        ),
    )


@jurisdiction.get("/methodology", response_class=HTMLResponse)
async def methodology(request: Request, view: EntityView = Depends(resolve_entity)) -> HTMLResponse:
    """How the system works — transparency page."""
    return templates.TemplateResponse(
        request, "methodology.html", _page(view, active="methodology")
    )


@app.get("/methodology", response_class=HTMLResponse)
async def methodology_redirect(request: Request) -> RedirectResponse:
    """Legacy flat /methodology -> canonical body."""
    return _redirect_to_default("/methodology", request)


@app.post("/report-error", response_class=HTMLResponse)
async def report_error(
    request: Request,
    chunk_id: int = Form(...),
    description: str = Form(...),
    email: str = Form(""),
) -> HTMLResponse:
    """Submit a correction report for a chunk."""
    if not description.strip():
        raise HTTPException(status_code=400, detail="Description is required")

    client = _get_db()
    correction = Correction(
        chunk_id=chunk_id,
        description=description.strip(),
        reporter_email=email.strip(),
    )
    insert_correction(client, correction)

    return templates.TemplateResponse(request, "partials/error_submitted.html")


@jurisdiction.post("/summarize", response_class=HTMLResponse)
async def summarize(
    request: Request,
    view: EntityView = Depends(resolve_entity),
    q: str = Form(""),
    date_from: str = Form(""),
    date_to: str = Form(""),
    doc_type: str = Form(""),
) -> HTMLResponse:
    """Generate a citation-backed LLM summary for a search query.

    Called via HTMX after search results render. Returns a partial
    with the summary text and inline citation links.
    """
    cfg = _get_config()
    if not q.strip() or not cfg.openai_api_key:
        return templates.TemplateResponse(
            request,
            "partials/summary.html",
            {"summary": None},
        )

    filters = SearchFilters(
        date_from=date.fromisoformat(date_from) if date_from else None,
        date_to=date.fromisoformat(date_to) if date_to else None,
        document_type=doc_type or None,
        entity_id=view.entity["id"],
    )

    client = _get_db()
    try:
        query_embedding = _embed_query(q)
        # Finance figure queries are answered from the structured budget_line_items
        # table (each figure citeable to its source chunk); everything else falls
        # back to text retrieval. See actalux.search.answer.assemble_evidence.
        enriched, route = assemble_evidence(
            client, q, query_embedding, filters=filters, reranker=_reranker(), max_results=10
        )
        logger.info("summarize route=%s for query: %s", route, q)
        summary = generate_summary(q, enriched, cfg.openai_api_key, cfg.summary_model)
    except (SearchError, SummaryError):
        logger.exception("Summary generation failed for: %s", q)
        return templates.TemplateResponse(
            request,
            "partials/summary.html",
            {"summary": None},
        )

    # Convert citation IDs to clickable links
    summary_html = _render_citation_links(summary.text, enriched)

    return templates.TemplateResponse(
        request,
        "partials/summary.html",
        {"summary": summary, "summary_html": summary_html},
    )


# --- Helpers ---


def _render_citation_links(text: str, results: list[dict[str, Any]]) -> str:
    """Replace [#qXXXX] citations with HTML links to chunk source pages."""
    import re

    # Build a lookup from hash_id to chunk_id
    id_map: dict[str, int] = {}
    for r in results:
        id_map[r["hash_id"]] = r["chunk_id"]

    parts: list[str] = []
    last = 0

    def replace_citation(match: re.Match[str]) -> str:
        hash_id = match.group(1)
        chunk_id = id_map.get(hash_id)
        if chunk_id is not None:
            return f'<a href="/chunk/{chunk_id}/source" class="source-link">[{hash_id}]</a>'
        return str(escape(match.group(0)))

    for match in re.finditer(r"\[(#q[0-9a-f]{4,})\]", text):
        parts.append(str(escape(text[last : match.start()])))
        parts.append(replace_citation(match))
        last = match.end()
    parts.append(str(escape(text[last:])))
    return "".join(parts)


# Registered after the flat routes so /document/{id} and /topic/budget win the
# match; with one body the place hub just redirects to it.
@app.get("/{state}/{place}")
async def place_hub(state: str, place: str) -> RedirectResponse:
    """Place hub: redirect to the (currently only) body; a directory in Phase C."""
    client = _get_db()
    bodies = [
        e
        for e in list_entities(client)
        if (e.get("place") or {}).get("state") == state
        and (e.get("place") or {}).get("slug") == place
    ]
    if not bodies:
        raise HTTPException(status_code=404, detail="Unknown place")
    return RedirectResponse(f"/{state}/{place}/{bodies[0]['body_slug']}", status_code=307)


# The JSON API lives under the literal /api/v1 prefix; include it before the
# jurisdiction router so its specific routes are matched first.
app.include_router(api_router)

# Include the jurisdiction router LAST so its greedy /{state}/{place}/{body}
# prefix is matched only after the specific flat routes above.
app.include_router(jurisdiction)
