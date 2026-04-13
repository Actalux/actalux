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
from datetime import date
from pathlib import Path
from typing import Any

from fastapi import FastAPI, Form, HTTPException, Request
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from markupsafe import Markup, escape

from actalux.config import Config, load_config
from actalux.db import get_chunk_with_context, get_client, get_document, insert_correction
from actalux.errors import SearchError, SummaryError
from actalux.ingest.embedder import load_model
from actalux.models import Correction
from actalux.search.hybrid import SearchFilters, hybrid_search
from actalux.search.summarize import generate_summary

logger = logging.getLogger(__name__)

TEMPLATE_DIR = Path(__file__).parent / "templates"
STATIC_DIR = Path(__file__).parent / "static"

app = FastAPI(title="Actalux", version="0.1.0")
app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")
templates = Jinja2Templates(directory=str(TEMPLATE_DIR))


def _window_snippet(content: str, query: str, width: int = 160) -> Markup:
    """Return an HTML-safe snippet windowed around the first query-term match.

    Produces a preview roughly ``width`` characters long, centered on the
    first occurrence of any query term. Query terms are wrapped in ``<mark>``
    tags. Leading/trailing ellipses are added when the window is truncated.
    Falls back to the first ``width`` characters of the content when no
    query terms match.
    """
    cleaned = re.sub(r"\s+", " ", (content or "").strip())
    if not cleaned:
        return Markup("")

    terms = [t for t in re.findall(r"[A-Za-z0-9]{3,}", (query or "").lower())]
    lowered = cleaned.lower()
    match_pos = -1
    for t in terms:
        pos = lowered.find(t)
        if pos != -1 and (match_pos == -1 or pos < match_pos):
            match_pos = pos

    if match_pos == -1:
        # No match — just head-truncate
        snippet = cleaned[:width]
        suffix = "…" if len(cleaned) > width else ""
        return Markup(str(escape(snippet)) + suffix)

    half = width // 2
    start = max(0, match_pos - half)
    end = min(len(cleaned), start + width)
    # Shift start back if we ran out of text on the right
    start = max(0, end - width)
    prefix = "…" if start > 0 else ""
    suffix = "…" if end < len(cleaned) else ""
    window = cleaned[start:end]

    # Wrap every matching term occurrence in <mark> (case-insensitive)
    def _highlight(text: str) -> str:
        if not terms:
            return str(escape(text))
        pattern = re.compile(
            r"(" + "|".join(re.escape(t) for t in terms) + r")",
            re.IGNORECASE,
        )
        parts: list[str] = []
        last = 0
        for m in pattern.finditer(text):
            parts.append(str(escape(text[last : m.start()])))
            parts.append("<mark>")
            parts.append(str(escape(m.group(0))))
            parts.append("</mark>")
            last = m.end()
        parts.append(str(escape(text[last:])))
        return "".join(parts)

    return Markup(prefix + _highlight(window) + suffix)


templates.env.filters["window_snippet"] = _window_snippet

# In-process cache for topic page queries (1-hour TTL)
_topic_cache: dict[str, tuple[float, list[Any]]] = {}
TOPIC_CACHE_TTL = 3600  # seconds


def _get_config() -> Config:
    """Load config (cached by load_config)."""
    return load_config()


def _get_db():
    """Get the Supabase client."""
    cfg = _get_config()
    return get_client(cfg.supabase_url, cfg.supabase_key)


def _embed_query(query: str) -> list[float]:
    """Embed a search query using the same model as ingest."""
    cfg = _get_config()
    model = load_model(cfg.embedding_model)
    vector = model.encode(query, normalize_embeddings=True)
    return vector.tolist()


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


# --- Budget topic preset queries ---

BUDGET_QUERIES = [
    "budget approval spending fiscal year",
    "tax levy revenue property tax",
    "salary compensation benefits",
    "capital improvement facilities construction",
    "fund balance reserves financial",
]


# --- Routes ---


@app.get("/", response_class=HTMLResponse)
async def home(request: Request) -> HTMLResponse:
    """Landing page with search box."""
    return templates.TemplateResponse(request, "home.html")


def _run_search(
    request: Request,
    q: str,
    date_from: str,
    date_to: str,
    doc_type: str,
) -> HTMLResponse:
    """Shared search handler for GET and POST routes."""
    is_htmx = bool(request.headers.get("HX-Request"))
    if not q.strip():
        template = "partials/search_results.html" if is_htmx else "search.html"
        return templates.TemplateResponse(
            request, template, {"results": [], "query": ""}
        )

    filters = SearchFilters(
        date_from=date.fromisoformat(date_from) if date_from else None,
        date_to=date.fromisoformat(date_to) if date_to else None,
        document_type=doc_type or None,
    )

    client = _get_db()
    try:
        query_embedding = _embed_query(q)
        results = hybrid_search(client, q, query_embedding, filters)
    except SearchError:
        logger.exception("Search failed for query: %s", q)
        results = []

    enriched = _enrich_results(client, results)
    template = "partials/search_results.html" if is_htmx else "search.html"

    return templates.TemplateResponse(
        request, template, {"results": enriched, "query": q}
    )


@app.get("/search", response_class=HTMLResponse)
async def search_get(
    request: Request,
    q: str = "",
    date_from: str = "",
    date_to: str = "",
    doc_type: str = "",
) -> HTMLResponse:
    """GET variant for linkable / restorable search URLs."""
    return _run_search(request, q, date_from, date_to, doc_type)


@app.post("/search", response_class=HTMLResponse)
async def search_post(
    request: Request,
    q: str = Form(""),
    date_from: str = Form(""),
    date_to: str = Form(""),
    doc_type: str = Form(""),
) -> HTMLResponse:
    """POST from the search form (works with or without HTMX)."""
    return _run_search(request, q, date_from, date_to, doc_type)


@app.get("/topic/budget", response_class=HTMLResponse)
async def topic_budget(request: Request) -> HTMLResponse:
    """Budget topic page with preset queries and cached results."""
    cached = _get_cached_topic("budget")
    if cached is not None:
        return templates.TemplateResponse(
            request,
            "topic_budget.html",
            {"sections": cached, "active": "topic-budget"},
        )

    client = _get_db()
    sections: list[dict[str, Any]] = []

    for query_text in BUDGET_QUERIES:
        try:
            query_embedding = _embed_query(query_text)
            results = hybrid_search(client, query_text, query_embedding, max_results=5)
            enriched = _enrich_results(client, results)
            sections.append({"query": query_text, "results": enriched})
        except SearchError:
            logger.exception("Budget topic query failed: %s", query_text)
            sections.append({"query": query_text, "results": []})

    _set_cached_topic("budget", sections)

    return templates.TemplateResponse(
        request,
        "topic_budget.html",
        {"sections": sections, "active": "topic-budget"},
    )


@app.get("/document/{doc_id}", response_class=HTMLResponse)
async def document_view(request: Request, doc_id: int) -> HTMLResponse:
    """Full document view."""
    client = _get_db()
    doc = get_document(client, doc_id)
    if not doc:
        raise HTTPException(status_code=404, detail="Document not found")

    return templates.TemplateResponse(
        request,
        "document.html",
        {"document": doc},
    )


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
    template = "partials/reader_pane.html" if embed else "chunk_source.html"

    return templates.TemplateResponse(
        request,
        template,
        {"chunk": context["chunk"], "context": context["context"], "document": doc},
    )


# In-process LRU cache for per-result summaries. Keyed by (chunk_id,
# normalized_query). Keeps the Claude cost down when users click around
# the same search. 500 entries is ~500 clicks before eviction; effectively
# unlimited for a single-instance server. Upgrade to a DB cache if we go
# multi-instance.
_SUMMARY_CACHE: dict[tuple[int, str], str] = {}
_SUMMARY_CACHE_MAX = 500


def _summary_cache_key(chunk_id: int, query: str) -> tuple[int, str]:
    normalized = " ".join((query or "").lower().split())
    return (chunk_id, normalized)


def _enriched_context(client: Any, chunks: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Turn raw chunk rows into the dict shape generate_summary expects."""
    enriched: list[dict[str, Any]] = []
    doc_cache: dict[int, dict[str, Any]] = {}
    for ch in chunks:
        doc_id = ch["document_id"]
        if doc_id not in doc_cache:
            doc_cache[doc_id] = get_document(client, doc_id) or {}
        doc = doc_cache[doc_id]
        hash_id = f"#q{ch['id']:04x}"[-6:]
        enriched.append(
            {
                "chunk_id": ch["id"],
                "hash_id": hash_id,
                "content": ch["content"],
                "section": ch.get("section", ""),
                "speaker": ch.get("speaker", ""),
                "meeting_date": doc.get("meeting_date", ""),
                "meeting_title": doc.get("meeting_title", ""),
                "document_id": doc_id,
                "document_type": doc.get("document_type", ""),
            }
        )
    return enriched


@app.get("/chunk/{chunk_id}/reader", response_class=HTMLResponse)
async def chunk_reader(
    request: Request, chunk_id: int, q: str = ""
) -> HTMLResponse:
    """Return the reader twin (summary pane + source pane) for a chunk.

    Called by HTMX when the user clicks a search result. Fetches the
    chunk and two neighbors from the same document, generates a
    citation-backed summary (cached), and renders both panes together.
    """
    client = _get_db()
    ctx = get_chunk_with_context(client, chunk_id, context_count=2)
    if not ctx["chunk"]:
        raise HTTPException(status_code=404, detail="Chunk not found")

    doc = get_document(client, ctx["chunk"]["document_id"])
    enriched_results = _enriched_context(client, ctx["context"])
    target_hash = f"#q{chunk_id:04x}"[-6:]

    # Generate or load cached summary (only when we have a query to summarize
    # against). Without a query, skip summarization — the pane shows a
    # "summary available when searching" placeholder.
    summary = None
    summary_html: str | None = None
    cache_hit = False
    cfg = _get_config()
    if q.strip() and cfg.openai_api_key:
        cache_key = _summary_cache_key(chunk_id, q)
        cached = _SUMMARY_CACHE.get(cache_key)
        if cached is not None:
            summary_html = cached
            cache_hit = True
        else:
            try:
                summary = generate_summary(
                    q, enriched_results, cfg.openai_api_key, cfg.summary_model
                )
                rendered = _render_citation_links(summary.text, enriched_results)
                summary_html = rendered
                # Evict oldest if over cap (dict preserves insertion order in py3.7+)
                if len(_SUMMARY_CACHE) >= _SUMMARY_CACHE_MAX:
                    _SUMMARY_CACHE.pop(next(iter(_SUMMARY_CACHE)))
                _SUMMARY_CACHE[cache_key] = rendered
            except SummaryError:
                logger.exception("Summary generation failed for chunk %d q=%r", chunk_id, q)
                summary_html = None

    return templates.TemplateResponse(
        request,
        "partials/reader_twin.html",
        {
            "chunk": ctx["chunk"],
            "context": ctx["context"],
            "document": doc,
            "query": q,
            "target_hash": target_hash,
            "summary": summary,
            "summary_html": summary_html,
            "cache_hit": cache_hit,
        },
    )


@app.get("/methodology", response_class=HTMLResponse)
async def methodology(request: Request) -> HTMLResponse:
    """How the system works — transparency page."""
    return templates.TemplateResponse(request, "methodology.html", {"active": "methodology"})


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


@app.post("/summarize", response_class=HTMLResponse)
async def summarize(
    request: Request,
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
    )

    client = _get_db()
    try:
        query_embedding = _embed_query(q)
        results = hybrid_search(client, q, query_embedding, filters, max_results=10)
        enriched = _enrich_results(client, results)
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

    def replace_citation(match: re.Match[str]) -> str:
        hash_id = match.group(1)
        chunk_id = id_map.get(hash_id)
        if chunk_id is not None:
            return f'<a href="/chunk/{chunk_id}/source" class="source-link">[{hash_id}]</a>'
        return match.group(0)

    return re.sub(r"\[(#q[0-9a-f]{4,5})\]", replace_citation, text)


def _enrich_results(client: Any, results: list[Any]) -> list[dict[str, Any]]:
    """Add document metadata (meeting_date, meeting_title) to search results."""
    doc_cache: dict[int, dict[str, Any]] = {}
    enriched: list[dict[str, Any]] = []

    for r in results:
        doc_id = r.document_id
        if doc_id not in doc_cache:
            doc_cache[doc_id] = get_document(client, doc_id) or {}

        doc = doc_cache[doc_id]
        chunk_hash = f"#q{r.chunk_id:04x}"[-6:]
        enriched.append(
            {
                "chunk_id": r.chunk_id,
                "hash_id": chunk_hash,
                "content": r.content,
                "section": r.section,
                "speaker": r.speaker,
                "rrf_score": round(r.rrf_score, 4),
                "meeting_date": doc.get("meeting_date", ""),
                "meeting_title": doc.get("meeting_title", ""),
                "document_id": doc_id,
                "document_type": doc.get("document_type", ""),
            }
        )

    return enriched
