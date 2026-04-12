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
import time
from datetime import date
from pathlib import Path
from typing import Any

from fastapi import FastAPI, Form, HTTPException, Request
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

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
            {"sections": cached},
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
        {"sections": sections},
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


@app.get("/methodology", response_class=HTMLResponse)
async def methodology(request: Request) -> HTMLResponse:
    """How the system works — transparency page."""
    return templates.TemplateResponse(request, "methodology.html")


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
    if not q.strip() or not cfg.anthropic_api_key:
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
        summary = generate_summary(q, enriched, cfg.anthropic_api_key)
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
