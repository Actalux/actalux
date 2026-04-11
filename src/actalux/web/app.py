"""FastAPI application for Actalux.

Endpoints:
  POST /search          — hybrid search with RRF
  GET  /topic/budget    — preset budget topic page
  GET  /document/{id}   — full document view
  GET  /chunk/{id}/source — citation context (chunk + neighbors)
  GET  /methodology     — how the system works
  POST /report-error    — submit a correction
"""

from __future__ import annotations

import logging
import time
from datetime import date
from pathlib import Path
from typing import Any

from fastapi import FastAPI, Form, HTTPException, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

from actalux.config import Config, load_config
from actalux.db import get_chunk_with_context, get_client, get_document, insert_correction
from actalux.errors import SearchError
from actalux.ingest.embedder import load_model
from actalux.models import Correction
from actalux.search.hybrid import SearchFilters, hybrid_search

logger = logging.getLogger(__name__)

TEMPLATE_DIR = Path(__file__).parent / "templates"
STATIC_DIR = Path(__file__).parent / "static"

app = FastAPI(title="Actalux", version="0.1.0")
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


@app.post("/search", response_class=HTMLResponse)
async def search(
    request: Request,
    q: str = Form(""),
    date_from: str = Form(""),
    date_to: str = Form(""),
    doc_type: str = Form(""),
) -> HTMLResponse:
    """Hybrid search endpoint. Returns HTMX partial or full page."""
    if not q.strip():
        return templates.TemplateResponse(
            request,
            "partials/search_results.html",
            {"results": [], "query": ""},
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

    # Fetch meeting metadata for each result
    enriched = _enrich_results(client, results)

    template = "partials/search_results.html"
    if not request.headers.get("HX-Request"):
        template = "search.html"

    return templates.TemplateResponse(
        request,
        template,
        {"results": enriched, "query": q},
    )


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
async def chunk_source(request: Request, chunk_id: int) -> HTMLResponse:
    """Citation context: the chunk plus surrounding chunks."""
    client = _get_db()
    context = get_chunk_with_context(client, chunk_id, context_count=2)
    if not context["chunk"]:
        raise HTTPException(status_code=404, detail="Chunk not found")

    # Get the parent document for metadata
    doc = get_document(client, context["chunk"]["document_id"])

    return templates.TemplateResponse(
        request,
        "chunk_source.html",
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


# --- Helpers ---


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
