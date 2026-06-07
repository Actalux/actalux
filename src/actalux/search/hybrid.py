"""Hybrid search combining semantic (pgvector) and keyword (FTS) retrieval.

Uses Reciprocal Rank Fusion (RRF) to combine rankings from both sources.
"""

from __future__ import annotations

import logging
import re
import time
from collections.abc import Callable
from dataclasses import dataclass
from datetime import date
from typing import Any

from supabase import Client

from actalux.errors import RerankError, SearchError

logger = logging.getLogger(__name__)

RRF_K = 60
# Hyphen-like characters normalized out of FTS queries (see _normalize_fts_query):
# ASCII hyphen-minus, Unicode hyphen, non-breaking hyphen, figure dash, en/em dash.
_HYPHENS_RE = re.compile(r"[-‐‑‒–—]")
SEMANTIC_CANDIDATES = 50
KEYWORD_CANDIDATES = 50
MAX_RESULTS = 20
RERANK_POOL_SIZE = 50  # RRF candidates reranked before truncating to max_results
SIMILARITY_THRESHOLD = 0.35

# Reorders an RRF candidate pool for a query. Raises RerankError on failure so
# the search boundary can fall back to RRF order (a reranker outage must never
# break search). Built by the web layer from config; None = RRF-only.
Reranker = Callable[[str, list["SearchResult"]], list["SearchResult"]]


@dataclass(frozen=True)
class SearchResult:
    """A single search result with combined score and metadata."""

    chunk_id: int
    document_id: int
    content: str
    section: str
    speaker: str
    rrf_score: float
    semantic_rank: int | None = None
    keyword_rank: int | None = None


@dataclass(frozen=True)
class SearchFilters:
    """Optional filters applied before retrieval."""

    date_from: date | None = None
    date_to: date | None = None
    document_type: str | None = None


def hybrid_search(
    client: Client,
    query: str,
    query_embedding: list[float],
    filters: SearchFilters | None = None,
    max_results: int = MAX_RESULTS,
    *,
    reranker: Reranker | None = None,
    rerank_pool_size: int = RERANK_POOL_SIZE,
) -> list[SearchResult]:
    """Run hybrid search: semantic + keyword, combined with RRF.

    Both retrieval paths run independently, then results are merged using
    reciprocal rank fusion (k=60). When `reranker` is given, a deeper pool is
    fused (so the cross-encoder can lift a buried-but-relevant hit) and reordered
    before truncating to `max_results`; a reranker failure falls back to RRF order.
    """
    if not query.strip():
        return []

    f = filters or SearchFilters()
    start = time.monotonic()

    semantic_rows = _semantic_search(client, query_embedding, f)
    keyword_rows = _keyword_search(client, query, f)

    fuse_count = max(max_results, rerank_pool_size) if reranker is not None else max_results
    results = _reciprocal_rank_fusion(semantic_rows, keyword_rows, fuse_count)

    reranked = False
    if reranker is not None and results:
        results = _apply_reranker(reranker, query, results)
        reranked = True
    results = results[:max_results]

    elapsed_ms = (time.monotonic() - start) * 1000
    logger.info(
        "Hybrid search: %d semantic + %d keyword -> %d results (rerank=%s, %.0fms)",
        len(semantic_rows),
        len(keyword_rows),
        len(results),
        reranked,
        elapsed_ms,
    )
    return results


def _apply_reranker(
    reranker: Reranker, query: str, results: list[SearchResult]
) -> list[SearchResult]:
    """Reorder `results` via `reranker`, falling back to RRF order on failure.

    A reranker outage (API down, timeout, ratelimit) must degrade to plain RRF
    search, never raise -- so RerankError is caught here at the boundary.
    """
    try:
        return reranker(query, results)
    except RerankError as exc:
        logger.warning("rerank failed (%s); falling back to RRF order", exc)
        return results


def _semantic_search(
    client: Client,
    embedding: list[float],
    filters: SearchFilters,
) -> list[dict[str, Any]]:
    """Run semantic search via Supabase RPC."""
    params: dict[str, Any] = {
        "query_embedding": embedding,
        "match_threshold": SIMILARITY_THRESHOLD,
        "match_count": SEMANTIC_CANDIDATES,
    }
    if filters.date_from:
        params["filter_date_from"] = filters.date_from.isoformat()
    if filters.date_to:
        params["filter_date_to"] = filters.date_to.isoformat()
    if filters.document_type:
        params["filter_doc_type"] = filters.document_type

    try:
        result = client.rpc("semantic_search", params).execute()
        return result.data or []
    except Exception as exc:
        raise SearchError(f"Semantic search failed: {exc}") from exc


def _normalize_fts_query(query: str) -> str:
    """Replace hyphens with spaces so FTS doesn't demand a compound token.

    `websearch_to_tsquery('english', 'per-pupil ...')` turns a hyphenated term
    into a phrase query over the *compound* lexeme (`per-pupil <-> per <-> pupil`),
    which only matches documents whose text is hyphenated the same way. Our OCR'd
    sources mostly write the unhyphenated form ("Expenditures Per Pupil"), so the
    hyphenated query matched nothing: `keyword('per-pupil expenditure by building')`
    returned 0 rows, while the de-hyphenated form returned 18 (answer at rank 4).
    De-hyphenating is strictly more permissive — a hyphenated document still emits
    the split lexemes — so this only adds matches, never removes them.
    """
    return _HYPHENS_RE.sub(" ", query)


def _keyword_search(
    client: Client,
    query: str,
    filters: SearchFilters,
) -> list[dict[str, Any]]:
    """Run keyword search via Supabase RPC."""
    params: dict[str, Any] = {
        "search_query": _normalize_fts_query(query),
        "match_count": KEYWORD_CANDIDATES,
    }
    if filters.date_from:
        params["filter_date_from"] = filters.date_from.isoformat()
    if filters.date_to:
        params["filter_date_to"] = filters.date_to.isoformat()
    if filters.document_type:
        params["filter_doc_type"] = filters.document_type

    try:
        result = client.rpc("keyword_search", params).execute()
        return result.data or []
    except Exception as exc:
        raise SearchError(f"Keyword search failed: {exc}") from exc


def _reciprocal_rank_fusion(
    semantic_rows: list[dict[str, Any]],
    keyword_rows: list[dict[str, Any]],
    max_results: int,
) -> list[SearchResult]:
    """Combine two ranked lists using RRF (k=60).

    RRF score = sum over lists of 1 / (k + rank).
    Higher is better.
    """
    # Build score map: chunk_id -> (rrf_score, semantic_rank, keyword_rank, row_data)
    scores: dict[int, dict[str, Any]] = {}

    for rank, row in enumerate(semantic_rows, start=1):
        cid = row["chunk_id"]
        if cid not in scores:
            scores[cid] = {
                "row": row,
                "rrf_score": 0.0,
                "semantic_rank": None,
                "keyword_rank": None,
            }
        scores[cid]["rrf_score"] += 1.0 / (RRF_K + rank)
        scores[cid]["semantic_rank"] = rank

    for rank, row in enumerate(keyword_rows, start=1):
        cid = row["chunk_id"]
        if cid not in scores:
            scores[cid] = {
                "row": row,
                "rrf_score": 0.0,
                "semantic_rank": None,
                "keyword_rank": None,
            }
        scores[cid]["rrf_score"] += 1.0 / (RRF_K + rank)
        scores[cid]["keyword_rank"] = rank

    # Sort by RRF score descending
    ranked = sorted(scores.items(), key=lambda x: x[1]["rrf_score"], reverse=True)

    results: list[SearchResult] = []
    for chunk_id, info in ranked[:max_results]:
        row = info["row"]
        results.append(
            SearchResult(
                chunk_id=chunk_id,
                document_id=row["document_id"],
                content=row["content"],
                section=row["section"],
                speaker=row["speaker"],
                rrf_score=info["rrf_score"],
                semantic_rank=info["semantic_rank"],
                keyword_rank=info["keyword_rank"],
            )
        )

    return results
