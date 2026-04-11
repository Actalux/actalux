"""Hybrid search combining semantic (pgvector) and keyword (FTS) retrieval.

Uses Reciprocal Rank Fusion (RRF) to combine rankings from both sources.
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from datetime import date
from typing import Any

from supabase import Client

from actalux.errors import SearchError

logger = logging.getLogger(__name__)

RRF_K = 60
SEMANTIC_CANDIDATES = 50
KEYWORD_CANDIDATES = 50
MAX_RESULTS = 20
SIMILARITY_THRESHOLD = 0.35


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
) -> list[SearchResult]:
    """Run hybrid search: semantic + keyword, combined with RRF.

    Both retrieval paths run independently, then results are merged
    using reciprocal rank fusion (k=60).
    """
    if not query.strip():
        return []

    f = filters or SearchFilters()
    start = time.monotonic()

    semantic_rows = _semantic_search(client, query_embedding, f)
    keyword_rows = _keyword_search(client, query, f)

    results = _reciprocal_rank_fusion(semantic_rows, keyword_rows, max_results)

    elapsed_ms = (time.monotonic() - start) * 1000
    logger.info(
        "Hybrid search: %d semantic + %d keyword -> %d results (%.0fms)",
        len(semantic_rows),
        len(keyword_rows),
        len(results),
        elapsed_ms,
    )
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


def _keyword_search(
    client: Client,
    query: str,
    filters: SearchFilters,
) -> list[dict[str, Any]]:
    """Run keyword search via Supabase RPC."""
    params: dict[str, Any] = {
        "search_query": query,
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
