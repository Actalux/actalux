"""Hybrid search combining semantic (pgvector) and keyword (FTS) retrieval.

Uses Reciprocal Rank Fusion (RRF) to combine rankings from both sources.
"""

from __future__ import annotations

import logging
import re
import time
from collections.abc import Callable
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass
from datetime import date
from typing import Any

from supabase import Client

from actalux.errors import RerankError, SearchError

logger = logging.getLogger(__name__)

RRF_K = 60
# Upper bound on concurrent retrieval round-trips when query expansion fans out
# (one semantic + one keyword RPC per variant). Caps load on the Supabase RPC.
_MAX_SEARCH_WORKERS = 8
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
    # Scope to one public body (places/entities model). None = no scope, which
    # for a single-entity corpus is identical to scoping to it.
    entity_id: int | None = None


def hybrid_search(
    client: Client,
    query: str,
    query_embedding: list[float],
    filters: SearchFilters | None = None,
    max_results: int = MAX_RESULTS,
    *,
    reranker: Reranker | None = None,
    rerank_pool_size: int = RERANK_POOL_SIZE,
    expansions: list[tuple[str, list[float]]] | None = None,
) -> list[SearchResult]:
    """Run hybrid search: semantic + keyword, combined with RRF.

    Both retrieval paths run independently, then results are merged using
    reciprocal rank fusion (k=60). When `reranker` is given, a deeper pool is
    fused (so the cross-encoder can lift a buried-but-relevant hit) and reordered
    before truncating to `max_results`; a reranker failure falls back to RRF order.

    `expansions` are query-expansion variants — ``(phrasing, embedding)`` pairs
    built upstream (the LLM hop + embedding live in the web layer, keeping this a
    pure leaf module). When present, each variant contributes its own semantic and
    keyword candidate lists, all fused together, so a record phrased unlike the
    user's query ("Proposition O" vs "bond measure") can still surface. Variant
    searches run concurrently and are best-effort — a failed one is dropped, never
    fatal; reranking always uses the user's original `query`.
    """
    if not query.strip():
        return []

    f = filters or SearchFilters()
    start = time.monotonic()

    variants: list[tuple[str, list[float]]] = [(query, query_embedding), *(expansions or [])]
    semantic_lists, keyword_lists = _gather_candidate_lists(client, variants, f)

    fuse_count = max(max_results, rerank_pool_size) if reranker is not None else max_results
    results = _fuse_ranked_lists(semantic_lists, keyword_lists, fuse_count)

    reranked = False
    if reranker is not None and results:
        results = _apply_reranker(reranker, query, results)
        reranked = True
    results = results[:max_results]

    elapsed_ms = (time.monotonic() - start) * 1000
    logger.info(
        "Hybrid search: %d variant(s), %d semantic + %d keyword rows -> %d results "
        "(rerank=%s, %.0fms)",
        len(variants),
        sum(len(s) for s in semantic_lists),
        sum(len(k) for k in keyword_lists),
        len(results),
        reranked,
        elapsed_ms,
    )
    return results


def _gather_candidate_lists(
    client: Client,
    variants: list[tuple[str, list[float]]],
    filters: SearchFilters,
) -> tuple[list[list[dict[str, Any]]], list[list[dict[str, Any]]]]:
    """Retrieve semantic + keyword candidate rows for each query variant.

    ``variants[0]`` is the user's query; its retrieval errors propagate (a failed
    primary search is a failed search). Variants 1..n are query-expansion
    phrasings and are best-effort: a failed expansion search is logged and
    dropped, so expansion can only add recall, never break a search that would
    otherwise work. The per-variant searches run concurrently — each is an
    independent blocking Supabase round-trip, so fanning them out bounds the added
    latency to roughly one round-trip rather than N.
    """
    if len(variants) == 1:
        q, emb = variants[0]
        return [_semantic_search(client, emb, filters)], [_keyword_search(client, q, filters)]

    semantic_lists: list[list[dict[str, Any]]] = []
    keyword_lists: list[list[dict[str, Any]]] = []
    with ThreadPoolExecutor(max_workers=min(_MAX_SEARCH_WORKERS, 2 * len(variants))) as pool:
        sem_futures = [pool.submit(_semantic_search, client, emb, filters) for _, emb in variants]
        kw_futures = [pool.submit(_keyword_search, client, q, filters) for q, _ in variants]
        for i, (sem_future, kw_future) in enumerate(zip(sem_futures, kw_futures)):
            is_primary = i == 0
            sem = _resolve_search_future(sem_future, is_primary, i)
            if sem is not None:
                semantic_lists.append(sem)
            kw = _resolve_search_future(kw_future, is_primary, i)
            if kw is not None:
                keyword_lists.append(kw)
    return semantic_lists, keyword_lists


def _resolve_search_future(
    future: Future[list[dict[str, Any]]], is_primary: bool, variant_index: int
) -> list[dict[str, Any]] | None:
    """Read one search future's rows: re-raise for the primary, drop for expansions."""
    try:
        return future.result()
    except SearchError:
        if is_primary:
            raise
        logger.warning(
            "expansion search failed for variant %d; dropping it", variant_index, exc_info=True
        )
        return None


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
    if filters.entity_id is not None:
        params["filter_entity_id"] = filters.entity_id

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
    if filters.entity_id is not None:
        params["filter_entity_id"] = filters.entity_id

    try:
        result = client.rpc("keyword_search", params).execute()
        return result.data or []
    except Exception as exc:
        raise SearchError(f"Keyword search failed: {exc}") from exc


def _accumulate_rrf(
    scores: dict[int, dict[str, Any]],
    rows: list[dict[str, Any]],
    rank_field: str,
) -> None:
    """Add one ranked list's RRF contribution into ``scores`` in place.

    Each row adds 1/(k + rank) to its chunk's score; ``rank_field``
    (``semantic_rank`` / ``keyword_rank``) keeps the best (smallest) rank the
    chunk reached in any list of that kind, for debugging.
    """
    for rank, row in enumerate(rows, start=1):
        cid = row["chunk_id"]
        entry = scores.get(cid)
        if entry is None:
            entry = {"row": row, "rrf_score": 0.0, "semantic_rank": None, "keyword_rank": None}
            scores[cid] = entry
        entry["rrf_score"] += 1.0 / (RRF_K + rank)
        prev = entry[rank_field]
        entry[rank_field] = rank if prev is None else min(prev, rank)


def _fuse_ranked_lists(
    semantic_lists: list[list[dict[str, Any]]],
    keyword_lists: list[list[dict[str, Any]]],
    max_results: int,
) -> list[SearchResult]:
    """Fuse any number of ranked candidate lists with RRF (k=60). Higher is better.

    RRF score = sum over every list of 1 / (k + rank). A chunk found by several
    lists — by both the semantic and keyword paths, or by multiple
    query-expansion variants — accumulates score from each, which is exactly the
    robustness expansion is meant to buy.
    """
    scores: dict[int, dict[str, Any]] = {}
    for rows in semantic_lists:
        _accumulate_rrf(scores, rows, "semantic_rank")
    for rows in keyword_lists:
        _accumulate_rrf(scores, rows, "keyword_rank")

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


def _reciprocal_rank_fusion(
    semantic_rows: list[dict[str, Any]],
    keyword_rows: list[dict[str, Any]],
    max_results: int,
) -> list[SearchResult]:
    """Two-list RRF (semantic + keyword) — the single-variant case.

    Kept as the named entry point the search tests target; delegates to the
    general N-list fuser.
    """
    return _fuse_ranked_lists([semantic_rows], [keyword_rows], max_results)
