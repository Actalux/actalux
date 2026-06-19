"""Shared retrieval primitives for the web layer.

Both the HTML app (``app.py``) and the JSON API (``api.py``) need the same
config, Supabase client, query embedder, and reranker. They live here — a leaf
module that imports neither — so the API can reuse them without importing the
HTML app (which would create a cycle, since the app includes the API router).
"""

from __future__ import annotations

import logging

from actalux.config import Config, load_config
from actalux.db import get_client
from actalux.ingest.embedder import load_model
from actalux.search.hybrid import Reranker
from actalux.search.rerank import rerank_results
from actalux.search.summarize import generate_query_variants

logger = logging.getLogger(__name__)


def get_config() -> Config:
    """Load config (env-backed; cheap to re-read)."""
    return load_config()


def get_db():
    """Supabase client built from config (publishable, RLS-enforced key)."""
    cfg = get_config()
    return get_client(cfg.supabase_url, cfg.supabase_key)


def embed_queries(queries: list[str]) -> list[list[float]]:
    """Embed several queries in one batched model call (same model as ingest)."""
    cfg = get_config()
    model = load_model(cfg.embedding_model)
    vectors = model.encode(queries, normalize_embeddings=True)
    return [v.tolist() for v in vectors]


def embed_query(query: str) -> list[float]:
    """Embed a single search query using the same model as ingest."""
    return embed_queries([query])[0]


def expand_and_embed(query: str) -> list[tuple[str, list[float]]]:
    """Build query-expansion variants for ``query`` as ``(phrasing, embedding)`` pairs.

    Returns the *additional* variants to fuse alongside the original query (the
    original is passed to ``hybrid_search`` separately). Empty when expansion is
    disabled (``ACTALUX_QUERY_EXPANSION`` != "on"), no OpenAI key is configured,
    or the LLM hop yields nothing. Expansion is a best-effort recall optimization,
    so ANY failure (config, LLM, embedding) degrades to ``[]`` — plain
    single-query retrieval — rather than breaking a search that would otherwise
    work. See ``hybrid_search`` for how the pools are fused.
    """
    try:
        cfg = get_config()
        if cfg.query_expansion_mode != "on" or not cfg.openai_api_key:
            return []
        variants = generate_query_variants(
            query, cfg.openai_api_key, cfg.expansion_model, n=cfg.expansion_count
        )
        if not variants:
            return []
        return list(zip(variants, embed_queries(variants)))
    except Exception:
        logger.warning("query expansion failed; using single-query retrieval", exc_info=True)
        return []


def build_reranker() -> Reranker | None:
    """Build the search reranker from config, or None for RRF-only retrieval.

    Active only when ACTALUX_RERANK=api and a ZeroEntropy key is present, so a
    missing key or the default "off" mode degrades to RRF rather than erroring.
    """
    cfg = get_config()
    if cfg.rerank_mode != "api" or not cfg.zeroentropy_api_key:
        return None
    key, model = cfg.zeroentropy_api_key, cfg.rerank_model
    return lambda query, results: rerank_results(query, results, key, model)
