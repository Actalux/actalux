"""Shared retrieval primitives for the web layer.

Both the HTML app (``app.py``) and the JSON API (``api.py``) need the same
config, Supabase client, query embedder, and reranker. They live here — a leaf
module that imports neither — so the API can reuse them without importing the
HTML app (which would create a cycle, since the app includes the API router).
"""

from __future__ import annotations

import logging
import re

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


# Name-corrections, cached per place for the process lifetime: they change only on
# re-seed, so a redeploy picks up new rows (same cadence as the embedding model).
_corrections_cache: dict[int, list[tuple[str, str]]] = {}


def _load_corrections(place_id: int) -> list[tuple[str, str]]:
    """(mangled, canonical) pairs for a place from name_corrections, cached."""
    if place_id not in _corrections_cache:
        rows = (
            get_db()
            .table("name_corrections")
            .select("mangled,canonical")
            .eq("place_id", place_id)
            .eq("active", True)
            .execute()
            .data
        )
        _corrections_cache[place_id] = [(r["mangled"], r["canonical"]) for r in rows]
    return _corrections_cache[place_id]


def _reset_corrections_cache() -> None:
    """Clear the corrections cache. For tests only."""
    _corrections_cache.clear()


def apply_corrections(query: str, pairs: list[tuple[str, str]], *, cap: int = 8) -> list[str]:
    """Alternate query phrasings from name-corrections, BOTH directions.

    A query carrying a known mangling also searches the canonical spelling, and a
    query carrying the canonical also searches the mangling — so the corpus is
    reached whichever spelling it used (ASR transcripts vs. OCR minutes). Matching is
    word-boundaried + case-insensitive; the STORED text is never touched, only the
    query is widened. Pure (the substitution core, unit-testable without a DB).
    """
    variants: list[str] = []
    seen = {query.lower()}
    for mangled, canonical in pairs:
        for src, dst in ((mangled, canonical), (canonical, mangled)):
            pattern = re.compile(rf"\b{re.escape(src)}\b", re.IGNORECASE)
            if pattern.search(query):
                variant = pattern.sub(dst, query)
                if variant.lower() not in seen:
                    seen.add(variant.lower())
                    variants.append(variant)
                    if len(variants) >= cap:
                        return variants
    return variants


def correction_variants(query: str, place_id: int | None) -> list[str]:
    """Name-correction query variants for a place; [] if off or on any failure.

    Jurisdiction-scoped (a mangling in one town can be a real name in another), so it
    needs the place. Best-effort, like the LLM expansion: a load/parse failure
    degrades to plain retrieval rather than breaking a working search.
    """
    if place_id is None:
        return []
    try:
        return apply_corrections(query, _load_corrections(place_id))
    except Exception:
        logger.warning("name-corrections expansion failed; skipping", exc_info=True)
        return []


def search_expansions(query: str, place_id: int | None = None) -> list[tuple[str, list[float]]]:
    """All query-expansion variants — LLM rephrasings + name-corrections — embedded.

    The single entry point the search call sites use: it fuses the (gated, best-
    effort) LLM variants with the place's name-correction variants and returns
    ``(phrasing, embedding)`` pairs to search alongside the original query. Each is
    independent and best-effort, so either contributing nothing just narrows recall.
    """
    llm = expand_and_embed(query)
    corr_texts = correction_variants(query, place_id)
    if not corr_texts:
        return llm
    llm_texts = {text for text, _ in llm}
    fresh = [t for t in corr_texts if t not in llm_texts]
    corr = list(zip(fresh, embed_queries(fresh))) if fresh else []
    return llm + corr


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
