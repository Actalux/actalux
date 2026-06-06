"""Application configuration from environment variables."""

from __future__ import annotations

import os
from dataclasses import dataclass, field


@dataclass(frozen=True)
class Config:
    """Immutable application configuration."""

    supabase_url: str = field(default_factory=lambda: os.environ["ACTALUX_SUPABASE_URL"])
    # Publishable key: RLS-enforced, safe for the public web app.
    supabase_key: str = field(default_factory=lambda: os.environ["ACTALUX_SUPABASE_KEY"])
    # Service (secret) key: bypasses RLS. Used only by ingest/backfill/load
    # writers, never by the web app, so the web host doesn't need to carry it.
    supabase_service_key: str = field(
        default_factory=lambda: os.environ.get("ACTALUX_SUPABASE_SERVICE_KEY", "")
    )
    buttondown_api_key: str = field(
        default_factory=lambda: os.environ.get("BUTTONDOWN_API_KEY", "")
    )
    anthropic_api_key: str = field(default_factory=lambda: os.environ.get("ANTHROPIC_API_KEY", ""))
    openai_api_key: str = field(default_factory=lambda: os.environ.get("OPENAI_API_KEY", ""))
    summary_model: str = "gpt-5-mini"
    # ZeroEntropy hosted reranker. Key gates the API call; zerank-1-small is the
    # Apache-2.0 model that won the retrieval eval (+24% nDCG@10; see eval/README.md).
    zeroentropy_api_key: str = field(
        default_factory=lambda: os.environ.get("ZEROENTROPY_API_KEY", "")
    )
    rerank_model: str = "zerank-1-small"
    # "off" (RRF only, default) or "api" (rerank the RRF pool via ZeroEntropy).
    # Default off so the reranker is a deliberate, deploy-time opt-in (set
    # ACTALUX_RERANK=api in the web host's secrets) with no surprise cost/latency.
    rerank_mode: str = field(default_factory=lambda: os.environ.get("ACTALUX_RERANK", "off"))
    # RRF candidates reranked before truncating to search_max_results. Reranking
    # a deeper pool is what lets the cross-encoder lift a buried-but-relevant hit.
    rerank_pool_size: int = 50
    embedding_model: str = "BAAI/bge-small-en-v1.5"
    embedding_dim: int = 384
    chunk_target_words: int = 200
    chunk_overlap_sentences: int = 2
    # Ingest-time PII guard: "block" (skip flagged docs, default), "warn", "off".
    pii_guard_mode: str = field(
        default_factory=lambda: os.environ.get("ACTALUX_PII_GUARD", "block")
    )
    search_similarity_threshold: float = 0.35
    search_max_results: int = 20
    search_rrf_k: int = 60
    topic_cache_ttl_seconds: int = 3600
    rate_limit_search_per_minute: int = 30
    rate_limit_corrections_per_hour: int = 5


def load_config() -> Config:
    """Load config from environment. Raises KeyError if required vars are missing."""
    return Config()
