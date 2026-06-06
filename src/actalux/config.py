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
    summary_model: str = "gpt-4o-mini"
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
