"""Application configuration from environment variables."""

from __future__ import annotations

import os
from dataclasses import dataclass, field


@dataclass(frozen=True)
class Config:
    """Immutable application configuration."""

    supabase_url: str = field(default_factory=lambda: os.environ["SUPABASE_URL"])
    supabase_key: str = field(default_factory=lambda: os.environ["SUPABASE_KEY"])
    buttondown_api_key: str = field(
        default_factory=lambda: os.environ.get("BUTTONDOWN_API_KEY", "")
    )
    anthropic_api_key: str = field(default_factory=lambda: os.environ.get("ANTHROPIC_API_KEY", ""))
    embedding_model: str = "BAAI/bge-small-en-v1.5"
    embedding_dim: int = 384
    chunk_target_words: int = 200
    chunk_overlap_sentences: int = 2
    search_similarity_threshold: float = 0.35
    search_max_results: int = 20
    search_rrf_k: int = 60
    topic_cache_ttl_seconds: int = 3600
    rate_limit_search_per_minute: int = 30
    rate_limit_corrections_per_hour: int = 5


def load_config() -> Config:
    """Load config from environment. Raises KeyError if required vars are missing."""
    return Config()
