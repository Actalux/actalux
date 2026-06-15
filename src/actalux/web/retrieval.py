"""Shared retrieval primitives for the web layer.

Both the HTML app (``app.py``) and the JSON API (``api.py``) need the same
config, Supabase client, query embedder, and reranker. They live here — a leaf
module that imports neither — so the API can reuse them without importing the
HTML app (which would create a cycle, since the app includes the API router).
"""

from __future__ import annotations

from actalux.config import Config, load_config
from actalux.db import get_client
from actalux.ingest.embedder import load_model
from actalux.search.hybrid import Reranker
from actalux.search.rerank import rerank_results


def get_config() -> Config:
    """Load config (env-backed; cheap to re-read)."""
    return load_config()


def get_db():
    """Supabase client built from config (publishable, RLS-enforced key)."""
    cfg = get_config()
    return get_client(cfg.supabase_url, cfg.supabase_key)


def embed_query(query: str) -> list[float]:
    """Embed a search query using the same model as ingest."""
    cfg = get_config()
    model = load_model(cfg.embedding_model)
    vector = model.encode(query, normalize_embeddings=True)
    return vector.tolist()


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
