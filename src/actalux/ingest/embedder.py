"""Generate embeddings for chunks using bge-small-en-v1.5.

Embeddings are computed locally (no API calls). The model is loaded once
and cached for the lifetime of the process.
"""

from __future__ import annotations

import logging
import threading
from typing import TYPE_CHECKING

from actalux.errors import EmbeddingError

if TYPE_CHECKING:
    from sentence_transformers import SentenceTransformer

    from actalux.models import Chunk

logger = logging.getLogger(__name__)

_model: SentenceTransformer | None = None
# Guards the one-time model load: the startup warm-up thread and a first request
# can both reach load_model() before _model is set. Without this lock they each
# start their own ~8s load, contending — the "first query paid ~10s" symptom.
_model_lock = threading.Lock()


def load_model(model_name: str = "BAAI/bge-small-en-v1.5") -> SentenceTransformer:
    """Load the embedding model. Caches globally after first call (thread-safe)."""
    global _model
    if _model is not None:
        return _model

    with _model_lock:
        if _model is not None:  # another thread loaded it while we waited
            return _model
        try:
            from sentence_transformers import SentenceTransformer

            logger.info("Loading embedding model: %s", model_name)
            _model = SentenceTransformer(model_name)
            logger.info(
                "Model loaded successfully (dim=%d)", _model.get_sentence_embedding_dimension()
            )
            return _model
        except Exception as exc:
            raise EmbeddingError(f"Failed to load embedding model {model_name}: {exc}") from exc


def embed_chunks(chunks: list[Chunk], model_name: str = "BAAI/bge-small-en-v1.5") -> list[Chunk]:
    """Add embedding vectors to a list of chunks.

    Returns new Chunk objects with the embedding field populated.
    The original chunks are not mutated (frozen dataclass).
    """
    if not chunks:
        return []

    model = load_model(model_name)
    texts = [chunk.content for chunk in chunks]

    try:
        logger.info("Embedding %d chunks...", len(texts))
        vectors = model.encode(texts, show_progress_bar=len(texts) > 50, normalize_embeddings=True)
    except Exception as exc:
        raise EmbeddingError(f"Embedding failed for {len(texts)} chunks: {exc}") from exc

    from dataclasses import replace

    result: list[Chunk] = []
    for chunk, vector in zip(chunks, vectors):
        result.append(replace(chunk, embedding=vector.tolist()))

    logger.info("Embedded %d chunks (dim=%d)", len(result), len(result[0].embedding))
    return result
