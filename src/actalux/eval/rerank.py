"""Self-hosted cross-encoder reranking for the eval harness (Phase B).

A reranker scores each (query, passage) pair with a cross-encoder and reorders
the RRF candidate pool by relevance. The weights run locally via
sentence-transformers -- the same library Actalux already uses for bge-small
embeddings -- so there is no API key, no per-token cost, and no network call at
query time.

Two ZeroEntropy models are wired as arms:
- zerank-1-small (Apache-2.0): self-hostable with no licensing constraint; the
  long-term default if it captures most of the reranking gain.
- zerank-2 (CC-BY-NC): newer/larger, with calibrated scores. Run here for
  research/eval comparison; production use would need the non-commercial
  determination or a hosted-API agreement.

Both score via CrossEncoder.predict([(query, passage), ...]) -> higher is more
relevant. Each model loads on first use and caches for the process.

One reranker per process: zerank-1-small's custom modeling code patches the
CrossEncoder class globally and hardcodes its own weights path, so a second
reranker in the same process would silently score with zerank-1-small's
weights. The CLI enforces this; combine separate runs with --combined-report.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from sentence_transformers import CrossEncoder

    from actalux.search.hybrid import SearchResult

logger = logging.getLogger(__name__)

RERANK_MAX_LENGTH = 1024  # cap pair length; chunks are ~200 words, so truncation is rare
RERANK_DTYPE = "auto"  # load each checkpoint in its native (bf16) precision


@dataclass(frozen=True)
class RerankerSpec:
    """How to load and run one self-hosted reranker."""

    name: str  # short CLI / arm key
    repo: str  # HuggingFace repo id
    trust_remote_code: bool  # zerank-1-small ships custom modeling code
    # Pairs fed to model.predict per call. Bounds the batch regardless of a
    # model's own internal batching -- zerank-1-small batches by a GPU-sized
    # char budget and computes full-vocab logits (a causal LM), so it needs a
    # small chunk to stay within memory; zerank-2 is a light classifier head.
    predict_chunk: int


RERANKERS: dict[str, RerankerSpec] = {
    "zerank-1-small": RerankerSpec(
        name="zerank-1-small",
        repo="zeroentropy/zerank-1-small-reranker",
        trust_remote_code=True,
        predict_chunk=8,
    ),
    "zerank-2": RerankerSpec(
        name="zerank-2",
        repo="zeroentropy/zerank-2-reranker",
        trust_remote_code=False,
        predict_chunk=16,
    ),
}

_models: dict[str, CrossEncoder] = {}


def load_reranker(name: str) -> CrossEncoder:
    """Load a reranker by short name, caching the model per process."""
    if name in _models:
        return _models[name]
    if name not in RERANKERS:
        raise ValueError(f"unknown reranker {name!r}; known: {sorted(RERANKERS)}")
    spec = RERANKERS[name]

    from sentence_transformers import CrossEncoder

    logger.info("Loading reranker %s (%s)...", spec.name, spec.repo)
    model = CrossEncoder(
        spec.repo,
        trust_remote_code=spec.trust_remote_code,
        max_length=RERANK_MAX_LENGTH,
        model_kwargs={"dtype": RERANK_DTYPE},
    )
    # Confirm the precision actually loaded -- a silent fp32 fallback would
    # roughly double resident memory for the 4B model.
    param_dtype = next(model.model.parameters()).dtype
    logger.info("Reranker %s loaded (device=%s, dtype=%s)", spec.name, model.device, param_dtype)
    _models[name] = model
    return model


def rerank_pool(name: str, query: str, pool: list[SearchResult]) -> list[SearchResult]:
    """Reorder `pool` by the reranker's (query, passage) relevance scores.

    Returns the same SearchResult objects in descending score order. The pool
    is not mutated; only its order changes, which is all the eval scores on
    (it maps each result's chunk_id to a cached relevance grade).
    """
    if not pool:
        return []
    model = load_reranker(name)
    scores = _score_pairs(model, query, [r.content for r in pool], RERANKERS[name].predict_chunk)
    order = sorted(range(len(pool)), key=lambda i: scores[i], reverse=True)
    return [pool[i] for i in order]


def _score_pairs(model: CrossEncoder, query: str, passages: list[str], chunk: int) -> list[float]:
    """Score (query, passage) pairs, returned aligned to `passages`.

    Pairs are sorted longest-first and fed in chunks of `chunk`. The chunk caps
    the batch size handed to the model (some ship their own GPU-sized internal
    batching that overflows memory on a single big call); sorting keeps each
    chunk's pairs similar in length so padding is tight. A CrossEncoder scores
    each pair independently, so restoring the original order yields identical
    scores -- a pure performance/memory optimization with no numerical effect.
    """
    import torch

    order = sorted(range(len(passages)), key=lambda i: len(passages[i]), reverse=True)
    scores = [0.0] * len(passages)
    for start in range(0, len(order), chunk):
        idxs = order[start : start + chunk]
        # no_grad is essential: zerank-1-small's custom predict runs a causal-LM
        # forward without disabling autograd, so the retained activation graph
        # (not the batch) is what blows past memory. The context reaches the
        # forward inside predict; zerank-2's standard predict already uses it.
        with torch.no_grad():
            raw = model.predict(
                [(query, passages[i]) for i in idxs], batch_size=chunk, show_progress_bar=False
            )
        for j, i in enumerate(idxs):
            scores[i] = float(raw[j])
    return scores
