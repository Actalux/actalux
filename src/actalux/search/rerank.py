"""Hosted cross-encoder reranking, across interchangeable vendors.

The production retrieval path fuses semantic + keyword results with RRF, then
(optionally) reorders the fused pool with a cross-encoder reranker. The eval
harness measured zerank-1-small lifting nDCG@10 from 0.72 to 0.90 (+24%) over
RRF on the labeled query set (see eval/README.md).

ZeroEntropy — the incumbent, and the vendor that measurement was done against —
shuts down 2026-09-04, so this module is provider-agnostic. Self-hosting the same
Apache-2.0 weights was measured and rejected on latency: 877 ms at the production
pool depth of 50 against ZeroEntropy's ~194 ms, and flat across batch size, so it
is compute-bound rather than tunable (see scripts/bench_modal_rerank.py).

Only the induced *order* is consumed, never the absolute scores, which is what
makes vendors substitutable at all — score scales differ between models and are
not comparable, but a permutation is a permutation. Choosing a replacement is
therefore a measurement question, answered on this corpus rather than on vendor
leaderboards: run the eval's provider arms and compare nDCG@10.

A reranker outage must never break search: callers run rerank at the search
boundary and fall back to RRF order on RerankError.
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from actalux.errors import RerankError

if TYPE_CHECKING:
    from actalux.search.hybrid import SearchResult

logger = logging.getLogger(__name__)

ZE_RERANK_URL = "https://api.zeroentropy.dev/v1/models/rerank"
DOC_CHARS = 2000  # chunks are ~200 words (~1200 chars); a defensive per-doc cap
REQUEST_TIMEOUT = 10.0  # seconds; interactive search can't wait longer
MAX_RETRIES = 3
# "fast" trades the highest accuracy tier for subsecond latency, which is the
# right call for interactive search; the eval ran without it and still won big.
LATENCY_MODE = "fast"


@dataclass(frozen=True)
class RerankProvider:
    """One hosted reranker: where to POST, which model, and its own knobs.

    The three vendors differ only in URL, model string, one or two payload
    fields, and which key holds the ranked list — all take ``{query, documents}``
    with a bearer token and answer a list of ``{"index", "relevance_score"}``.
    That shared shape is why swapping vendors is a table entry, not a client.
    """

    name: str
    url: str
    default_model: str
    # Provider-specific payload fields merged into every request.
    extra: dict[str, object] = field(default_factory=dict)
    # Top-level key holding the ranked list. Voyage answers `data` where the other
    # two answer `results` — its published docs say `results`, so this was found by
    # calling the API rather than by reading them.
    results_key: str = "results"


PROVIDERS: dict[str, RerankProvider] = {
    # The incumbent. Shuts down 2026-09-04, which is why the others exist.
    "zeroentropy": RerankProvider(
        name="zeroentropy",
        url=ZE_RERANK_URL,
        default_model="zerank-1-small",
        extra={"latency": LATENCY_MODE},
    ),
    # max_tokens_per_doc defaults to 4096 upstream; our chunks are ~1200 chars, so
    # capping it lower costs nothing and keeps a pathological chunk from dominating
    # the request's token bill.
    "cohere": RerankProvider(
        name="cohere",
        url="https://api.cohere.com/v2/rerank",
        default_model="rerank-v3.5",
        extra={"max_tokens_per_doc": 1024},
    ),
    # truncation=True lets the API clip an over-long pair instead of erroring the
    # whole request — one bad chunk should not cost the search its reranking.
    "voyage": RerankProvider(
        name="voyage",
        url="https://api.voyageai.com/v1/rerank",
        default_model="rerank-2.5-lite",
        extra={"truncation": True},
        results_key="data",
    ),
}


def get_provider(name: str) -> RerankProvider:
    """Look up a rerank provider by name, or fail with the known set."""
    try:
        return PROVIDERS[name]
    except KeyError:
        raise ValueError(f"unknown rerank provider {name!r}; known: {sorted(PROVIDERS)}") from None


def rerank_results(
    query: str,
    results: list[SearchResult],
    api_key: str,
    model: str,
    provider: str = "zeroentropy",
) -> list[SearchResult]:
    """Return `results` reordered by the reranker's relevance scores.

    Pure reorder of the same objects -- raises RerankError on any API failure so
    the caller can fall back to the original (RRF) order.
    """
    if not results:
        return []
    documents = [r.content[:DOC_CHARS] for r in results]
    order = _request_rerank_order(query, documents, api_key, model, get_provider(provider))
    return [results[i] for i in order]


def _request_rerank_order(
    query: str,
    documents: list[str],
    api_key: str,
    model: str,
    provider: RerankProvider,
) -> list[int]:
    """POST the documents to the rerank endpoint; return input indices in score order.

    Indices the API omits are appended in their original order so the returned
    permutation always covers every input document. Retries on 429 (honoring
    Retry-After) and transient network errors; raises RerankError when exhausted.
    """
    import httpx

    payload: dict[str, object] = {
        "model": model or provider.default_model,
        "query": query,
        "documents": documents,
        **provider.extra,
    }
    headers = {"Authorization": f"Bearer {api_key}"}
    last_error = "unknown"
    for attempt in range(MAX_RETRIES):
        try:
            resp = httpx.post(provider.url, json=payload, headers=headers, timeout=REQUEST_TIMEOUT)
        except httpx.HTTPError as exc:
            last_error = f"network error: {exc}"
            time.sleep(0.5 * (attempt + 1))
            continue
        if resp.status_code == 429:
            wait = float(resp.headers.get("retry-after", 0.5 * (attempt + 1)))
            logger.warning("%s reranker ratelimited (429); waiting %.1fs", provider.name, wait)
            time.sleep(wait)
            last_error = "ratelimited (429)"
            continue
        if resp.status_code != 200:
            raise RerankError(
                f"{provider.name} reranker returned {resp.status_code}: {resp.text[:200]}"
            )
        try:
            results = resp.json()[provider.results_key]
            ordered = [r["index"] for r in results]
        except (ValueError, KeyError, TypeError) as exc:
            raise RerankError(f"{provider.name} returned an unreadable response: {exc}") from exc
        # Indices are used to subscript the pool, so a bad one is an IndexError at
        # the call site — and the search boundary only catches RerankError, so it
        # would surface as a 500 rather than a fallback to RRF order. With three
        # interchangeable vendors this is no longer hypothetical: a response shape
        # only one of them can produce must degrade like any other rerank failure.
        if any(not isinstance(i, int) or i < 0 or i >= len(documents) for i in ordered):
            raise RerankError(
                f"{provider.name} returned an out-of-range index for a "
                f"{len(documents)}-document pool: {ordered[:10]}"
            )
        seen = set(ordered)
        ordered += [i for i in range(len(documents)) if i not in seen]
        return ordered
    raise RerankError(f"reranker failed after {MAX_RETRIES} retries: {last_error}")
