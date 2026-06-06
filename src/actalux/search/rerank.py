"""Hosted cross-encoder reranking via the ZeroEntropy API.

The production retrieval path fuses semantic + keyword results with RRF, then
(optionally) reorders the fused pool with a cross-encoder reranker. The eval
harness measured zerank-1-small lifting nDCG@10 from 0.72 to 0.90 (+24%) over
RRF on the labeled query set (see eval/README.md). CPU self-hosting is too slow
for interactive search (~244 ms/passage), so we call ZeroEntropy's GPU-backed
endpoint, which serves the same Apache-2.0 weights in ~100-300 ms.

A reranker outage must never break search: callers run rerank at the search
boundary and fall back to RRF order on RerankError.
"""

from __future__ import annotations

import logging
import time
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


def rerank_results(
    query: str,
    results: list[SearchResult],
    api_key: str,
    model: str,
) -> list[SearchResult]:
    """Return `results` reordered by the reranker's relevance scores.

    Pure reorder of the same objects -- raises RerankError on any API failure so
    the caller can fall back to the original (RRF) order.
    """
    if not results:
        return []
    documents = [r.content[:DOC_CHARS] for r in results]
    order = _request_rerank_order(query, documents, api_key, model)
    return [results[i] for i in order]


def _request_rerank_order(query: str, documents: list[str], api_key: str, model: str) -> list[int]:
    """POST the documents to the rerank endpoint; return input indices in score order.

    Indices the API omits are appended in their original order so the returned
    permutation always covers every input document. Retries on 429 (honoring
    Retry-After) and transient network errors; raises RerankError when exhausted.
    """
    import httpx

    payload = {"model": model, "query": query, "documents": documents, "latency": LATENCY_MODE}
    headers = {"Authorization": f"Bearer {api_key}"}
    last_error = "unknown"
    for attempt in range(MAX_RETRIES):
        try:
            resp = httpx.post(ZE_RERANK_URL, json=payload, headers=headers, timeout=REQUEST_TIMEOUT)
        except httpx.HTTPError as exc:
            last_error = f"network error: {exc}"
            time.sleep(0.5 * (attempt + 1))
            continue
        if resp.status_code == 429:
            wait = float(resp.headers.get("retry-after", 0.5 * (attempt + 1)))
            logger.warning("reranker ratelimited (429); waiting %.1fs", wait)
            time.sleep(wait)
            last_error = "ratelimited (429)"
            continue
        if resp.status_code != 200:
            raise RerankError(f"reranker returned {resp.status_code}: {resp.text[:200]}")
        results = resp.json()["results"]
        ordered = [r["index"] for r in results]
        seen = set(ordered)
        ordered += [i for i in range(len(documents)) if i not in seen]
        return ordered
    raise RerankError(f"reranker failed after {MAX_RETRIES} retries: {last_error}")
