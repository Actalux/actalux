"""Evidence assembly for the AI-answer path.

The answer path (``/summarize`` and the answer-quality eval) needs one set of
citeable quotes to hand the summary LLM. This module owns the choice of where
those quotes come from: a figure-shaped finance query is served from the
structured ``budget_line_items`` table (see ``finance.py``); everything else
falls back to hybrid retrieval over the text chunks. Routing lives here -- not
in the route -- so the eval exercises the exact production decision.

``enrich_results`` is the canonical shaping of a ``SearchResult`` into the dict
the summary builder, the result cards, and the citation linker all read; it
replaces the two near-identical copies that previously lived in the web app and
the eval.
"""

from __future__ import annotations

import logging
from concurrent.futures import ThreadPoolExecutor
from typing import Any

from supabase import Client

from actalux.db import get_chunk_citation_ids, get_documents
from actalux.models import chunk_hash_id
from actalux.search.finance import build_finance_evidence, finance_intent
from actalux.search.hybrid import (
    Expansions,
    ExpansionsProvider,
    Reranker,
    SearchFilters,
    SearchResult,
    hybrid_search,
)

logger = logging.getLogger(__name__)

# route labels returned alongside the evidence, for logging / eval arm naming
ROUTE_FINANCE = "structured-finance"
ROUTE_TEXT = "text"


def enrich_results(client: Client, results: list[SearchResult]) -> list[dict[str, Any]]:
    """Attach document metadata to search results in one round-trip.

    Produces the dict shape the summary builder (hash_id, content, meeting_date,
    section), the result cards (chunk_id, meeting_title, document_type, summary),
    and the citation linker (hash_id -> cite_ref) all consume. ``cite_ref`` is the
    stable citation_id when the chunk has one, else the numeric chunk id, so both
    the displayed hash and the /chunk/{ref} link route on the durable identity.
    """
    # The two lookups are independent round-trips; run them concurrently so enrich
    # costs ~one round-trip, not two (same threadpool pattern as hybrid search).
    with ThreadPoolExecutor(max_workers=2) as pool:
        docs_future = pool.submit(get_documents, client, [r.document_id for r in results])
        cites_future = pool.submit(get_chunk_citation_ids, client, [r.chunk_id for r in results])
        docs = docs_future.result()
        citation_ids = cites_future.result()
    enriched: list[dict[str, Any]] = []
    for r in results:
        doc = docs.get(r.document_id, {})
        citation_id = citation_ids.get(r.chunk_id, "")
        cite_ref: str | int = citation_id or r.chunk_id
        enriched.append(
            {
                "chunk_id": r.chunk_id,
                "citation_id": citation_id,
                "cite_ref": cite_ref,
                "hash_id": chunk_hash_id(cite_ref),
                "content": r.content,
                "section": r.section,
                "speaker": r.speaker,
                "rrf_score": round(r.rrf_score, 4),
                "meeting_date": doc.get("meeting_date", ""),
                "meeting_title": doc.get("meeting_title", ""),
                "document_id": r.document_id,
                "document_type": doc.get("document_type", ""),
                "source_portal": doc.get("source_portal", ""),
                "summary": doc.get("summary", ""),
                "entity_id": doc.get("entity_id"),
            }
        )
    return enriched


def assemble_evidence(
    client: Client,
    query: str,
    embedding: list[float],
    *,
    filters: SearchFilters | None = None,
    reranker: Reranker | None = None,
    max_results: int = 10,
    finance_routing: bool = True,
    expansions: Expansions | ExpansionsProvider | None = None,
) -> tuple[list[dict[str, Any]], str]:
    """Build the citeable evidence for an answer, and report which path served it.

    Tries structured finance first (when ``finance_routing`` is on and the query
    is a figure-shaped finance ask with matching rows); otherwise runs hybrid
    retrieval over the text chunks. ``expansions`` are optional query-expansion
    variants (or a provider callable) passed through to ``hybrid_search`` to widen
    recall on the text path; a provider is resolved only inside ``hybrid_search``,
    so a finance-routed query returns before it runs (no expansion LLM call).
    Returns ``(evidence, route_label)``.
    """
    if finance_routing:
        intent = finance_intent(query)
        if intent is not None:
            entity_id = filters.entity_id if filters else None
            evidence = build_finance_evidence(
                client, intent, entity_id=entity_id, max_items=max_results
            )
            if evidence:
                return evidence, ROUTE_FINANCE
            logger.info("finance intent matched but no rows for %r; using text path", query)

    results = hybrid_search(
        client,
        query,
        embedding,
        filters or SearchFilters(),
        max_results=max_results,
        reranker=reranker,
        expansions=expansions,
    )
    return enrich_results(client, results), ROUTE_TEXT
