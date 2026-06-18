"""Per-step latency benchmark for the /ask pipeline (task #19).

Times the exact production calls (condense -> embed -> assemble_evidence ->
generate_summary) so we can see which step actually dominates before optimizing,
rather than guessing. Read-only (search + summary); makes a handful of LLM calls.

    doppler run --project mac --config dev -- uv run python scripts/bench_ask.py

Caveat: run from a dev machine, so the Supabase/ZeroEntropy network hops differ
from Fly (region ord). The LLM-call timings are location-independent (same API);
treat the *relative* breakdown as the signal, and cross-check the end-to-end
total against a live curl of /ask.
"""

from __future__ import annotations

import time
from typing import Any

from actalux.search.answer import assemble_evidence
from actalux.search.hybrid import SearchFilters
from actalux.search.summarize import condense_question, generate_summary
from actalux.web.retrieval import build_reranker, embed_query, get_config, get_db


def _ms(t0: float) -> float:
    return (time.perf_counter() - t0) * 1000.0


def _entity_id(client: Any) -> int | None:
    try:
        res = client.table("entities").select("id").limit(1).execute()
        return res.data[0]["id"] if res.data else None
    except Exception:
        return None


def run_once(
    label: str,
    *,
    history: list[dict[str, str]],
    question: str,
    cfg: Any,
    client: Any,
    reranker: Any,
    entity_id: int | None,
) -> None:
    print(f"\n=== {label} ===")
    filters = SearchFilters(entity_id=entity_id) if entity_id else SearchFilters()
    timings: dict[str, float] = {}

    standalone = question
    if history:
        t = time.perf_counter()
        standalone = condense_question(history, question, cfg.openai_api_key, cfg.summary_model)
        timings["condense (LLM)"] = _ms(t)

    t = time.perf_counter()
    embedding = embed_query(standalone)
    timings["embed (local)"] = _ms(t)

    t = time.perf_counter()
    enriched, route = assemble_evidence(
        client, standalone, embedding, filters=filters, reranker=reranker, max_results=10
    )
    timings[f"assemble_evidence [{route}]"] = _ms(t)

    t = time.perf_counter()
    summary = generate_summary(standalone, enriched, cfg.openai_api_key, cfg.summary_model)
    timings["generate_summary (LLM)"] = _ms(t)

    total = sum(timings.values())
    for name, val in timings.items():
        print(f"  {name:34s} {val:8.0f} ms  ({100 * val / total:4.1f}%)")
    print(f"  {'TOTAL':34s} {total:8.0f} ms")
    print(
        f"  standalone={standalone!r}  evidence={len(enriched)}  answer_chars={len(summary.text)}"
    )


def main() -> None:
    cfg = get_config()
    client = get_db()
    reranker = build_reranker()
    entity_id = _entity_id(client)
    print(
        f"model={cfg.summary_model}  reranker={'on' if reranker else 'off'}  entity_id={entity_id}"
    )

    standalone_q = "What has the district said about the budget?"
    # Cold run includes the bge-small model load; the warm run is steady-state.
    run_once(
        "STANDALONE (cold)",
        history=[],
        question=standalone_q,
        cfg=cfg,
        client=client,
        reranker=reranker,
        entity_id=entity_id,
    )
    run_once(
        "STANDALONE (warm)",
        history=[],
        question=standalone_q,
        cfg=cfg,
        client=client,
        reranker=reranker,
        entity_id=entity_id,
    )

    history = [
        {"role": "user", "content": "What has the district said about the budget?"},
        {
            "role": "assistant",
            "content": "The district adopts a budget each June for the July 1 fiscal year.",
        },
    ]
    run_once(
        "FOLLOW-UP (warm, +condense)",
        history=history,
        question="What about reserves?",
        cfg=cfg,
        client=client,
        reranker=reranker,
        entity_id=entity_id,
    )


if __name__ == "__main__":
    main()
