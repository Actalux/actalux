#!/usr/bin/env python3
"""Answer-quality eval CLI: how good are the generated summaries (synthesis)?

Runs the production summary path (rerank -> generate_summary) over the query set
and has an LLM judge grade each answer on faithfulness / completeness /
directness (each 0-3), plus the hard citation stats. Reranker is on whenever a
ZeroEntropy key is present, matching production. Answers + grades cache per
(query_id, model), so A/B-ing summary models (--model) reuses prior work.

Run (under: doppler run --project mac --config dev -- uv run python ...):
  scripts/eval_answers.py --limit 3                 # small judged sample
  scripts/eval_answers.py                            # full set, current model
  scripts/eval_answers.py --model openai/gpt-5       # A/B a different model
  scripts/eval_answers.py --regenerate               # force fresh answers
"""

from __future__ import annotations

import argparse
import logging
from datetime import UTC, datetime
from pathlib import Path

from actalux.config import load_config
from actalux.db import get_client
from actalux.eval import answer_quality, judge
from actalux.ingest.embedder import load_model
from actalux.search.rerank import rerank_results

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

RESULTS_DIR = answer_quality.REPO_ROOT / "eval" / "results"


def main() -> None:
    parser = argparse.ArgumentParser(description="Answer-quality eval (synthesis).")
    parser.add_argument("--limit", type=int, default=None, help="only the first N queries")
    parser.add_argument("--query-ids", type=str, default="", help="comma list of query ids")
    parser.add_argument(
        "--model",
        type=str,
        default="",
        help="summary model (default: config). A provider-prefixed id (e.g. "
        "'openai/gpt-5', 'anthropic/claude-haiku-4.5') routes via OpenRouter.",
    )
    parser.add_argument(
        "--reasoning",
        type=str,
        default="minimal",
        help="reasoning_effort for OpenAI reasoning models (minimal/low/medium/high)",
    )
    parser.add_argument(
        "--regenerate", action="store_true", help="regenerate answers even if cached"
    )
    parser.add_argument("--out", type=Path, default=None, help="report path (eval/results/)")
    args = parser.parse_args()

    cfg = load_config()
    if not cfg.openai_api_key:
        parser.error("OPENAI_API_KEY not set; run under doppler.")
    if not cfg.anthropic_api_key:
        parser.error("ANTHROPIC_API_KEY not set (the answer judge); run under doppler.")

    summary_model = args.model or cfg.summary_model
    # A provider-prefixed model id routes via OpenRouter (one key, many models);
    # a bare id (e.g. "gpt-5-mini") hits OpenAI directly.
    if "/" in summary_model:
        if not cfg.openrouter_api_key:
            parser.error("OPENROUTER_API_KEY not set; needed for a provider-prefixed --model.")
        gen_key, base_url = cfg.openrouter_api_key, "https://openrouter.ai/api/v1"
    else:
        gen_key, base_url = cfg.openai_api_key, None

    # Cache/report label: distinguish a reasoning variant of the same model so it
    # doesn't collide with the base model's cached answers.
    model_id = summary_model if args.reasoning == "minimal" else f"{summary_model}@{args.reasoning}"

    # Reranker on when a key exists, matching production (ACTALUX_RERANK is "off"
    # locally, but the deployed app reranks -- the eval should reflect that).
    reranker = None
    if cfg.zeroentropy_api_key:
        key, rmodel = cfg.zeroentropy_api_key, cfg.rerank_model
        reranker = lambda query, results: rerank_results(query, results, key, rmodel)  # noqa: E731

    client = get_client(cfg.supabase_url, cfg.supabase_key)
    embed_model = load_model(cfg.embedding_model)
    query_ids = {q.strip() for q in args.query_ids.split(",") if q.strip()} or None

    rows = answer_quality.run(
        client,
        embed_model,
        gen_key,
        cfg.anthropic_api_key,
        summary_model,
        reranker,
        model_id=model_id,
        base_url=base_url,
        reasoning_effort=args.reasoning,
        limit=args.limit,
        query_ids=query_ids,
        regenerate=args.regenerate,
    )

    body = answer_quality.render_markdown(rows, model_id, judge.JUDGE_MODEL)
    print("\n" + body)

    stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    safe_model = model_id.replace("/", "-").replace("@", "-")
    out = args.out or (RESULTS_DIR / f"answers_{safe_model}_{stamp}.md")
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(f"<!-- generated {stamp} | model={model_id} -->\n\n" + body)
    print(f"\nReport written to {out}")


if __name__ == "__main__":
    main()
