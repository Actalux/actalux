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
    parser.add_argument("--model", type=str, default="", help="summary model (default: config)")
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
        cfg.openai_api_key,
        cfg.anthropic_api_key,
        summary_model,
        reranker,
        limit=args.limit,
        query_ids=query_ids,
        regenerate=args.regenerate,
    )

    body = answer_quality.render_markdown(rows, summary_model, judge.JUDGE_MODEL)
    print("\n" + body)

    stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    safe_model = summary_model.replace("/", "-")
    out = args.out or (RESULTS_DIR / f"answers_{safe_model}_{stamp}.md")
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(f"<!-- generated {stamp} | model={summary_model} -->\n\n" + body)
    print(f"\nReport written to {out}")


if __name__ == "__main__":
    main()
