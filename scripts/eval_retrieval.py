"""Retrieval eval CLI: measure search quality on the committed query set.

Phase A establishes the RRF-only baseline. The judge is paid for once and
cached (eval/judgments.json); re-runs reuse grades. Spot-check a sample of
grades before trusting the aggregate.

Run (all under: doppler run --project mac --config dev -- uv run python ...):
  scripts/eval_retrieval.py --no-judge --limit 3   # plumbing only, no LLM spend
  scripts/eval_retrieval.py --limit 3              # small judged sample to eyeball
  scripts/eval_retrieval.py                        # full baseline
  scripts/eval_retrieval.py --spot-check 20        # review cached grades
"""

from __future__ import annotations

import argparse
import logging
import random
from datetime import UTC, datetime
from pathlib import Path

from actalux.config import load_config
from actalux.db import get_client
from actalux.eval import harness, judge
from actalux.ingest.embedder import load_model

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

SEED = 42
RESULTS_DIR = harness.REPO_ROOT / "eval" / "results"


def spot_check(n: int) -> None:
    """Print a random sample of cached judgments for manual review."""
    cache = judge.load_cache(harness.JUDGMENTS_PATH)
    items = list(cache["grades"].items())
    if not items:
        print("No judgments cached yet — run a judged eval first.")
        return
    rng = random.Random(SEED)
    sample = rng.sample(items, min(n, len(items)))
    model = cache.get("model")
    print(f"\nSpot-check: {len(sample)} of {len(items)} cached grades (judge={model})\n")
    for _key, rec in sample:
        print(f"[grade {rec['grade']}] query: {rec['query']}")
        print(f"            passage: {rec['chunk_preview']}")
        print()


def main() -> None:
    parser = argparse.ArgumentParser(description="Retrieval eval (Phase A baseline).")
    parser.add_argument("--limit", type=int, default=None, help="only the first N queries")
    parser.add_argument("--no-judge", action="store_true", help="skip the LLM judge (plumbing)")
    parser.add_argument(
        "--spot-check", type=int, metavar="N", help="print N cached grades, then exit"
    )
    parser.add_argument("--out", type=Path, default=None, help="report path (eval/results/)")
    args = parser.parse_args()

    if args.spot_check is not None:
        spot_check(args.spot_check)
        return

    cfg = load_config()
    if not args.no_judge and not cfg.anthropic_api_key:
        parser.error("ANTHROPIC_API_KEY not set; use --no-judge or run under doppler.")

    client = get_client(cfg.supabase_url, cfg.supabase_key)
    model = load_model(cfg.embedding_model)

    report = harness.run(
        client,
        model,
        cfg.anthropic_api_key,
        limit=args.limit,
        do_judge=not args.no_judge,
    )
    body = harness.render_markdown(report)
    print("\n" + body)

    if not args.no_judge:
        stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
        out = args.out or (RESULTS_DIR / f"baseline_{stamp}.md")
        out.parent.mkdir(parents=True, exist_ok=True)
        header = f"<!-- generated {stamp} | queries={len(report['queries'])} -->\n\n"
        out.write_text(header + body + "\n")
        print(f"\nReport written to {out}")


if __name__ == "__main__":
    main()
