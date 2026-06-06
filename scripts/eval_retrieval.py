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
from actalux.eval import harness, judge, rerank
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


def build_arms(spec: str, parser: argparse.ArgumentParser) -> dict[str, harness.Arm]:
    """Build the arm map: always the RRF baseline, plus any selected rerankers.

    Each reranker arm closes over its short name and reorders the pool via the
    self-hosted cross-encoder; the `n=name` default binds the loop variable.

    At most one reranker per process: the zerank custom modeling code patches
    sentence-transformers' CrossEncoder class globally and hardcodes its own
    weights path, so a second reranker loaded in the same process would
    silently score with the first model's weights. Run each separately, then
    combine with --combined-report.
    """
    selected = [r.strip() for r in spec.split(",") if r.strip()]
    unknown = [r for r in selected if r not in rerank.RERANKERS]
    if unknown:
        parser.error(f"unknown reranker(s) {unknown}; known: {sorted(rerank.RERANKERS)}")
    if len(selected) > 1:
        parser.error(
            f"only one reranker per process (got {selected}); the zerank custom code "
            "patches CrossEncoder globally. Run each separately, then --combined-report."
        )
    arms: dict[str, harness.Arm] = {"rrf_only": lambda _query, pool: pool}
    for name in selected:
        arms[name] = lambda query, pool, n=name: rerank.rerank_pool(n, query, pool)
    return arms


def main() -> None:
    parser = argparse.ArgumentParser(description="Retrieval eval (RRF baseline + reranker arms).")
    parser.add_argument("--limit", type=int, default=None, help="only the first N queries")
    parser.add_argument(
        "--query-ids",
        type=str,
        default="",
        help="comma list of query ids to run (e.g. judge a new probe without "
        "re-running the rerankers over the whole set)",
    )
    parser.add_argument("--no-judge", action="store_true", help="skip the LLM judge (plumbing)")
    parser.add_argument(
        "--spot-check", type=int, metavar="N", help="print N cached grades, then exit"
    )
    parser.add_argument("--out", type=Path, default=None, help="report path (eval/results/)")
    parser.add_argument(
        "--rerankers",
        type=str,
        default="",
        help="one self-hosted reranker arm alongside the RRF baseline, e.g. "
        "'zerank-2' (one per process; combine separate runs with --combined-report)",
    )
    parser.add_argument(
        "--api-rerank",
        action="store_true",
        help="add the ZeroEntropy hosted-API reranker arm (zerank-1-small) "
        "alongside the RRF baseline; needs ZEROENTROPY_API_KEY",
    )
    parser.add_argument(
        "--combined-report",
        action="store_true",
        help="build the multi-arm report from persisted rankings + judgments "
        "(no DB, models, or LLM), then exit",
    )
    args = parser.parse_args()

    if args.spot_check is not None:
        spot_check(args.spot_check)
        return

    if args.combined_report:
        report = harness.report_from_disk()
        body = harness.render_markdown(report)
        print("\n" + body)
        stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
        out = args.out or (RESULTS_DIR / f"combined_{stamp}.md")
        out.parent.mkdir(parents=True, exist_ok=True)
        header = f"<!-- generated {stamp} | arms={','.join(report['arms'])} -->\n\n"
        out.write_text(header + body + "\n")
        print(f"\nReport written to {out}")
        return

    arms = build_arms(args.rerankers, parser)

    cfg = load_config()
    if not args.no_judge and not cfg.anthropic_api_key:
        parser.error("ANTHROPIC_API_KEY not set; use --no-judge or run under doppler.")

    if args.api_rerank:
        if not cfg.zeroentropy_api_key:
            parser.error("ZEROENTROPY_API_KEY not set; needed for --api-rerank.")
        ze_key, ze_model = cfg.zeroentropy_api_key, cfg.rerank_model
        arms[rerank.API_ARM_NAME] = lambda query, pool, k=ze_key, m=ze_model: (
            rerank.rerank_pool_api(query, pool, k, m)
        )

    client = get_client(cfg.supabase_url, cfg.supabase_key)
    model = load_model(cfg.embedding_model)

    query_ids = {q.strip() for q in args.query_ids.split(",") if q.strip()} or None
    report = harness.run(
        client,
        model,
        cfg.anthropic_api_key,
        arms=arms,
        limit=args.limit,
        do_judge=not args.no_judge,
        query_ids=query_ids,
    )
    body = harness.render_markdown(report)
    print("\n" + body)

    if not args.no_judge:
        stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
        prefix = "rerank" if len(arms) > 1 else "baseline"
        out = args.out or (RESULTS_DIR / f"{prefix}_{stamp}.md")
        out.parent.mkdir(parents=True, exist_ok=True)
        header = (
            f"<!-- generated {stamp} | queries={len(report['queries'])} "
            f"| arms={','.join(arms)} -->\n\n"
        )
        out.write_text(header + body + "\n")
        print(f"\nReport written to {out}")


if __name__ == "__main__":
    main()
