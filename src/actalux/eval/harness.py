"""Retrieval eval orchestration.

Runs the production retrieval path (bge embedding -> hybrid pgvector + FTS ->
RRF) over the committed query set, judges the pooled top results once, and
scores each arm with nDCG@10 / MRR / recall@10. Phase A scores the RRF-only
baseline; Phase B adds a reranked arm over the same pool and judgments.

Pooling is TREC-style to depth JUDGE_DEPTH: only the top JUDGE_DEPTH of each
arm's ranking is judged. That fully covers the @10 metrics and bounds judge
cost -- a new arm only pays to grade the items it newly lifts into its top-K.
"""

from __future__ import annotations

import json
import logging
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from supabase import Client

from actalux.eval import judge, metrics
from actalux.search.hybrid import SearchResult, hybrid_search

logger = logging.getLogger(__name__)

POOL_SIZE = 100  # candidates retrieved per query (rerank room for Phase B)
JUDGE_DEPTH = 20  # top-N of each arm's ranking that gets relevance-judged
K = 10  # cutoff for the reported metrics

REPO_ROOT = Path(__file__).resolve().parents[3]
QUERIES_PATH = REPO_ROOT / "eval" / "queries.json"
JUDGMENTS_PATH = REPO_ROOT / "eval" / "judgments.json"


@dataclass
class ArmScore:
    """One arm's per-query metrics."""

    name: str
    ndcg_at_k: float
    mrr: float
    recall_at_k: float | None
    relevant_in_pool: int


@dataclass
class QueryReport:
    """Everything measured for one query."""

    query_id: str
    domain: str
    query: str
    expect_empty: bool
    arms: dict[str, ArmScore] = field(default_factory=dict)
    # For expect_empty integrity probes: relevant hits that leaked into top-K.
    leaked_in_top_k: int = 0
    coverage_ok: bool = True


def load_queries(path: Path = QUERIES_PATH) -> list[dict[str, Any]]:
    """Load the committed query set."""
    return json.loads(path.read_text())["queries"]


def retrieve_pool(
    client: Client,
    model: Any,
    query: str,
    pool_size: int = POOL_SIZE,
) -> list[SearchResult]:
    """Run the production hybrid retrieval and return the RRF-ordered pool."""
    embedding = model.encode(query, normalize_embeddings=True).tolist()
    return hybrid_search(client, query, embedding, max_results=pool_size)


def judge_pool(
    query_id: str,
    query: str,
    ranked: list[SearchResult],
    cache: dict[str, Any],
    api_key: str,
    depth: int = JUDGE_DEPTH,
    model: str = judge.JUDGE_MODEL,
) -> dict[int, int]:
    """Grade the top `depth` of `ranked`, reusing/extending the cache.

    Returns {chunk_id: grade} for every judged chunk. Cache misses are graded
    via the LLM; failures are logged and the pair is left ungraded rather than
    defaulted to 0.
    """
    grades_out: dict[int, int] = {}
    for result in ranked[:depth]:
        key = judge.cache_key(query_id, result.chunk_id)
        cached = cache["grades"].get(key)
        if cached is not None:
            grades_out[result.chunk_id] = cached["grade"]
            continue
        try:
            grade = judge.grade_relevance(query, result.content, api_key, model)
        except Exception as exc:  # noqa: BLE001 - skip-and-report, never default to 0
            logger.warning("judge failed for %s chunk %d: %s", query_id, result.chunk_id, exc)
            continue
        cache["grades"][key] = {
            "grade": grade,
            "query": query,
            "chunk_preview": result.content[:160],
        }
        grades_out[result.chunk_id] = grade
    return grades_out


def score_arm(
    name: str,
    ranked: list[SearchResult],
    grades: dict[int, int],
) -> tuple[ArmScore, bool]:
    """Score one arm's ranking against the judged grades.

    Returns the ArmScore and a coverage flag (False when a top-K item was left
    ungraded, so the query can be excluded from aggregates rather than scored
    on a hole).
    """
    top_k_ids = [r.chunk_id for r in ranked[:K]]
    coverage_ok = all(cid in grades for cid in top_k_ids)
    # Build the ranked grade list over judged items, preserving arm order.
    ranked_grades = [grades[r.chunk_id] for r in ranked if r.chunk_id in grades]
    return (
        ArmScore(
            name=name,
            ndcg_at_k=metrics.ndcg_at_k(ranked_grades, K),
            mrr=metrics.mrr(ranked_grades),
            recall_at_k=metrics.recall_at_k(ranked_grades, K),
            relevant_in_pool=metrics.relevant_count(ranked_grades),
        ),
        coverage_ok,
    )


def run(
    client: Client,
    model: Any,
    api_key: str,
    arms: dict[str, Callable[[list[SearchResult]], list[SearchResult]]] | None = None,
    limit: int | None = None,
    do_judge: bool = True,
) -> dict[str, Any]:
    """Run the eval and return a report dict.

    `arms` maps an arm name to a function reordering the retrieved pool; it
    defaults to the RRF-only baseline (identity over the already-RRF-ordered
    pool). Phase B passes a second arm that reranks the pool.
    """
    arms = arms or {"rrf_only": lambda pool: pool}
    queries = load_queries()
    if limit is not None:
        queries = queries[:limit]
    cache = judge.load_cache(JUDGMENTS_PATH)

    reports: list[QueryReport] = []
    for q in queries:
        pool = retrieve_pool(client, model, q["query"])
        # Judge the union of every arm's top-JUDGE_DEPTH so each arm is scored
        # on the same grade set.
        grades: dict[int, int] = {}
        if do_judge:
            for reorder in arms.values():
                ranked = reorder(pool)
                grades.update(judge_pool(q["id"], q["query"], ranked, cache, api_key))
            judge.save_cache(JUDGMENTS_PATH, cache)

        qr = QueryReport(
            query_id=q["id"],
            domain=q["domain"],
            query=q["query"],
            expect_empty=bool(q.get("expect_empty")),
        )
        for name, reorder in arms.items():
            ranked = reorder(pool)
            if qr.expect_empty:
                top_k_ids = [r.chunk_id for r in ranked[:K]]
                qr.leaked_in_top_k = sum(
                    1 for cid in top_k_ids if grades.get(cid, 0) >= metrics.RELEVANCE_THRESHOLD
                )
                continue
            score, coverage_ok = score_arm(name, ranked, grades)
            qr.arms[name] = score
            qr.coverage_ok = qr.coverage_ok and coverage_ok
        reports.append(qr)
        logger.info("scored %s (%s)", q["id"], q["query"][:48])

    return {"k": K, "arms": list(arms.keys()), "queries": reports}


def _mean(values: list[float]) -> float | None:
    return sum(values) / len(values) if values else None


def aggregate(report: dict[str, Any]) -> dict[str, dict[str, float | None]]:
    """Mean metrics per arm over scored (non-empty, full-coverage) queries."""
    out: dict[str, dict[str, float | None]] = {}
    scored = [qr for qr in report["queries"] if not qr.expect_empty and qr.coverage_ok and qr.arms]
    for arm in report["arms"]:
        present = [qr.arms[arm] for qr in scored if arm in qr.arms]
        out[arm] = {
            "ndcg_at_k": _mean([s.ndcg_at_k for s in present]),
            "mrr": _mean([s.mrr for s in present]),
            "recall_at_k": _mean([s.recall_at_k for s in present if s.recall_at_k is not None]),
            "relevant_in_pool": _mean([float(s.relevant_in_pool) for s in present]),
            "n_queries": float(len(present)),
        }
    return out


def render_markdown(report: dict[str, Any]) -> str:
    """Render a human-readable eval report."""
    k = report["k"]
    agg = aggregate(report)
    lines = [f"## Retrieval eval (k={k})", ""]

    lines.append("### Aggregate")
    lines.append(f"| arm | nDCG@{k} | MRR | recall@{k} | mean relevant in pool | n |")
    lines.append("|---|---|---|---|---|---|")
    for arm, m in agg.items():

        def fmt(x: float | None) -> str:
            return f"{x:.3f}" if isinstance(x, float) else "—"

        lines.append(
            f"| {arm} | {fmt(m['ndcg_at_k'])} | {fmt(m['mrr'])} | {fmt(m['recall_at_k'])} "
            f"| {fmt(m['relevant_in_pool'])} | {int(m['n_queries'] or 0)} |"
        )
    lines.append("")

    lines.append(f"### Per-query ({report['arms'][0]})")
    lines.append(f"| query | domain | nDCG@{k} | MRR | recall@{k} | rel-in-pool |")
    lines.append("|---|---|---|---|---|---|")
    arm0 = report["arms"][0]
    for qr in report["queries"]:
        if qr.expect_empty:
            continue
        s = qr.arms.get(arm0)
        if s is None:
            lines.append(f"| {qr.query} | {qr.domain} | (no coverage) | | | |")
            continue
        rec = f"{s.recall_at_k:.2f}" if s.recall_at_k is not None else "n/a"
        flag = "" if qr.coverage_ok else " ⚠"
        lines.append(
            f"| {qr.query}{flag} | {qr.domain} | {s.ndcg_at_k:.3f} | {s.mrr:.3f} "
            f"| {rec} | {s.relevant_in_pool} |"
        )
    lines.append("")

    empties = [qr for qr in report["queries"] if qr.expect_empty]
    if empties:
        lines.append("### Integrity probes (expect no relevant result)")
        for qr in empties:
            verdict = "PASS" if qr.leaked_in_top_k == 0 else f"FAIL ({qr.leaked_in_top_k} leaked)"
            lines.append(f"- `{qr.query}` → {verdict}")
        lines.append("")

    return "\n".join(lines)
