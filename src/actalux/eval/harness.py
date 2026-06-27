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

# An arm reorders the retrieved pool given the query. RRF-only is the identity;
# a reranker arm scores (query, passage) pairs and sorts by relevance.
Arm = Callable[[str, list[SearchResult]], list[SearchResult]]

POOL_SIZE = 100  # candidates retrieved per query (rerank room for Phase B)
JUDGE_DEPTH = 20  # top-N of each arm's ranking that gets relevance-judged
K = 10  # cutoff for the reported metrics

REPO_ROOT = Path(__file__).resolve().parents[3]
QUERIES_PATH = REPO_ROOT / "eval" / "queries.json"
JUDGMENTS_PATH = REPO_ROOT / "eval" / "judgments.json"
RANKINGS_PATH = REPO_ROOT / "eval" / "rankings.json"


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
    # For expect_empty integrity probes: per-arm relevant hits that leaked into
    # top-K (each arm orders the pool differently, so each is checked).
    leaked_in_top_k: dict[str, int] = field(default_factory=dict)
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
    base_url: str = judge.DEFAULT_BASE_URL,
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
            grade = judge.grade_relevance(query, result.content, api_key, model, base_url)
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
    ranked_ids: list[int],
    grades: dict[int, int],
) -> tuple[ArmScore, bool]:
    """Score one arm's ranking (chunk_ids in arm order) against judged grades.

    Returns the ArmScore and a coverage flag (False when a top-K item was left
    ungraded, so the query can be excluded from aggregates rather than scored
    on a hole). Scoring on chunk_ids lets the same function serve the live run
    and the from-disk merge, where only persisted rankings are available.
    """
    coverage_ok = all(cid in grades for cid in ranked_ids[:K])
    # Ranked grade list over judged items, preserving arm order.
    ranked_grades = [grades[cid] for cid in ranked_ids if cid in grades]
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
    arms: dict[str, Arm] | None = None,
    limit: int | None = None,
    do_judge: bool = True,
    query_ids: set[str] | None = None,
    base_url: str = judge.DEFAULT_BASE_URL,
) -> dict[str, Any]:
    """Run the eval and return a report dict.

    `arms` maps an arm name to a function reordering the retrieved pool given
    the query; it defaults to the RRF-only baseline (identity over the
    already-RRF-ordered pool). Phase B passes reranker arms over the same pool.
    `query_ids` runs only those queries (e.g. to judge a newly added probe
    without re-running the rerankers over the whole set).
    """
    arms = arms or {"rrf_only": lambda _query, pool: pool}
    queries = load_queries()
    if query_ids is not None:
        queries = [q for q in queries if q["id"] in query_ids]
    if limit is not None:
        queries = queries[:limit]
    cache = judge.load_cache(JUDGMENTS_PATH)

    reports: list[QueryReport] = []
    run_rankings: dict[str, dict[str, list[int]]] = {}
    for q in queries:
        pool = retrieve_pool(client, model, q["query"])
        # Judge the union of every arm's top-JUDGE_DEPTH so each arm is scored
        # on the same grade set.
        grades: dict[int, int] = {}
        if do_judge:
            for reorder in arms.values():
                ranked = reorder(q["query"], pool)
                grades.update(
                    judge_pool(q["id"], q["query"], ranked, cache, api_key, base_url=base_url)
                )
            judge.save_cache(JUDGMENTS_PATH, cache)

        qr = QueryReport(
            query_id=q["id"],
            domain=q["domain"],
            query=q["query"],
            expect_empty=bool(q.get("expect_empty")),
        )
        for name, reorder in arms.items():
            ranked_ids = [r.chunk_id for r in reorder(q["query"], pool)]
            run_rankings.setdefault(q["id"], {})[name] = ranked_ids
            if qr.expect_empty:
                qr.leaked_in_top_k[name] = sum(
                    1 for cid in ranked_ids[:K] if grades.get(cid, 0) >= metrics.RELEVANCE_THRESHOLD
                )
                continue
            score, coverage_ok = score_arm(name, ranked_ids, grades)
            qr.arms[name] = score
            qr.coverage_ok = qr.coverage_ok and coverage_ok
        reports.append(qr)
        logger.info("scored %s (%s)", q["id"], q["query"][:48])

    # Persist this run's rankings (judged runs only) so a from-disk merge can
    # score every arm against the final judgment union. Rerankers must run in
    # separate processes (their custom code patches CrossEncoder globally), so
    # each run contributes its arms here.
    if do_judge:
        _merge_rankings(RANKINGS_PATH, run_rankings)
    return {"k": K, "arms": list(arms.keys()), "queries": reports}


def _merge_rankings(path: Path, new: dict[str, dict[str, list[int]]]) -> None:
    """Merge per-(query, arm) rankings into rankings.json, updating in place."""
    existing: dict[str, dict[str, list[int]]] = {}
    if path.exists():
        existing = json.loads(path.read_text())
    for qid, arm_rankings in new.items():
        existing.setdefault(qid, {}).update(arm_rankings)
    path.write_text(json.dumps(existing, indent=2) + "\n")


def _ordered_arms(rankings: dict[str, dict[str, list[int]]]) -> list[str]:
    """Every arm present across queries, with the RRF baseline first."""
    seen: list[str] = []
    for arm_rankings in rankings.values():
        for arm in arm_rankings:
            if arm not in seen:
                seen.append(arm)
    seen.sort(key=lambda a: (a != "rrf_only", a))
    return seen


def report_from_disk(
    rankings_path: Path = RANKINGS_PATH,
    judgments_path: Path = JUDGMENTS_PATH,
    queries_path: Path = QUERIES_PATH,
) -> dict[str, Any]:
    """Build the multi-arm report from persisted rankings + cached judgments.

    No models, no DB, no LLM: scores every arm against the *final* judgment
    union, which is the only correct way to compare recall@K across arms that
    cannot share a process. This is the reproducible combined report.
    """
    rankings = json.loads(rankings_path.read_text())
    cache = judge.load_cache(judgments_path)
    judged = cache["grades"]
    arms = _ordered_arms(rankings)

    reports: list[QueryReport] = []
    for q in load_queries(queries_path):
        qid = q["id"]
        arm_rankings = rankings.get(qid)
        if arm_rankings is None:
            continue
        # Grades for this query: any judged (qid, cid) reachable from its arms.
        grades: dict[int, int] = {}
        for ids in arm_rankings.values():
            for cid in ids:
                key = judge.cache_key(qid, cid)
                if key in judged:
                    grades[cid] = judged[key]["grade"]

        qr = QueryReport(
            query_id=qid,
            domain=q["domain"],
            query=q["query"],
            expect_empty=bool(q.get("expect_empty")),
        )
        for name in arms:
            ranked_ids = arm_rankings.get(name)
            if ranked_ids is None:
                continue
            if qr.expect_empty:
                qr.leaked_in_top_k[name] = sum(
                    1 for cid in ranked_ids[:K] if grades.get(cid, 0) >= metrics.RELEVANCE_THRESHOLD
                )
                continue
            score, coverage_ok = score_arm(name, ranked_ids, grades)
            qr.arms[name] = score
            qr.coverage_ok = qr.coverage_ok and coverage_ok
        reports.append(qr)

    return {"k": K, "arms": arms, "queries": reports}


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

    arms = report["arms"]
    lines.append(f"### Per-query nDCG@{k} (rel-in-pool = relevant items the first stage surfaced)")
    lines.append("| query | domain | " + " | ".join(arms) + " | rel-in-pool |")
    lines.append("|---|---|" + "---|" * (len(arms) + 1))
    for qr in report["queries"]:
        if qr.expect_empty:
            continue
        cells = []
        rel_in_pool = "—"
        for arm in arms:
            s = qr.arms.get(arm)
            cells.append(f"{s.ndcg_at_k:.3f}" if s is not None else "—")
            if s is not None and rel_in_pool == "—":
                rel_in_pool = str(s.relevant_in_pool)
        flag = "" if qr.coverage_ok else " ⚠"
        lines.append(
            f"| {qr.query}{flag} | {qr.domain} | " + " | ".join(cells) + f" | {rel_in_pool} |"
        )
    lines.append("")

    empties = [qr for qr in report["queries"] if qr.expect_empty]
    if empties:
        lines.append("### Integrity probes (expect no relevant result)")
        for qr in empties:
            verdicts = []
            for arm in arms:
                leaked = qr.leaked_in_top_k.get(arm, 0)
                verdicts.append(f"{arm}: {'PASS' if leaked == 0 else f'FAIL ({leaked})'}")
            lines.append(f"- `{qr.query}` → " + " · ".join(verdicts))
        lines.append("")

    return "\n".join(lines)
