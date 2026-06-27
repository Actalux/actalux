"""Answer-quality eval (synthesis): how good are the generated summaries?

The retrieval eval (harness.py) scores ranking/recall; this scores the ANSWER.
For each query it runs the production summary path -- bge embedding ->
hybrid_search (with reranker) -> generate_summary -- then has an LLM judge grade
the answer on faithfulness / completeness / directness (judge.grade_answer),
each against only the quotes the answer was given. It also reports the hard
citation stats generate_summary already computes (found / verified / dropped).

Answers and grades are cached per (query_id, model) so re-runs are cheap and an
A/B across summary models reuses prior work; --regenerate forces a fresh pass.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from supabase import Client

from actalux.errors import SummaryError
from actalux.eval import judge
from actalux.eval.harness import load_queries
from actalux.search.answer import assemble_evidence
from actalux.search.hybrid import Reranker
from actalux.search.summarize import Summary, generate_summary

logger = logging.getLogger(__name__)

REPO_ROOT = Path(__file__).resolve().parents[3]
ANSWERS_PATH = REPO_ROOT / "eval" / "answers.json"
ANSWER_JUDGMENTS_PATH = REPO_ROOT / "eval" / "answer_judgments.json"

SUMMARY_MAX_RESULTS = 10  # matches the production /summarize route
QUOTE_CHARS = 700  # per-quote cap in the judge prompt; chunks are ~200 words


@dataclass
class AnswerRow:
    """One query's generated answer plus its judged scores."""

    query_id: str
    query: str
    domain: str
    answer: str
    citations_found: int
    citations_verified: int
    citations_dropped: int
    scores: dict[str, int]  # faithfulness / completeness / directness


def _quotes_block(enriched: list[dict[str, Any]]) -> str:
    """Render the quotes the judge sees -- the only evidence the answer could use."""
    lines: list[str] = []
    for q in enriched:
        head = " | ".join(
            p for p in [q["hash_id"], str(q.get("meeting_date") or ""), q.get("section") or ""] if p
        )
        lines.append(f"[{head}]\n{q['content'][:QUOTE_CHARS]}")
    return "\n\n".join(lines)


def generate_answer(
    client: Client,
    embed_model: Any,
    query: str,
    openai_key: str,
    summary_model: str,
    reranker: Reranker | None,
    *,
    base_url: str | None = None,
    reasoning_effort: str = "minimal",
    finance_routing: bool = False,
) -> tuple[Summary, list[dict[str, Any]]]:
    """Run the production answer path; return the Summary and the quotes it saw.

    With ``finance_routing`` on, figure-shaped finance queries are served from the
    structured budget table (matching production); off reproduces the text-only
    baseline. Routing through ``assemble_evidence`` keeps the eval on the exact
    production path.
    """
    embedding = embed_model.encode(query, normalize_embeddings=True).tolist()
    enriched, _route = assemble_evidence(
        client,
        query,
        embedding,
        reranker=reranker,
        max_results=SUMMARY_MAX_RESULTS,
        finance_routing=finance_routing,
    )
    summary = generate_summary(
        query,
        enriched,
        openai_key,
        summary_model,
        base_url=base_url,
        reasoning_effort=reasoning_effort,
    )
    return summary, enriched


def _load(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text()) if path.exists() else {}


def _save(path: Path, data: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2, sort_keys=True) + "\n")


def run(
    client: Client,
    embed_model: Any,
    openai_key: str,
    judge_key: str,
    summary_model: str,
    reranker: Reranker | None,
    *,
    model_id: str | None = None,
    base_url: str | None = None,
    reasoning_effort: str = "minimal",
    finance_routing: bool = False,
    limit: int | None = None,
    query_ids: set[str] | None = None,
    regenerate: bool = False,
    judge_model: str = judge.JUDGE_MODEL,
    judge_base_url: str = judge.DEFAULT_BASE_URL,
) -> list[AnswerRow]:
    """Generate + judge answers for the query set, caching per (query_id, model_id).

    `model_id` is the cache/report label (defaults to `summary_model`); pass a
    distinct one for a config variant of the same model (e.g. a reasoning bump)
    so it doesn't collide with the base model's cached answers.
    """
    model_id = model_id or summary_model
    queries = load_queries()
    if query_ids is not None:
        queries = [q for q in queries if q["id"] in query_ids]
    if limit is not None:
        queries = queries[:limit]

    answers = _load(ANSWERS_PATH)
    grades = _load(ANSWER_JUDGMENTS_PATH)
    rows: list[AnswerRow] = []

    for q in queries:
        qid = q["id"]
        cached = answers.get(qid, {}).get(model_id)
        if cached is None or regenerate:
            try:
                summary, enriched = generate_answer(
                    client,
                    embed_model,
                    q["query"],
                    openai_key,
                    summary_model,
                    reranker,
                    base_url=base_url,
                    reasoning_effort=reasoning_effort,
                    finance_routing=finance_routing,
                )
            except SummaryError as exc:  # skip-and-report; a bad arm shouldn't crash the run
                logger.warning("answer generation failed for %s (%s): %s", qid, model_id, exc)
                continue
            cached = {
                "answer": summary.text,
                "found": summary.citations_found,
                "verified": summary.citations_verified,
                "dropped": summary.citations_dropped,
                "quotes": _quotes_block(enriched),
            }
            answers.setdefault(qid, {})[model_id] = cached
            _save(ANSWERS_PATH, answers)
            grades.get(qid, {}).pop(model_id, None)  # answer changed -> regrade

        score = grades.get(qid, {}).get(model_id)
        if score is None:
            try:
                score = judge.grade_answer(
                    q["query"],
                    cached["answer"],
                    cached["quotes"],
                    judge_key,
                    judge_model,
                    judge_base_url,
                )
            except Exception as exc:  # noqa: BLE001 - skip-and-report, don't score a hole
                logger.warning("answer judge failed for %s: %s", qid, exc)
                continue
            grades.setdefault(qid, {})[model_id] = score
            _save(ANSWER_JUDGMENTS_PATH, grades)

        rows.append(
            AnswerRow(
                query_id=qid,
                query=q["query"],
                domain=q["domain"],
                answer=cached["answer"],
                citations_found=cached["found"],
                citations_verified=cached["verified"],
                citations_dropped=cached["dropped"],
                scores=score,
            )
        )
        logger.info("scored %s (%s)", qid, q["query"][:48])

    return rows


def _mean(values: list[float]) -> float:
    return sum(values) / len(values) if values else 0.0


def aggregate(rows: list[AnswerRow]) -> dict[str, float]:
    """Mean per-dimension scores plus citation health across all rows."""
    out = {dim: _mean([r.scores[dim] for r in rows]) for dim in judge.ANSWER_DIMENSIONS}
    out["citation_drop_rate"] = _mean(
        [r.citations_dropped / r.citations_found for r in rows if r.citations_found]
    )
    out["mean_citations"] = _mean([float(r.citations_verified) for r in rows])
    out["n_queries"] = float(len(rows))
    return out


def render_markdown(rows: list[AnswerRow], summary_model: str, judge_model: str) -> str:
    """Human-readable answer-quality report."""
    agg = aggregate(rows)
    dims = judge.ANSWER_DIMENSIONS
    dim_cells = " | ".join(f"{agg[d]:.2f}" for d in dims)
    drop = f"{agg['citation_drop_rate']:.2f}"
    cites = f"{agg['mean_citations']:.1f}"
    n = int(agg["n_queries"])
    lines = [
        f"## Answer-quality eval (model={summary_model}, judge={judge_model})",
        "",
        "### Aggregate (each dimension 0-3)",
        "| " + " | ".join(dims) + " | citation drop | mean cites | n |",
        "|" + "---|" * (len(dims) + 3),
        f"| {dim_cells} | {drop} | {cites} | {n} |",
        "",
        "### Per-query",
        "| query | domain | " + " | ".join(dims) + " | cites (verified/found) |",
        "|---|---|" + "---|" * (len(dims) + 1),
    ]
    for r in rows:
        cells = " | ".join(str(r.scores[d]) for d in dims)
        lines.append(
            f"| {r.query} | {r.domain} | {cells} | {r.citations_verified}/{r.citations_found} |"
        )
    return "\n".join(lines) + "\n"
