"""Tests for the multi-arm eval merge path.

These cover the from-disk report that scores every arm against the final
judgment union -- the only correct way to compare recall across rerankers that
cannot share a process. No models, DB, or LLM are involved.
"""

from __future__ import annotations

import json
from pathlib import Path

from actalux.eval import harness


def _write_fixture(tmp: Path) -> tuple[Path, Path, Path]:
    """A 2-query fixture: q1 scored, q2 an expect_empty integrity probe.

    On q1 the two relevant items (10, 12) sit at ranks 1 and 3 under RRF; the
    reranker lifts them to ranks 1 and 2. On q2 one relevant item leaks into
    both arms' top-K.
    """
    queries = tmp / "queries.json"
    rankings = tmp / "rankings.json"
    judgments = tmp / "judgments.json"
    queries.write_text(
        json.dumps(
            {
                "queries": [
                    {"id": "q1", "domain": "finance", "query": "total operating budget"},
                    {
                        "id": "q2",
                        "domain": "governance",
                        "query": "closed session topics",
                        "expect_empty": True,
                    },
                ]
            }
        )
    )
    rankings.write_text(
        json.dumps(
            {
                "q1": {"rrf_only": [10, 11, 12, 13], "zerank-2": [12, 10, 11, 13]},
                "q2": {"rrf_only": [20, 21], "zerank-2": [21, 20]},
            }
        )
    )
    judgments.write_text(
        json.dumps(
            {
                "model": "test",
                "grades": {
                    "q1::10": {"grade": 3},
                    "q1::11": {"grade": 0},
                    "q1::12": {"grade": 3},
                    "q1::13": {"grade": 1},
                    "q2::20": {"grade": 2},
                    "q2::21": {"grade": 0},
                },
            }
        )
    )
    return rankings, judgments, queries


def test_report_from_disk_scores_each_arm(tmp_path: Path) -> None:
    rankings, judgments, queries = _write_fixture(tmp_path)
    rep = harness.report_from_disk(rankings, judgments, queries)

    assert rep["arms"] == ["rrf_only", "zerank-2"]  # RRF baseline first
    q1 = next(q for q in rep["queries"] if q.query_id == "q1")
    # The reranker's tighter ordering must score at least as high on nDCG.
    assert q1.arms["zerank-2"].ndcg_at_k > q1.arms["rrf_only"].ndcg_at_k
    # Both surface the two relevant items inside the cutoff.
    assert q1.arms["rrf_only"].recall_at_k == 1.0
    assert q1.arms["zerank-2"].recall_at_k == 1.0
    assert q1.arms["rrf_only"].mrr == 1.0
    assert q1.arms["rrf_only"].relevant_in_pool == 2


def test_report_from_disk_tracks_leaks_per_arm(tmp_path: Path) -> None:
    rankings, judgments, queries = _write_fixture(tmp_path)
    rep = harness.report_from_disk(rankings, judgments, queries)
    q2 = next(q for q in rep["queries"] if q.query_id == "q2")
    assert q2.expect_empty
    assert q2.leaked_in_top_k == {"rrf_only": 1, "zerank-2": 1}


def test_merge_rankings_combines_separate_runs(tmp_path: Path) -> None:
    """Each reranker runs in its own process; merge must accumulate arms."""
    path = tmp_path / "rankings.json"
    harness._merge_rankings(path, {"q1": {"rrf_only": [1, 2], "zerank-1-small": [2, 1]}})
    harness._merge_rankings(path, {"q1": {"rrf_only": [1, 2], "zerank-2": [1, 2]}})
    merged = json.loads(path.read_text())
    assert set(merged["q1"]) == {"rrf_only", "zerank-1-small", "zerank-2"}


def test_ordered_arms_puts_baseline_first() -> None:
    rankings = {"q1": {"zerank-2": [1], "rrf_only": [1], "zerank-1-small": [1]}}
    assert harness._ordered_arms(rankings)[0] == "rrf_only"
