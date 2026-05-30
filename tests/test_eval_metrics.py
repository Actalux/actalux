"""Tests for ranking metrics (pure functions; no DB or LLM)."""

import math

from actalux.eval import metrics


def test_dcg_first_position_undiscounted():
    # Position 1 (index 0) has discount log2(2) = 1, so gain is undiscounted.
    assert metrics.dcg([3]) == 3.0
    # Position 2 discounted by log2(3).
    assert metrics.dcg([0, 3]) == 3.0 / math.log2(3)


def test_ndcg_perfect_ranking_is_one():
    assert metrics.ndcg_at_k([3, 2, 1, 0], 4) == 1.0


def test_ndcg_reversed_ranking_below_one():
    perfect = metrics.ndcg_at_k([3, 2, 1], 3)
    worst = metrics.ndcg_at_k([1, 2, 3], 3)
    assert perfect == 1.0
    assert worst < perfect


def test_ndcg_all_zero_is_zero_not_error():
    assert metrics.ndcg_at_k([0, 0, 0], 3) == 0.0


def test_ndcg_respects_cutoff():
    # A relevant item past k must not count toward DCG@k.
    assert metrics.ndcg_at_k([0, 0, 3], 2) == 0.0


def test_mrr_first_relevant_position():
    assert metrics.mrr([0, 0, 2]) == 1.0 / 3
    assert metrics.mrr([3, 0, 0]) == 1.0


def test_mrr_threshold_excludes_weak_grades():
    # Grade 1 is below the relevance threshold of 2.
    assert metrics.mrr([1, 1, 2]) == 1.0 / 3
    assert metrics.mrr([1, 1, 1]) == 0.0


def test_recall_at_k_counts_relevant_in_topk():
    # Two relevant in the pool (grades 2 and 3); one lands in the top 2.
    assert metrics.recall_at_k([2, 0, 0, 3], 2) == 0.5


def test_recall_at_k_undefined_when_no_relevant():
    assert metrics.recall_at_k([0, 1, 1], 2) is None


def test_relevant_count():
    assert metrics.relevant_count([0, 1, 2, 3]) == 2
