"""Ranking-quality metrics over graded relevance.

Pure functions: a ranked list of integer relevance grades in, a score out.
Grades follow the judge's 0-3 scale (see judge.py). "Relevant" means grade
>= RELEVANCE_THRESHOLD.

nDCG uses the standard log2 discount; the ideal ranking is the best possible
ordering of the *same* graded items, so when two arms rank the same pooled
candidates their nDCG shares one denominator and is directly comparable.
"""

from __future__ import annotations

import math

# A grade at or above this counts as a relevant hit for MRR/recall.
RELEVANCE_THRESHOLD = 2


def dcg(grades: list[int]) -> float:
    """Discounted cumulative gain of a ranked grade list (position 1 = index 0)."""
    return sum(g / math.log2(i + 2) for i, g in enumerate(grades))


def ndcg_at_k(ranked_grades: list[int], k: int) -> float:
    """nDCG@k: arm's DCG@k over the ideal DCG@k of the same graded items.

    Returns 0.0 when there is no gain to be had (all grades zero), which keeps
    the metric defined for fully-irrelevant pools without special-casing callers.
    """
    actual = dcg(ranked_grades[:k])
    ideal = dcg(sorted(ranked_grades, reverse=True)[:k])
    return actual / ideal if ideal > 0 else 0.0


def mrr(ranked_grades: list[int], threshold: int = RELEVANCE_THRESHOLD) -> float:
    """Reciprocal rank of the first relevant item; 0.0 if none are relevant."""
    for i, g in enumerate(ranked_grades):
        if g >= threshold:
            return 1.0 / (i + 1)
    return 0.0


def relevant_count(grades: list[int], threshold: int = RELEVANCE_THRESHOLD) -> int:
    """Number of relevant items among the graded set."""
    return sum(1 for g in grades if g >= threshold)


def recall_at_k(
    ranked_grades: list[int],
    k: int,
    threshold: int = RELEVANCE_THRESHOLD,
) -> float | None:
    """Fraction of the pool's relevant items that land in the top k.

    Returns None when the pool holds no relevant items (recall undefined), so
    these queries can be excluded from the mean rather than counted as 0.
    """
    total = relevant_count(ranked_grades, threshold)
    if total == 0:
        return None
    hits = relevant_count(ranked_grades[:k], threshold)
    return hits / total
