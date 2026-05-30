"""Tests for the reranker's length-sorted batching.

The optimization feeds pairs to the model longest-first, then restores the
original order. These verify scores realign correctly -- the only place a bug
could silently reorder relevance scores.
"""

from __future__ import annotations

from actalux.eval import rerank


class _FakeModel:
    """Scores a passage by its length and records each predict call's pairs."""

    def __init__(self) -> None:
        self.calls: list[list[str]] = []

    def predict(self, pairs, batch_size=None, show_progress_bar=None):  # noqa: ANN001
        self.calls.append([p[1] for p in pairs])
        return [len(p[1]) for p in pairs]


def test_score_pairs_realigns_to_input_order() -> None:
    model = _FakeModel()
    passages = ["bbb", "a", "cccc", "dd"]  # lengths 3, 1, 4, 2
    scores = rerank._score_pairs(model, "q", passages, chunk=8)
    # Scores come back aligned to the INPUT order, not the sorted order.
    assert scores == [3.0, 1.0, 4.0, 2.0]
    # One chunk (chunk >= len), fed longest-first.
    assert model.calls == [["cccc", "bbb", "dd", "a"]]


def test_score_pairs_chunks_and_realigns() -> None:
    model = _FakeModel()
    passages = ["bbb", "a", "cccc", "dd"]
    scores = rerank._score_pairs(model, "q", passages, chunk=2)
    assert scores == [3.0, 1.0, 4.0, 2.0]  # still aligned to input order
    # Two chunks of 2, longest-first across the whole set.
    assert model.calls == [["cccc", "bbb"], ["dd", "a"]]


def test_score_pairs_empty() -> None:
    assert rerank._score_pairs(_FakeModel(), "q", [], chunk=8) == []
