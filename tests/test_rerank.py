"""Tests for the hosted reranker client and the search-boundary fallback.

No network: httpx.post is monkeypatched to return canned API responses. These
pin the two behaviors that matter in production -- the reorder maps API indices
back onto the pool, and any failure degrades to RRF order rather than breaking
search.
"""

from __future__ import annotations

import httpx
import pytest

from actalux.errors import RerankError
from actalux.search import hybrid, rerank
from actalux.search.hybrid import SearchResult, _apply_reranker


def _result(chunk_id: int, content: str = "x") -> SearchResult:
    return SearchResult(
        chunk_id=chunk_id,
        document_id=1,
        content=content,
        section="",
        speaker="",
        rrf_score=0.0,
    )


class _FakeResponse:
    def __init__(self, status_code: int = 200, payload: dict | None = None, text: str = "") -> None:
        self.status_code = status_code
        self._payload = payload or {}
        self.text = text
        self.headers: dict[str, str] = {}

    def json(self) -> dict:
        return self._payload


def _patch_post(monkeypatch, response: _FakeResponse) -> None:
    monkeypatch.setattr(httpx, "post", lambda *args, **kwargs: response)


class TestRerankResults:
    def test_reorders_by_relevance(self, monkeypatch) -> None:
        pool = [_result(10), _result(20), _result(30)]
        payload = {
            "results": [
                {"index": 2, "relevance_score": 0.9},
                {"index": 0, "relevance_score": 0.5},
                {"index": 1, "relevance_score": 0.1},
            ]
        }
        _patch_post(monkeypatch, _FakeResponse(payload=payload))
        out = rerank.rerank_results("q", pool, "key", "zerank-1-small")
        assert [r.chunk_id for r in out] == [30, 10, 20]

    def test_appends_omitted_indices(self, monkeypatch) -> None:
        """An index the API leaves out is appended in its original position."""
        pool = [_result(10), _result(20), _result(30)]
        payload = {
            "results": [
                {"index": 2, "relevance_score": 0.9},
                {"index": 0, "relevance_score": 0.5},
            ]
        }
        _patch_post(monkeypatch, _FakeResponse(payload=payload))
        out = rerank.rerank_results("q", pool, "key", "zerank-1-small")
        assert [r.chunk_id for r in out] == [30, 10, 20]

    def test_empty_pool_no_call(self) -> None:
        assert rerank.rerank_results("q", [], "key", "zerank-1-small") == []

    def test_non_200_raises_rerank_error(self, monkeypatch) -> None:
        _patch_post(monkeypatch, _FakeResponse(status_code=401, text="API Key Invalid"))
        with pytest.raises(RerankError):
            rerank.rerank_results("q", [_result(1)], "bad", "zerank-1-small")


class TestApplyRerankerFallback:
    def test_success_returns_reranked_order(self) -> None:
        pool = [_result(1), _result(2)]
        out = _apply_reranker(lambda _q, _r: [pool[1], pool[0]], "q", pool)
        assert [r.chunk_id for r in out] == [2, 1]

    def test_rerank_error_falls_back_to_rrf_order(self) -> None:
        pool = [_result(1), _result(2)]

        def boom(_q, _r):
            raise RerankError("reranker down")

        out = _apply_reranker(boom, "q", pool)
        assert [r.chunk_id for r in out] == [1, 2]


class TestHybridSearchRerankIntegration:
    def test_reranks_deeper_pool_then_truncates(self, monkeypatch) -> None:
        """With a reranker, fuse the deeper pool, rerank it, then cut to max_results."""
        rows = [
            {"chunk_id": i, "document_id": 1, "content": f"c{i}", "section": "", "speaker": ""}
            for i in range(30)
        ]
        monkeypatch.setattr(hybrid, "_semantic_search", lambda *a, **k: rows)
        monkeypatch.setattr(hybrid, "_keyword_search", lambda *a, **k: [])

        seen: dict[str, int] = {}

        def reranker(_q, results: list[SearchResult]) -> list[SearchResult]:
            seen["pool"] = len(results)
            return list(reversed(results))

        out = hybrid.hybrid_search(client=None, query="q", query_embedding=[0.1], reranker=reranker)
        # Reranker saw the deep pool (all 30 candidates, > the default 20), not 20.
        assert seen["pool"] == 30
        # Returned set is truncated to max_results.
        assert len(out) == hybrid.MAX_RESULTS
