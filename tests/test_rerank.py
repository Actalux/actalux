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


class TestProviders:
    """The vendor table, which is what makes the ZeroEntropy sunset survivable.

    A wrong URL or model string would not raise — it would rerank against the
    wrong endpoint or silently fall back to RRF — so the request each provider
    actually builds is pinned here rather than trusted.
    """

    def _capture(self, monkeypatch, provider: str = "zeroentropy") -> dict:
        """Record the outgoing request, answering under that provider's own key."""
        sent: dict = {}
        results_key = rerank.PROVIDERS[provider].results_key

        def fake_post(url, json=None, headers=None, timeout=None):  # noqa: A002
            sent["url"] = url
            sent["json"] = json
            sent["headers"] = headers
            return _FakeResponse(payload={results_key: [{"index": 0}, {"index": 1}]})

        monkeypatch.setattr(httpx, "post", fake_post)
        return sent

    def test_zeroentropy_is_the_default_so_production_is_unchanged(self, monkeypatch) -> None:
        sent = self._capture(monkeypatch)
        rerank.rerank_results("q", [_result(1), _result(2)], "key", "zerank-1-small")
        assert sent["url"] == "https://api.zeroentropy.dev/v1/models/rerank"
        assert sent["json"]["latency"] == "fast"

    def test_cohere_request_shape(self, monkeypatch) -> None:
        sent = self._capture(monkeypatch, "cohere")
        rerank.rerank_results("q", [_result(1), _result(2)], "key", "", provider="cohere")
        assert sent["url"] == "https://api.cohere.com/v2/rerank"
        assert sent["json"]["model"] == "rerank-v3.5"
        assert sent["json"]["max_tokens_per_doc"] == 1024
        assert "latency" not in sent["json"]  # a ZeroEntropy-only field
        assert sent["headers"]["Authorization"] == "Bearer key"

    def test_voyage_request_shape(self, monkeypatch) -> None:
        sent = self._capture(monkeypatch, "voyage")
        rerank.rerank_results("q", [_result(1), _result(2)], "key", "", provider="voyage")
        assert sent["url"] == "https://api.voyageai.com/v1/rerank"
        assert sent["json"]["model"] == "rerank-2.5-lite"
        assert sent["json"]["truncation"] is True
        assert "max_tokens_per_doc" not in sent["json"]  # a Cohere-only field

    def test_explicit_model_overrides_the_provider_default(self, monkeypatch) -> None:
        sent = self._capture(monkeypatch, "voyage")
        rerank.rerank_results("q", [_result(1), _result(2)], "key", "rerank-2.5", provider="voyage")
        assert sent["json"]["model"] == "rerank-2.5"

    def test_every_provider_reorders_from_its_own_response_key(self, monkeypatch) -> None:
        # Each answers a list of {"index", "relevance_score"} — but under its own
        # top-level key. Voyage uses `data` where the others use `results`, and its
        # published docs say `results`, so this was found by calling the API. Read
        # the key from the table, not from a shared assumption.
        for name, spec in rerank.PROVIDERS.items():
            monkeypatch.setattr(
                httpx,
                "post",
                lambda *a, _k=spec.results_key, **kw: _FakeResponse(
                    payload={_k: [{"index": 1}, {"index": 0}]}
                ),
            )
            out = rerank.rerank_results("q", [_result(10), _result(20)], "key", "", provider=name)
            assert [r.chunk_id for r in out] == [20, 10], name

    def test_voyage_reads_data_not_results(self, monkeypatch) -> None:
        # Pinned separately because it is the one divergence, and a silent revert
        # to "results" would make every Voyage rerank fall back to RRF order —
        # search would still answer, just unranked, with nothing surfacing it.
        assert rerank.PROVIDERS["voyage"].results_key == "data"
        _patch_post(monkeypatch, _FakeResponse(payload={"data": [{"index": 1}, {"index": 0}]}))
        out = rerank.rerank_results("q", [_result(7), _result(8)], "key", "", provider="voyage")
        assert [r.chunk_id for r in out] == [8, 7]

    def test_unknown_provider_names_the_known_set(self) -> None:
        with pytest.raises(ValueError, match="cohere"):
            rerank.get_provider("nope")


class TestMalformedProviderResponse:
    """A vendor answering oddly must degrade to RRF, not 500 the search."""

    def test_out_of_range_index_raises_rerank_error(self, monkeypatch) -> None:
        # Would otherwise be an IndexError at the subscript, which the search
        # boundary does not catch — so the whole request fails instead of falling
        # back to RRF order.
        _patch_post(monkeypatch, _FakeResponse(payload={"results": [{"index": 99}]}))
        with pytest.raises(RerankError, match="out-of-range"):
            rerank.rerank_results("q", [_result(1), _result(2)], "key", "")

    def test_missing_results_key_raises_rerank_error(self, monkeypatch) -> None:
        _patch_post(monkeypatch, _FakeResponse(payload={"unexpected": []}))
        with pytest.raises(RerankError, match="unreadable"):
            rerank.rerank_results("q", [_result(1)], "key", "")

    def test_a_bad_response_degrades_to_rrf_at_the_search_boundary(self, monkeypatch) -> None:
        _patch_post(monkeypatch, _FakeResponse(payload={"results": [{"index": 99}]}))
        pool = [_result(1), _result(2)]
        reranker = lambda q, rs: rerank.rerank_results(q, rs, "key", "")  # noqa: E731
        assert [r.chunk_id for r in _apply_reranker(reranker, "q", pool)] == [1, 2]
