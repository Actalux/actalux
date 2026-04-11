"""Tests for hybrid search RRF logic.

RRF is a pure function — no database or embedding model needed.
"""

from actalux.search.hybrid import SearchResult, _reciprocal_rank_fusion

RRF_K = 60


class TestReciprocalRankFusion:
    """Test RRF combination of semantic and keyword results."""

    def _make_row(self, chunk_id: int, doc_id: int = 1) -> dict:
        return {
            "chunk_id": chunk_id,
            "document_id": doc_id,
            "content": f"chunk {chunk_id} content",
            "section": "Test Section",
            "speaker": "",
        }

    def test_empty_inputs(self) -> None:
        results = _reciprocal_rank_fusion([], [], max_results=10)
        assert results == []

    def test_semantic_only(self) -> None:
        semantic = [self._make_row(1), self._make_row(2)]
        results = _reciprocal_rank_fusion(semantic, [], max_results=10)
        assert len(results) == 2
        assert results[0].chunk_id == 1
        assert results[0].semantic_rank == 1
        assert results[0].keyword_rank is None

    def test_keyword_only(self) -> None:
        keyword = [self._make_row(3), self._make_row(4)]
        results = _reciprocal_rank_fusion([], keyword, max_results=10)
        assert len(results) == 2
        assert results[0].chunk_id == 3
        assert results[0].keyword_rank == 1
        assert results[0].semantic_rank is None

    def test_overlap_ranks_higher(self) -> None:
        """A chunk appearing in both lists should rank above one in only one list."""
        # chunk 10 appears in both; chunk 1 is #1 semantic only; chunk 20 is #1 keyword only
        semantic = [self._make_row(1), self._make_row(10)]
        keyword = [self._make_row(20), self._make_row(10)]

        results = _reciprocal_rank_fusion(semantic, keyword, max_results=10)

        # chunk 10 gets score from both lists, should be first
        assert results[0].chunk_id == 10
        assert results[0].semantic_rank == 2
        assert results[0].keyword_rank == 2

    def test_rrf_score_calculation(self) -> None:
        """Verify exact RRF score for a chunk appearing at rank 1 in both lists."""
        semantic = [self._make_row(1)]
        keyword = [self._make_row(1)]

        results = _reciprocal_rank_fusion(semantic, keyword, max_results=10)
        expected_score = 1.0 / (RRF_K + 1) + 1.0 / (RRF_K + 1)

        assert len(results) == 1
        assert abs(results[0].rrf_score - expected_score) < 1e-10

    def test_max_results_limit(self) -> None:
        semantic = [self._make_row(i) for i in range(30)]
        results = _reciprocal_rank_fusion(semantic, [], max_results=5)
        assert len(results) == 5

    def test_ordering_by_score(self) -> None:
        """Higher-ranked items in source lists should produce higher RRF scores."""
        semantic = [self._make_row(i) for i in range(1, 6)]
        results = _reciprocal_rank_fusion(semantic, [], max_results=5)

        scores = [r.rrf_score for r in results]
        assert scores == sorted(scores, reverse=True)

    def test_result_type(self) -> None:
        semantic = [self._make_row(1)]
        results = _reciprocal_rank_fusion(semantic, [], max_results=10)
        assert isinstance(results[0], SearchResult)
        assert results[0].content == "chunk 1 content"
        assert results[0].section == "Test Section"

    def test_disjoint_lists(self) -> None:
        """Two lists with no overlap — all items should appear."""
        semantic = [self._make_row(1), self._make_row(2)]
        keyword = [self._make_row(3), self._make_row(4)]
        results = _reciprocal_rank_fusion(semantic, keyword, max_results=10)
        ids = {r.chunk_id for r in results}
        assert ids == {1, 2, 3, 4}
