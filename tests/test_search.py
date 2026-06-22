"""Tests for hybrid search behavior.

RRF is a pure function — no database or embedding model needed.
"""

from datetime import date

import pytest

from actalux.errors import SearchError
from actalux.search.hybrid import (
    AGENDA_RANK_PENALTY,
    SearchFilters,
    SearchResult,
    _demote_low_priority_types,
    _fuse_ranked_lists,
    _keyword_search,
    _normalize_fts_query,
    _reciprocal_rank_fusion,
    _semantic_search,
    hybrid_search,
)

RRF_K = 60


def _row(chunk_id: int, doc_id: int = 1) -> dict:
    return {
        "chunk_id": chunk_id,
        "document_id": doc_id,
        "content": f"chunk {chunk_id} content",
        "section": "",
        "speaker": "",
    }


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


class _FakeRpc:
    def __init__(self, data: list[dict]) -> None:
        self.data = data

    def execute(self) -> "_FakeRpc":
        return self


class _FakeClient:
    def __init__(self) -> None:
        self.calls: list[tuple[str, dict]] = []

    def rpc(self, name: str, params: dict) -> _FakeRpc:
        self.calls.append((name, params))
        return _FakeRpc([{"chunk_id": 1}])


class TestSearchRpcParams:
    """Search RPC calls should pass filters in the database contract shape."""

    def test_semantic_search_passes_filters(self) -> None:
        client = _FakeClient()
        filters = SearchFilters(
            date_from=date(2024, 1, 1),
            date_to=date(2024, 12, 31),
            document_type="minutes",
        )

        rows = _semantic_search(client, [0.1, 0.2], filters)

        assert rows == [{"chunk_id": 1}]
        assert client.calls == [
            (
                "semantic_search",
                {
                    "query_embedding": [0.1, 0.2],
                    "match_threshold": 0.35,
                    "match_count": 50,
                    "filter_date_from": "2024-01-01",
                    "filter_date_to": "2024-12-31",
                    "filter_doc_type": "minutes",
                },
            )
        ]

    def test_keyword_search_omits_empty_filters(self) -> None:
        client = _FakeClient()

        rows = _keyword_search(client, "budget", SearchFilters())

        assert rows == [{"chunk_id": 1}]
        assert client.calls == [
            (
                "keyword_search",
                {
                    "search_query": "budget",
                    "match_count": 50,
                },
            )
        ]

    def test_searches_pass_entity_filter(self) -> None:
        client = _FakeClient()
        filters = SearchFilters(entity_id=7)

        _semantic_search(client, [0.1, 0.2], filters)
        _keyword_search(client, "budget", filters)

        assert client.calls[0][1]["filter_entity_id"] == 7
        assert client.calls[1][1]["filter_entity_id"] == 7

    def test_searches_omit_entity_filter_when_unset(self) -> None:
        client = _FakeClient()

        _semantic_search(client, [0.1, 0.2], SearchFilters())

        assert "filter_entity_id" not in client.calls[0][1]

    def test_keyword_search_normalizes_hyphens(self) -> None:
        # A hyphenated term must reach the FTS RPC de-hyphenated, or
        # websearch_to_tsquery demands a compound lexeme the OCR'd corpus
        # rarely has (see _normalize_fts_query). Regression guard for fin05.
        client = _FakeClient()

        _keyword_search(client, "per-pupil expenditure by building", SearchFilters())

        assert client.calls[0][1]["search_query"] == "per pupil expenditure by building"


class TestNormalizeFtsQuery:
    """The FTS query is de-hyphenated to avoid compound-lexeme phrase queries."""

    def test_ascii_hyphen(self) -> None:
        assert _normalize_fts_query("per-pupil") == "per pupil"

    def test_en_and_em_dashes(self) -> None:
        assert _normalize_fts_query("grades 9–12") == "grades 9 12"
        assert _normalize_fts_query("budget—2024") == "budget 2024"

    def test_unaffected_query_unchanged(self) -> None:
        assert _normalize_fts_query("annual budget resolution") == "annual budget resolution"


class TestFuseRankedLists:
    """The N-list RRF fuser underpins query expansion: a chunk found only by a
    variant's list still enters the pool, and a chunk found by several lists
    accumulates score."""

    def test_union_across_variant_lists(self) -> None:
        # chunk 2 appears only in the second semantic list — it must still surface.
        results = _fuse_ranked_lists([[_row(1)], [_row(2)]], [], max_results=10)
        assert {r.chunk_id for r in results} == {1, 2}

    def test_overlap_across_lists_outranks_and_keeps_best_rank(self) -> None:
        # chunk 9: rank 2 in list A, rank 1 in list B -> accumulates, ranks first.
        results = _fuse_ranked_lists([[_row(1), _row(9)], [_row(9)]], [], max_results=10)
        assert results[0].chunk_id == 9
        assert results[0].semantic_rank == 1  # best (min) rank across the lists


class _RaiseRpc:
    """An RPC whose execute() raises — _semantic/_keyword_search wrap it as SearchError."""

    def execute(self) -> None:
        raise RuntimeError("rpc down")


class _RowFakeClient:
    """Fake Supabase client returning preset rows keyed by embedding / query."""

    def __init__(
        self,
        semantic: dict | None = None,
        keyword: dict | None = None,
        errors: set | None = None,
    ) -> None:
        self.semantic = semantic or {}  # tuple(embedding) -> rows
        self.keyword = keyword or {}  # normalized search_query -> rows
        self.errors = errors or set()  # {"semantic:<key>", "keyword:<key>"}
        self.calls: list[tuple[str, dict]] = []

    def rpc(self, name: str, params: dict):
        self.calls.append((name, params))
        if name == "semantic_search":
            key = tuple(params["query_embedding"])
            if f"semantic:{key}" in self.errors:
                return _RaiseRpc()
            return _FakeRpc(self.semantic.get(key, []))
        key = params["search_query"]
        if f"keyword:{key}" in self.errors:
            return _RaiseRpc()
        return _FakeRpc(self.keyword.get(key, []))


class TestHybridSearchExpansion:
    """hybrid_search fuses query-expansion variants alongside the primary query."""

    def test_expansion_surfaces_chunk_missed_by_primary(self) -> None:
        target = _row(608, 60)
        client = _RowFakeClient(
            semantic={(0.0,): [], (1.0,): [target]},
            keyword={"bond measure": [], "Proposition O": []},
        )
        results = hybrid_search(
            client,
            "bond measure",
            [0.0],
            SearchFilters(),
            expansions=[("Proposition O", [1.0])],
        )
        assert [r.chunk_id for r in results] == [608]

    def test_no_expansion_issues_two_rpcs(self) -> None:
        client = _RowFakeClient(semantic={(0.0,): []}, keyword={"q": []})
        hybrid_search(client, "q", [0.0], SearchFilters())
        assert len(client.calls) == 2  # one semantic + one keyword

    def test_expansion_fans_out_per_variant(self) -> None:
        client = _RowFakeClient(
            semantic={(0.0,): [], (1.0,): [], (2.0,): []},
            keyword={"q": [], "a": [], "b": []},
        )
        hybrid_search(client, "q", [0.0], SearchFilters(), expansions=[("a", [1.0]), ("b", [2.0])])
        assert len(client.calls) == 6  # 3 variants x (semantic + keyword)

    def test_expansion_variant_error_is_dropped(self) -> None:
        # The expansion's semantic RPC fails; the primary hit must still return.
        client = _RowFakeClient(
            semantic={(0.0,): [_row(1)], (1.0,): []},
            keyword={"q": [], "variant": []},
            errors={"semantic:(1.0,)"},
        )
        results = hybrid_search(
            client, "q", [0.0], SearchFilters(), expansions=[("variant", [1.0])]
        )
        assert [r.chunk_id for r in results] == [1]

    def test_primary_error_propagates(self) -> None:
        client = _RowFakeClient(errors={"semantic:(0.0,)"})
        with pytest.raises(SearchError):
            hybrid_search(client, "q", [0.0], SearchFilters(), expansions=[("v", [1.0])])


def _sr(chunk_id: int, dtype: str) -> SearchResult:
    return SearchResult(
        chunk_id=chunk_id,
        document_id=chunk_id,
        content="",
        section="",
        speaker="",
        rrf_score=0.0,
        document_type=dtype,
    )


class TestDemoteLowPriorityTypes:
    """Agendas are nudged below comparable records, but never banished."""

    def test_agenda_demoted_below_adjacent_record(self) -> None:
        out = _demote_low_priority_types([_sr(1, "agenda"), _sr(2, "minutes")])
        assert [r.chunk_id for r in out] == [2, 1]

    def test_strong_agenda_not_banished(self) -> None:
        # A top-ranked agenda drops by exactly the penalty but still outranks
        # records that were more than AGENDA_RANK_PENALTY places below it.
        records = [_sr(i, "minutes") for i in range(1, AGENDA_RANK_PENALTY + 5)]
        out = _demote_low_priority_types([_sr(0, "agenda"), *records])
        assert [r.chunk_id for r in out].index(0) == AGENDA_RANK_PENALTY
        assert out[-1].document_type == "minutes"  # not pushed to the bottom

    def test_no_agendas_unchanged(self) -> None:
        rows = [_sr(1, "minutes"), _sr(2, "transcript"), _sr(3, "minutes")]
        assert _demote_low_priority_types(rows) == rows

    def test_relative_order_among_agendas_preserved(self) -> None:
        rows = [_sr(1, "agenda"), _sr(2, "agenda"), _sr(3, "minutes")]
        assert [r.chunk_id for r in _demote_low_priority_types(rows)] == [3, 1, 2]
