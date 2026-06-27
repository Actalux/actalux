"""Tests for the shared retrieval helpers (``web.retrieval``).

``expand_and_embed`` is the query-expansion entry point: gated by config and
best-effort — it degrades to ``[]`` on any failure so a recall optimization can
never break a search. These tests patch the LLM + embedder, so no network call
or model load happens.
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import patch

from actalux.web import retrieval


def _cfg(**overrides) -> SimpleNamespace:
    base = dict(
        query_expansion_mode="on",
        openrouter_api_key="sk-test",
        openrouter_base_url="https://openrouter.ai/api/v1",
        expansion_model="openai/gpt-4o-mini",
        expansion_count=3,
    )
    base.update(overrides)
    return SimpleNamespace(**base)


class TestExpandAndEmbed:
    def test_disabled_returns_empty_without_llm(self) -> None:
        with (
            patch.object(retrieval, "get_config", return_value=_cfg(query_expansion_mode="off")),
            patch.object(retrieval, "generate_query_variants") as gen,
        ):
            assert retrieval.expand_and_embed("bond measure") == []
        gen.assert_not_called()

    def test_no_api_key_returns_empty_without_llm(self) -> None:
        with (
            patch.object(retrieval, "get_config", return_value=_cfg(openrouter_api_key="")),
            patch.object(retrieval, "generate_query_variants") as gen,
        ):
            assert retrieval.expand_and_embed("q") == []
        gen.assert_not_called()

    def test_pairs_variants_with_embeddings(self) -> None:
        with (
            patch.object(retrieval, "get_config", return_value=_cfg()),
            patch.object(
                retrieval,
                "generate_query_variants",
                return_value=["Proposition O", "bond issue"],
            ),
            patch.object(retrieval, "embed_queries", return_value=[[0.1], [0.2]]),
        ):
            out = retrieval.expand_and_embed("bond measure")
        assert out == [("Proposition O", [0.1]), ("bond issue", [0.2])]

    def test_no_variants_skips_embedding(self) -> None:
        with (
            patch.object(retrieval, "get_config", return_value=_cfg()),
            patch.object(retrieval, "generate_query_variants", return_value=[]),
            patch.object(retrieval, "embed_queries") as emb,
        ):
            assert retrieval.expand_and_embed("q") == []
        emb.assert_not_called()

    def test_degrades_to_empty_on_failure(self) -> None:
        with patch.object(retrieval, "get_config", side_effect=RuntimeError("no env")):
            assert retrieval.expand_and_embed("q") == []


_PAIRS = [("york", "Jeffery Yorg"), ("merrimack", "Meramec")]


class TestApplyCorrections:
    def test_mangling_expands_to_canonical(self) -> None:
        # a query carrying the ASR mangling also searches the canonical spelling
        assert retrieval.apply_corrections("York voted aye", _PAIRS) == ["Jeffery Yorg voted aye"]

    def test_canonical_expands_to_mangling(self) -> None:
        # and the reverse: the canonical also reaches chunks that carry the mangling
        assert retrieval.apply_corrections("Jeffery Yorg motion", _PAIRS) == ["york motion"]

    def test_case_insensitive(self) -> None:
        assert retrieval.apply_corrections("MERRIMACK Ave", _PAIRS) == ["Meramec Ave"]

    def test_word_boundaried_no_substring_false_match(self) -> None:
        # 'york' must not fire inside 'Yorktown'
        assert retrieval.apply_corrections("Yorktown Road", _PAIRS) == []

    def test_no_match_returns_empty(self) -> None:
        assert retrieval.apply_corrections("school budget", _PAIRS) == []

    def test_respects_cap(self) -> None:
        pairs = [(f"m{i}", f"C{i}") for i in range(20)]
        query = " ".join(f"m{i}" for i in range(20))
        assert len(retrieval.apply_corrections(query, pairs, cap=5)) == 5


class TestCorrectionVariants:
    def test_none_place_returns_empty(self) -> None:
        assert retrieval.correction_variants("York", None) == []

    def test_uses_loaded_corrections(self) -> None:
        with patch.object(retrieval, "_load_corrections", return_value=_PAIRS):
            assert retrieval.correction_variants("York", 10) == ["Jeffery Yorg"]

    def test_degrades_to_empty_on_failure(self) -> None:
        with patch.object(retrieval, "_load_corrections", side_effect=RuntimeError("db")):
            assert retrieval.correction_variants("York", 10) == []


class TestSearchExpansions:
    def test_combines_llm_and_corrections(self) -> None:
        with (
            patch.object(retrieval, "expand_and_embed", return_value=[("Prop O", [0.9])]),
            patch.object(retrieval, "correction_variants", return_value=["Jeffery Yorg"]),
            patch.object(retrieval, "embed_queries", return_value=[[0.5]]),
        ):
            out = retrieval.search_expansions("York", 10)
        assert out == [("Prop O", [0.9]), ("Jeffery Yorg", [0.5])]

    def test_no_corrections_returns_llm_only(self) -> None:
        with (
            patch.object(retrieval, "expand_and_embed", return_value=[("Prop O", [0.9])]),
            patch.object(retrieval, "correction_variants", return_value=[]),
            patch.object(retrieval, "embed_queries") as emb,
        ):
            out = retrieval.search_expansions("q", 10)
        assert out == [("Prop O", [0.9])]
        emb.assert_not_called()

    def test_dedupes_correction_against_llm_variant(self) -> None:
        with (
            patch.object(retrieval, "expand_and_embed", return_value=[("Meramec", [0.9])]),
            patch.object(retrieval, "correction_variants", return_value=["Meramec"]),
            patch.object(retrieval, "embed_queries") as emb,
        ):
            out = retrieval.search_expansions("merrimack", 10)
        assert out == [("Meramec", [0.9])]  # the duplicate correction text is dropped
        emb.assert_not_called()
