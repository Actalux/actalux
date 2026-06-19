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
        openai_api_key="sk-test",
        expansion_model="gpt-4o-mini",
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
            patch.object(retrieval, "get_config", return_value=_cfg(openai_api_key="")),
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
