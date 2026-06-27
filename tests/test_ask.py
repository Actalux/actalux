"""Tests for the Ask page (the cited, multi-turn, server-stateless chatbot).

Covers the pure helpers (history parsing/bounding, source selection, the daily
cap) and the route behaviour (happy path, per-IP + daily caps, empty question),
patching the retrieval/LLM stack so no network call is made.
"""

from __future__ import annotations

import json
from types import SimpleNamespace
from unittest.mock import Mock, patch

from fastapi import HTTPException
from fastapi.testclient import TestClient

from actalux.search.summarize import Summary
from actalux.web import app as app_module
from actalux.web.api import _reset_rate_limits
from actalux.web.app import (
    _blocked_turn,
    _bound_history,
    _cited_sources,
    _history_to_turns,
    _parse_ask_history,
    _register_ask_within_daily_cap,
    app,
)

client = TestClient(app, raise_server_exceptions=False)

_FAKE_ENTITY = {
    "id": 1,
    "body_slug": "schools",
    "type": "school_district",
    "display_name": "Clayton School District",
    "place": {"state": "mo", "slug": "clayton", "display_name": "Clayton"},
}

# Duck-typed config for the pure-helper tests (only the bound fields are read).
_CFG = SimpleNamespace(ask_history_max_turns=8, ask_history_max_chars=8000)

# Full fake config for route tests.
_ROUTE_CFG = SimpleNamespace(
    openrouter_api_key="sk-test",
    openrouter_base_url="https://openrouter.ai/api/v1",
    summary_model="openai/gpt-5-mini",
    condense_model="openai/gpt-4o-mini",
    rate_limit_ask_per_minute=8,
    ask_daily_message_cap=400,
    ask_history_max_turns=8,
    ask_history_max_chars=8000,
    ask_question_max_chars=2000,
)

_ENRICHED = [
    {
        "chunk_id": 1,
        "hash_id": "#q0001",
        "content": "The ending fund balance rose to $24M.",
        "section": "Budget Summary",
        "meeting_date": "2024-07-01",
        "meeting_title": "2024-2025 Budget",
        "document_id": 262,
        "document_type": "budget",
        "summary": "",
    },
    {
        "chunk_id": 2,
        "hash_id": "#q0002",
        "content": "Revenue was $80M.",
        "section": "Revenue",
        "meeting_date": "2024-07-01",
        "meeting_title": "2024-2025 Budget",
        "document_id": 262,
        "document_type": "budget",
        "summary": "",
    },
]


class TestParseAskHistory:
    def test_empty_string_is_empty(self) -> None:
        assert _parse_ask_history("", _CFG) == []

    def test_malformed_json_is_empty(self) -> None:
        assert _parse_ask_history("{not json", _CFG) == []

    def test_non_list_is_empty(self) -> None:
        assert _parse_ask_history('{"role": "user"}', _CFG) == []

    def test_valid_pairs_survive(self) -> None:
        raw = '[{"role":"user","content":"hi"},{"role":"assistant","content":"hello [#q0001]"}]'
        assert _parse_ask_history(raw, _CFG) == [
            {"role": "user", "content": "hi"},
            {"role": "assistant", "content": "hello [#q0001]"},
        ]

    def test_bad_roles_and_empty_content_dropped(self) -> None:
        raw = (
            '[{"role":"system","content":"x"},'
            '{"role":"user","content":"  "},'
            '{"role":"assistant","content":42},'
            '{"role":"user","content":"keep"}]'
        )
        assert _parse_ask_history(raw, _CFG) == [{"role": "user", "content": "keep"}]

    def test_turn_cap_keeps_most_recent(self) -> None:
        cfg = SimpleNamespace(ask_history_max_turns=2, ask_history_max_chars=8000)
        raw = (
            '[{"role":"user","content":"a"},'
            '{"role":"assistant","content":"b"},'
            '{"role":"user","content":"c"}]'
        )
        out = _parse_ask_history(raw, cfg)
        assert [t["content"] for t in out] == ["b", "c"]


class TestBoundHistory:
    def test_char_budget_trims_oldest_first(self) -> None:
        cfg = SimpleNamespace(ask_history_max_turns=8, ask_history_max_chars=10)
        history = [
            {"role": "user", "content": "aaaaa"},
            {"role": "assistant", "content": "bbbbb"},
            {"role": "user", "content": "ccccc"},
        ]
        out = _bound_history(history, cfg)
        # Total 15 > 10; oldest dropped until <= 10 -> last two (10 chars).
        assert [t["content"] for t in out] == ["bbbbb", "ccccc"]


class TestHistoryToTurns:
    def test_pairs_user_and_assistant(self) -> None:
        history = [
            {"role": "user", "content": "q1"},
            {"role": "assistant", "content": "a1"},
            {"role": "user", "content": "q2"},
        ]
        turns = _history_to_turns(history)
        assert [t["question"] for t in turns] == ["q1", "q2"]
        assert turns[0]["answer_html"] == "a1"
        assert turns[1]["answer_html"] == ""  # trailing user, no answer yet

    def test_answer_text_is_html_escaped(self) -> None:
        history = [
            {"role": "user", "content": "q"},
            {"role": "assistant", "content": "<b>x</b>"},
        ]
        assert _history_to_turns(history)[0]["answer_html"] == "&lt;b&gt;x&lt;/b&gt;"


class TestCitedSources:
    def test_only_cited_rows_in_citation_order(self) -> None:
        text = "Revenue was high. [#q0002]"
        out = _cited_sources(text, _ENRICHED)
        assert [r["chunk_id"] for r in out] == [2]

    def test_dedup_repeated_citation(self) -> None:
        text = "A [#q0001] then again [#q0001]."
        out = _cited_sources(text, _ENRICHED)
        assert [r["chunk_id"] for r in out] == [1]

    def test_uncited_returns_empty(self) -> None:
        assert _cited_sources("No citations here.", _ENRICHED) == []


class TestDailyCap:
    def test_increments_and_blocks_at_cap(self) -> None:
        app_module._ask_daily_count.clear()
        assert _register_ask_within_daily_cap(2) is True
        assert _register_ask_within_daily_cap(2) is True
        assert _register_ask_within_daily_cap(2) is False
        app_module._ask_daily_count.clear()

    def test_zero_cap_blocks_immediately(self) -> None:
        app_module._ask_daily_count.clear()
        assert _register_ask_within_daily_cap(0) is False


class TestBlockedTurn:
    def test_shape(self) -> None:
        t = _blocked_turn("why?", "slow down")
        assert t["blocked"] is True
        assert t["question"] == "why?"
        assert t["answer_html"] == "slow down"
        assert t["summary"] is None and t["sources"] == []


def _patch_route(**overrides):
    """Common patch stack for route tests; overrides replace individual mocks."""
    defaults = {
        "get_entity_by_path": patch(
            "actalux.web.app.get_entity_by_path", return_value=_FAKE_ENTITY
        ),
        "get_db": patch("actalux.web.app._get_db", return_value=Mock()),
        "get_config": patch("actalux.web.app._get_config", return_value=_ROUTE_CFG),
        "embed": patch("actalux.web.app._embed_query", return_value=[0.0] * 384),
        "reranker": patch("actalux.web.app._reranker", return_value=None),
        "condense": patch("actalux.web.app.condense_question", side_effect=lambda h, q, *a, **k: q),
        "assemble": patch("actalux.web.app.assemble_evidence", return_value=(_ENRICHED, "text")),
        "summary": patch(
            "actalux.web.app.generate_summary",
            return_value=Summary(
                text="The ending fund balance rose. [#q0001]",
                citations_found=1,
                citations_verified=1,
                citations_dropped=0,
            ),
        ),
    }
    defaults.update(overrides)
    return defaults


class TestAskRoute:
    def setup_method(self) -> None:
        _reset_rate_limits()
        app_module._ask_daily_count.clear()

    def test_get_renders_page(self) -> None:
        with (
            patch("actalux.web.app._get_db"),
            patch("actalux.web.app.get_entity_by_path", return_value=_FAKE_ENTITY),
        ):
            r = client.get("/mo/clayton/schools/ask")
        assert r.status_code == 200
        assert "Ask the archive" in r.text
        assert 'id="ask-history"' in r.text

    def test_happy_path_returns_cited_turn_and_grows_history(self) -> None:
        patches = _patch_route()
        with (
            patches["get_entity_by_path"],
            patches["get_db"],
            patches["get_config"],
            patches["embed"],
            patches["reranker"],
            patches["condense"],
            patches["assemble"],
            patches["summary"],
        ):
            r = client.post(
                "/mo/clayton/schools/ask",
                data={"q": "How did the fund balance change?", "history": "[]"},
                headers={"HX-Request": "true"},
            )
        assert r.status_code == 200
        # Cited answer links to the chunk source.
        assert "/chunk/1/source" in r.text
        assert "ending fund balance rose" in r.text
        # OOB history input present and grown beyond the empty list.
        assert 'hx-swap-oob="true"' in r.text
        assert 'value="[]"' not in r.text
        # The Sources disclosure lists the cited passage.
        assert "Sources (1)" in r.text

    def test_empty_question_htmx_returns_nothing(self) -> None:
        with (
            patch("actalux.web.app.get_entity_by_path", return_value=_FAKE_ENTITY),
            patch("actalux.web.app._get_db"),
            patch("actalux.web.app._get_config", return_value=_ROUTE_CFG),
        ):
            r = client.post(
                "/mo/clayton/schools/ask",
                data={"q": "   ", "history": "[]"},
                headers={"HX-Request": "true"},
            )
        assert r.status_code == 200
        assert r.text == ""

    def test_daily_cap_blocks_without_calling_llm(self) -> None:
        cfg = SimpleNamespace(**{**_ROUTE_CFG.__dict__, "ask_daily_message_cap": 0})
        gen = Mock()
        with (
            patch("actalux.web.app.get_entity_by_path", return_value=_FAKE_ENTITY),
            patch("actalux.web.app._get_db"),
            patch("actalux.web.app._get_config", return_value=cfg),
            patch("actalux.web.app.generate_summary", gen),
        ):
            r = client.post(
                "/mo/clayton/schools/ask",
                data={"q": "anything", "history": "[]"},
                headers={"HX-Request": "true"},
            )
        assert r.status_code == 200
        assert "question limit" in r.text  # apostrophe is HTML-escaped in output
        assert 'value="[]"' in r.text  # blocked turn does not grow history
        gen.assert_not_called()

    def test_long_question_is_bounded_before_llm(self) -> None:
        seen: dict[str, str] = {}

        def capture(history, question, *a, **k):
            seen["question"] = question
            return question

        patches = _patch_route(condense=patch("actalux.web.app.condense_question", capture))
        with (
            patches["get_entity_by_path"],
            patches["get_db"],
            patches["get_config"],
            patches["embed"],
            patches["reranker"],
            patches["condense"],
            patches["assemble"],
            patches["summary"],
        ):
            client.post(
                "/mo/clayton/schools/ask",
                data={"q": "x" * 5000, "history": "[]"},
                headers={"HX-Request": "true"},
            )
        assert len(seen["question"]) == _ROUTE_CFG.ask_question_max_chars

    def test_rate_limit_blocks_without_calling_llm(self) -> None:
        gen = Mock()
        with (
            patch("actalux.web.app.get_entity_by_path", return_value=_FAKE_ENTITY),
            patch("actalux.web.app._get_db"),
            patch("actalux.web.app._get_config", return_value=_ROUTE_CFG),
            patch("actalux.web.app._enforce_rate", side_effect=HTTPException(status_code=429)),
            patch("actalux.web.app.generate_summary", gen),
        ):
            r = client.post(
                "/mo/clayton/schools/ask",
                data={"q": "anything", "history": "[]"},
                headers={"HX-Request": "true"},
            )
        assert r.status_code == 200
        assert "asking quickly" in r.text
        gen.assert_not_called()


class TestAskStreamRoute:
    """The streaming /ask/stream endpoint emits verified sentences + a done event."""

    def setup_method(self) -> None:
        _reset_rate_limits()
        app_module._ask_daily_count.clear()

    def _stream_patches(self, **overrides):
        def fake_stream(query, results, *a, **k):
            yield "The ending fund balance rose. [#q0001]"
            yield Summary(
                text="The ending fund balance rose. [#q0001]",
                citations_found=1,
                citations_verified=1,
                citations_dropped=0,
            )

        p = _patch_route(**overrides)
        p["stream"] = patch("actalux.web.app.generate_summary_stream", side_effect=fake_stream)
        return p

    @staticmethod
    def _events(text: str) -> list[dict]:
        return [json.loads(line) for line in text.splitlines() if line.strip()]

    def test_streams_sentence_then_done_with_history(self) -> None:
        p = self._stream_patches()
        with (
            p["get_entity_by_path"],
            p["get_db"],
            p["get_config"],
            p["embed"],
            p["reranker"],
            p["condense"],
            p["assemble"],
            p["stream"],
        ):
            r = client.post(
                "/mo/clayton/schools/ask/stream",
                data={"q": "How has the fund balance changed?", "history": "[]"},
            )
        assert r.status_code == 200
        events = self._events(r.text)
        types = [e["type"] for e in events]
        assert "sentence" in types
        assert types[-1] == "done"
        sentence = next(e for e in events if e["type"] == "sentence")
        assert "fund balance rose" in sentence["html"]
        assert "/chunk/" in sentence["html"]  # citation rendered as a source link
        done = next(e for e in events if e["type"] == "done")
        hist = json.loads(done["history"])
        assert hist[-2] == {"role": "user", "content": "How has the fund balance changed?"}
        assert hist[-1]["role"] == "assistant"
        assert "Sources" in done["sources_html"]

    def test_rate_limit_streams_notice_not_answer(self) -> None:
        # Condense must never run when the per-IP limiter trips first.
        p = self._stream_patches(
            condense=patch(
                "actalux.web.app.condense_question",
                side_effect=AssertionError("condense should not run when rate-limited"),
            ),
        )
        with (
            patch("actalux.web.app._enforce_rate", side_effect=HTTPException(status_code=429)),
            p["get_entity_by_path"],
            p["get_db"],
            p["get_config"],
            p["embed"],
            p["reranker"],
            p["condense"],
            p["assemble"],
            p["stream"],
        ):
            r = client.post("/mo/clayton/schools/ask/stream", data={"q": "hi", "history": "[]"})
        events = self._events(r.text)
        assert any(e["type"] == "notice" for e in events)
        assert all(e["type"] != "sentence" for e in events)
