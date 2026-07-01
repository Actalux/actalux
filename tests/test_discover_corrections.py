"""Regression tests for the transcript scan's per-document content fetch.

Selecting ``content`` for every transcript in one query serialized tens of MB and
tripped the role ``statement_timeout`` (PG 57014) — and even the ``--manifest`` CI
path hit it, because it lists all transcripts before narrowing to the run's meetings.
The scan now lists metadata only and pulls each body on demand. These pin that
contract: the bulk select must not request ``content``, and ``_doc_text`` must fetch
content per-document only when the row was listed without it.
"""

from __future__ import annotations

import argparse
from typing import Any

import scripts.discover_corrections as dc


class _Resp:
    def __init__(self, data: list[dict]) -> None:
        self.data = data


class _RecordingQuery:
    """A chainable query stand-in that records selected columns and projects rows.

    Projection matters here: PostgREST returns only the selected columns, so a row
    listed without ``content`` has no ``content`` key at all — which is the exact
    signal ``_doc_text`` uses to decide whether to fetch the body on demand.
    """

    def __init__(self, rows: list[dict], selected: list[str]) -> None:
        self._rows = [dict(r) for r in rows]
        self._selected = selected
        self._wanted: set[str] = set()

    def select(self, *cols: str, **_kw: Any) -> _RecordingQuery:
        # Record the projection but do NOT apply it yet: PostgREST filters server-side
        # on the full row regardless of the select list, so projecting here would hide
        # filter columns (document_type, replaces_id) that select() does not name.
        self._wanted = {c.strip() for c in ",".join(cols).split(",") if c.strip()}
        self._selected.append(",".join(sorted(self._wanted)))
        return self

    def eq(self, col: str, val: Any) -> _RecordingQuery:
        self._rows = [r for r in self._rows if r.get(col) == val]
        return self

    def is_(self, col: str, val: str) -> _RecordingQuery:
        if val == "null":
            self._rows = [r for r in self._rows if r.get(col) is None]
        return self

    def in_(self, col: str, vals: list) -> _RecordingQuery:
        wanted = set(vals)
        self._rows = [r for r in self._rows if r.get(col) in wanted]
        return self

    def order(self, _col: str, desc: bool = False) -> _RecordingQuery:
        return self

    def range(self, start: int, end: int) -> _RecordingQuery:
        self._rows = self._rows[start : end + 1]
        return self

    def execute(self) -> _Resp:
        projected = [{k: v for k, v in r.items() if k in self._wanted} for r in self._rows]
        return _Resp(projected)


class _RecordingClient:
    def __init__(self, rows: list[dict]) -> None:
        self._rows = rows
        self.selected: list[str] = []

    def table(self, _name: str) -> _RecordingQuery:
        return _RecordingQuery(self._rows, self.selected)


def _args(**over: Any) -> argparse.Namespace:
    base = {"video_id": None, "meeting_date": None, "manifest": None, "limit": None}
    base.update(over)
    return argparse.Namespace(**base)


def test_select_transcripts_does_not_fetch_content() -> None:
    # The bug: `content` in the bulk select serialized every transcript body at once.
    def _t(doc_id: int, date: str, vid: str) -> dict:
        return {
            "id": doc_id,
            "entity_id": 7,
            "meeting_date": date,
            "video_id": vid,
            "content": "body",
            "document_type": "transcript",
            "replaces_id": None,
        }

    rows = [_t(1, "2024-01-01", "a"), _t(2, "2024-02-01", "b")]
    client = _RecordingClient(rows)
    got = dc._select_transcripts(client, [7], _args())

    assert client.selected == ["entity_id,id,meeting_date,video_id"]
    assert all("content" not in r for r in got)  # projected out — fetched on demand instead
    assert {r["id"] for r in got} == {1, 2}


def test_doc_text_fetches_content_on_demand_when_absent(monkeypatch) -> None:
    calls: list[int] = []

    def fake_get_content(_client: Any, doc_id: int) -> str:
        calls.append(doc_id)
        return "  On-demand body for Bill No. 7156.  "

    monkeypatch.setattr(dc, "get_document_content", fake_get_content)

    # Row has no `content` key (as listed by _select_transcripts) -> fetch by id.
    text = dc._doc_text(object(), {"id": 42})
    assert text == "On-demand body for Bill No. 7156."
    assert calls == [42]


def test_doc_text_uses_inline_content_without_fetch(monkeypatch) -> None:
    # Auth rows (agenda/minutes) still arrive with `content` inline -> no extra query.
    def boom(*_a: Any, **_k: Any) -> str:
        raise AssertionError("get_document_content must not be called when content is inline")

    monkeypatch.setattr(dc, "get_document_content", boom)
    assert dc._doc_text(object(), {"id": 9, "content": "  inline text  "}) == "inline text"


def test_doc_text_falls_back_to_chunks_when_content_empty(monkeypatch) -> None:
    monkeypatch.setattr(dc, "get_document_content", lambda *_a, **_k: "   ")
    monkeypatch.setattr(
        dc,
        "get_document_chunks",
        lambda *_a, **_k: [{"content": "chunk one"}, {"content": "chunk two"}],
    )
    assert dc._doc_text(object(), {"id": 5}) == "chunk one\n\nchunk two"
