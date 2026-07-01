"""Safety test for the orphan-speaker-identity cleanup (pure, fake client).

The load-bearing invariant of a destructive script: it must select ONLY identities on
superseded documents (``replaces_id`` set) and never a current document's. This pins
that discriminator so a future edit can't silently widen the delete to live rows.
"""

from __future__ import annotations

from typing import Any

import scripts.cleanup_orphan_speaker_identities as mod


class _Resp:
    def __init__(self, data: list[dict]) -> None:
        self.data = data


class _Not:
    def __init__(self, query: _Query) -> None:
        self._q = query

    def is_(self, col: str, val: str) -> _Query:
        if val == "null":  # PostgREST `not.is.null` -> keep rows where the column is set
            self._q._rows = [r for r in self._q._rows if r.get(col) is not None]
        return self._q


class _Query:
    def __init__(self, rows: list[dict]) -> None:
        self._rows = [dict(r) for r in rows]

    def select(self, *_cols: str, **_kw: Any) -> _Query:
        return self

    def in_(self, col: str, vals: list) -> _Query:
        wanted = set(vals)
        self._rows = [r for r in self._rows if r.get(col) in wanted]
        return self

    @property
    def not_(self) -> _Not:
        return _Not(self)

    def execute(self) -> _Resp:
        return _Resp(self._rows)


class _Client:
    def __init__(self, docs: list[dict]) -> None:
        self._docs = docs

    def table(self, _name: str) -> _Query:
        return _Query(self._docs)


def test_superseded_doc_ids_selects_only_replaced() -> None:
    docs = [
        {"id": 1, "replaces_id": None},  # current -> excluded
        {"id": 2, "replaces_id": 99},  # superseded -> included
        {"id": 3, "replaces_id": None},  # current -> excluded
        {"id": 4, "replaces_id": 7},  # superseded -> included
    ]
    assert mod._superseded_doc_ids(_Client(docs), [1, 2, 3, 4]) == {2, 4}


def test_superseded_doc_ids_batches_without_dropping() -> None:
    # More docs than one batch (_BATCH=100): every superseded id must still be found.
    docs = [{"id": i, "replaces_id": (i - 1 if i % 2 == 0 else None)} for i in range(1, 251)]
    expected = {i for i in range(1, 251) if i % 2 == 0}
    assert mod._superseded_doc_ids(_Client(docs), [d["id"] for d in docs]) == expected
