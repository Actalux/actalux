"""Unit tests for the per-board read layer (graph.store), with an in-memory client.

These cover the Model B reads that route tests patch out: member_by_slug resolving
``persons.slug`` + body to the right per-board subject (and falling back to a subject
slug pre-migration), body_members exposing the public person slug, place_lexicon
collapsing a person's per-board subjects back to one entry, and the person dossier
spine. Mis-attributing a member is the worst failure here, so the resolution paths
get real assertions rather than only end-to-end route smoke tests.
"""

from __future__ import annotations

from typing import Any

from actalux.graph.store import body_members, member_by_slug, person_dossier, place_lexicon


class _Resp:
    def __init__(self, data: list[dict], count: int | None = None) -> None:
        self.data = data
        self.count = count


class _Query:
    """A tiny chainable stand-in for the supabase/PostgREST query builder."""

    def __init__(self, rows: list[dict]) -> None:
        self._rows = list(rows)
        self._count = False
        self._order: str | None = None
        self._desc = False
        self._range: tuple[int, int] | None = None
        self._limit: int | None = None

    def select(self, *_cols: str, count: str | None = None) -> _Query:
        self._count = count == "exact"
        return self

    def eq(self, col: str, val: Any) -> _Query:
        self._rows = [r for r in self._rows if r.get(col) == val]
        return self

    def in_(self, col: str, vals: list) -> _Query:
        wanted = set(vals)
        self._rows = [r for r in self._rows if r.get(col) in wanted]
        return self

    def order(self, col: str, desc: bool = False) -> _Query:
        self._order = col
        self._desc = desc
        return self

    def range(self, start: int, end: int) -> _Query:
        self._range = (start, end)
        return self

    def limit(self, n: int) -> _Query:
        self._limit = n
        return self

    def execute(self) -> _Resp:
        rows = self._rows
        total = len(rows)  # PostgREST exact count is the full matching set, pre-limit
        if self._order is not None:
            rows = sorted(
                rows,
                key=lambda r: (r.get(self._order) is None, r.get(self._order) or ""),
                reverse=self._desc,
            )
        if self._range is not None:
            start, end = self._range
            rows = rows[start : end + 1]
        elif self._limit is not None:
            rows = rows[: self._limit]
        return _Resp(rows, total if self._count else None)


class _FakeClient:
    def __init__(self, **tables: list[dict]) -> None:
        self._tables = tables

    def table(self, name: str) -> _Query:
        return _Query(self._tables.get(name, []))


# A cross-body person (Buse: council=2 primary, plan-commission=3) plus a single-board
# person (Reim on PC), in the post-migration shape the seeder writes.
def _clayton_graph() -> _FakeClient:
    return _FakeClient(
        persons=[
            {"id": 10, "slug": "susan-buse", "canonical_name": "Susan Buse", "publishable": True},
            {"id": 11, "slug": "ron-reim", "canonical_name": "Ron Reim", "publishable": True},
        ],
        subjects=[
            {
                "id": 1,
                "slug": "susan-buse",
                "canonical_name": "Susan Buse",
                "metadata": {"role": "Councilmember"},
                "person_id": 10,
                "entity_id": 2,
                "place_id": 1,
                "type": "person",
                "publishable": True,
            },
            {
                "id": 2,
                "slug": "susan-buse--plan-commission",
                "canonical_name": "Susan Buse",
                "metadata": {"role": "Commissioner"},
                "person_id": 10,
                "entity_id": 3,
                "place_id": 1,
                "type": "person",
                "publishable": True,
            },
            {
                "id": 3,
                "slug": "ron-reim",
                "canonical_name": "Ron Reim",
                "metadata": {"role": "Commissioner"},
                "person_id": 11,
                "entity_id": 3,
                "place_id": 1,
                "type": "person",
                "publishable": True,
            },
        ],
        memberships=[
            {
                "subject_id": 1,
                "entity_id": 2,
                "role": "Councilmember",
                "start_date": "2020-06-23",
                "end_date": None,
            },
            {
                "subject_id": 2,
                "entity_id": 3,
                "role": "Commissioner",
                "start_date": None,
                "end_date": None,
            },
            {
                "subject_id": 3,
                "entity_id": 3,
                "role": "Commissioner",
                "start_date": None,
                "end_date": None,
            },
        ],
        subject_aliases=[
            {"subject_id": 1, "raw_alias": "Buse", "normalized_alias": "buse", "source": "roster"},
            {
                "subject_id": 1,
                "raw_alias": "Susan Buse",
                "normalized_alias": "susan buse",
                "source": "roster",
            },
            {"subject_id": 2, "raw_alias": "Buse", "normalized_alias": "buse", "source": "roster"},
            {
                "subject_id": 2,
                "raw_alias": "Susan Buse",
                "normalized_alias": "susan buse",
                "source": "roster",
            },
            {"subject_id": 3, "raw_alias": "Reim", "normalized_alias": "reim", "source": "roster"},
        ],
        entities=[
            {"id": 2, "body_slug": "council", "place_id": 1},
            {"id": 3, "body_slug": "plan-commission", "place_id": 1},
        ],
        member_vote_records=[
            {"subject_id": 1, "entity_id": 2, "meeting_date": "2021-01-01"},
            {"subject_id": 1, "entity_id": 2, "meeting_date": "2023-05-05"},
            {"subject_id": 2, "entity_id": 3, "meeting_date": "2024-02-02"},
        ],
    )


def test_member_by_slug_resolves_each_board_via_person() -> None:
    client = _clayton_graph()
    # The public persons.slug resolves to the per-board subject for the body in the path.
    council = member_by_slug(client, place_id=1, slug="susan-buse", entity_id=2)
    pc = member_by_slug(client, place_id=1, slug="susan-buse", entity_id=3)
    assert council is not None and pc is not None
    assert council["id"] == 1 and council["slug"] == "susan-buse"  # subject id is per-board
    assert pc["id"] == 2 and pc["slug"] == "susan-buse"  # public slug, NOT the internal one
    assert council["role"] == "Councilmember"
    assert pc["role"] == "Commissioner"


def test_member_by_slug_not_a_member_of_this_body() -> None:
    client = _clayton_graph()
    # Reim is on the PC only; resolving him under council returns None (no membership).
    assert member_by_slug(client, place_id=1, slug="ron-reim", entity_id=2) is None


def test_member_by_slug_fallback_to_subject_slug_pre_migration() -> None:
    # Before the per-board migration there are no persons rows; resolution must still
    # work by subject slug so member URLs never 404 across the migration window.
    client = _FakeClient(
        persons=[],
        subjects=[
            {
                "id": 9,
                "slug": "ron-reim",
                "canonical_name": "Ron Reim",
                "metadata": {},
                "person_id": None,
                "entity_id": 3,
                "place_id": 1,
                "type": "person",
                "publishable": True,
            }
        ],
        memberships=[
            {
                "subject_id": 9,
                "entity_id": 3,
                "role": "Commissioner",
                "start_date": None,
                "end_date": None,
            }
        ],
    )
    member = member_by_slug(client, place_id=1, slug="ron-reim", entity_id=3)
    assert member is not None and member["id"] == 9 and member["slug"] == "ron-reim"


def test_member_by_slug_unknown_returns_none() -> None:
    assert member_by_slug(_clayton_graph(), place_id=1, slug="nobody", entity_id=2) is None


def test_body_members_exposes_public_person_slug() -> None:
    client = _clayton_graph()
    pc_members = body_members(client, entity_id=3)
    by_name = {m["canonical_name"]: m for m in pc_members}
    # The PC roster includes Buse via her per-board subject, but the link uses her
    # public persons.slug (clean), never the internal 'susan-buse--plan-commission'.
    assert by_name["Susan Buse"]["slug"] == "susan-buse"
    assert by_name["Ron Reim"]["slug"] == "ron-reim"


def test_place_lexicon_collapses_per_board_subjects_to_one_person() -> None:
    client = _clayton_graph()
    lex = place_lexicon(client, place_id=1)
    by_slug = {e["slug"]: e for e in lex}
    # Buse's two per-board subjects collapse to ONE lexicon entry carrying both bodies
    # (the pre-split contract the ledger/diarization consumers depend on).
    assert "susan-buse--plan-commission" not in by_slug
    buse = by_slug["susan-buse"]
    assert {b["body_slug"] for b in buse["bodies"]} == {"council", "plan-commission"}
    assert {a["normalized"] for a in buse["aliases"]} == {"buse", "susan buse"}
    assert buse["current"] is True  # an open council term


def test_person_dossier_spans_bodies_with_cited_counts() -> None:
    client = _clayton_graph()
    dossier = person_dossier(client, "susan-buse")
    assert dossier is not None
    assert dossier["person"]["slug"] == "susan-buse"
    by_eid = {t["entity_id"]: t for t in dossier["tenures"]}
    assert set(by_eid) == {2, 3}
    assert by_eid[2]["actions"] == 2
    assert by_eid[2]["first_date"] == "2021-01-01"
    assert by_eid[2]["last_date"] == "2023-05-05"
    assert by_eid[3]["actions"] == 1


def test_person_dossier_unknown_returns_none() -> None:
    assert person_dossier(_clayton_graph(), "nobody") is None
