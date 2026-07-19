"""Durable vote identity (vote_ref) — migrate_028 / connections-graph §4.2."""

from __future__ import annotations

import importlib.util
import sys
from datetime import date
from pathlib import Path

import pytest

from actalux.ingest.hashing import compute_vote_ref
from actalux.ingest.votes_parser import ParsedVote

# extract_votes lives in scripts/ (not an installed package); load it by path.
_path = Path(__file__).resolve().parent.parent / "scripts" / "extract_votes.py"
_spec = importlib.util.spec_from_file_location("extract_votes", _path)
ev = importlib.util.module_from_spec(_spec)
sys.modules[_spec.name] = ev
_spec.loader.exec_module(ev)


# --- compute_vote_ref (pure) ---


def test_compute_vote_ref_deterministic():
    assert compute_vote_ref("a1b2c3d4", 0) == compute_vote_ref("a1b2c3d4", 0)


def test_compute_vote_ref_ordinal_distinguishes():
    # Two motions sharing one chunk (same citation_id) must get distinct refs.
    assert compute_vote_ref("a1b2c3d4", 0) != compute_vote_ref("a1b2c3d4", 1)


def test_compute_vote_ref_citation_distinguishes():
    assert compute_vote_ref("a1b2c3d4", 0) != compute_vote_ref("ffffffff", 0)


def test_compute_vote_ref_requires_citation_id():
    with pytest.raises(ValueError):
        compute_vote_ref("", 0)


# --- _build_votes (ordinal assignment, hard-fail, skip) ---


def _pv(motion: str, anchor: str) -> ParsedVote:
    return ParsedVote(
        motion=motion,
        result="passed",
        result_basis="stated",
        vote_count_yes=7,
        vote_count_no=0,
        vote_count_abstain=0,
        moved_by="A",
        seconded_by="B",
        members=(),
        source_quote=anchor,
        anchors=(anchor,),
    )


def _doc(doc_id: int = 5) -> dict:
    return {
        "id": doc_id,
        "meeting_date": "2026-01-01",
        "content": "x",
        "source_portal": "civicplus",
    }


def test_build_votes_assigns_per_citation_ordinal(monkeypatch):
    chunk = {"id": 10, "citation_id": "deadbeef", "content": "motion one ; motion two"}
    pv1, pv2 = _pv("motion one", "motion one"), _pv("motion two", "motion two")
    monkeypatch.setattr(
        ev, "_parser_for", lambda doc: (lambda _c: [pv1, pv2], lambda _a, _ch: chunk)
    )

    votes, skipped = ev._build_votes(_doc(), [chunk])

    assert skipped == 0
    assert [v.vote_ref for v in votes] == [
        compute_vote_ref("deadbeef", 0),
        compute_vote_ref("deadbeef", 1),
    ]
    assert len({v.vote_ref for v in votes}) == 2  # distinct despite shared chunk


def test_build_votes_hard_fails_without_citation_id(monkeypatch):
    chunk = {"id": 11, "citation_id": None, "content": "motion"}
    monkeypatch.setattr(
        ev, "_parser_for", lambda doc: (lambda _c: [_pv("m", "m")], lambda _a, _ch: chunk)
    )

    # Never substitute "" (would collide every such vote) — fail loudly instead.
    with pytest.raises(SystemExit):
        ev._build_votes(_doc(6), [chunk])


def test_build_votes_skips_uncited_vote(monkeypatch):
    monkeypatch.setattr(
        ev, "_parser_for", lambda doc: (lambda _c: [_pv("m", "m")], lambda _a, _ch: None)
    )

    votes, skipped = ev._build_votes(_doc(7), [])

    assert votes == [] and skipped == 1


# --- plan_vote_reconcile (pure) ---


def _vote(ref: str) -> ev.Vote:
    return ev.Vote(
        document_id=13,
        meeting_date=date(2024, 12, 11),
        motion="Approve the agenda as posted.",
        result="passed",
        citation_id="6a4066b7",
        vote_ref=ref,
    )


def test_reconcile_unchanged_parse_updates_in_place():
    # The 3-week CI failure mode: identical re-parse must not re-insert (UNIQUE
    # (document_id, vote_ref)); it rewrites the existing rows, keeping their ids.
    prior = [(101, "ref-a"), (102, "ref-b")]
    votes = [_vote("ref-a"), _vote("ref-b")]

    to_insert, to_update, stale = ev.plan_vote_reconcile(prior, votes)

    assert to_insert == []
    assert [(i, v.vote_ref) for i, v in to_update] == [(101, "ref-a"), (102, "ref-b")]
    assert stale == []


def test_reconcile_mixed_insert_update_delete():
    prior = [(101, "ref-a"), (102, "ref-gone")]
    votes = [_vote("ref-a"), _vote("ref-new")]

    to_insert, to_update, stale = ev.plan_vote_reconcile(prior, votes)

    assert [v.vote_ref for v in to_insert] == ["ref-new"]
    assert [(i, v.vote_ref) for i, v in to_update] == [(101, "ref-a")]
    assert stale == [102]


def test_reconcile_empty_parse_prunes_everything():
    prior = [(101, "ref-a"), (102, None)]

    to_insert, to_update, stale = ev.plan_vote_reconcile(prior, [])

    assert to_insert == [] and to_update == []
    assert stale == [101, 102]


def test_reconcile_null_ref_legacy_rows_always_stale():
    # Pre-migration-028 rows carry NULL vote_ref; a fresh ref'd parse replaces them.
    prior = [(55, None)]
    votes = [_vote("ref-a")]

    to_insert, to_update, stale = ev.plan_vote_reconcile(prior, votes)

    assert [v.vote_ref for v in to_insert] == ["ref-a"]
    assert stale == [55]
