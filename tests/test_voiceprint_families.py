"""Unit tests for the evidence-family taxonomy (Phase C consensus substrate)."""

from __future__ import annotations

from actalux.diarization.families import (
    FAMILY_ADJACENCY,
    FAMILY_DISCOURSE,
    FAMILY_HUMAN,
    FAMILY_VOTE,
    family_of,
)


def test_adjacency_bases_are_one_family():
    # roll call, self-intro, presenter-intro are all "a name spoken next to a voice" -> NOT
    # independent of one another, so they must map to the SAME family.
    for basis in ("rollcall", "self_intro", "presenter_intro"):
        assert family_of(basis, "inferred_high") == FAMILY_ADJACENCY


def test_distinct_mechanisms_are_distinct_families():
    assert family_of("vote_anchor", "inferred_high") == FAMILY_VOTE
    assert family_of("discourse", "inferred_medium") == FAMILY_DISCOURSE


def test_confirmed_collapses_to_human_regardless_of_basis():
    for basis in ("rollcall", "vote_anchor", "discourse", "manual", None, "anything"):
        assert family_of(basis, "confirmed") == FAMILY_HUMAN


def test_manual_and_missing_basis_are_human():
    assert family_of("manual", "inferred_high") == FAMILY_HUMAN
    assert family_of(None, "inferred_high") == FAMILY_HUMAN
    assert family_of("", "inferred_high") == FAMILY_HUMAN


def test_unknown_basis_is_its_own_family():
    # A resolver signal added later forms its own independent family until explicitly grouped.
    assert family_of("new_signal", "inferred_high") == "new_signal"
    assert family_of("gesture", "inferred_low") == "gesture"
