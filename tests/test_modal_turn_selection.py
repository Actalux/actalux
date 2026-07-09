"""Unit tests for per-turn embedding selection (select_turn_spans). Pure, no GPU/audio."""

from __future__ import annotations

from actalux.diarization.modal_runner import (
    EMBED_MAX_TURNS,
    EMBED_MIN_SECONDS,
    EMBED_TURN_MAX_SECONDS,
    select_turn_spans,
)


def _seconds(spans: list[tuple[float, float]]) -> list[float]:
    return [round(e - s, 3) for s, e in spans]


def test_long_monologue_still_yields_several_turns():
    """A cluster whose longest turn exceeds the budget must not collapse to one turn.

    Regression (the bug that silently emptied every gallery): turns were embedded
    longest-first against ONE cumulative 180 s budget, and a single turn was truncated to the
    whole remaining budget. A 600 s opening turn consumed it, the loop broke, and the cluster
    returned exactly 1 turn — below pooling's floor, so every long-winded official (board
    members, superintendents) was dropped from the gallery without an error.
    """
    spans = [(0.0, 600.0)] + [(1000.0 + i * 30.0, 1020.0 + i * 30.0) for i in range(5)]
    selected = select_turn_spans(spans)
    assert len(selected) == 6  # not 1
    assert selected[0] == (0.0, EMBED_TURN_MAX_SECONDS)  # the monologue is truncated, not hoarded
    assert all(e - s <= EMBED_TURN_MAX_SECONDS for s, e in selected)


def test_turn_count_is_capped_longest_first():
    spans = [(float(i * 100), float(i * 100 + i + 4)) for i in range(EMBED_MAX_TURNS + 5)]
    selected = select_turn_spans(spans)
    assert len(selected) == EMBED_MAX_TURNS
    # longest-first: the selected durations are the largest available, descending
    assert _seconds(selected) == sorted(_seconds(selected), reverse=True)
    assert min(_seconds(selected)) >= EMBED_MIN_SECONDS


def test_turns_below_the_floor_never_consume_a_slot():
    """Sub-EMBED_MIN_SECONDS turns are dropped (the embedder would reject them anyway)."""
    spans = [(0.0, 1.0), (10.0, 11.5), (20.0, 30.0), (40.0, 50.0)]
    selected = select_turn_spans(spans)
    assert selected == [(20.0, 30.0), (40.0, 50.0)]


def test_selection_is_deterministic_for_equal_length_turns():
    spans = [(50.0, 60.0), (10.0, 20.0), (30.0, 40.0)]
    assert select_turn_spans(spans) == select_turn_spans(list(reversed(spans)))


def test_empty_cluster_selects_nothing():
    assert select_turn_spans([]) == []
