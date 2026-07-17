"""Tests for the identity proposer (pure node-to-proposal logic; safety guards)."""

from __future__ import annotations

import numpy as np

from actalux.diarization.linking.proposer import (
    build_proposals,
    per_condition_prototypes,
    resolve_node_official,
    unanchored_recurring_nodes,
)


def test_per_condition_prototypes_separate_and_normalized() -> None:
    samples = [
        (np.array([2.0, 0.0]), "zoom"),
        (np.array([0.0, 2.0]), "zoom"),
        (np.array([1.0, 1.0]), "in_person"),
    ]
    protos = per_condition_prototypes(samples)
    assert set(protos) == {"zoom", "in_person"}
    assert np.isclose(np.linalg.norm(protos["zoom"]), 1.0)
    assert np.isclose(np.linalg.norm(protos["in_person"]), 1.0)


def test_resolve_node_official_single_empty_ambiguous() -> None:
    assert resolve_node_official([0, 1], {0: 100, 1: 100}) == 100  # one official
    assert resolve_node_official([0, 1], {}) is None  # no anchor -> nothing to propagate
    assert resolve_node_official([0, 1], {0: 100, 1: 200}) is None  # two officials -> ambiguous


def test_build_proposals_propagates_single_official() -> None:
    scores = np.array([[1.0, 0.9, 0.1], [0.9, 1.0, 0.1], [0.1, 0.1, 1.0]])
    pred = [0, 0, 1]  # idx0,idx1 in one node; idx2 in another
    index_official = {0: 100, 2: 200}  # idx1 is un-anchored
    identity: list[tuple[int, str] | None] = [(10, "S0"), (11, "S0"), (12, "S0")]
    proposals = build_proposals(pred, index_official, scores, identity)
    assert len(proposals) == 1
    p = proposals[0]
    assert (p.document_id, p.cluster_label, p.person_id) == (11, "S0", 100)
    assert p.score == 0.9
    assert np.isclose(p.margin, 0.8)  # 0.9 to official 100 minus 0.1 to official 200


def test_build_proposals_records_runner_up_alternatives() -> None:
    # a reviewer judging a thin margin needs to see WHO the voice nearly matched, not just by how
    # much: official 200 is the runner-up at 0.1, and the margin is measured against it
    scores = np.array([[1.0, 0.9, 0.1], [0.9, 1.0, 0.1], [0.1, 0.1, 1.0]])
    proposals = build_proposals(
        [0, 0, 1], {0: 100, 2: 200}, scores, [(10, "S0"), (11, "S0"), (12, "S0")]
    )
    assert proposals[0].alternatives == ((200, 0.1),)


def test_build_proposals_alternatives_empty_without_other_officials() -> None:
    scores = np.array([[1.0, 0.9], [0.9, 1.0]])
    proposals = build_proposals([0, 0], {0: 100}, scores, [(10, "S0"), (11, "S0")])
    assert proposals[0].alternatives == ()
    assert proposals[0].margin == proposals[0].score  # nothing to measure a margin against


def test_build_proposals_ambiguous_node_proposes_nothing() -> None:
    scores = np.eye(3)
    pred = [0, 0, 0]  # all one node
    index_official = {0: 100, 2: 200}  # node holds two officials -> ambiguous
    identity: list[tuple[int, str] | None] = [(10, "S0"), (11, "S1"), (12, "S2")]
    assert build_proposals(pred, index_official, scores, identity) == []


def test_build_proposals_virtual_prototype_anchors_but_is_not_proposed() -> None:
    # index 0 is a gallery prototype (identity None): it anchors the official but is never proposed
    scores = np.array([[1.0, 0.9], [0.9, 1.0]])
    pred = [0, 0]
    index_official = {0: 100}
    identity: list[tuple[int, str] | None] = [None, (11, "S0")]
    proposals = build_proposals(pred, index_official, scores, identity)
    assert len(proposals) == 1
    assert (proposals[0].document_id, proposals[0].person_id) == (11, 100)


def test_unanchored_recurring_nodes_flags_multi_meeting_nameless_voice() -> None:
    # node 0: 3 meetings, no anchor -> flagged; node 1: anchored -> never flagged;
    # node 2: nameless but only 2 meetings -> below the recurrence bar
    pred = [0, 0, 0, 1, 1, 2, 2]
    identity = [(1, "S0"), (2, "S0"), (3, "S0"), (4, "S0"), (5, "S0"), (6, "S0"), (7, "S0")]
    seconds = [10.0, 20.0, 30.0, 5.0, 5.0, 5.0, 5.0]
    flagged = unanchored_recurring_nodes(pred, {3: 42}, identity, seconds, min_meetings=3)
    assert [n["node_id"] for n in flagged] == [0]
    assert flagged[0]["n_meetings"] == 3
    assert flagged[0]["total_seconds"] == 60.0


def test_unanchored_recurring_nodes_ignores_virtual_prototypes() -> None:
    # a gallery prototype (identity None) neither counts as a meeting nor names the node here —
    # but it IS an anchor, so its node is excluded
    pred = [0, 0, 0, 0]
    identity = [(1, "S0"), (2, "S0"), (3, "S0"), None]
    flagged = unanchored_recurring_nodes(
        pred, {3: 42}, identity, [10.0, 10.0, 10.0, 600.0], min_meetings=3
    )
    assert flagged == []


def test_unanchored_recurring_nodes_sorted_widest_first() -> None:
    pred = [0, 0, 0, 1, 1, 1, 1]
    identity = [(1, "S0"), (2, "S0"), (3, "S0"), (4, "S0"), (5, "S0"), (6, "S0"), (7, "S0")]
    seconds = [1.0] * 7
    flagged = unanchored_recurring_nodes(pred, {}, identity, seconds, min_meetings=3)
    assert [n["n_meetings"] for n in flagged] == [4, 3]
