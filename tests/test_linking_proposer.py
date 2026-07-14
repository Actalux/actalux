"""Tests for the identity proposer (pure node-to-proposal logic; safety guards)."""

from __future__ import annotations

import numpy as np

from actalux.diarization.linking.proposer import (
    build_proposals,
    per_condition_prototypes,
    resolve_node_official,
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
