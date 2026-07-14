"""Turn linked voice-nodes into official-identity proposals for human review.

The cross-meeting linker groups anonymous clusters into voice-nodes. This turns a node into a NAMED
proposal ONLY by propagating an official anchor already inside the node (or a matched gallery
prototype): the node's un-anchored clusters inherit that official as a below-gate, human-reviewed
proposal. Safety invariants (docs/architecture/linking-backend-decision-2026-07-12.md, migrate_040):

  1. A name propagates only from an EXISTING official anchor — un-anchored/citizen nodes get
     nothing, so schools' protected classes (personnel/teachers/students) can never be named here.
  2. A node holding two different officials is AMBIGUOUS (a suspected bad merge) — proposes nothing.
  3. Proposals are written (by the script) at confidence 'inferred_medium', basis 'voiceprint' —
     below the public-display RLS gate, never self-enrolling, always human-confirmed to enroll.

Pure — the Modal embedding and DB writes live in scripts/linking/propose_identities.py.
"""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np


def per_condition_prototypes(samples: list[tuple[np.ndarray, str]]) -> dict[str, np.ndarray]:
    """L2-normalized mean voiceprint per acoustic condition (dual per-condition prototypes).

    An official's Zoom samples and room-mic samples average into SEPARATE centroids, so a query is
    scored against the condition-matched prototype instead of one blurred average. A condition with
    no sample is absent from the result.
    """
    by_cond: dict[str, list[np.ndarray]] = {}
    for vec, cond in samples:
        by_cond.setdefault(cond, []).append(np.asarray(vec, dtype=np.float64))
    protos: dict[str, np.ndarray] = {}
    for cond, vecs in by_cond.items():
        mean = np.mean(np.stack(vecs), axis=0)
        norm = float(np.linalg.norm(mean))
        protos[cond] = mean / norm if norm > 0 else mean
    return protos


def resolve_node_official(member_indices: list[int], index_official: dict[int, int]) -> int | None:
    """The single official a node unambiguously belongs to, or ``None``.

    Returns the ``person_id`` when the node's anchored members all point to ONE official; ``None``
    when the node has no anchor (nothing to propagate) or two+ distinct officials (ambiguous — a
    suspected bad merge that must not name anyone).
    """
    officials = {index_official[i] for i in member_indices if i in index_official}
    return next(iter(officials)) if len(officials) == 1 else None


@dataclass(frozen=True)
class Proposal:
    """One below-gate identity proposal for an un-anchored cluster, plus its match evidence."""

    document_id: int
    cluster_label: str
    person_id: int
    score: float  # best pair score to the node's anchored member(s) of this official
    margin: float  # score minus the best pair score to any OTHER official's anchor
    node_id: int


def build_proposals(
    pred: list[int],
    index_official: dict[int, int],
    scores: np.ndarray,
    identity: list[tuple[int, str] | None],
) -> list[Proposal]:
    """Propose the node's official for every un-anchored cluster in a single-official node.

    Parameters
    ----------
    pred
        Node id per cluster index (from ``constrained_complete_linkage``).
    index_official
        Maps an anchored index (a confirmed official cluster, or a virtual gallery prototype) to its
        ``person_id``. Indices absent here are un-anchored.
    scores
        The ``(N, N)`` pair-score matrix (AS-norm) the linking used.
    identity
        ``(document_id, cluster_label)`` per index, or ``None`` for a virtual gallery-prototype row
        (which anchors but is never itself proposed).

    Returns
    -------
    list[Proposal]
        One per un-anchored REAL cluster in a single-official node. ``score`` is its best pair score
        to that official's anchors in the node; ``margin`` is ``score`` minus the best pair score to
        any anchor of a DIFFERENT official (small/negative margins are still emitted — the human
        adjudicates — but recorded so the reviewer sees a weak match).
    """
    nodes: dict[int, list[int]] = {}
    for idx, node in enumerate(pred):
        nodes.setdefault(node, []).append(idx)
    by_official: dict[int, list[int]] = {}
    for idx, pid in index_official.items():
        by_official.setdefault(pid, []).append(idx)

    proposals: list[Proposal] = []
    for node_id, members in nodes.items():
        official = resolve_node_official(members, index_official)
        if official is None:
            continue
        own_anchors = [i for i in members if index_official.get(i) == official]
        other_anchors = [i for pid, idxs in by_official.items() if pid != official for i in idxs]
        for idx in members:
            entry = identity[idx]
            if idx in index_official or entry is None:
                continue  # anchored, or a virtual prototype row -> never proposed
            document_id, cluster_label = entry
            score = max(float(scores[idx, a]) for a in own_anchors)
            if other_anchors:
                margin = score - max(float(scores[idx, a]) for a in other_anchors)
            else:
                margin = score
            proposals.append(Proposal(document_id, cluster_label, official, score, margin, node_id))
    return proposals
