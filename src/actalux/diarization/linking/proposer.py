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

# How many runners-up to record per proposal. Enough for a reviewer to see who else the voice came
# close to; not so many that the evidence row becomes the whole roster.
ALTERNATIVES_KEPT = 3


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
    # The runners-up this match beat, best first: (person_id, score) per OTHER official. A human
    # judging a thin margin needs to see who else the voice nearly matched, not just by how much.
    alternatives: tuple[tuple[int, float], ...] = ()


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
        for idx in members:
            entry = identity[idx]
            if idx in index_official or entry is None:
                continue  # anchored, or a virtual prototype row -> never proposed
            document_id, cluster_label = entry
            score = max(float(scores[idx, a]) for a in own_anchors)
            # best score to each OTHER official: the runners-up, and the margin over them
            others = sorted(
                (
                    (pid, max(float(scores[idx, a]) for a in idxs))
                    for pid, idxs in by_official.items()
                    if pid != official
                ),
                key=lambda t: t[1],
                reverse=True,
            )
            margin = score - others[0][1] if others else score
            proposals.append(
                Proposal(
                    document_id,
                    cluster_label,
                    official,
                    score,
                    margin,
                    node_id,
                    alternatives=tuple(others[:ALTERNATIVES_KEPT]),
                )
            )
    return proposals


# A voice-node spanning fewer meetings than this is routine (one-off public comment, a visiting
# presenter); at or above it, a nameless recurring voice is worth a human look — often a new
# official or staff member the roster does not know yet.
MIN_RECURRING_MEETINGS = 3


def unanchored_recurring_nodes(
    pred: list[int],
    index_official: dict[int, int],
    identity: list[tuple[int, str] | None],
    speech_seconds: list[float],
    *,
    min_meetings: int = MIN_RECURRING_MEETINGS,
) -> list[dict[str, object]]:
    """Anchor-less voice-nodes recurring across meetings — flag-only "who is this?" candidates.

    The proposer can only name people the roster already knows; a NEW official (or recurring staff)
    first appears as a linked node with no anchor at all. Surfacing those nodes turns the linker
    into a roster-maintenance prompt without ever naming anyone: no entity, no voiceprint, no
    identity row is created here — per the tracked-vs-named content policy, a human decides whether
    the voice belongs to an official worth rostering. Sorted by meeting span, widest first.
    """
    nodes: dict[int, list[int]] = {}
    for idx, node in enumerate(pred):
        nodes.setdefault(node, []).append(idx)
    flagged: list[dict[str, object]] = []
    for node_id, members in nodes.items():
        if any(i in index_official for i in members):
            continue
        real = [i for i in members if identity[i] is not None]
        meetings = {identity[i][0] for i in real}  # type: ignore[index]
        if len(meetings) < min_meetings:
            continue
        flagged.append(
            {
                "node_id": node_id,
                "n_clusters": len(real),
                "n_meetings": len(meetings),
                "document_ids": sorted(meetings),
                "total_seconds": round(sum(speech_seconds[i] for i in real), 1),
            }
        )
    flagged.sort(key=lambda r: (-int(r["n_meetings"]), -float(r["total_seconds"])))  # type: ignore[arg-type]
    return flagged
