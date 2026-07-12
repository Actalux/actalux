"""Constrained complete-linkage agglomeration — the linker's grouping stage ``[C]``.

Given a calibrated similarity matrix (higher = closer), greedily merge the two clusters with
the strongest *weakest link* (complete-linkage / farthest-neighbor, the robust choice for
speaker linking per Ghaemmaghami 2015) until no allowed merge clears the threshold. Two hard
constraints steer it:

- ``cannot_link``: distinct clusters recorded in the same meeting are different people — never
  merge them. The prohibition propagates: once two indices are cannot-linked, no merge may
  ever place them in the same node.
- ``must_link``: a very-high-confidence anchor (self-intro / Zoom active-tile label) for the
  same roster slug across meetings is the same person — pre-merge those seeds.

Pure numpy — no torch, no DB. See docs/architecture/linking-prototype-phase1.md.
"""

from __future__ import annotations

import numpy as np


def _validate_constraints(
    cannot_link: set[frozenset[int]],
    must_link: set[frozenset[int]],
) -> None:
    """Raise ``ValueError`` if any pair is both a must-link and a cannot-link (contradiction)."""
    contradictory = must_link & cannot_link
    if contradictory:
        raise ValueError(
            f"contradictory constraints: {sorted(map(sorted, contradictory))} are both "
            "must_link and cannot_link"
        )


def _complete_linkage(a: set[int], b: set[int], scores: np.ndarray) -> float:
    """Complete-linkage similarity of two clusters: the minimum score over all cross pairs."""
    return min(float(scores[i, j]) for i in a for j in b)


def _is_forbidden(a: set[int], b: set[int], cannot_link: set[frozenset[int]]) -> bool:
    """True if merging ``a`` and ``b`` would unite any cannot-link pair."""
    return any(frozenset((i, j)) in cannot_link for i in a for j in b)


def _premerge_must_link(
    clusters: list[set[int]],
    must_link: set[frozenset[int]],
) -> list[set[int]]:
    """Union the clusters implied by each must-link pair (hard seeds), in-place-safe."""
    for pair in must_link:
        members = tuple(pair)
        if len(members) != 2:
            continue  # ignore degenerate self-pairs
        i, j = members
        cluster_i = next(c for c in clusters if i in c)
        cluster_j = next(c for c in clusters if j in c)
        if cluster_i is cluster_j:
            continue
        clusters = [c for c in clusters if c is not cluster_i and c is not cluster_j]
        clusters.append(cluster_i | cluster_j)
    return clusters


def _best_merge(
    clusters: list[set[int]],
    scores: np.ndarray,
    threshold: float,
    cannot_link: set[frozenset[int]],
) -> tuple[int, int] | None:
    """Index pair of the highest-scoring allowed merge at or above threshold, or ``None``."""
    best: tuple[int, int] | None = None
    best_score = -np.inf
    for a_idx in range(len(clusters)):
        for b_idx in range(a_idx + 1, len(clusters)):
            a, b = clusters[a_idx], clusters[b_idx]
            if _is_forbidden(a, b, cannot_link):
                continue
            link = _complete_linkage(a, b, scores)
            if link >= threshold and link > best_score:
                best_score, best = link, (a_idx, b_idx)
    return best


def _label_by_first_appearance(clusters: list[set[int]], n: int) -> list[int]:
    """Assign contiguous node ids 0..K-1 in the order members first appear scanning 0..N-1."""
    labels = [-1] * n
    node_of: dict[frozenset[int], int] = {}
    next_id = 0
    for i in range(n):
        cluster = frozenset(next(c for c in clusters if i in c))
        if cluster not in node_of:
            node_of[cluster] = next_id
            next_id += 1
        labels[i] = node_of[cluster]
    return labels


def constrained_complete_linkage(
    scores: np.ndarray,
    *,
    threshold: float,
    cannot_link: set[frozenset[int]] | None = None,
    must_link: set[frozenset[int]] | None = None,
) -> list[int]:
    """Agglomerative complete-linkage clustering honoring must-link / cannot-link constraints.

    Parameters
    ----------
    scores
        Symmetric ``(N, N)`` similarity matrix (higher = closer), e.g. from
        :func:`~actalux.diarization.linking.scoring.asnorm_matrix`.
    threshold
        Stop merging once no allowed merge has complete-linkage similarity ``>= threshold``.
    cannot_link
        Pairs ``frozenset({i, j})`` that must never share a node; the prohibition propagates
        through merges.
    must_link
        Pairs ``frozenset({i, j})`` pre-merged as hard seeds before agglomeration.

    Returns
    -------
    list[int]
        Node id per index (length ``N``), contiguous ``0..K-1`` in first-appearance order.

    Raises
    ------
    ValueError
        If any pair is both a must-link and a cannot-link constraint.
    """
    cannot_link = cannot_link or set()
    must_link = must_link or set()
    _validate_constraints(cannot_link, must_link)

    n = int(np.asarray(scores).shape[0])
    if n == 0:
        return []

    clusters: list[set[int]] = [{i} for i in range(n)]
    clusters = _premerge_must_link(clusters, must_link)

    while True:
        pair = _best_merge(clusters, scores, threshold, cannot_link)
        if pair is None:
            break
        a_idx, b_idx = pair
        merged = clusters[a_idx] | clusters[b_idx]
        clusters = [c for k, c in enumerate(clusters) if k not in (a_idx, b_idx)]
        clusters.append(merged)

    return _label_by_first_appearance(clusters, n)
