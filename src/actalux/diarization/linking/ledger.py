"""Transparent, family-aware evidence ledger — the resolution-side scaffold.

Once the linker has grouped clusters into a voice node, resolving that node to a roster slug
is an accumulation of heterogeneous evidence: a self-intro, a Zoom screen name, a roll-call
adjacency, a vote alignment, a discourse label. This ledger scores each candidate slug with
two deliberate properties:

- **Diminishing returns within a family.** Many cues of the *same kind* (five roll calls) must
  not out-shout genuinely independent corroboration — so within a family only the strongest cue
  counts fully and the rest are discounted.
- **Multi-family agreement is rewarded.** A candidate backed by two *different* families
  (adjacency + vote) outscores one backed by a single family carrying the same total raw
  weight, because the score sums per-family contributions.

Every number is auditable via :meth:`EvidenceLedger.explain` — no hidden normalization. The
coarse family taxonomy matches ``diarization/families.py``. See
docs/architecture/linking-prototype-phase1.md.
"""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass

# Within one evidence family, cues beyond the strongest contribute at this discounted rate, so
# N cues of one kind cannot dominate genuinely independent corroboration. Locked decision.
FAMILY_WITHIN_DISCOUNT = 0.3


@dataclass(frozen=True)
class EvidenceObservation:
    """One piece of resolution evidence pointing a voice node at a candidate roster slug.

    Attributes
    ----------
    channel
        The fine-grained signal, e.g. ``"self_intro"``, ``"screen_name"``, ``"rollcall"``,
        ``"discourse"``.
    family
        The coarse evidence family the channel rolls up to, e.g. ``"adjacency"``, ``"screen"``,
        ``"vote"``, ``"discourse"``, ``"human"`` (independence is defined at family level).
    weight
        The evidence strength (confidence/quality of this single cue).
    candidate_slug
        The roster slug this evidence points at.
    source_document_id
        The recording the evidence came from (provenance for the audit trail).
    """

    channel: str
    family: str
    weight: float
    candidate_slug: str
    source_document_id: int


class EvidenceLedger:
    """Accumulates evidence observations and scores candidate slugs family-aware.

    The ledger is append-only and fully transparent: :meth:`score_by_candidate` derives scores
    with no hidden normalization, and :meth:`explain` returns the exact contributions behind
    any candidate's score.
    """

    def __init__(self) -> None:
        self._observations: list[EvidenceObservation] = []

    def add(self, obs: EvidenceObservation) -> None:
        """Append one evidence observation to the ledger."""
        self._observations.append(obs)

    def score_by_candidate(self) -> dict[str, float]:
        """Score every candidate slug.

        A family's contribution is ``max(weights) + FAMILY_WITHIN_DISCOUNT * sum(other
        weights)`` (diminishing returns within the family); the candidate's score is the sum of
        its families' contributions (rewarding multi-family agreement).

        Returns
        -------
        dict[str, float]
            Candidate slug -> score.
        """
        by_candidate: dict[str, dict[str, list[float]]] = defaultdict(lambda: defaultdict(list))
        for obs in self._observations:
            by_candidate[obs.candidate_slug][obs.family].append(obs.weight)

        scores: dict[str, float] = {}
        for candidate, families in by_candidate.items():
            scores[candidate] = sum(_family_contribution(weights) for weights in families.values())
        return scores

    def explain(self, candidate_slug: str) -> list[EvidenceObservation]:
        """Return a candidate's evidence observations, sorted by weight descending."""
        matches = [o for o in self._observations if o.candidate_slug == candidate_slug]
        return sorted(matches, key=lambda o: o.weight, reverse=True)


def _family_contribution(weights: list[float]) -> float:
    """One family's contribution: the strongest cue plus discounted remaining cues."""
    ordered = sorted(weights, reverse=True)
    return ordered[0] + FAMILY_WITHIN_DISCOUNT * sum(ordered[1:])
