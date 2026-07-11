"""Evidence-family taxonomy — which *kind* of evidence anchored a voiceprint sample.

Phase C's consensus gate (``matching.enabled_officials``) enables a non-confirmed official
only when ≥2 *independent* evidence families corroborate the same acoustic voice. "Independent"
is defined here: every enrollment basis maps to a coarse family, and two samples count as
independent evidence only when their families differ. Grouping the fine-grained bases into
families keeps the consensus test honest — a roll call and a self-introduction are both
speaker-adjacency evidence (a name spoken next to a voice), so they must NOT count as two
independent votes; a roll call and a vote-alignment anchor genuinely are two mechanisms.

Any sample a human has confirmed collapses to the ``human`` family regardless of its basis: a
verified label is the strongest single evidence and is treated uniformly. An unknown basis maps
to a family named after itself, so a basis added later (a new resolver signal) is forward-
compatible — it forms its own independent family until it is explicitly grouped here.

Pure (no numpy/DB/GPU) so it is trivially unit-tested and importable anywhere.
Design: docs/architecture/voiceprint-scale-design.md (consensus enablement).
"""

from __future__ import annotations

# The speaker-identity confidence tier a human sets; also the neutral non-confirmed default lives
# in ``matching.Sample``. Kept here so both the family map and the gate read one spelling.
CONFIRMED_CONFIDENCE = "confirmed"

# Coarse evidence families. Every enrollment basis resolves to exactly one of these (or, if
# unknown, to a family named after the basis itself — see ``family_of``).
FAMILY_ADJACENCY = (
    "adjacency"  # a name spoken next to a voice: roll call, self-intro, presenter-intro
)
FAMILY_VOTE = "vote"  # a recorded-vote alignment anchor
FAMILY_DISCOURSE = "discourse"  # an LLM discourse label inferred from meeting text
FAMILY_HUMAN = "human"  # a human-confirmed / manually-entered label
FAMILY_SCREEN = "screen"  # a platform-rendered name label read off the recording (Zoom OCR)

# Fine basis -> coarse family. Roll call, self-intro, and presenter-intro are all *adjacency*
# evidence (a spoken name adjacent to a diarization cluster) and so are NOT independent of one
# another; vote, discourse, and screen_name are genuinely distinct mechanisms; 'manual' is
# human evidence.
FAMILY_OF_BASIS = {
    "rollcall": FAMILY_ADJACENCY,
    "self_intro": FAMILY_ADJACENCY,
    "presenter_intro": FAMILY_ADJACENCY,
    "vote_anchor": FAMILY_VOTE,
    "discourse": FAMILY_DISCOURSE,
    "manual": FAMILY_HUMAN,
    "screen_name": FAMILY_SCREEN,
}


def family_of(basis: str | None, confidence: str) -> str:
    """The evidence family of a voiceprint sample from its ``basis`` + ``confidence`` tier.

    A human-``confirmed`` sample is always the ``human`` family (a verified label is uniform,
    strongest single evidence, whatever basis carried it). Otherwise the fine basis maps through
    ``FAMILY_OF_BASIS``; a basis absent from the map (a future resolver signal) becomes its own
    family so it counts as independent evidence until explicitly grouped. A missing/empty basis
    on a non-confirmed sample is treated as ``human`` (the honest label a name-less anchor carries,
    matching ``enrollment.select_enrollable``'s ``basis or "manual"``).
    """
    if confidence == CONFIRMED_CONFIDENCE:
        return FAMILY_HUMAN
    if not basis:
        return FAMILY_HUMAN
    return FAMILY_OF_BASIS.get(basis, basis)
