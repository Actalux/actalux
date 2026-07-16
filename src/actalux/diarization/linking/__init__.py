"""Cross-meeting speaker-linking prototype (phase 1) — pure-numpy library.

Standalone, DB/Modal/torch-free implementation of the linker's score → cluster → evaluate
stages plus the resolution-side evidence ledger. Jurisdiction-general: no place/body
constants live here. See docs/architecture/linking-prototype-phase1.md.
"""

from __future__ import annotations

from actalux.diarization.linking.benchmark import (
    cannot_link_audit,
    cannot_link_same_meeting,
    loo_operating_point,
    poison_blast_radius,
)
from actalux.diarization.linking.cluster import constrained_complete_linkage
from actalux.diarization.linking.evaluate import (
    bcubed_prf,
    coverage,
    macro_recall_by_official,
    pairwise_prf,
    per_condition_pair_f1,
    purity,
)
from actalux.diarization.linking.ledger import (
    FAMILY_WITHIN_DISCOUNT,
    EvidenceLedger,
    EvidenceObservation,
)
from actalux.diarization.linking.observations import (
    VoiceNode,
    VoiceObservation,
    embedding_matrix,
    load_observations,
    save_observations,
)
from actalux.diarization.linking.scoring import (
    AS_NORM_COHORT_TOPK,
    EPS,
    asnorm_matrix,
    cosine_matrix,
)

__all__ = [
    "AS_NORM_COHORT_TOPK",
    "EPS",
    "FAMILY_WITHIN_DISCOUNT",
    "EvidenceLedger",
    "EvidenceObservation",
    "VoiceNode",
    "VoiceObservation",
    "asnorm_matrix",
    "bcubed_prf",
    "cannot_link_audit",
    "cannot_link_same_meeting",
    "constrained_complete_linkage",
    "cosine_matrix",
    "coverage",
    "embedding_matrix",
    "load_observations",
    "loo_operating_point",
    "macro_recall_by_official",
    "pairwise_prf",
    "per_condition_pair_f1",
    "poison_blast_radius",
    "purity",
    "save_observations",
]
