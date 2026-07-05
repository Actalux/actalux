"""Shared voiceprint-enrollment primitives (pure, no DB/GPU).

Used by both the steady-state enroller (``scripts/enroll_voiceprints.py``) and the
recalibration harness (``scripts/recalibrate_voiceprints.py``) so there is one mechanism
for "which clusters are enrollable" and "how a cluster's turns become one gallery row."
Design: docs/architecture/voiceprint-recalibration-plan.md §3-§4.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from actalux.diarization.pooling import Pooled, pool_turn_embeddings

# Name anchors are deterministic (a spoken name -> this voice), so enrolling from an auto
# inferred_high with one of these bases is safe. 'presenter_intro' and 'discourse' seed at
# inferred_medium (below the public bar) — both are corroborated name evidence whose imprecision
# is contained by the gallery's own acoustic gates. basis='voiceprint' is NEVER enrollable --
# that would let a biometric guess train the gallery (poison loop).
NAME_ANCHOR_BASES = ("rollcall", "self_intro", "vote_anchor", "presenter_intro", "discourse")
# Bases admitted at inferred_medium (held below the public-display gate) yet still enrollable —
# a presenter introduction and an LLM discourse label. A roll call / self-intro must be
# inferred_high to enroll; these two are trusted one tier lower because their error is contained
# downstream by the gallery's label-purity + calibration gates.
_MEDIUM_ENROLLABLE_BASES = ("presenter_intro", "discourse")


@dataclass(frozen=True)
class EnrollableCluster:
    """A confirmed/name-anchored official cluster eligible to enter the gallery."""

    person_id: int
    source_subject_id: int
    source_identity_id: int
    document_id: int
    cluster_label: str
    source_basis: str
    canonical_name: str
    # The speaker-identity tier this cluster was drawn from; carried into the calibration
    # ``Sample`` so Gate A can trust human-``confirmed`` samples as a core. Defaults to a neutral
    # non-confirmed tier so a caller that omits it (or an older fixture) behaves as before.
    confidence: str = "inferred_high"


def select_enrollable(
    identities: list[dict[str, Any]],
    subjects_by_id: dict[int, dict[str, Any]],
    *,
    confirmed_only: bool,
) -> list[EnrollableCluster]:
    """Filter identity rows to enrollable official clusters.

    Eligible when the cluster maps to a publishable subject with a ``person_id`` and is
    either human-``confirmed`` or (unless ``confirmed_only``) a name anchor at its clean
    tier: an ``inferred_high`` roll call / self-intro, or an ``inferred_medium``
    ``presenter_intro`` / ``discourse`` (held below the public bar but still enrollable). All
    bases must be in ``NAME_ANCHOR_BASES``; ``basis='voiceprint'`` is never eligible.
    """
    out: list[EnrollableCluster] = []
    for row in identities:
        subject_id = row.get("subject_id")
        if subject_id is None:
            continue
        subject = subjects_by_id.get(subject_id)
        if not subject or not subject.get("publishable") or subject.get("person_id") is None:
            continue
        confidence, basis = row.get("confidence"), row.get("basis")
        if confidence == "rejected":
            continue  # a human-denied cluster never enrolls (survives resolver re-passes)
        if basis == "voiceprint":
            continue  # never train the gallery on a biometric guess
        # A name anchor seeds the gallery at its clean tier: roll call / self-intro publish
        # at inferred_high; presenter_intro / discourse are deliberately held at inferred_medium
        # (below the public-display gate) yet still enroll, their imprecision contained downstream.
        eligible = confidence == "confirmed" or (
            not confirmed_only
            and basis in NAME_ANCHOR_BASES
            and (
                confidence == "inferred_high"
                or (basis in _MEDIUM_ENROLLABLE_BASES and confidence == "inferred_medium")
            )
        )
        if not eligible:
            continue
        out.append(
            EnrollableCluster(
                person_id=subject["person_id"],
                source_subject_id=subject_id,
                source_identity_id=row["id"],
                document_id=row["document_id"],
                cluster_label=row["cluster_label"],
                # a human-confirmed row may carry no basis; 'manual' is the honest label
                # and satisfies the source_basis NOT NULL + CHECK (migrate_040).
                source_basis=basis or "manual",
                canonical_name=subject.get("canonical_name", "?"),
                confidence=confidence or "inferred_high",
            )
        )
    return out


def cluster_spans(turns: list[dict[str, Any]], cluster_label: str) -> list[list[float]]:
    """``[[start_s, end_s], ...]`` for one cluster, in time order, from its turn rows."""
    spans = [
        [float(t["start_seconds"]), float(t["end_seconds"])]
        for t in turns
        if t["cluster_label"] == cluster_label
    ]
    return sorted(spans, key=lambda s: s[0])


def span_seconds(spans: list[list[float]]) -> float:
    """Total speech seconds across a cluster's spans (a quality estimate for dry-run)."""
    return sum(max(0.0, b - a) for a, b in spans)


def superseded_doc_ids(docs: list[dict[str, Any]]) -> set[int]:
    """Ids of documents that have been superseded (``replaces_id`` set)."""
    return {d["id"] for d in docs if d.get("replaces_id") is not None}


def pool_cluster(
    turns: list[tuple[tuple[float, ...], float]],
    *,
    trim_fraction: float,
    min_coherent_turns: int,
    purity_floor: float,
) -> Pooled | None:
    """Pool a cluster's per-turn ``(vector, seconds)`` list into one voiceprint (Gate B)."""
    if not turns:
        return None
    vectors = [v for v, _ in turns]
    durations = [s for _, s in turns]
    return pool_turn_embeddings(
        vectors,
        durations,
        trim_fraction=trim_fraction,
        min_coherent_turns=min_coherent_turns,
        purity_floor=purity_floor,
    )


def voiceprint_row(
    ec: EnrollableCluster, pooled: Pooled, model: str, *, calibration_id: int | None = None
) -> dict[str, Any]:
    """A ``subject_voiceprints`` insert row for one enrolled cluster (with purity provenance)."""
    return {
        "person_id": ec.person_id,
        "source_subject_id": ec.source_subject_id,
        "source_document_id": ec.document_id,
        "source_identity_id": ec.source_identity_id,
        "cluster_label": ec.cluster_label,
        "embedding": list(pooled.vector),
        "source_basis": ec.source_basis,
        "model": model,
        "seconds": round(pooled.seconds, 2),
        "purity": round(pooled.purity, 4),
        "n_turns": pooled.n_turns,
        "coherent_turns": pooled.coherent_turns,
        "calibration_id": calibration_id,
    }
