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
# inferred_high with one of these bases is safe. basis='voiceprint' is NEVER enrollable --
# that would let a biometric guess train the gallery (poison loop).
NAME_ANCHOR_BASES = ("rollcall", "self_intro", "vote_anchor")


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


def select_enrollable(
    identities: list[dict[str, Any]],
    subjects_by_id: dict[int, dict[str, Any]],
    *,
    confirmed_only: bool,
) -> list[EnrollableCluster]:
    """Filter identity rows to enrollable official clusters.

    Eligible when the cluster maps to a publishable subject with a ``person_id`` and is
    either human-``confirmed`` or (unless ``confirmed_only``) a name-anchored
    ``inferred_high`` (``NAME_ANCHOR_BASES``). ``basis='voiceprint'`` is never eligible.
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
        if basis == "voiceprint":
            continue  # never train the gallery on a biometric guess
        eligible = confidence == "confirmed" or (
            not confirmed_only and confidence == "inferred_high" and basis in NAME_ANCHOR_BASES
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
