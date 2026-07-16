"""Ground-truth labels for cached clusters — the one place anchors become ``person_id``s.

Every linker tool needs the same mapping: which cached ``(document_id, cluster_label)`` carries an
official anchor, and whose. The measurement CLI, the cohort bake-off, and the identity proposer all
depend on it agreeing exactly — a tool that resolved anchors even slightly differently would measure
(or propose against) a different benchmark than the others.

An anchor is an *enrollable* cluster (``enrollment.select_enrollable``): a name-anchored or
human-confirmed identity tied to a publishable person via its subject. ``basis='voiceprint'`` is
never an anchor, so a biometric guess can never become its own ground truth.

DB-backed, like :mod:`actalux.diarization.linking.cohort`; neither is re-exported from the package
``__init__``, so importing ``actalux.diarization.linking`` stays pure numpy.
"""

from __future__ import annotations

from supabase import Client

from actalux.db import fetch_all_rows
from actalux.diarization.enrollment import select_enrollable
from actalux.diarization.linking.observations import VoiceObservation


def fetch_person_labels(
    client: Client, place_id: int, obs: list[VoiceObservation]
) -> dict[tuple[int, str], int]:
    """Map each anchored ``(document_id, cluster_label)`` in ``obs`` to its official ``person_id``.

    Subjects are place-scoped so a stale cross-place ``subject_id`` cannot leak a label in.
    """
    doc_ids = sorted({o.document_id for o in obs})
    identities = fetch_all_rows(
        lambda: (
            client.table("speaker_identities")
            .select("id,document_id,cluster_label,subject_id,confidence,basis")
            .in_("document_id", doc_ids)
        )
    )
    subjects_by_id = {
        s["id"]: s
        for s in fetch_all_rows(
            lambda: (
                client.table("subjects")
                .select("id,person_id,publishable,canonical_name")
                .eq("place_id", place_id)
            )
        )
    }
    enrollable = select_enrollable(identities, subjects_by_id, confirmed_only=False)
    return {(ec.document_id, ec.cluster_label): ec.person_id for ec in enrollable}
