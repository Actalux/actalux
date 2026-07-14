"""Frozen AS-norm cohort — load the active background-voice yardstick for a place.

The cross-meeting linker scores AS-norm (:func:`actalux.diarization.linking.scoring.asnorm_matrix`)
against a diverse, target-disjoint impostor cohort. In production that cohort is FROZEN and stored
(``linking_cohort_vectors``, migrate_047) rather than sampled from the trial set at runtime — a
self/trial cohort is transductive (adding a meeting silently re-scores old identities) and
re-introduces speaker imbalance (docs/architecture/linking-backend-decision-2026-07-12.md). This
module loads the active cohort's vectors as an ``(M, 256)`` matrix. The vectors are unlabeled by
design — there is no identity to attach.
"""

from __future__ import annotations

import json

import numpy as np
from supabase import Client

from actalux.db import fetch_all_rows


def _parse_embedding(value: object) -> list[float]:
    """Coerce a pgvector ``VECTOR`` column into a list of floats.

    PostgREST returns a ``VECTOR`` as a bracketed string (``"[1,2,3]"``) or, when already decoded,
    a sequence. Accept both so the loader is robust to client/serialization differences.
    """
    if isinstance(value, str):
        return [float(x) for x in json.loads(value)]
    return [float(x) for x in value]  # type: ignore[union-attr]


def _active_cohort_row(client: Client, place_id: int | None) -> dict | None:
    """The active cohort for ``place_id``, preferring a place-scoped one over a shared one.

    A place-scoped active cohort wins; otherwise a shared cohort (``place_id IS NULL``) is used, so
    a new town can lean on a shared/open-corpus background until it has its own.
    """
    if place_id is not None:
        scoped = fetch_all_rows(
            lambda: (
                client.table("linking_cohorts")
                .select("id,slug,model,place_id")
                .eq("is_active", True)
                .eq("place_id", place_id)
            )
        )
        if scoped:
            return scoped[0]
    shared = fetch_all_rows(
        lambda: (
            client.table("linking_cohorts")
            .select("id,slug,model,place_id")
            .eq("is_active", True)
            .is_("place_id", "null")
        )
    )
    return shared[0] if shared else None


def load_active_cohort(client: Client, place_id: int | None) -> np.ndarray:
    """Load the active frozen cohort's vectors as an ``(M, 256)`` float64 matrix.

    Returns an empty ``(0, 0)`` matrix when no active cohort exists (or it has no vectors). The
    caller decides how to treat that — production hard-fails; the measurement CLI can fall back to a
    self-sampled cohort.
    """
    cohort = _active_cohort_row(client, place_id)
    if cohort is None:
        return np.empty((0, 0))
    rows = fetch_all_rows(
        lambda: (
            client.table("linking_cohort_vectors").select("embedding").eq("cohort_id", cohort["id"])
        )
    )
    if not rows:
        return np.empty((0, 0))
    return np.asarray([_parse_embedding(r["embedding"]) for r in rows], dtype=np.float64)
