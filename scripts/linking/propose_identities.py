"""Propose official identities for un-named voice clusters (cross-meeting voiceprint matching).

Loads an ALL-CLUSTER embedding cache (``build_embedding_cache.py --include-unanchored``), links
clusters across meetings (AS-norm vs the FROZEN cohort + constrained complete-linkage at the
operating threshold), and for each linked node that contains an official anchor, proposes that
official for the node's un-anchored clusters (``proposer.build_proposals``). This is the payoff of
the voice-first pivot: recognizing an official in a meeting where they were never named.

Safety (docs/architecture/linking-backend-decision-2026-07-12.md, migrate_040): proposals are
written BELOW the public gate (``speaker_identities`` confidence ``inferred_medium``, basis
``voiceprint``) and audited in ``voiceprint_match_evidence``, then surfaced to a human via
``review_identities.py`` / ``confirm_speaker.py``. A biometric guess never auto-publishes, never
self-enrolls, and never overwrites a human decision (confirmed/rejected) or a name anchor.

DRY-RUN by default: prints the proposals it WOULD write. ``--write`` performs the (gated) inserts.

Run (dry-run):
    doppler run --project mac --config dev -- \\
      uv run python scripts/linking/propose_identities.py \\
      --state mo --place clayton --body schools --threshold <operating point from the LOO eval>
"""

from __future__ import annotations

import argparse
import logging
import os
from pathlib import Path

import numpy as np
from supabase import Client

from actalux.config import load_config
from actalux.db import fetch_all_rows, get_client, get_place_by_path
from actalux.diarization.enrollment import select_enrollable
from actalux.diarization.linking.benchmark import cannot_link_same_meeting
from actalux.diarization.linking.cluster import constrained_complete_linkage
from actalux.diarization.linking.cohort import _parse_embedding, load_active_cohort
from actalux.diarization.linking.observations import (
    VoiceObservation,
    embedding_matrix,
    load_observation_dir,
)
from actalux.diarization.linking.proposer import Proposal, build_proposals, per_condition_prototypes
from actalux.diarization.linking.scoring import asnorm_matrix
from actalux.errors import ActaluxError

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

EMBED_MODEL = "pyannote/wespeaker-voxceleb-resnet34-LM"  # frozen; matches the gallery + cohort
AGGREGATION = "max-anchor"  # score = max pair score to the node's anchored member(s)
# A biometric proposal must never clobber a human decision or a stronger name anchor.
PROTECTED_CONFIDENCE = {"inferred_high", "confirmed", "rejected"}


def service_client() -> Client:
    cfg = load_config()
    key = os.environ.get("ACTALUX_SUPABASE_SERVICE_KEY", "")
    if not key:
        raise ActaluxError("ACTALUX_SUPABASE_SERVICE_KEY is required (service-only tables)")
    return get_client(cfg.supabase_url, key)


def fetch_person_labels(
    client: Client, place_id: int, obs: list[VoiceObservation]
) -> dict[tuple[int, str], int]:
    """Map each anchored ``(document_id, cluster_label)`` to its official's ``person_id``.

    Mirrors ``run_linking_prototype.fetch_labels``: an anchor is an enrollable (name-anchored or
    confirmed) cluster tied to a publishable person via its subject.
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


def body_entity_ids(client: Client, place_id: int, body: str) -> list[int]:
    """Entity ids for one body_slug in a place."""
    entities = fetch_all_rows(
        lambda: client.table("entities").select("id,body_slug").eq("place_id", place_id)
    )
    ids = [e["id"] for e in entities if e.get("body_slug") == body]
    if not ids:
        raise ActaluxError(f"no entity for body {body!r} in place {place_id}")
    return ids


def load_gallery_prototypes(
    client: Client, place_id: int, entity_ids: list[int]
) -> list[tuple[np.ndarray, int]]:
    """Per-condition gallery prototypes for the body's officials (virtual anchors), cleared-only.

    Trusts a voiceprint only when its ``calibration_id`` resolves to a ``cleared`` calibration
    (migrate_041). Returns one ``(centroid, person_id)`` per (official, acoustic condition). Empty
    when the body has no cleared gallery yet (the current schools case).
    """
    cleared = {
        r["id"]
        for r in fetch_all_rows(
            lambda: (
                client.table("voiceprint_calibration")
                .select("id,status")
                .eq("place_id", place_id)
                .eq("status", "cleared")
            )
        )
    }
    if not cleared:
        return []
    body_persons = {
        s["person_id"]
        for s in fetch_all_rows(
            lambda: (
                client.table("subjects")
                .select("person_id,entity_id,publishable")
                .in_("entity_id", entity_ids)
            )
        )
        if s.get("publishable") and s.get("person_id") is not None
    }
    rows = fetch_all_rows(
        lambda: (
            client.table("subject_voiceprints")
            .select("person_id,embedding,acoustic_condition,calibration_id")
            .in_("person_id", list(body_persons))
        )
    )
    by_person: dict[int, list[tuple[np.ndarray, str]]] = {}
    for r in rows:
        if r.get("calibration_id") not in cleared:
            continue
        vec = np.asarray(_parse_embedding(r["embedding"]), dtype=np.float64)
        cond = r.get("acoustic_condition") or "unknown"
        by_person.setdefault(r["person_id"], []).append((vec, cond))
    protos: list[tuple[np.ndarray, int]] = []
    for pid, samples in by_person.items():
        for centroid in per_condition_prototypes(samples).values():
            protos.append((centroid, pid))
    return protos


def _link(
    obs: list[VoiceObservation],
    index_official: dict[int, int],
    identity: list[tuple[int, str] | None],
    embeddings: np.ndarray,
    cohort: np.ndarray,
    threshold: float,
) -> list[Proposal]:
    """Score AS-norm vs the frozen cohort, link at ``threshold``, and build the proposals."""
    scores = asnorm_matrix(embeddings, cohort)
    cannot_link = cannot_link_same_meeting(obs)  # only real clusters (indices < len(obs))
    pred = constrained_complete_linkage(scores, threshold=threshold, cannot_link=cannot_link)
    return build_proposals(pred, index_official, scores, identity)


def _write_proposals(
    client: Client,
    entity_ids: list[int],
    proposals: list[Proposal],
    *,
    threshold: float,
) -> int:
    """Insert below-gate proposals + evidence, skipping protected rows and unresolved subjects."""
    person_ids = {p.person_id for p in proposals}
    subject_of = {
        r["person_id"]: r["id"]
        for r in fetch_all_rows(
            lambda: (
                client.table("subjects")
                .select("id,person_id,entity_id,publishable")
                .in_("entity_id", entity_ids)
                .in_("person_id", list(person_ids))
            )
        )
        if r.get("publishable")
    }
    doc_ids = sorted({p.document_id for p in proposals})
    existing = {
        (r["document_id"], r["cluster_label"]): r["confidence"]
        for r in fetch_all_rows(
            lambda: (
                client.table("speaker_identities")
                .select("document_id,cluster_label,confidence")
                .in_("document_id", doc_ids)
            )
        )
    }
    threshold_version = f"linking-frozen-cohort/thr={threshold:.4f}"
    written = 0
    for p in proposals:
        subject_id = subject_of.get(p.person_id)
        if subject_id is None:
            logger.warning("skip: person %d has no publishable subject in this body", p.person_id)
            continue
        if existing.get((p.document_id, p.cluster_label)) in PROTECTED_CONFIDENCE:
            continue  # never overwrite a human decision or a stronger name anchor
        client.table("speaker_identities").upsert(
            {
                "document_id": p.document_id,
                "cluster_label": p.cluster_label,
                "subject_id": subject_id,
                "confidence": "inferred_medium",
                "basis": "voiceprint",
            },
            on_conflict="document_id,cluster_label",
        ).execute()
        client.table("voiceprint_match_evidence").insert(
            {
                "document_id": p.document_id,
                "cluster_label": p.cluster_label,
                "proposed_person_id": p.person_id,
                "score": p.score,
                "margin": p.margin,
                "model": EMBED_MODEL,
                "threshold_version": threshold_version,
                "aggregation": AGGREGATION,
            }
        ).execute()
        written += 1
    return written


def run(args: argparse.Namespace) -> None:
    client = service_client()
    place = get_place_by_path(client, args.state, args.place)
    if not place:
        raise ActaluxError(f"no place {args.state}/{args.place}")
    place_id = place["id"]
    entity_ids = body_entity_ids(client, place_id, args.body)

    cache_dir = Path(args.cache_dir) / f"{args.state}_{args.place}_{args.body}"
    obs = load_observation_dir(cache_dir)
    if not obs:
        raise ActaluxError(f"no cached observations under {cache_dir}; run build_embedding_cache")

    anchors = fetch_person_labels(client, place_id, obs)
    identity: list[tuple[int, str] | None] = [(o.document_id, o.cluster_label) for o in obs]
    index_official = {
        i: anchors[(o.document_id, o.cluster_label)]
        for i, o in enumerate(obs)
        if (o.document_id, o.cluster_label) in anchors
    }
    embeddings = embedding_matrix(obs)

    if args.use_gallery:
        protos = load_gallery_prototypes(client, place_id, entity_ids)
        if protos:
            base_n = embeddings.shape[0]
            embeddings = np.vstack([embeddings, np.asarray([v for v, _ in protos])])
            for k, (_, pid) in enumerate(protos):
                index_official[base_n + k] = pid
                identity.append(None)  # virtual prototype: anchors, never proposed
            logger.info("augmented with %d gallery prototype(s)", len(protos))

    cohort = load_active_cohort(client, place_id)
    if cohort.size == 0:
        raise ActaluxError("no active frozen cohort — build_cohort.py + activate one first")

    proposals = [
        p
        for p in _link(obs, index_official, identity, embeddings, cohort, args.threshold)
        if p.margin >= args.min_margin
    ]
    logger.info(
        "%d proposal(s) at threshold=%.4f, min_margin=%.3f (%d anchored, %d clusters)",
        len(proposals),
        args.threshold,
        args.min_margin,
        len(index_official),
        len(obs),
    )
    for p in sorted(proposals, key=lambda x: x.score, reverse=True):
        logger.info(
            "  doc %d %s -> person %d  score=%.3f margin=%.3f",
            p.document_id,
            p.cluster_label,
            p.person_id,
            p.score,
            p.margin,
        )

    if not args.write:
        logger.info("dry-run: no writes. Re-run with --write to insert below-gate proposals.")
        return
    written = _write_proposals(client, entity_ids, proposals, threshold=args.threshold)
    logger.info("wrote %d proposal(s) at inferred_medium/voiceprint (for human review)", written)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--state", required=True)
    parser.add_argument("--place", required=True)
    parser.add_argument("--body", required=True, help="body_slug, e.g. schools / plan-commission")
    parser.add_argument("--cache-dir", default="data/linking_cache")
    parser.add_argument(
        "--threshold",
        type=float,
        required=True,
        help="linkage operating threshold (from the leave-one-official-out eval; not invented)",
    )
    parser.add_argument(
        "--min-margin",
        type=float,
        default=0.0,
        help="only propose when winner-minus-runner-up margin >= this",
    )
    parser.add_argument(
        "--use-gallery",
        action="store_true",
        help="augment with per-condition gallery prototypes as virtual anchors (needs a "
        "cleared gallery)",
    )
    parser.add_argument(
        "--write",
        action="store_true",
        help="GATED: insert proposals (default is dry-run reporting only)",
    )
    run(parser.parse_args())


if __name__ == "__main__":
    main()
