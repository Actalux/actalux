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

The scoring method + threshold come from the body's stored operating point (migrate_048, set via
``set_operating_point.py``) so a measured decision — not a hand-passed number — drives every run;
``--threshold`` remains as an explicit asnorm override for experiments.

Run (dry-run):
    doppler run --project mac --config dev -- \\
      uv run python scripts/linking/propose_identities.py \\
      --state mo --place clayton --body schools
"""

from __future__ import annotations

import argparse
import logging
import os
from typing import Any

import numpy as np
from supabase import Client

from actalux.config import load_config
from actalux.db import fetch_all_rows, get_client, get_place_by_path
from actalux.diarization.enrollment import EMBED_MODEL, NAME_ANCHOR_BASES
from actalux.diarization.linking.benchmark import cannot_link_same_meeting
from actalux.diarization.linking.cache import MODE_ALL, cache_dir, require_mode
from actalux.diarization.linking.calibration import Calibrator, calibrated_matrix
from actalux.diarization.linking.cluster import constrained_complete_linkage
from actalux.diarization.linking.cohort import (
    load_active_cohort,
    load_active_operating_point,
    parse_pgvector,
)
from actalux.diarization.linking.labels import fetch_person_labels
from actalux.diarization.linking.observations import (
    embedding_matrix,
    load_observation_dir,
)
from actalux.diarization.linking.proposer import (
    Proposal,
    build_proposals,
    per_condition_prototypes,
    unanchored_recurring_nodes,
)
from actalux.diarization.linking.scoring import asnorm_matrix
from actalux.errors import ActaluxError

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

AGGREGATION = "max-anchor"  # score = max pair score to the node's anchored member(s)
# A biometric proposal must never clobber a human decision or a stronger name anchor.
PROTECTED_CONFIDENCE = {"inferred_high", "confirmed", "rejected"}
# Stand-in speech seconds for a virtual gallery-prototype row in calibrated scoring. Prototypes are
# multi-sample centroids, so their duration feature should never be the pair's minimum — large but
# finite (an infinity would blow up the standardized feature), and min(600, x) = x for real
# clusters, which is the intended semantics.
PROTOTYPE_SECONDS = 600.0


def service_client() -> Client:
    cfg = load_config()
    key = os.environ.get("ACTALUX_SUPABASE_SERVICE_KEY", "")
    if not key:
        raise ActaluxError("ACTALUX_SUPABASE_SERVICE_KEY is required (service-only tables)")
    return get_client(cfg.supabase_url, key)


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
) -> list[tuple[np.ndarray, int, str]]:
    """Per-condition gallery prototypes for the body's officials (virtual anchors), cleared-only.

    Trusts a voiceprint only when its ``calibration_id`` resolves to a ``cleared`` calibration
    (migrate_041) THAT COVERS THIS BODY — the calibration's ``entity_id`` is one of the body's
    entities, or NULL (place-wide). Calibration is a per-body gate: clearing the plan commission
    says nothing about whether the schools gallery is trustworthy, and a place-wide filter would let
    one body's clearance unlock another's prototypes. Returns one ``(centroid, person_id,
    condition)`` per (official, acoustic condition); empty when this body has no cleared gallery
    (the schools case).
    """
    scope = set(entity_ids)
    cleared = {
        r["id"]
        for r in fetch_all_rows(
            lambda: (
                client.table("voiceprint_calibration")
                .select("id,status,entity_id")
                .eq("place_id", place_id)
                .eq("status", "cleared")
            )
        )
        if r.get("entity_id") is None or r.get("entity_id") in scope
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
        vec = np.asarray(parse_pgvector(r["embedding"]), dtype=np.float64)
        cond = r.get("acoustic_condition") or "unknown"
        by_person.setdefault(r["person_id"], []).append((vec, cond))
    protos: list[tuple[np.ndarray, int, str]] = []
    for pid, samples in by_person.items():
        for cond, centroid in per_condition_prototypes(samples).items():
            protos.append((centroid, pid, cond))
    return protos


def resolve_operating_point(
    cli_threshold: float | None, op: dict[str, Any] | None
) -> dict[str, Any]:
    """Which scoring method + threshold this run uses, and where that decision came from.

    An explicit ``--threshold`` is an experiment override: it always scores plain AS-norm (a stored
    calibrator was fitted against a specific threshold — mixing it with an ad-hoc one would look
    like the measured configuration while being neither). Otherwise the body's stored operating
    point (migrate_048) supplies method/threshold/calibrator, and its ``cohort_id`` is enforced
    against the active cohort downstream. No stored point and no override is a hard error — the
    threshold is a measured decision, never a default.
    """
    if cli_threshold is not None:
        return {
            "threshold": float(cli_threshold),
            "method": "asnorm",
            "calibrator": None,
            "cohort_id": None,
            "version": f"cli-override/thr={cli_threshold:.4f}",
        }
    if op is None:
        raise ActaluxError(
            "no active operating point for this body (set_operating_point.py) and no --threshold "
            "override given"
        )
    return {
        "threshold": float(op["threshold"]),
        "method": op["method"],
        "calibrator": op.get("calibrator"),
        "cohort_id": op["cohort_id"],
        "version": f"op={op['id']}/method={op['method']}/thr={float(op['threshold']):.4f}",
    }


def skip_reason(existing: dict[str, Any] | None) -> str | None:
    """Why a voiceprint proposal must not touch this ``speaker_identities`` row (None = writable).

    A biometric guess is the WEAKEST evidence we hold, so it may only fill a row that is absent, has
    no basis, or is itself a previous voiceprint proposal. Two ways a row is off-limits:

    - **Protected tier** — ``confirmed``/``rejected`` are human decisions (also trigger-protected in
      migrate_035/043) and ``inferred_high`` is anon-visible.
    - **Any name-anchor basis, at ANY tier** — a ``rollcall``/``self_intro`` row held at
      ``inferred_medium`` is NOT enrollable at that tier, so it never reaches the proposer's anchor
      set and is invisible to the link. Skipping on confidence alone would let the upsert rewrite a
      name-derived attribution's basis to 'voiceprint'. Name evidence outranks voice evidence
      regardless of tier.
    """
    if existing is None:
        return None
    if existing.get("confidence") in PROTECTED_CONFIDENCE:
        return f"protected confidence {existing.get('confidence')!r}"
    basis = existing.get("basis")
    if basis in NAME_ANCHOR_BASES:
        return f"name-anchored basis {basis!r} outranks a voiceprint guess"
    return None


def _write_proposals(
    client: Client,
    entity_ids: list[int],
    proposals: list[Proposal],
    seconds_by_cluster: dict[tuple[int, str], float],
    *,
    threshold_version: str,
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
        (r["document_id"], r["cluster_label"]): r
        for r in fetch_all_rows(
            lambda: (
                client.table("speaker_identities")
                .select("document_id,cluster_label,confidence,basis")
                .in_("document_id", doc_ids)
            )
        )
    }
    written = 0
    for p in proposals:
        subject_id = subject_of.get(p.person_id)
        if subject_id is None:
            logger.warning("skip: person %d has no publishable subject in this body", p.person_id)
            continue
        reason = skip_reason(existing.get((p.document_id, p.cluster_label)))
        if reason is not None:
            logger.info("skip doc %d %s: %s", p.document_id, p.cluster_label, reason)
            continue
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
                "target_seconds": seconds_by_cluster.get((p.document_id, p.cluster_label)),
                # the runners-up this match beat — what a reviewer needs to judge a thin margin
                "alternatives": [{"person_id": pid, "score": s} for pid, s in p.alternatives],
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

    # The proposer names clusters no anchor covers, so it needs the ALL-cluster cache; an anchored
    # cache would yield zero proposals and look like a clean run.
    cache_path = cache_dir(args.cache_dir, args.state, args.place, args.body, mode=MODE_ALL)
    require_mode(cache_path, MODE_ALL)
    obs = load_observation_dir(cache_path)
    if not obs:
        raise ActaluxError(
            f"no cached observations under {cache_path}; run build_embedding_cache "
            f"--include-unanchored"
        )

    anchors = fetch_person_labels(client, place_id, obs)
    identity: list[tuple[int, str] | None] = [(o.document_id, o.cluster_label) for o in obs]
    index_official = {
        i: anchors[(o.document_id, o.cluster_label)]
        for i, o in enumerate(obs)
        if (o.document_id, o.cluster_label) in anchors
    }
    embeddings = embedding_matrix(obs)
    conditions = [o.acoustic_condition for o in obs]
    seconds = [o.speech_seconds for o in obs]

    if args.use_gallery:
        protos = load_gallery_prototypes(client, place_id, entity_ids)
        if protos:
            base_n = embeddings.shape[0]
            embeddings = np.vstack([embeddings, np.asarray([v for v, _, _ in protos])])
            for k, (_, pid, cond) in enumerate(protos):
                index_official[base_n + k] = pid
                identity.append(None)  # virtual prototype: anchors, never proposed
                conditions.append(cond)
                seconds.append(PROTOTYPE_SECONDS)
            logger.info("augmented with %d gallery prototype(s)", len(protos))

    resolved = resolve_operating_point(
        args.threshold, load_active_operating_point(client, place_id, args.body)
    )
    logger.info("operating point: %s", resolved["version"])
    cohort = load_active_cohort(
        client,
        place_id,
        expected_model=EMBED_MODEL,
        expected_cohort_id=resolved["cohort_id"],
    )
    if cohort.size == 0:
        raise ActaluxError("no active frozen cohort — build_cohort.py + activate one first")

    if resolved["method"] == "calibrated":
        calibrator = Calibrator.from_dict(resolved["calibrator"])
        scores = calibrated_matrix(embeddings, cohort, conditions, seconds, calibrator)
    else:
        scores = asnorm_matrix(embeddings, cohort)
    cannot_link = cannot_link_same_meeting(obs)  # only real clusters (indices < len(obs))
    pred = constrained_complete_linkage(
        scores, threshold=resolved["threshold"], cannot_link=cannot_link
    )
    proposals = [
        p
        for p in build_proposals(pred, index_official, scores, identity)
        if p.margin >= args.min_margin
    ]
    logger.info(
        "%d proposal(s) at %s, min_margin=%.3f (%d anchored, %d clusters)",
        len(proposals),
        resolved["version"],
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

    # Flag-only roster prompt: a nameless voice recurring across meetings is often a new official
    # the roster does not know yet. Reported, never named — no entity, no identity row.
    for node in unanchored_recurring_nodes(pred, index_official, identity, seconds):
        logger.info(
            "unanchored recurring voice: node %s spans %d meetings / %d clusters / %.0fs "
            "(docs %s) — who is this?",
            node["node_id"],
            node["n_meetings"],
            node["n_clusters"],
            node["total_seconds"],
            node["document_ids"],
        )

    if not args.write:
        logger.info("dry-run: no writes. Re-run with --write to insert below-gate proposals.")
        return
    seconds_by_cluster = {(o.document_id, o.cluster_label): o.speech_seconds for o in obs}
    written = _write_proposals(
        client, entity_ids, proposals, seconds_by_cluster, threshold_version=resolved["version"]
    )
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
        help="EXPERIMENT OVERRIDE: link plain AS-norm at this threshold, bypassing the stored "
        "operating point (default: read the body's active operating point, migrate_048)",
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
