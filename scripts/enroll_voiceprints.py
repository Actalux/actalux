#!/usr/bin/env python3
"""Enroll official voiceprints into the gallery for a CLEARED jurisdiction.

Steady-state enroller: for each name-anchored / confirmed official cluster in a place, embed
its turns (on Modal), pool them into one voiceprint (Gate B, contamination-trimmed), and
store it in ``subject_voiceprints``. Pooling params come from that place's CLEARED
``voiceprint_calibration`` row — enrollment REFUSES if the place has no cleared calibration,
so voiceprints are never enrolled for production before the matcher has been calibrated and a
human has reviewed it (the candidate→cleared gate). Design:
docs/architecture/voiceprint-recalibration-plan.md.

A private citizen's voiceprint is never extracted or stored: only clusters already resolved
to a publishable official are enrolled, and officials-only is DB-enforced (migrate_040).

The initial candidate gallery for a not-yet-cleared place is produced by
``scripts/recalibrate_voiceprints.py`` (which also emits the calibration row); this script is
for re-enrolling a place that is already cleared (e.g. after new meetings arrive).

Dry-run by default; ``--apply`` downloads audio, embeds on Modal, and upserts.

Usage:
    # dry-run
    doppler run --project mac --config dev -- \\
      uv run python scripts/enroll_voiceprints.py --state mo --place clayton

    # apply (needs the diarization group for the Modal client, and the app deployed)
    doppler run --project actalux --config dev -- uv run --group diarization \\
      python scripts/enroll_voiceprints.py --state mo --place clayton --apply
"""

from __future__ import annotations

import argparse
import logging
from collections import defaultdict
from pathlib import Path
from typing import Any

from supabase import Client

from actalux.config import load_config
from actalux.db import fetch_all_rows, get_client, get_place_by_path
from actalux.diarization.enrollment import (
    NAME_ANCHOR_BASES,
    EnrollableCluster,
    cluster_spans,
    pool_cluster,
    select_enrollable,
    span_seconds,
    superseded_doc_ids,
    voiceprint_row,
)
from actalux.errors import ActaluxError

# Re-exported so tests that import from this module keep resolving the shared helpers.
__all__ = [
    "NAME_ANCHOR_BASES",
    "EnrollableCluster",
    "cluster_spans",
    "pool_cluster",
    "select_enrollable",
    "span_seconds",
    "superseded_doc_ids",
    "voiceprint_row",
]

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

AUDIO_DIR = Path("data/audio")  # transient; audio is deleted after embedding
DEFAULT_MIN_ENROLL_SECONDS = 10.0
# A WARP session is one egress IP; retry across rotated IPs when a proxy is set.
WARP_DOWNLOAD_RETRIES = 6


def _service_client() -> Client:
    """A service-key Supabase client (the voiceprint tables are service-only)."""
    cfg = load_config()
    import os

    key = os.environ.get("ACTALUX_SUPABASE_SERVICE_KEY", "")
    if not key:
        raise ActaluxError(
            "ACTALUX_SUPABASE_SERVICE_KEY is required (subject_voiceprints is service-only)"
        )
    return get_client(cfg.supabase_url, key)


def _cleared_calibration(
    client: Client, place_id: int, entity_id: int | None
) -> dict[str, Any] | None:
    """Latest CLEARED calibration for the place (entity-specific preferred, else place-wide)."""
    rows = fetch_all_rows(
        lambda: (
            client.table("voiceprint_calibration")
            .select("*")
            .eq("place_id", place_id)
            .eq("status", "cleared")
        )
    )
    if entity_id is not None:
        # a body may use its own calibration or the place-wide one — but NEVER another body's.
        specific = [r for r in rows if r.get("entity_id") == entity_id]
        candidates = specific or [r for r in rows if r.get("entity_id") is None]
    else:
        # place-wide enrollment uses only a place-wide row, never a single body's.
        candidates = [r for r in rows if r.get("entity_id") is None]
    if not candidates:
        return None
    return max(candidates, key=lambda r: r.get("calibrated_at") or "")


def _place_docs(
    client: Client, place_id: int, body: str | None
) -> tuple[dict[int, dict], int | None]:
    """Documents for a place's entities -> ``({doc_id: doc}, entity_id_for_body|None)``."""
    entities = fetch_all_rows(
        lambda: client.table("entities").select("id,body_slug").eq("place_id", place_id)
    )
    if body:
        entities = [e for e in entities if e.get("body_slug") == body]
    if not entities:
        raise ActaluxError(f"no entities for place {place_id} (body={body!r})")
    entity_ids = [e["id"] for e in entities]
    entity_id_for_body = entities[0]["id"] if body else None
    docs = fetch_all_rows(
        lambda: (
            client.table("documents")
            .select("id,video_id,replaces_id,meeting_date,entity_id")
            .in_("entity_id", entity_ids)
        )
    )
    return {d["id"]: d for d in docs}, entity_id_for_body


def main() -> None:
    parser = argparse.ArgumentParser(description="Enroll official voiceprints for a cleared place.")
    parser.add_argument("--state", required=True, help="place state slug, e.g. mo")
    parser.add_argument("--place", required=True, help="place slug, e.g. clayton")
    parser.add_argument("--body", help="restrict to one body_slug (e.g. council); default all")
    parser.add_argument("--apply", action="store_true", help="write to the DB (default: dry-run)")
    parser.add_argument(
        "--confirmed-only",
        action="store_true",
        help="enroll only human-confirmed clusters (exclude name-anchored inferred_high)",
    )
    parser.add_argument(
        "--min-seconds",
        type=float,
        default=DEFAULT_MIN_ENROLL_SECONDS,
        help="minimum pooled speech seconds to enroll a sample (default: %(default)s)",
    )
    parser.add_argument("--limit", type=int, help="cap the number of meetings processed")
    parser.add_argument("--proxy", help="SOCKS proxy for yt-dlp audio download (WARP in CI)")
    parser.add_argument("--keep-audio", action="store_true", help="don't delete downloaded audio")
    parser.add_argument(
        "--reembed",
        action="store_true",
        help="re-embed meetings already in the gallery (default: skip them, so a killed "
        "run resumes cheaply without re-downloading)",
    )
    args = parser.parse_args()

    client = _service_client()
    place = get_place_by_path(client, args.state, args.place)
    if not place:
        raise ActaluxError(f"no place {args.state}/{args.place}")
    place_id = place["id"]

    docs_by_id, entity_id_for_body = _place_docs(client, place_id, args.body)
    cal = _cleared_calibration(client, place_id, entity_id_for_body)
    if not cal:
        raise ActaluxError(
            f"no CLEARED voiceprint_calibration for {args.state}/{args.place}"
            f"{'/' + args.body if args.body else ''}; run recalibrate_voiceprints.py and have a "
            f"human promote the candidate to 'cleared' before enrolling."
        )
    pool_params = {
        "trim_fraction": cal["trim_fraction"],
        "min_coherent_turns": cal["min_coherent_turns"],
        "purity_floor": cal["purity_floor"],
    }
    calibration_id = cal["id"]
    logger.info("using cleared calibration id=%s (%s)", calibration_id, pool_params)

    doc_ids = sorted(docs_by_id)
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
            )  # place-scoped: a stale cross-place subject_id can't enroll
        )
    }
    enrollable = select_enrollable(identities, subjects_by_id, confirmed_only=args.confirmed_only)
    # Gate A carries forward: only enroll officials the cleared calibration enabled (an
    # incoherent clerk / mislabeled anchor stays out of a cleared gallery).
    enabled_ids = set((cal.get("report") or {}).get("enabled_person_ids") or [])
    enrollable = [ec for ec in enrollable if ec.person_id in enabled_ids]
    if not enrollable:
        logger.info("no enrollable clusters for enabled officials; nothing to do")
        return

    superseded = superseded_doc_ids(list(docs_by_id.values()))
    already_enrolled: set[int] = set()
    if not args.reembed:
        already_enrolled = {
            r["source_document_id"]
            for r in fetch_all_rows(
                lambda: client.table("subject_voiceprints").select("source_document_id")
            )
        }

    ready: list[EnrollableCluster] = []
    skipped_superseded = skipped_no_video = skipped_done = 0
    for ec in enrollable:
        doc = docs_by_id.get(ec.document_id, {})
        if ec.document_id in superseded:
            skipped_superseded += 1
        elif ec.document_id in already_enrolled:
            skipped_done += 1
        elif not doc.get("video_id"):
            skipped_no_video += 1
        else:
            ready.append(ec)

    by_doc: dict[int, list[EnrollableCluster]] = defaultdict(list)
    for ec in ready:
        by_doc[ec.document_id].append(ec)
    docs_to_process = sorted(by_doc)
    if args.limit:
        docs_to_process = docs_to_process[: args.limit]

    logger.info(
        "enrollable: %d clusters / %d persons / %d meetings "
        "(skipped %d superseded, %d without video_id, %d already enrolled)",
        len(ready),
        len({ec.person_id for ec in ready}),
        len(by_doc),
        skipped_superseded,
        skipped_no_video,
        skipped_done,
    )

    if not args.apply:
        _dry_run_report(client, by_doc, docs_to_process, args.min_seconds)
        logger.info("DRY RUN — would enroll from %d meeting(s)", len(docs_to_process))
        logger.info("re-run with --apply (needs `--group diarization` + a deployed Modal app)")
        return

    _apply(client, by_doc, docs_to_process, docs_by_id, pool_params, calibration_id, args)
    pruned = _prune_superseded(client, superseded)
    logger.info("done: pruned %d superseded sample(s)", pruned)


def _dry_run_report(
    client: Client,
    by_doc: dict[int, list[EnrollableCluster]],
    docs_to_process: list[int],
    min_seconds: float,
) -> None:
    """Log per-person enrollment counts + short-cluster skips, without touching Modal."""
    from actalux.db import get_diarization_turns

    per_person: dict[str, int] = defaultdict(int)
    short = 0
    for doc_id in docs_to_process:
        turns = get_diarization_turns(client, doc_id)
        for ec in by_doc[doc_id]:
            if span_seconds(cluster_spans(turns, ec.cluster_label)) < min_seconds:
                short += 1
            else:
                per_person[ec.canonical_name] += 1
    for name, n in sorted(per_person.items(), key=lambda kv: -kv[1]):
        logger.info("  %-28s %d sample(s)", name, n)
    if short:
        logger.info(
            "  (%d cluster(s) below --min-seconds=%.0fs would be skipped)", short, min_seconds
        )


def _apply(
    client: Client,
    by_doc: dict[int, list[EnrollableCluster]],
    docs_to_process: list[int],
    docs_by_id: dict[int, dict[str, Any]],
    pool_params: dict[str, Any],
    calibration_id: int,
    args: argparse.Namespace,
) -> None:
    """Download audio, embed turns on Modal, pool (Gate B), upsert gallery rows per meeting."""
    from actalux.diarization.modal_runner import EMBED_MODEL, ModalRunner
    from actalux.ingest.youtube import download_audio

    runner = ModalRunner()
    retries = WARP_DOWNLOAD_RETRIES if args.proxy else 1
    enrolled = 0
    for doc_id in docs_to_process:
        clusters = by_doc[doc_id]
        video_id = docs_by_id[doc_id]["video_id"]
        turns = _turns(client, doc_id)
        payload = [
            {"cluster_label": ec.cluster_label, "spans": cluster_spans(turns, ec.cluster_label)}
            for ec in clusters
        ]
        try:
            audio = download_audio(video_id, AUDIO_DIR, proxy=args.proxy, retries=retries)
        except Exception:  # noqa: BLE001 - one meeting's download failure must not abort the batch
            logger.exception("audio download failed for doc %d (%s); skipping", doc_id, video_id)
            continue
        try:
            turns_by_label = runner.embed_cluster_turns(str(audio), payload)
        finally:
            if not args.keep_audio:
                audio.unlink(missing_ok=True)
        rows = []
        for ec in clusters:
            pooled = pool_cluster(turns_by_label.get(ec.cluster_label, []), **pool_params)
            if pooled is None or pooled.seconds < args.min_seconds:
                continue
            rows.append(voiceprint_row(ec, pooled, EMBED_MODEL, calibration_id=calibration_id))
        # replace-per-meeting so a now-rejected cluster's stale row is removed.
        client.table("subject_voiceprints").delete().eq("source_document_id", doc_id).execute()
        if rows:
            client.table("subject_voiceprints").insert(rows).execute()
            enrolled += len(rows)
            logger.info("doc %d (%s): enrolled %d sample(s)", doc_id, video_id, len(rows))
    logger.info(
        "enrolled %d gallery sample(s) across %d meeting(s)", enrolled, len(docs_to_process)
    )


def _turns(client: Client, doc_id: int) -> list[dict[str, Any]]:
    from actalux.db import get_diarization_turns

    return get_diarization_turns(client, doc_id)


def _prune_superseded(client: Client, superseded: set[int]) -> int:
    """Delete gallery samples whose source document has been superseded."""
    if not superseded:
        return 0
    rows = fetch_all_rows(
        lambda: (
            client.table("subject_voiceprints")
            .select("id")
            .in_("source_document_id", sorted(superseded))
        )
    )
    ids = [r["id"] for r in rows]
    if ids:
        client.table("subject_voiceprints").delete().in_("id", ids).execute()
    return len(ids)


if __name__ == "__main__":
    main()
