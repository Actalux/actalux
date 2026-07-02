#!/usr/bin/env python3
"""Enroll confirmed / name-anchored official voiceprints into the gallery.

For each enrollable cluster — a ``speaker_identities`` row that is human-``confirmed``
or a name-anchored ``inferred_high`` (roll call / self-intro / vote anchor) — re-extract
that cluster's voice embedding from the meeting audio and store it in
``subject_voiceprints``. The embedding comes from the cluster's STORED
``diarization_turns`` spans, not a fresh diarization: re-diarizing would renumber the
``SPEAKER_NN`` labels, so the stored spans are the only stable reference to "this voice".

A private citizen's voiceprint is never extracted or stored: only clusters already
resolved to a publishable official are enrolled, and officials-only is DB-enforced
(migrate_040 trigger). Design: docs/architecture/voiceprint-speaker-id-plan.md §5.

Dry-run by default (reports what would be enrolled, no GPU, no writes); ``--apply``
downloads audio, extracts embeddings on Modal, and upserts. Idempotent — the gallery
key is ``(person_id, source_document_id, cluster_label)``.

Usage:
    # dry-run (no Modal, no writes)
    doppler run --project mac --config dev -- \\
      uv run python scripts/enroll_voiceprints.py

    # apply (needs the diarization group for the Modal client, and the app deployed)
    doppler run --project actalux --config dev -- \\
      uv run --group diarization python scripts/enroll_voiceprints.py --apply
"""

from __future__ import annotations

import argparse
import logging
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from supabase import Client

from actalux.config import load_config
from actalux.db import fetch_all_rows, get_client
from actalux.errors import ActaluxError

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

AUDIO_DIR = Path("data/audio")  # transient; audio is deleted after embedding
# Name anchors are deterministic (a spoken name -> this voice), so enrolling from an
# auto inferred_high with one of these bases is safe. basis='voiceprint' is NEVER
# enrollable — that would let a biometric guess train the gallery (poison loop).
NAME_ANCHOR_BASES = ("rollcall", "self_intro", "vote_anchor")
# Quality floor for a stored sample. The embedder itself drops clusters under ~3s
# (NaN-prone); this is the higher bar for what we keep. Tunable (plan §9 open decision).
DEFAULT_MIN_ENROLL_SECONDS = 10.0
# A WARP session is one egress IP; retry across rotated IPs when a proxy is set.
WARP_DOWNLOAD_RETRIES = 6


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

    Eligible when the cluster maps to a publishable subject with a ``person_id`` and
    is either human-``confirmed`` or (unless ``confirmed_only``) a name-anchored
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
                # a human-confirmed row may carry no basis; 'manual' is the honest
                # label and satisfies the source_basis NOT NULL + CHECK (migrate_040).
                cluster_label=row["cluster_label"],
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


def voiceprint_row(
    ec: EnrollableCluster, vector: tuple[float, ...], seconds: float, model: str
) -> dict[str, Any]:
    """A ``subject_voiceprints`` insert row for one enrolled cluster."""
    return {
        "person_id": ec.person_id,
        "source_subject_id": ec.source_subject_id,
        "source_document_id": ec.document_id,
        "source_identity_id": ec.source_identity_id,
        "cluster_label": ec.cluster_label,
        "embedding": list(vector),
        "source_basis": ec.source_basis,
        "model": model,
        "seconds": round(seconds, 2),
    }


def superseded_doc_ids(docs: list[dict[str, Any]]) -> set[int]:
    """Ids of documents that have been superseded (``replaces_id`` set)."""
    return {d["id"] for d in docs if d.get("replaces_id") is not None}


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


def main() -> None:
    parser = argparse.ArgumentParser(description="Enroll official voiceprints into the gallery.")
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
        help="minimum speech seconds to enroll a sample (default: %(default)s)",
    )
    parser.add_argument("--limit", type=int, help="cap the number of meetings processed")
    parser.add_argument("--proxy", help="SOCKS proxy for yt-dlp audio download (WARP in CI)")
    parser.add_argument("--keep-audio", action="store_true", help="don't delete downloaded audio")
    args = parser.parse_args()

    client = _service_client()

    identities = fetch_all_rows(
        lambda: client.table("speaker_identities").select(
            "id,document_id,cluster_label,subject_id,confidence,basis"
        )
    )
    subjects_by_id = {
        s["id"]: s
        for s in fetch_all_rows(
            lambda: client.table("subjects").select("id,person_id,publishable,canonical_name")
        )
    }
    enrollable = select_enrollable(identities, subjects_by_id, confirmed_only=args.confirmed_only)
    if not enrollable:
        logger.info("no enrollable clusters found; nothing to do")
        return

    doc_ids = sorted({ec.document_id for ec in enrollable})
    docs = fetch_all_rows(
        lambda: (
            client.table("documents")
            .select("id,video_id,replaces_id,meeting_date")
            .in_("id", doc_ids)
        )
    )
    docs_by_id = {d["id"]: d for d in docs}
    superseded = superseded_doc_ids(docs)

    # Drop enrollables we can't or shouldn't process, with a reason.
    ready: list[EnrollableCluster] = []
    skipped_superseded = skipped_no_video = 0
    for ec in enrollable:
        doc = docs_by_id.get(ec.document_id, {})
        if ec.document_id in superseded:
            skipped_superseded += 1
            continue
        if not doc.get("video_id"):
            skipped_no_video += 1
            continue
        ready.append(ec)

    by_doc: dict[int, list[EnrollableCluster]] = defaultdict(list)
    for ec in ready:
        by_doc[ec.document_id].append(ec)
    docs_to_process = sorted(by_doc)
    if args.limit:
        docs_to_process = docs_to_process[: args.limit]

    persons = {ec.person_id for ec in ready}
    logger.info(
        "enrollable: %d clusters / %d persons / %d meetings "
        "(skipped %d superseded, %d without video_id)",
        len(ready),
        len(persons),
        len(by_doc),
        skipped_superseded,
        skipped_no_video,
    )

    if not args.apply:
        _dry_run_report(client, by_doc, docs_to_process, args.min_seconds)
        pruned = _count_prunable(client, superseded)
        logger.info(
            "DRY RUN — would enroll from %d meeting(s); would prune %d superseded sample(s)",
            len(docs_to_process),
            pruned,
        )
        logger.info("re-run with --apply (needs `--group diarization` + a deployed Modal app)")
        return

    _apply(client, by_doc, docs_to_process, docs_by_id, args)
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
            secs = span_seconds(cluster_spans(turns, ec.cluster_label))
            if secs < min_seconds:
                short += 1
                continue
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
    args: argparse.Namespace,
) -> None:
    """Download audio, extract embeddings on Modal, upsert gallery rows — one meeting at a time."""
    from actalux.diarization.modal_runner import ModalRunner
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
            embeddings = runner.embed_clusters(str(audio), payload)
        finally:
            if not args.keep_audio:
                audio.unlink(missing_ok=True)
        rows = []
        for ec in clusters:
            emb = embeddings.get(ec.cluster_label)
            if emb is None or emb.seconds < args.min_seconds:
                continue
            rows.append(voiceprint_row(ec, emb.vector, emb.seconds, emb.model))
        if rows:
            client.table("subject_voiceprints").upsert(
                rows, on_conflict="person_id,source_document_id,cluster_label"
            ).execute()
            enrolled += len(rows)
            logger.info("doc %d (%s): enrolled %d sample(s)", doc_id, video_id, len(rows))
    logger.info(
        "enrolled %d gallery sample(s) across %d meeting(s)", enrolled, len(docs_to_process)
    )


def _turns(client: Client, doc_id: int) -> list[dict[str, Any]]:
    from actalux.db import get_diarization_turns

    return get_diarization_turns(client, doc_id)


def _count_prunable(client: Client, superseded: set[int]) -> int:
    """How many gallery samples sit on superseded documents (dry-run)."""
    if not superseded:
        return 0
    rows = fetch_all_rows(
        lambda: (
            client.table("subject_voiceprints")
            .select("id,source_document_id")
            .in_("source_document_id", sorted(superseded))
        )
    )
    return len(rows)


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
