"""Build the ``[E]`` embedding cache for the cross-meeting linking prototype.

One pooled 256-d wespeaker centroid per anchored official cluster per meeting, reusing the
recalibration embed path (``download_audio`` -> Modal ``embed_cluster_turns`` -> ``pool_cluster``)
so the cached vectors are byte-identical to the calibration gallery's substrate. This is the only
heavy/GPU/cost stage of the linker; the scoring/clustering/evaluation stages iterate freely on the
cache (see docs/architecture/linking-prototype-phase1.md).

Resumable: one ``.npz`` per document under ``<out-dir>/<state>_<place>_<body>/doc_<id>.npz``; a
re-run skips documents already cached (including ones that legitimately produced zero clusters).
Because YouTube bot-checks make downloads flaky and long, run this DETACHED (``nohup``), not as a
harness background task.

Population: only anchored (``select_enrollable``) clusters are embedded — the benchmark voices.
``acoustic_condition`` is a PRECISE-positive proxy: ``"zoom"`` iff the meeting produced a
``screen_name`` identity (a Zoom gallery tile was OCR'd for someone in it), else ``"in_person"``
(the uncertain bucket — a Zoom meeting whose tiles we never read also lands here). ``meeting_date``
is read straight off ``documents``. Both are metadata for the evaluator's secondary condition
split; the primary go/no-go metric (across-*meeting* F1) does not depend on the condition label.

Run (detached, WARP proxy for YouTube bot-checks):
    doppler run --project mac --config dev -- \\
      uv run --group diarization python scripts/linking/build_embedding_cache.py \\
      --state mo --place clayton --body schools --proxy --out-dir data/linking_cache
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
from pathlib import Path

import numpy as np
from supabase import Client

from actalux.config import load_config
from actalux.db import fetch_all_rows, get_client, get_diarization_turns, get_place_by_path
from actalux.diarization.enrollment import (
    cluster_spans,
    pool_cluster,
    select_enrollable,
    span_seconds,
)
from actalux.diarization.linking.observations import VoiceObservation, save_observations
from actalux.errors import ActaluxError

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

AUDIO_DIR = Path("data/audio")
WARP_DOWNLOAD_RETRIES = 6
# Gate B pooling, fixed identical to recalibrate_voiceprints.POOL_PARAMS (recalibration plan §5):
# trimmed-mean robustness + require >=2 coherent turns; purity_floor 0 (pooling only trims).
POOL_PARAMS = {"trim_fraction": 0.25, "min_coherent_turns": 2, "purity_floor": 0.0}
# Match recalibrate_voiceprints --min-seconds default so a cached centroid corresponds to the same
# clusters the calibration gallery would enroll.
DEFAULT_MIN_SECONDS = 10.0


def service_client() -> Client:
    """A service-key Supabase client (voiceprint / below-gate identity rows are service-only)."""
    cfg = load_config()
    key = os.environ.get("ACTALUX_SUPABASE_SERVICE_KEY", "")
    if not key:
        raise ActaluxError("ACTALUX_SUPABASE_SERVICE_KEY is required (service-only tables)")
    return get_client(cfg.supabase_url, key)


def _body_entity_ids(client: Client, place_id: int, body: str) -> list[int]:
    """Entity ids for one body_slug in a place."""
    entities = fetch_all_rows(
        lambda: client.table("entities").select("id,body_slug").eq("place_id", place_id)
    )
    ids = [e["id"] for e in entities if e.get("body_slug") == body]
    if not ids:
        raise ActaluxError(f"no entity for body {body!r} in place {place_id}")
    return ids


def _load_anchored(
    client: Client, place_id: int, entity_ids: list[int]
) -> tuple[dict[int, dict], dict[int, list], set[int]]:
    """Return (docs_by_id, anchored-clusters-by-doc, zoom_doc_ids) for a body.

    ``zoom_doc_ids`` = documents that produced a ``screen_name`` identity (definite Zoom render).
    """
    docs = fetch_all_rows(
        lambda: (
            client.table("documents")
            .select("id,video_id,replaces_id,entity_id,meeting_date")
            .in_("entity_id", entity_ids)
        )
    )
    docs_by_id = {d["id"]: d for d in docs}
    superseded = {d["id"] for d in docs if d.get("replaces_id") is not None}

    identities = fetch_all_rows(
        lambda: (
            client.table("speaker_identities")
            .select("id,document_id,cluster_label,subject_id,confidence,basis")
            .in_("document_id", list(docs_by_id))
        )
    )
    zoom_doc_ids = {r["document_id"] for r in identities if r.get("basis") == "screen_name"}
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

    by_doc: dict[int, list] = {}
    for ec in enrollable:
        doc = docs_by_id.get(ec.document_id)
        if not doc or ec.document_id in superseded or not doc.get("video_id"):
            continue
        by_doc.setdefault(ec.document_id, []).append(ec)
    return docs_by_id, by_doc, zoom_doc_ids


def _embed_document(
    runner,
    client: Client,
    doc: dict,
    clusters: list,
    *,
    condition: str,
    proxy: str | None,
    min_seconds: float,
    keep_audio: bool,
) -> list[VoiceObservation] | None:
    """Download + embed + pool one meeting's anchored clusters -> observations (None on failure)."""
    from actalux.ingest.youtube import download_audio

    # put scripts/ (parent-of-parent) on the path for the same-dir transcribe_meetings import
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
    from transcribe_meetings import reconnect_warp

    video_id = doc["video_id"]
    turns = get_diarization_turns(client, doc["id"])
    payload = [
        {"cluster_label": ec.cluster_label, "spans": cluster_spans(turns, ec.cluster_label)}
        for ec in clusters
    ]
    retries = WARP_DOWNLOAD_RETRIES if proxy else 1
    try:
        audio = download_audio(
            video_id,
            AUDIO_DIR,
            proxy=proxy,
            retries=retries,
            on_retry=reconnect_warp if proxy else None,
        )
    except Exception:  # noqa: BLE001 - one meeting's download failure retries on the next run
        logger.exception("download failed for doc %d (%s); retries next run", doc["id"], video_id)
        return None
    try:
        turns_by_label = runner.embed_cluster_turns(str(audio), payload)
    finally:
        if not keep_audio:
            audio.unlink(missing_ok=True)

    obs: list[VoiceObservation] = []
    for ec in clusters:
        pooled = pool_cluster(turns_by_label.get(ec.cluster_label, []), **POOL_PARAMS)
        if pooled is None or pooled.seconds < min_seconds:
            continue
        obs.append(
            VoiceObservation(
                document_id=doc["id"],
                cluster_label=ec.cluster_label,
                embedding=np.asarray(pooled.vector, dtype=np.float32),
                # full speech behind the cluster (all turns), not the capped enrollment weight
                speech_seconds=span_seconds(cluster_spans(turns, ec.cluster_label)),
                acoustic_condition=condition,
                meeting_date=doc.get("meeting_date"),
            )
        )
    return obs


def build(args: argparse.Namespace) -> None:
    client = service_client()
    place = get_place_by_path(client, args.state, args.place)
    if not place:
        raise ActaluxError(f"no place {args.state}/{args.place}")
    entity_ids = _body_entity_ids(client, place["id"], args.body)
    docs_by_id, by_doc, zoom_doc_ids = _load_anchored(client, place["id"], entity_ids)

    out_dir = Path(args.out_dir) / f"{args.state}_{args.place}_{args.body}"
    out_dir.mkdir(parents=True, exist_ok=True)
    doc_ids = sorted(by_doc)
    if args.limit:
        doc_ids = doc_ids[: args.limit]

    from actalux.diarization.modal_runner import ModalRunner

    runner = ModalRunner()
    logger.info(
        "%s/%s/%s: %d anchored meetings (%d zoom-proxy) -> %s",
        args.state,
        args.place,
        args.body,
        len(doc_ids),
        sum(1 for d in doc_ids if d in zoom_doc_ids),
        out_dir,
    )
    embedded = 0
    for doc_id in doc_ids:
        out_path = out_dir / f"doc_{doc_id}.npz"
        if out_path.is_file() and out_path.stat().st_size > 0:
            continue  # resumable: already cached
        condition = "zoom" if doc_id in zoom_doc_ids else "in_person"
        obs = _embed_document(
            runner,
            client,
            docs_by_id[doc_id],
            by_doc[doc_id],
            condition=condition,
            proxy=args.proxy,
            min_seconds=args.min_seconds,
            keep_audio=args.keep_audio,
        )
        if obs is None:
            continue  # download failure — leave uncached so the next run retries
        save_observations(obs, out_path)  # may be empty (doc produced no poolable cluster)
        embedded += 1
        logger.info("  doc %d [%s]: cached %d cluster(s)", doc_id, condition, len(obs))
    logger.info("done: %d/%d meetings newly cached under %s", embedded, len(doc_ids), out_dir)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--state", required=True)
    parser.add_argument("--place", required=True)
    parser.add_argument("--body", required=True, help="body_slug, e.g. schools / plan-commission")
    parser.add_argument("--out-dir", default="data/linking_cache")
    parser.add_argument("--limit", type=int, default=0, help="cap meetings (pilot); 0 = all")
    parser.add_argument("--min-seconds", type=float, default=DEFAULT_MIN_SECONDS)
    parser.add_argument(
        "--proxy",
        help="SOCKS proxy for yt-dlp audio download (WARP endpoint); rotates egress on retry",
    )
    parser.add_argument("--keep-audio", action="store_true")
    build(parser.parse_args())


if __name__ == "__main__":
    main()
