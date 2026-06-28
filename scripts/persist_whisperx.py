#!/usr/bin/env python3
"""Attach staged speaker-attribution layers to freshly-ingested transcript documents.

Runs AFTER scripts/ingest.py --manifest. For each entry in the WhisperX manifest it
finds the current (non-superseded) transcript document for the meeting, persists its
``<stem>.attribution.json`` — raw verbatim text, word-level speaker turns, the
name-correction audit, and the media asset — via the SERVICE client, then resolves
speaker identities (deterministic; publishes only clean anchors, the rest land in the
review queue). Idempotent (the layer is cleared + re-written, identities reconciled),
so re-running is safe.

Why a separate step: the document id doesn't exist until ingest runs, and the new
tables are keyed to it. Splitting transcription (GPU) from persistence keeps each
step re-runnable on its own.

Run (DB creds from Doppler mac; needs the service key to write):
  doppler run --project mac --config dev -- \
    uv run python scripts/persist_whisperx.py
"""

from __future__ import annotations

import argparse
import json
import logging
from pathlib import Path

from supabase import Client

from actalux.config import load_config
from actalux.db import get_client
from actalux.identity.resolve import resolve_document
from actalux.transcription.pipeline import SpeakerLayer, persist_speaker_layer

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

OUTPUT_DIR = Path("data/documents")
MANIFEST_PATH = OUTPUT_DIR / "whisperx_manifest.json"


def stem_from_source_file(source_file: str) -> str:
    """The sidecar stem for a manifest entry's transcript filename (drop a .txt suffix)."""
    return source_file[:-4] if source_file.endswith(".txt") else source_file


def current_transcript_ids(client: Client, video_id: str, entity_id: int) -> list[int]:
    """The current (non-superseded) transcript document id(s) for a meeting.

    Looked up by ``(video_id, entity_id)`` rather than filename so it is robust to the
    version chain: after a re-transcribe creates a new version, the old row carries a
    ``replaces_id`` and only the current one does not. Returns every live match so the
    caller can treat >1 (a data anomaly) as an error rather than silently picking one.
    """
    rows = (
        client.table("documents")
        .select("id, replaces_id")
        .eq("video_id", video_id)
        .eq("entity_id", entity_id)
        .eq("document_type", "transcript")
        .execute()
        .data
    )
    return sorted(r["id"] for r in rows if r.get("replaces_id") is None)


def main() -> None:
    parser = argparse.ArgumentParser(description="Persist staged WhisperX speaker layers.")
    parser.add_argument("--manifest", default=str(MANIFEST_PATH), help="manifest JSON path")
    args = parser.parse_args()

    cfg = load_config()
    if not cfg.supabase_service_key:
        raise SystemExit("ACTALUX_SUPABASE_SERVICE_KEY is required to write the speaker layer")
    service = get_client(cfg.supabase_url, cfg.supabase_service_key)

    manifest_path = Path(args.manifest)
    if not manifest_path.exists():
        raise SystemExit(f"manifest not found: {manifest_path}")
    entries = json.loads(manifest_path.read_text())
    out_dir = manifest_path.parent  # sidecars are staged next to the manifest

    persisted = 0
    for entry in entries:
        stem = stem_from_source_file(entry["source_file"])
        sidecar = out_dir / f"{stem}.attribution.json"
        if not sidecar.exists():
            logger.warning("no attribution sidecar for %s; skipping", stem)
            continue
        att = json.loads(sidecar.read_text())
        doc_ids = current_transcript_ids(service, att["video_id"], att["entity_id"])
        if not doc_ids:
            logger.warning("no ingested transcript for video %s; skipping", att["video_id"])
            continue
        if len(doc_ids) > 1:
            # Duplicate live rows for one meeting are a data anomaly — don't guess.
            logger.error(
                "multiple live transcripts for video %s entity %s (%s); skipping",
                att["video_id"],
                att["entity_id"],
                doc_ids,
            )
            continue
        doc_id = doc_ids[0]
        layer = SpeakerLayer.from_dict(att["layer"])
        persist_speaker_layer(
            service,
            doc_id,
            layer,
            media_url=att["source_url"],
            entity_id=att.get("entity_id"),
            duration_seconds=att.get("duration_seconds"),
        )
        persisted += 1
        # Resolve identities right after the turns exist (deterministic; publishes only
        # clean anchors, the rest go to the review queue). Service client for read+write.
        proposals = resolve_document(service, service, doc_id, att["entity_id"])
        published = sum(1 for p in proposals if p.confidence == "inferred_high")
        logger.info(
            "persisted speaker layer for doc %s (%d turns, %d identities: %d published)",
            doc_id,
            len(layer.turns),
            len(proposals),
            published,
        )

    logger.info("persisted %d/%d staged layer(s)", persisted, len(entries))


if __name__ == "__main__":
    main()
