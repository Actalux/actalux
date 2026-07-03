"""Re-run identity resolution over a place's existing transcripts (no re-transcription).

Resolution normally runs once, inside the transcription pipeline (persist_whisperx.py).
When the resolver itself learns a new anchor pattern (e.g. presenter_intro), the already
persisted diarization turns are still valid — only the name->cluster resolution needs to
re-run. This script does exactly that: for every live transcript document of a place (or
one body), call resolve_document(), which reconciles speaker_identities (confirmed rows
are never touched; stale auto rows are retracted).

Usage:
    doppler run --project mac --config dev -- uv run python scripts/reresolve_identities.py \
        --state mo --place clayton --apply
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from supabase import Client  # noqa: E402

from actalux.config import load_config  # noqa: E402
from actalux.db import fetch_all_rows, get_client, get_place_by_path  # noqa: E402
from actalux.diarization.enrollment import superseded_doc_ids  # noqa: E402
from actalux.errors import ActaluxError  # noqa: E402
from actalux.identity.resolve import resolve_document  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("reresolve_identities")


def _service_client() -> Client:
    """A service-key Supabase client (speaker_identities writes are service-only)."""
    import os

    cfg = load_config()
    key = os.environ.get("ACTALUX_SUPABASE_SERVICE_KEY", "")
    if not key:
        raise ActaluxError("ACTALUX_SUPABASE_SERVICE_KEY is required")
    return get_client(cfg.supabase_url, key)


def _docs_with_turns(client: Client, place_id: int, body: str | None) -> list[dict]:
    """Live (non-superseded) documents of the place that have persisted diarization turns."""
    entities = fetch_all_rows(
        lambda: client.table("entities").select("id,body_slug").eq("place_id", place_id)
    )
    if body:
        entities = [e for e in entities if e.get("body_slug") == body]
    if not entities:
        raise ActaluxError(f"no entities for place {place_id} (body={body!r})")
    entity_ids = [e["id"] for e in entities]
    docs = fetch_all_rows(
        lambda: (
            client.table("documents")
            .select("id,entity_id,replaces_id")
            .in_("entity_id", entity_ids)
            .eq("document_type", "transcript")
        )
    )
    superseded = superseded_doc_ids(docs)
    live_ids = [d["id"] for d in docs if d["id"] not in superseded]
    with_turns = {
        r["document_id"]
        for r in fetch_all_rows(
            lambda: (
                client.table("diarization_turns").select("document_id").in_("document_id", live_ids)
            )
        )
    }
    return sorted((d for d in docs if d["id"] in with_turns), key=lambda d: d["id"])


def main() -> None:
    parser = argparse.ArgumentParser(description="Re-resolve speaker identities for a place.")
    parser.add_argument("--state", required=True, help="place state slug, e.g. mo")
    parser.add_argument("--place", required=True, help="place slug, e.g. clayton")
    parser.add_argument("--body", help="restrict to one body_slug; default all bodies")
    parser.add_argument("--limit", type=int, help="cap the number of documents processed")
    parser.add_argument("--apply", action="store_true", help="persist (default: list targets only)")
    args = parser.parse_args()

    service = _service_client()
    place = get_place_by_path(service, args.state, args.place)
    if not place:
        raise ActaluxError(f"no place {args.state}/{args.place}")

    docs = _docs_with_turns(service, place["id"], args.body)
    if args.limit:
        docs = docs[: args.limit]
    logger.info(
        "%s/%s%s: %d transcript document(s) with turns",
        args.state,
        args.place,
        f"/{args.body}" if args.body else "",
        len(docs),
    )
    if not args.apply:
        logger.info("dry run — pass --apply to re-resolve and persist")
        return

    total = published = 0
    for doc in docs:
        proposals = resolve_document(service, service, doc["id"], doc["entity_id"])
        pub = sum(1 for p in proposals if p.confidence == "inferred_high")
        total += len(proposals)
        published += pub
        logger.info("doc %s: %d identities (%d published)", doc["id"], len(proposals), pub)
    logger.info(
        "re-resolved %d document(s): %d identities (%d published)", len(docs), total, published
    )


if __name__ == "__main__":
    main()
