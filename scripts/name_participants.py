"""Name self-identified public participants (tier 2) over a place's transcripts.

Detection reuses the resolver's introduction surface patterns but, unlike the resolver,
KEEPS non-roster names — the "named-in-transcript" tier (no persistent entity, no
voiceprint, no cross-meeting linkage). Universal minor suppression is applied before any
body flag; persistence then honors each body's ``public_participant_naming`` policy
(auto -> approved, review -> proposed, off -> nothing) and skips clusters a tracked
official already owns.

This is a manual/backfill tool. It is deliberately NOT wired into the nightly ingest
pipeline. Uses the diarization turns already persisted — no re-transcription.

Usage:
    doppler run --project mac --config dev -- uv run python scripts/name_participants.py \
        --state mo --place clayton --apply
    doppler run --project mac --config dev -- uv run python scripts/name_participants.py \
        --state mo --place clayton --body plan-commission --apply
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
from actalux.identity.name_extraction import STOP_WORDS, place_stop_tokens  # noqa: E402
from actalux.identity.participant_names import (  # noqa: E402
    detect_participant_names,
    persist_participant_names,
    turns_for_participant_naming,
)
from actalux.identity.resolve import RosterMember, members_for_entity  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("name_participants")


def _service_client() -> Client:
    """A service-key Supabase client (transcript_speaker_names writes are service-only)."""
    import os

    cfg = load_config()
    key = os.environ.get("ACTALUX_SUPABASE_SERVICE_KEY", "")
    if not key:
        raise ActaluxError("ACTALUX_SUPABASE_SERVICE_KEY is required")
    return get_client(cfg.supabase_url, key)


def _docs_with_turns(client: Client, place_id: int, body: str | None) -> list[dict]:
    """Live (non-superseded) transcript documents of the place that have diarization turns."""
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
    parser = argparse.ArgumentParser(description="Name self-identified public participants.")
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
    stops = STOP_WORDS | place_stop_tokens(place)

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
        logger.info("dry run — pass --apply to detect + persist tier-2 names")

    members_cache: dict[int, list[RosterMember]] = {}
    total_proposed = total_written = 0
    for doc in docs:
        entity_id = doc["entity_id"]
        members = members_cache.get(entity_id)
        if members is None:
            members = members_for_entity(service, entity_id)
            members_cache[entity_id] = members
        turns = turns_for_participant_naming(service, doc["id"])
        proposals = detect_participant_names(turns, members, stops)
        total_proposed += len(proposals)
        if args.apply:
            written = persist_participant_names(service, doc["id"], entity_id, proposals)
            total_written += written
            logger.info(
                "doc %s: %d tier-2 name(s) detected, %d written", doc["id"], len(proposals), written
            )
        elif proposals:
            logger.info("doc %s: %d tier-2 name(s) detected (dry run)", doc["id"], len(proposals))
    logger.info(
        "%d document(s): %d tier-2 name(s) detected, %d written",
        len(docs),
        total_proposed,
        total_written,
    )


if __name__ == "__main__":
    main()
