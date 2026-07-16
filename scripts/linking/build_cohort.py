"""Freeze a linking AS-norm cohort from one or more [E] embedding caches (source-agnostic).

The cross-meeting linker scores AS-norm against a diverse, target-disjoint impostor cohort
(docs/architecture/linking-backend-decision-2026-07-12.md). This freezes such a cohort into
``linking_cohort_vectors`` (migrate_047) from any pre-embedded source(s): other Clayton bodies'
meetings, another town's public meetings, or an open corpus embedded through the SAME 256-d
wespeaker model. The vectors are UNLABELED — a background yardstick, not a gallery. The CALLER is
responsible for supplying sources that are target-disjoint from the bodies the cohort will score.

Run (dry-run — report count + condition balance, no writes):
    doppler run --project mac --config dev -- \\
      uv run python scripts/linking/build_cohort.py \\
      --slug mo-clayton-external-v1 --source clayton-council-pc \\
      --state mo --place clayton --activate --dry-run \\
      data/linking_cache/mo_clayton_council data/linking_cache/mo_clayton_plan-commission
"""

from __future__ import annotations

import argparse
import logging
import os
from collections import Counter
from pathlib import Path

from supabase import Client

from actalux.config import load_config
from actalux.db import fetch_all_rows, get_client, get_place_by_path
from actalux.diarization.enrollment import EMBED_MODEL
from actalux.diarization.linking.observations import VoiceObservation, load_observation_dir
from actalux.errors import ActaluxError

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

VECTORS_PER_BATCH = 500


def service_client() -> Client:
    cfg = load_config()
    key = os.environ.get("ACTALUX_SUPABASE_SERVICE_KEY", "")
    if not key:
        raise ActaluxError("ACTALUX_SUPABASE_SERVICE_KEY is required (service-only tables)")
    return get_client(cfg.supabase_url, key)


def load_sources(cache_dirs: list[str]) -> list[VoiceObservation]:
    """Concatenate the observations across every [E] cache directory given."""
    obs: list[VoiceObservation] = []
    for d in cache_dirs:
        loaded = load_observation_dir(Path(d))
        logger.info("loaded %d clusters from %s", len(loaded), d)
        obs.extend(loaded)
    return obs


def condition_balance(obs: list[VoiceObservation]) -> dict[str, int]:
    """Count the cohort's acoustic-condition split (audit: how Zoom/in-person diverse is it?)."""
    return dict(Counter(o.acoustic_condition for o in obs))


def document_entities(client: Client, doc_ids: list[int]) -> dict[int, int | None]:
    """Map each source document to the body that produced it — provenance + disjointness check."""
    rows = fetch_all_rows(
        lambda: client.table("documents").select("id,entity_id").in_("id", doc_ids)
    )
    return {r["id"]: r.get("entity_id") for r in rows}


def verify_disjoint(
    client: Client, place_id: int, bodies: list[str], doc_entities: dict[int, int | None]
) -> None:
    """Hard-fail if any source meeting belongs to a body the cohort will be scored against.

    Target-disjointness is what makes an external cohort safe: if the target's own officials sit in
    the impostor pool, their sibling clusters inflate the normalizer and suppress exactly the true
    matches we want (the self-cohort contamination that made AS-norm look degenerate in phase 1).
    The build cannot infer the target, so the caller names it — and this refuses to guess.
    """
    entities = fetch_all_rows(
        lambda: client.table("entities").select("id,body_slug").eq("place_id", place_id)
    )
    for body in bodies:
        target_ids = {e["id"] for e in entities if e.get("body_slug") == body}
        if not target_ids:
            raise ActaluxError(f"no entity for body {body!r} in place {place_id}")
        overlap = sorted(d for d, e in doc_entities.items() if e in target_ids)
        if overlap:
            raise ActaluxError(
                f"cohort sources are NOT disjoint from {body!r}: {len(overlap)} source meeting(s) "
                f"belong to it (e.g. {overlap[:5]}). A target's own voices in the impostor pool "
                f"suppress its true matches — rebuild the cohort from other bodies."
            )
        logger.info("disjointness OK: no source meeting belongs to %r", body)


def _insert_vectors(
    client: Client,
    cohort_id: int,
    obs: list[VoiceObservation],
    doc_entities: dict[int, int | None],
) -> None:
    """Insert the cohort vectors in batches (unlabeled — only condition + coarse provenance)."""
    for start in range(0, len(obs), VECTORS_PER_BATCH):
        batch = obs[start : start + VECTORS_PER_BATCH]
        client.table("linking_cohort_vectors").insert(
            [
                {
                    "cohort_id": cohort_id,
                    "embedding": [float(x) for x in o.embedding],
                    "acoustic_condition": o.acoustic_condition,
                    "source_entity_id": doc_entities.get(o.document_id),
                    "source_document_id": o.document_id,
                }
                for o in batch
            ]
        ).execute()
        logger.info("inserted vectors %d..%d", start, start + len(batch))


def _activate(client: Client, cohort_id: int, place_id: int | None) -> None:
    """Make this the sole active cohort in its scope (deactivate siblings first, then activate)."""
    if place_id is not None:
        client.table("linking_cohorts").update({"is_active": False}).eq(
            "place_id", place_id
        ).execute()
    else:
        client.table("linking_cohorts").update({"is_active": False}).is_(
            "place_id", "null"
        ).execute()
    client.table("linking_cohorts").update({"is_active": True}).eq("id", cohort_id).execute()


def build(args: argparse.Namespace) -> None:
    obs = load_sources(args.cache_dirs)
    if not obs:
        raise ActaluxError("no observations loaded from the given cache dirs")
    balance = condition_balance(obs)
    logger.info("cohort '%s': %d vectors, condition balance %s", args.slug, len(obs), balance)

    if args.dry_run:
        logger.info(
            "dry-run: no writes (slug=%s source=%s activate=%s)",
            args.slug,
            args.source,
            args.activate,
        )
        return

    client = service_client()
    place_id = None
    if args.state and args.place:
        place = get_place_by_path(client, args.state, args.place)
        if not place:
            raise ActaluxError(f"no place {args.state}/{args.place}")
        place_id = place["id"]

    doc_entities = document_entities(client, sorted({o.document_id for o in obs}))
    if args.verify_disjoint_body:
        if place_id is None:
            raise ActaluxError("--verify-disjoint-body needs --state/--place to resolve the body")
        verify_disjoint(client, place_id, args.verify_disjoint_body, doc_entities)

    header = (
        client.table("linking_cohorts")
        .insert(
            {
                "slug": args.slug,
                "place_id": place_id,
                "model": args.model,
                "source": args.source,
                # counted after the vectors actually land: a partial insert must not leave a header
                # claiming a vector count the cohort does not have
                "n_vectors": 0,
                "condition_balance": balance,
                "notes": args.notes,
                "is_active": False,  # activated last, after siblings are deactivated
            }
        )
        .execute()
    )
    cohort_id = header.data[0]["id"]
    logger.info("created linking_cohorts id=%d", cohort_id)
    _insert_vectors(client, cohort_id, obs, doc_entities)
    client.table("linking_cohorts").update({"n_vectors": len(obs)}).eq("id", cohort_id).execute()
    if args.activate:
        _activate(client, cohort_id, place_id)
        logger.info("activated cohort id=%d for place_id=%s", cohort_id, place_id)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("cache_dirs", nargs="+", help="[E] embedding cache dir(s) to ingest")
    parser.add_argument("--slug", required=True, help="unique cohort slug")
    parser.add_argument("--source", required=True, help="provenance label, e.g. clayton-council-pc")
    parser.add_argument("--state", help="scope the cohort to a place (with --place); omit = shared")
    parser.add_argument("--place", help="scope the cohort to a place (with --state); omit = shared")
    parser.add_argument(
        "--model", default=EMBED_MODEL, help="embedding model (must match the gallery)"
    )
    parser.add_argument("--notes", help="free-text notes stored on the cohort header")
    parser.add_argument(
        "--verify-disjoint-body",
        action="append",
        metavar="BODY",
        help="hard-fail if any source meeting belongs to this body_slug (repeatable). Pass every "
        "body the cohort will score against — target voices in the impostor pool suppress their "
        "own true matches",
    )
    parser.add_argument(
        "--activate", action="store_true", help="make this the active cohort in scope"
    )
    parser.add_argument("--dry-run", action="store_true", help="report only; no DB writes")
    build(parser.parse_args())


if __name__ == "__main__":
    main()
