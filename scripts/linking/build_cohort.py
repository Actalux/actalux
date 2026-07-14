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
from actalux.db import get_client, get_place_by_path
from actalux.diarization.linking.observations import VoiceObservation, load_observation_dir
from actalux.errors import ActaluxError

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# The embedding model is FROZEN (migrate_040 / Phase-0 spike). A cohort must be embedded with the
# same model as the gallery it normalizes against, or the cosine geometry does not line up.
DEFAULT_MODEL = "pyannote/wespeaker-voxceleb-resnet34-LM"
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
    """Count the acoustic-condition split of the cohort (audit: is it Zoom/in-person balanced?)."""
    return dict(Counter(o.acoustic_condition for o in obs))


def _insert_vectors(client: Client, cohort_id: int, obs: list[VoiceObservation]) -> None:
    """Insert the cohort vectors in batches (unlabeled — only condition + coarse provenance)."""
    for start in range(0, len(obs), VECTORS_PER_BATCH):
        batch = obs[start : start + VECTORS_PER_BATCH]
        client.table("linking_cohort_vectors").insert(
            [
                {
                    "cohort_id": cohort_id,
                    "embedding": [float(x) for x in o.embedding],
                    "acoustic_condition": o.acoustic_condition,
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

    header = (
        client.table("linking_cohorts")
        .insert(
            {
                "slug": args.slug,
                "place_id": place_id,
                "model": args.model,
                "source": args.source,
                "n_vectors": len(obs),
                "condition_balance": balance,
                "notes": args.notes,
                "is_active": False,  # activated last, after siblings are deactivated
            }
        )
        .execute()
    )
    cohort_id = header.data[0]["id"]
    logger.info("created linking_cohorts id=%d", cohort_id)
    _insert_vectors(client, cohort_id, obs)
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
        "--model", default=DEFAULT_MODEL, help="embedding model (must match the gallery)"
    )
    parser.add_argument("--notes", help="free-text notes stored on the cohort header")
    parser.add_argument(
        "--activate", action="store_true", help="make this the active cohort in scope"
    )
    parser.add_argument("--dry-run", action="store_true", help="report only; no DB writes")
    build(parser.parse_args())


if __name__ == "__main__":
    main()
