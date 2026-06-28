"""Phase 0b (orgs): backfill the organization tier for a place.

Pairs with migrate_036 (the additive DDL) and reads a per-place org config
(scripts/orgs/<state>_<place>.json). For that place it idempotently:

  1. upserts each ``organizations`` row (keyed on state+slug);
  2. sets ``entities.organization_id`` for every body the org owns;
  3. links the org to the geographies it serves via ``org_serves_place``.

Additive only — it creates the org tier and wires existing bodies to it. It does
NOT touch subjects, memberships, edges, or any attribution: the per-board person
split lives in the (rewritten) roster seeder, and edges are rebuilt by the
projector. Re-runnable: every write is an upsert or an idempotent update.

Run (prefix with `doppler run --project mac --config dev --`):
  uv run python scripts/backfill_orgs.py                       # dry run, mo/clayton
  uv run python scripts/backfill_orgs.py --place mo/clayton --apply
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parent))

from actalux.config import load_config  # noqa: E402
from actalux.db import get_client  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

ORG_CONFIG_DIR = Path(__file__).resolve().parent / "orgs"


def load_org_config(place: str) -> dict[str, Any]:
    """Load the per-place org config (scripts/orgs/<state>_<place>.json)."""
    state, place_slug = place.split("/")
    path = ORG_CONFIG_DIR / f"{state}_{place_slug}.json"
    if not path.exists():
        raise SystemExit(f"No org config at {path}")
    return json.loads(path.read_text(encoding="utf-8"))


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--place", default="mo/clayton", help="state/place (default mo/clayton)")
    parser.add_argument("--apply", action="store_true", help="write to the DB (default: dry run)")
    args = parser.parse_args()

    config = load_config()
    if args.apply and not config.supabase_service_key:
        raise SystemExit("ACTALUX_SUPABASE_SERVICE_KEY is required to --apply.")
    client = get_client(config.supabase_url, config.supabase_service_key or config.supabase_key)

    cfg = load_org_config(args.place)
    state, place_slug = args.place.split("/")

    place_rows = (
        client.table("places").select("id").eq("state", state).eq("slug", place_slug).execute().data
    )
    if not place_rows:
        raise SystemExit(f"Place {args.place} not found.")
    place_id = place_rows[0]["id"]

    entities = (
        client.table("entities").select("id,body_slug").eq("place_id", place_id).execute().data
    )
    ent_id_by_slug = {e["body_slug"]: e["id"] for e in entities}

    verb = "Applying" if args.apply else "Would apply"
    for org in cfg["organizations"]:
        # Validate bodies before any write so a config typo fails loudly, not halfway.
        for body in org["bodies"]:
            if body not in ent_id_by_slug:
                raise SystemExit(
                    f"Org {org['slug']!r} lists unknown body {body!r} for {args.place}."
                )
        logger.info(
            "%s org %s (%s): owns %s, serves %s",
            verb,
            org["slug"],
            org["organization_type"],
            org["bodies"],
            org["serves"],
        )
        if not args.apply:
            continue

        org_row = (
            client.table("organizations")
            .upsert(
                {
                    "slug": org["slug"],
                    "name": org["name"],
                    "organization_type": org["organization_type"],
                    "state": org["state"],
                },
                on_conflict="state,slug",
            )
            .execute()
        )
        org_id = org_row.data[0]["id"]

        for body in org["bodies"]:
            client.table("entities").update({"organization_id": org_id}).eq(
                "id", ent_id_by_slug[body]
            ).execute()

        for served in org["serves"]:
            served_rows = (
                client.table("places")
                .select("id")
                .eq("state", org["state"])
                .eq("slug", served)
                .execute()
                .data
            )
            if not served_rows:
                logger.warning("  serves %r: place not in DB yet — skipping link.", served)
                continue
            client.table("org_serves_place").upsert(
                {
                    "organization_id": org_id,
                    "place_id": served_rows[0]["id"],
                    "relation": "serves",
                },
                on_conflict="organization_id,place_id,relation",
            ).execute()

    if not args.apply:
        logger.info("Dry run. Re-run with --apply to write.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
