"""Backfill the first-class geography fields for a place (geography half).

Pairs with migrate_039 (the additive DDL) and reads a per-place geography config
(scripts/places/<state>_<place>.json). For that place it idempotently sets:

  1. ``place_type``  (city|county|state|...);
  2. ``county``      (the containing county name, if not already set);
  3. ``geoid``       (the Census GEOID -- the stable join key for future polygons);
  4. ``parent_place_id`` (resolved from a parent "state/slug", else left NULL);
  5. ``metadata``    (provenance + parent GEOIDs, merged onto any existing metadata).

Additive only -- it enriches the existing `places` row. It does NOT mint new
places (the minimal-seam scope: no county/state rows yet), touch organizations,
or change any id. Re-runnable: the write is an idempotent update keyed on
(state, slug). Every value is verified against an authoritative source and lives
in the config (CLAUDE.md: never invent geography; per-place config, not constants).

Run (prefix with `doppler run --project mac --config dev --`):
  uv run python scripts/backfill_places.py                        # dry run, mo/clayton
  uv run python scripts/backfill_places.py --place mo/clayton --apply
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

PLACE_CONFIG_DIR = Path(__file__).resolve().parent / "places"


def load_place_config(place: str) -> dict[str, Any]:
    """Load the per-place geography config (scripts/places/<state>_<place>.json)."""
    state, place_slug = place.split("/")
    path = PLACE_CONFIG_DIR / f"{state}_{place_slug}.json"
    if not path.exists():
        raise SystemExit(f"No place config at {path}")
    return json.loads(path.read_text(encoding="utf-8"))


def resolve_parent_id(client: Any, parent: str | None) -> int | None:
    """Resolve a parent "state/slug" to a places.id, or None when unset/absent."""
    if not parent:
        return None
    p_state, p_slug = parent.split("/")
    rows = (
        client.table("places").select("id").eq("state", p_state).eq("slug", p_slug).execute().data
    )
    if not rows:
        # The minimal-seam scope mints no county/state rows yet -- a configured
        # parent that doesn't exist is left NULL (the column is the seam), not invented.
        logger.warning("  parent %r not in DB -- leaving parent_place_id NULL.", parent)
        return None
    return rows[0]["id"]


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--place", default="mo/clayton", help="state/place (default mo/clayton)")
    parser.add_argument("--apply", action="store_true", help="write to the DB (default: dry run)")
    args = parser.parse_args()

    config = load_config()
    if args.apply and not config.supabase_service_key:
        raise SystemExit("ACTALUX_SUPABASE_SERVICE_KEY is required to --apply.")
    client = get_client(config.supabase_url, config.supabase_service_key or config.supabase_key)

    cfg = load_place_config(args.place)
    state, place_slug = args.place.split("/")

    place_rows = (
        client.table("places")
        .select("id,display_name,county,place_type,geoid,metadata")
        .eq("state", state)
        .eq("slug", place_slug)
        .execute()
        .data
    )
    if not place_rows:
        raise SystemExit(f"Place {args.place} not found.")
    current = place_rows[0]
    place_id = current["id"]

    parent_id = resolve_parent_id(client, cfg.get("parent"))

    # Merge config metadata onto whatever is already there (don't clobber prior keys).
    merged_metadata = {**(current.get("metadata") or {}), **cfg.get("metadata", {})}

    update = {
        "place_type": cfg["place_type"],
        "county": cfg["county"],
        "geoid": cfg["geoid"],
        "parent_place_id": parent_id,
        "metadata": merged_metadata,
    }

    verb = "Applying" if args.apply else "Would apply"
    logger.info(
        "%s geography for %s (%s): type=%s county=%s geoid=%s parent_id=%s",
        verb,
        args.place,
        current["display_name"],
        update["place_type"],
        update["county"],
        update["geoid"],
        parent_id,
    )

    if not args.apply:
        logger.info("Dry run. Re-run with --apply to write.")
        return 0

    client.table("places").update(update).eq("id", place_id).execute()
    logger.info("Updated place %s (id=%s).", args.place, place_id)
    return 0


if __name__ == "__main__":
    sys.exit(main())
