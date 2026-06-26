"""Seed the name-corrections lexicon (mangling -> canonical) for one place.

The corrections are the single home for proper-noun spelling fixes (officials, staff,
streets, businesses, schools), consumed by search recall and the downstream newsletter.
Jurisdiction-scoped (cardinal repo rule): the source file is per place
(scripts/corrections/<state>_<place>.json) and every row carries the resolved place_id,
because the same string can be a mangling in one town and a real name in another.

The file is the source of truth: a re-run replaces the place's corrections wholesale
(delete-then-insert), so a removed line disappears from the DB. Dry-run by default;
writing needs the service key.

Run (prefix with `doppler run --project mac --config dev --`):
  uv run python scripts/seed_corrections.py                 # dry run
  uv run python scripts/seed_corrections.py --apply         # write
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from collections import Counter
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from actalux.config import load_config  # noqa: E402
from actalux.db import get_client, get_place_by_path  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

DEFAULT_FILE = Path(__file__).resolve().parent / "corrections" / "mo_clayton.json"


def build_rows(place_id: int, corrections: list[dict]) -> list[dict]:
    """Normalized name_corrections rows for one place (mangled lowercased + stripped).

    The mangling is the match key, so it is normalized to lowercase here; consumers
    match it case-insensitively and word-boundaried. The last entry wins on a dup key
    (the UNIQUE (place_id, mangled) constraint would otherwise reject the insert).
    """
    by_mangled: dict[str, dict] = {}
    for c in corrections:
        mangled = c["mangled"].strip().lower()
        if not mangled or not c.get("canonical"):
            continue
        if mangled in by_mangled and by_mangled[mangled]["canonical"] != c["canonical"]:
            logger.warning(
                "duplicate mangling %r -> %r / %r; keeping the last",
                mangled,
                by_mangled[mangled]["canonical"],
                c["canonical"],
            )
        by_mangled[mangled] = {
            "place_id": place_id,
            "mangled": mangled,
            "canonical": c["canonical"],
            "category": c.get("category"),
            "provenance": c.get("provenance"),
            "active": True,
        }
    return list(by_mangled.values())


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--file", type=Path, default=DEFAULT_FILE, help="corrections JSON path")
    parser.add_argument("--apply", action="store_true", help="write to the DB (default: dry run)")
    args = parser.parse_args()

    config = load_config()
    if args.apply and not config.supabase_service_key:
        raise SystemExit("ACTALUX_SUPABASE_SERVICE_KEY is required to --apply.")
    client = get_client(config.supabase_url, config.supabase_service_key or config.supabase_key)

    data = json.loads(args.file.read_text(encoding="utf-8"))
    state, place_slug = data["place"].split("/")
    place = get_place_by_path(client, state, place_slug)
    if not place:
        raise SystemExit(f"Unknown place {data['place']!r}; seed the place first.")
    place_id = place["id"]

    rows = build_rows(place_id, data["corrections"])
    cats = Counter(r["category"] for r in rows)

    if args.apply:
        # File is the source of truth for this place: replace wholesale.
        client.table("name_corrections").delete().eq("place_id", place_id).execute()
        if rows:
            client.table("name_corrections").insert(rows).execute()

    verb = "Seeded" if args.apply else "Would seed"
    logger.info(
        "%s %s: %d corrections %s", verb, data["place"], len(rows), dict(cats.most_common())
    )
    if not args.apply:
        logger.info("Dry run. Re-run with --apply to write.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
