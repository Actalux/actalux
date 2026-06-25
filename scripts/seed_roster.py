"""Seed the curated roster (subjects + memberships + aliases) from a data file.

The roster (data/roster/<place>.json) is the source of truth for who sits on each
body and the name variants that resolve to them — the ground truth the vote
projector resolves roll-call names against (connections-graph Phase 1). Rosters
change (elections), so this is a re-runnable upsert, not a one-shot: edit the JSON
and re-run. Idempotent — re-running reproduces the same subjects/memberships/aliases.

Every roster member is a public official -> publishable. The subjects table guards
that with a trigger (a publishable person needs a roster membership), so each member
is written in the order the trigger requires: insert the subject (publishable
defaults false), attach its membership, then flip publishable=true.

Dry-run by default. Writing needs the service key (RLS bypass + trigger path).

Run (prefix with `doppler run --project mac --config dev --`):
  uv run python scripts/seed_roster.py                       # dry run, all bodies
  uv run python scripts/seed_roster.py --body council        # dry run, one body
  uv run python scripts/seed_roster.py --apply               # write all bodies
"""

from __future__ import annotations

import argparse
import json
import logging
import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from actalux.config import load_config  # noqa: E402
from actalux.db import get_client, get_entity_by_path  # noqa: E402
from actalux.graph.resolve import normalize_name  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

DEFAULT_ROSTER = Path(__file__).resolve().parent / "roster" / "mo_clayton.json"


def slugify(name: str) -> str:
    """URL slug for a subject: lowercase, non-alphanumeric runs -> single hyphen."""
    return re.sub(r"[^a-z0-9]+", "-", name.lower()).strip("-")


def _alias_rows(subject_id: int, canonical_name: str, raw_aliases: list[str]) -> list[dict]:
    """Distinct normalized-alias rows for a subject (canonical name + listed aliases).

    The canonical name is always an alias so a document that prints the full name
    resolves; the listed aliases cover the forms actually seen in roll calls (for
    council, the bare surname is the load-bearing one). Normalization is the same
    function the resolver applies to incoming names, so the two always agree.
    """
    seen: dict[str, str] = {}  # normalized -> first raw form (kept for audit)
    for raw in [canonical_name, *raw_aliases]:
        norm = normalize_name(raw)
        if norm and norm not in seen:
            seen[norm] = raw
    return [
        {"subject_id": subject_id, "normalized_alias": norm, "raw_alias": raw, "source": "roster"}
        for norm, raw in seen.items()
    ]


def _plan_member(member: dict, entity_id: int, place_id: int) -> dict:
    """The subject/membership/alias payloads for one roster member (no DB writes)."""
    canonical = member["canonical_name"]
    slug = member.get("slug") or slugify(canonical)
    subject = {
        "type": "person",
        "subject_role": "official",
        "canonical_name": canonical,
        "slug": slug,
        "place_id": place_id,
        "minting_basis": "roster",
        # publishable is intentionally omitted (defaults false); flipped true after
        # the membership exists, per the minting-gate trigger.
        "metadata": {
            "role": member.get("role"),
            "ward": member.get("ward"),
            "term_start_basis": member.get("term_start_basis"),
            "term_end_basis": member.get("term_end_basis"),
            "source": member.get("source"),
        },
    }
    membership = {
        "entity_id": entity_id,
        "role": member.get("role"),
        "start_date": member.get("term_start"),
        "end_date": member.get("term_end"),
    }
    return {"subject": subject, "membership": membership, "aliases": member.get("aliases", [])}


def _apply_member(client, plan: dict, entity_id: int) -> tuple[int, int]:
    """Write one member (subject -> membership -> aliases -> publishable). Returns
    (alias_count, 1) for tallying. Idempotent: memberships/aliases are replaced."""
    subject_row = (
        client.table("subjects").upsert(plan["subject"], on_conflict="place_id,type,slug").execute()
    )
    subject_id = subject_row.data[0]["id"]

    # Replace this member's membership for the body (delete-then-insert handles a
    # NULL start_date, which a unique-key upsert cannot dedupe).
    client.table("memberships").delete().eq("subject_id", subject_id).eq(
        "entity_id", entity_id
    ).execute()
    client.table("memberships").insert({"subject_id": subject_id, **plan["membership"]}).execute()

    # Replace aliases (so a removed alias in the file is removed in the DB).
    client.table("subject_aliases").delete().eq("subject_id", subject_id).execute()
    aliases = _alias_rows(subject_id, plan["subject"]["canonical_name"], plan["aliases"])
    if aliases:
        client.table("subject_aliases").insert(aliases).execute()

    # Now that the membership exists, the trigger permits publishable.
    client.table("subjects").update({"publishable": True}).eq("id", subject_id).execute()
    return len(aliases), 1


def seed_body(client, body_slug: str, members: list[dict], place: str, *, apply: bool) -> None:
    """Seed one body's roster. ``place`` is 'state/place' (e.g. 'mo/clayton')."""
    state, place_slug = place.split("/")
    entity = get_entity_by_path(client, state, place_slug, body_slug)
    if not entity:
        raise SystemExit(f"Unknown body {place}/{body_slug}; seed the entity first (migrate_012).")
    entity_id, place_id = entity["id"], entity["place_id"]

    n_subjects = n_aliases = 0
    for member in members:
        plan = _plan_member(member, entity_id, place_id)
        start = plan["membership"]["start_date"] or "?"
        end = plan["membership"]["end_date"] or "present"
        window = f"{start}..{end}"
        if apply:
            a, s = _apply_member(client, plan, entity_id)
            n_aliases += a
            n_subjects += s
        else:
            a = len(_alias_rows(0, plan["subject"]["canonical_name"], plan["aliases"]))
            n_aliases += a
            n_subjects += 1
        logger.info(
            "  %-22s slug=%-22s %s  %d aliases",
            plan["subject"]["canonical_name"],
            plan["subject"]["slug"],
            window,
            a,
        )
    verb = "Seeded" if apply else "Would seed"
    logger.info("%s %s/%s: %d subjects, %d aliases.", verb, place, body_slug, n_subjects, n_aliases)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--roster", type=Path, default=DEFAULT_ROSTER, help="roster JSON path")
    parser.add_argument("--body", help="scope to one body_slug (e.g. council)")
    parser.add_argument("--apply", action="store_true", help="write to the DB (default: dry run)")
    args = parser.parse_args()

    config = load_config()
    if args.apply and not config.supabase_service_key:
        raise SystemExit("ACTALUX_SUPABASE_SERVICE_KEY is required to --apply.")
    client = get_client(config.supabase_url, config.supabase_service_key or config.supabase_key)

    roster = json.loads(args.roster.read_text(encoding="utf-8"))
    place = roster["place"]
    bodies = roster["bodies"]
    if args.body:
        if args.body not in bodies:
            raise SystemExit(f"Body {args.body!r} not in roster (have: {', '.join(bodies)}).")
        bodies = {args.body: bodies[args.body]}

    for body_slug, members in bodies.items():
        logger.info("== %s/%s ==", place, body_slug)
        seed_body(client, body_slug, members, place, apply=args.apply)

    if not args.apply:
        logger.info("Dry run. Re-run with --apply to write.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
