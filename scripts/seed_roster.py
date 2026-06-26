"""Seed the curated roster (subjects + memberships + aliases) from a data file.

The roster (scripts/roster/<place>.json) is the source of truth for who sits on
each body and the name variants that resolve to them — the ground truth the vote
projector resolves roll-call names against (connections-graph Phase 1). Rosters
change (elections), so this is a re-runnable upsert, not a one-shot: edit the JSON
and re-run. Idempotent — re-running reproduces the same subjects/memberships/aliases.

A person may sit on more than one body (e.g. an alderman who also serves on the
Plan Commission). The roster lists them once per body; this seeder GROUPS entries
by subject (slug) across all bodies, so the subject carries one unioned alias set
and one membership per body. That is why grouping reads the whole roster even under
``--body``: a body's surname alias must not clobber another body's full-name alias
for the same person.

Every roster member is a public official -> publishable. The subjects table guards
that with a trigger (a publishable person needs a roster membership), so each member
is written in the order the trigger requires: insert the subject (publishable
defaults false), attach its memberships, then flip publishable=true.

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


def _subject_metadata(member: dict) -> dict:
    """Display/audit metadata stored on the subject (role/ward/source)."""
    return {
        "role": member.get("role"),
        "ward": member.get("ward"),
        "term_start_basis": member.get("term_start_basis"),
        "term_end_basis": member.get("term_end_basis"),
        "source": member.get("source"),
    }


def _membership_row(member: dict, entity_id: int) -> dict:
    """One body-membership for a subject (the per-body role + term window)."""
    return {
        "entity_id": entity_id,
        "role": member.get("role"),
        "start_date": member.get("term_start"),
        "end_date": member.get("term_end"),
    }


def build_subjects(bodies: dict, entity_by_body: dict, place_id: int) -> dict[str, dict]:
    """Group roster entries by slug into one plan per subject across all bodies.

    A person on two bodies appears once per body in the file; here their aliases are
    unioned and a membership is collected per body, so the seeder writes a single
    subject. Subject-level metadata is taken from the first listing (the per-body
    role lives on each membership). ``place_id`` is attached to each plan.
    """
    groups: dict[str, dict] = {}
    for body_slug, members in bodies.items():
        entity_id = entity_by_body[body_slug]["id"]
        for member in members:
            canonical = member["canonical_name"]
            slug = member.get("slug") or slugify(canonical)
            group = groups.get(slug)
            if group is None:
                group = {
                    "subject": {
                        "type": "person",
                        "subject_role": "official",
                        "canonical_name": canonical,
                        "slug": slug,
                        "place_id": place_id,
                        "minting_basis": "roster",
                        "metadata": _subject_metadata(member),
                    },
                    "aliases": set(),
                    "memberships": [],
                }
                groups[slug] = group
            elif group["subject"]["canonical_name"] != canonical:
                logger.warning(
                    "slug %s maps to differing names %r / %r; keeping the first",
                    slug,
                    group["subject"]["canonical_name"],
                    canonical,
                )
            group["aliases"].update([canonical, *member.get("aliases", [])])
            group["memberships"].append(_membership_row(member, entity_id))
    return groups


def _apply_subject(client, plan: dict) -> tuple[int, int]:
    """Write one grouped subject (subject -> memberships -> aliases -> publishable).

    Idempotent: all of the subject's memberships and aliases are replaced from the
    plan (the roster owns them), so a re-run reproduces exactly the file's state.
    Returns (alias_count, membership_count) for tallying.
    """
    subject_row = (
        client.table("subjects").upsert(plan["subject"], on_conflict="place_id,type,slug").execute()
    )
    subject_id = subject_row.data[0]["id"]

    # Replace ALL of this subject's memberships from the plan (the plan is built from
    # the whole roster, so it carries every body the person sits on). Delete-then-
    # insert handles NULL term dates a unique-key upsert cannot dedupe.
    client.table("memberships").delete().eq("subject_id", subject_id).execute()
    rows = [{"subject_id": subject_id, **m} for m in plan["memberships"]]
    client.table("memberships").insert(rows).execute()

    # Replace aliases (so a removed alias in the file is removed in the DB).
    client.table("subject_aliases").delete().eq("subject_id", subject_id).execute()
    aliases = _alias_rows(subject_id, plan["subject"]["canonical_name"], sorted(plan["aliases"]))
    if aliases:
        client.table("subject_aliases").insert(aliases).execute()

    # Now that a membership exists, the trigger permits publishable.
    client.table("subjects").update({"publishable": True}).eq("id", subject_id).execute()
    return len(aliases), len(plan["memberships"])


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--roster", type=Path, default=DEFAULT_ROSTER, help="roster JSON path")
    parser.add_argument("--body", help="scope writes to subjects on one body_slug (e.g. council)")
    parser.add_argument("--apply", action="store_true", help="write to the DB (default: dry run)")
    args = parser.parse_args()

    config = load_config()
    if args.apply and not config.supabase_service_key:
        raise SystemExit("ACTALUX_SUPABASE_SERVICE_KEY is required to --apply.")
    client = get_client(config.supabase_url, config.supabase_service_key or config.supabase_key)

    roster = json.loads(args.roster.read_text(encoding="utf-8"))
    place = roster["place"]
    bodies = roster["bodies"]
    if args.body and args.body not in bodies:
        raise SystemExit(f"Body {args.body!r} not in roster (have: {', '.join(bodies)}).")

    state, place_slug = place.split("/")
    entity_by_body: dict[str, dict] = {}
    for body_slug in bodies:
        entity = get_entity_by_path(client, state, place_slug, body_slug)
        if not entity:
            raise SystemExit(f"Unknown body {place}/{body_slug}; seed the entity first (migrate).")
        entity_by_body[body_slug] = entity
    place_id = next(iter(entity_by_body.values()))["place_id"]

    groups = build_subjects(bodies, entity_by_body, place_id)
    if args.body:
        target_eid = entity_by_body[args.body]["id"]
        groups = {
            slug: g
            for slug, g in groups.items()
            if any(m["entity_id"] == target_eid for m in g["memberships"])
        }

    eid_to_body = {e["id"]: b for b, e in entity_by_body.items()}
    n_subjects = n_aliases = n_memberships = 0
    for plan in groups.values():
        if args.apply:
            a, m = _apply_subject(client, plan)
        else:
            a = len(_alias_rows(0, plan["subject"]["canonical_name"], sorted(plan["aliases"])))
            m = len(plan["memberships"])
        n_subjects += 1
        n_aliases += a
        n_memberships += m
        bodies_on = ", ".join(eid_to_body.get(ms["entity_id"], "?") for ms in plan["memberships"])
        logger.info(
            "  %-24s slug=%-24s [%s]  %d aliases",
            plan["subject"]["canonical_name"],
            plan["subject"]["slug"],
            bodies_on,
            a,
        )
    verb = "Seeded" if args.apply else "Would seed"
    logger.info(
        "%s %s: %d subjects, %d memberships, %d aliases.",
        verb,
        place,
        n_subjects,
        n_memberships,
        n_aliases,
    )
    if not args.apply:
        logger.info("Dry run. Re-run with --apply to write.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
