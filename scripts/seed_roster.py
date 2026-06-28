"""Seed the curated roster: persons + per-board subjects + memberships + aliases.

The roster (scripts/roster/<place>.json) is the source of truth for who sits on
each body and the name variants that resolve to them — the ground truth the vote
projector resolves roll-call names against (connections-graph Phase 1). Rosters
change (elections), so this is a re-runnable upsert, not a one-shot: edit the JSON
and re-run. Idempotent — re-running reproduces the same persons/subjects/aliases.

Person model (Model B, per docs/architecture/phase0-person-org-schema.md). A person
is ONE ``persons`` row (the global identity). Their record on each governing body is
its OWN ``subjects`` row (one per body), tied back by ``subjects.person_id``. So a
person on two bodies (an alderman who also sits on the Plan Commission) becomes two
subjects under one person — different boards are different rows by construction, so
nothing can silently merge two people. The primary board (lowest entity_id) keeps
the clean slug = the person slug; every other board gets an internal slug
``{slug}--{body_slug}``, which keeps the blanket ``subjects UNIQUE(place_id,type,slug)``
intact (the public identity is ``persons.slug``; ``subjects.slug`` is an internal
attestation key). The roster lists a cross-body person once per body; this seeder
GROUPS entries by slug, so it reads the whole roster even under ``--body`` (a body's
surname alias must not clobber another body's full-name alias for the same person).

Distinct people MUST carry distinct slugs (the curator's assertion: same slug = same
person). Two roster entries that share a slug but disagree on canonical_name are a
roster error (a typo, or an accidental merge) and HARD-FAIL — the seeder never
silently collapses two names into one person.

Every roster member is a public official -> publishable. Two minting-gate triggers
guard that (a publishable subject needs a membership; a publishable person needs a
publishable subject), so writes follow the order the triggers require: insert the
person (publishable deferred), then per board upsert the subject, attach its
membership + aliases, flip the subject publishable, and finally flip the person
publishable.

Run ``--body`` only for incremental roster edits AFTER the initial per-board seed:
the first full run (no ``--body``) is what splits cross-body people across boards;
a ``--body`` run touches only that body's subjects.

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


def build_people(bodies: dict, entity_by_body: dict, place_id: int) -> dict[str, dict]:
    """Group roster entries by slug into one person, with one subject per body.

    Returns ``{person_slug: plan}`` where each plan is::

        {"person":   {"slug", "canonical_name"},
         "subjects": [{"subject": <row>, "entity_id", "body_slug",
                       "membership": <row>, "aliases": <shared set>}, ...]}

    A person on N bodies yields N per-board subjects tied by one person: the primary
    board (lowest entity_id) keeps the clean slug = the person slug; every other board
    gets ``{slug}--{body_slug}``. Aliases are unioned across the person's bodies and
    copied onto each per-board subject — the resolver scopes candidates by body, so a
    surname alias must sit on each board's subject. Per-board role/ward/term come from
    that body's listing. Subject-level metadata is that board's listing too (a
    cross-body member's role differs between, say, council and the Plan Commission).

    Hard-fails on a slug that maps to two different canonical names: same slug = one
    person is the curator's assertion, so a name mismatch on a shared slug is a roster
    error, never a silent collapse.
    """
    people: dict[str, dict] = {}
    for body_slug, members in bodies.items():
        entity_id = entity_by_body[body_slug]["id"]
        for member in members:
            canonical = member["canonical_name"]
            slug = member.get("slug") or slugify(canonical)
            person = people.get(slug)
            if person is None:
                person = {
                    "slug": slug,
                    "canonical_name": canonical,
                    "aliases": set(),
                    "by_body": {},
                }
                people[slug] = person
            elif person["canonical_name"] != canonical:
                raise SystemExit(
                    f"Roster slug {slug!r} maps to two names: {person['canonical_name']!r} and "
                    f"{canonical!r}. Distinct people need distinct slugs; a shared slug means the "
                    "same person. Fix the roster (pin a slug, or correct the name)."
                )
            person["aliases"].update([canonical, *member.get("aliases", [])])
            # A body lists a member once; first listing per body wins for role/term.
            person["by_body"].setdefault(entity_id, (body_slug, member))

    plans: dict[str, dict] = {}
    for person in people.values():
        bodies_on = sorted(person["by_body"])  # entity_ids ascending; primary = lowest
        primary = bodies_on[0]
        subjects = []
        for entity_id in bodies_on:
            body_slug, member = person["by_body"][entity_id]
            subj_slug = person["slug"] if entity_id == primary else f"{person['slug']}--{body_slug}"
            subjects.append(
                {
                    "subject": {
                        "type": "person",
                        "subject_role": "official",
                        "canonical_name": person["canonical_name"],
                        "slug": subj_slug,
                        "place_id": place_id,
                        "entity_id": entity_id,
                        "minting_basis": "roster",
                        "metadata": _subject_metadata(member),
                    },
                    "entity_id": entity_id,
                    "body_slug": body_slug,
                    "membership": _membership_row(member, entity_id),
                    "aliases": person["aliases"],  # shared union; copied to each board
                }
            )
        plans[person["slug"]] = {
            "person": {"slug": person["slug"], "canonical_name": person["canonical_name"]},
            "subjects": subjects,
        }
    return plans


def _apply_person(client, plan: dict) -> tuple[int, int, int]:
    """Write one person + its per-board subjects (persons, subjects, memberships, aliases).

    Order satisfies both minting gates AND is recovery-safe: upsert the person DEMOTED
    (publishable=false) -> per board upsert the subject, DEMOTE it publishable=false,
    replace its single membership + its aliases, re-publish the subject -> finally flip
    the person publishable (now it has >=1 publishable subject).

    Demote-before-replace at BOTH levels is what makes a partial failure self-healing
    (these are separate HTTP writes, not one transaction). The two minting gates only
    fire on publishable=true: `subjects_minting_gate` (a publishable subject needs a
    membership) and `persons_minting_gate` (a publishable person needs a publishable
    subject). If any write fails mid-run, every touched row is left publishable=false —
    a state a re-run repairs from the top. The person upsert itself sets publishable=false
    so that re-running over a row a PRIOR run left publishable=true (with its subject
    since demoted) does not trip the person gate before the script can repair it. The
    public never sees a half-state: anon reads only publishable rows.

    Idempotent: each subject's memberships and aliases are replaced from the plan
    (delete-then-insert handles NULL term dates a unique-key upsert cannot dedupe), so
    a re-run reproduces exactly the file's state. Returns (subjects, memberships,
    aliases) written for tallying.
    """
    person_row = (
        client.table("persons")
        .upsert(
            {
                "slug": plan["person"]["slug"],
                "canonical_name": plan["person"]["canonical_name"],
                "publishable": False,  # demote until >=1 subject is published (gate-safe re-run)
            },
            on_conflict="slug",
        )
        .execute()
    )
    person_id = person_row.data[0]["id"]

    n_subjects = n_memberships = n_aliases = 0
    for sp in plan["subjects"]:
        subject_row = (
            client.table("subjects")
            .upsert({**sp["subject"], "person_id": person_id}, on_conflict="place_id,type,slug")
            .execute()
        )
        subject_id = subject_row.data[0]["id"]

        # Demote before replacing the membership so a partial failure (delete ok, insert
        # fails) leaves a recoverable state, not a publishable membership-less subject.
        client.table("subjects").update({"publishable": False}).eq("id", subject_id).execute()

        # Replace this subject's membership (one body per per-board subject).
        client.table("memberships").delete().eq("subject_id", subject_id).execute()
        client.table("memberships").insert(
            [{"subject_id": subject_id, **sp["membership"]}]
        ).execute()

        # Replace aliases (so a removed alias in the file is removed in the DB).
        client.table("subject_aliases").delete().eq("subject_id", subject_id).execute()
        aliases = _alias_rows(subject_id, sp["subject"]["canonical_name"], sorted(sp["aliases"]))
        if aliases:
            client.table("subject_aliases").insert(aliases).execute()

        # Now that a membership exists, the trigger permits publishable.
        client.table("subjects").update({"publishable": True}).eq("id", subject_id).execute()
        n_subjects += 1
        n_memberships += 1
        n_aliases += len(aliases)

    # Now that >=1 publishable subject exists, the person may publish.
    client.table("persons").update({"publishable": True}).eq("id", person_id).execute()
    return n_subjects, n_memberships, n_aliases


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
    if args.body and args.apply:
        # The initial per-board split MUST be a full run: a --body run touches only one
        # body, so running it before the split would strand a cross-body person's
        # other-board membership on the unsplit subject (two on_body candidates ->
        # ambiguous) or drop it (broken member URLs). Once persons is populated (the full
        # seed has run), the subjects are split and --body is safe.
        migrated = client.table("persons").select("id", count="exact").limit(1).execute()
        if (migrated.count or 0) == 0:
            raise SystemExit(
                "--body needs the initial full per-board seed first (the persons table is "
                "empty). Run `seed_roster.py --apply` once without --body, then scope with --body."
            )

    state, place_slug = place.split("/")
    entity_by_body: dict[str, dict] = {}
    for body_slug in bodies:
        entity = get_entity_by_path(client, state, place_slug, body_slug)
        if not entity:
            raise SystemExit(f"Unknown body {place}/{body_slug}; seed the entity first (migrate).")
        entity_by_body[body_slug] = entity
    place_id = next(iter(entity_by_body.values()))["place_id"]

    plans = build_people(bodies, entity_by_body, place_id)
    if args.body:
        # Keep only this body's per-board subjects; drop persons with none on it.
        target_eid = entity_by_body[args.body]["id"]
        scoped: dict[str, dict] = {}
        for slug, plan in plans.items():
            subjects = [sp for sp in plan["subjects"] if sp["entity_id"] == target_eid]
            if subjects:
                scoped[slug] = {**plan, "subjects": subjects}
        plans = scoped

    eid_to_body = {e["id"]: b for b, e in entity_by_body.items()}
    n_persons = n_subjects = n_memberships = n_aliases = 0
    for plan in plans.values():
        if args.apply:
            s, m, a = _apply_person(client, plan)
        else:
            s = len(plan["subjects"])
            m = sum(1 for _ in plan["subjects"])
            a = sum(
                len(_alias_rows(0, sp["subject"]["canonical_name"], sorted(sp["aliases"])))
                for sp in plan["subjects"]
            )
        n_persons += 1
        n_subjects += s
        n_memberships += m
        n_aliases += a
        boards = ", ".join(eid_to_body.get(sp["entity_id"], "?") for sp in plan["subjects"])
        logger.info(
            "  %-24s person=%-22s [%s]  %d subjects",
            plan["person"]["canonical_name"],
            plan["person"]["slug"],
            boards,
            s,
        )
    verb = "Seeded" if args.apply else "Would seed"
    logger.info(
        "%s %s: %d persons, %d subjects, %d memberships, %d aliases.",
        verb,
        place,
        n_persons,
        n_subjects,
        n_memberships,
        n_aliases,
    )
    if not args.apply:
        logger.info("Dry run. Re-run with --apply to write.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
