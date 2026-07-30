"""Audit a body's spoken roll calls against the attendance its minutes record.

Reports members the roll left unanswered whom the minutes nonetheless seat as present, with both
sides quoted so every reading is checkable. Read-only: nothing is written to the database.

    doppler run --project mac --config dev -- uv run python scripts/audit_attendance.py \
        --state mo --place clayton --body council

Coverage is bounded by what the recording caught: most transcripts begin after the roll was
already called. The summary prints how many meetings were skipped and why, so a quiet run is
never mistaken for a clean one.
"""

from __future__ import annotations

import argparse
import json
from collections import defaultdict

from actalux.config import load_config
from actalux.db import get_client
from actalux.identity.attendance import (
    BASE_TITLE_FORMS,
    compare_attendance,
    merge_rolls,
    parse_minutes_attendance,
    roll_call_from_text,
)
from actalux.identity.resolve import ResolverTurn, RosterMember
from actalux.identity.vote_align import roll_call_attendance

# Minutes attendance sits in the opening; transcripts are read in full because the roll-call
# detector locates the region itself.
_MINUTES_CHUNKS = 3
# The roll is always at the very start of a meeting.
_TRANSCRIPT_CHUNKS = 6


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--state", required=True)
    parser.add_argument("--place", required=True)
    parser.add_argument("--body", required=True)
    parser.add_argument("--json-out", help="write the full result to this path")
    return parser.parse_args()


def _resolve_body(sb, state: str, place: str, body: str) -> tuple[int, int]:
    """Return ``(entity_id, place_id)`` for a jurisdiction-scoped body."""
    places = sb.table("places").select("id").eq("state", state).eq("slug", place).execute().data
    if not places:
        raise SystemExit(f"no place {state}/{place}")
    place_id = places[0]["id"]
    rows = (
        sb.table("entities")
        .select("id")
        .eq("place_id", place_id)
        .eq("body_slug", body)
        .execute()
        .data
    )
    if not rows:
        raise SystemExit(f"no body {body} in {state}/{place}")
    return rows[0]["id"], place_id


def _roster(sb, entity_id: int) -> list[RosterMember]:
    rows = (
        sb.table("subjects")
        .select("id,slug,canonical_name")
        .eq("entity_id", entity_id)
        .execute()
        .data
    )
    return [
        RosterMember(r["id"], r["slug"] or "", r["canonical_name"] or "", frozenset())
        for r in rows
        if r.get("canonical_name")
    ]


def _turns(sb, document_id: int) -> list[ResolverTurn]:
    rows = (
        sb.table("diarization_turns")
        .select("cluster_label,words,start_seconds")
        .eq("document_id", document_id)
        .order("start_seconds")
        .limit(5000)
        .execute()
        .data
    )
    turns = []
    for row in rows:
        words = row.get("words")
        if isinstance(words, str):
            try:
                words = json.loads(words.replace("'", '"'))
            except json.JSONDecodeError:
                words = []
        text = " ".join(w.get("word", "") for w in (words or []))
        turns.append(ResolverTurn(row["cluster_label"], text.strip()))
    return turns


def _title_forms(sb, place_id: int) -> tuple[str, ...]:
    """Honorific forms for this place: the generic set plus the glossary's recorded manglings.

    ASR mishears a body's honorific in ways specific to that body ("Alderwoman" comes back as
    "all the women"), so the manglings belong in the per-place glossary rather than in code.
    """
    rows = (
        sb.table("name_corrections")
        .select("mangled")
        .eq("place_id", place_id)
        .eq("category", "title")
        .eq("active", True)
        .execute()
        .data
    )
    return BASE_TITLE_FORMS + tuple(r["mangled"].lower() for r in rows if r.get("mangled"))


def _chunk_text(sb, document_id: int, limit: int) -> str:
    rows = (
        sb.table("chunks")
        .select("content")
        .eq("document_id", document_id)
        .order("id")
        .limit(limit)
        .execute()
        .data
    )
    return "\n".join(r["content"] for r in rows)


def main() -> None:
    args = _parse_args()
    config = load_config()
    sb = get_client(config.supabase_url, config.supabase_service_key or config.supabase_key)
    entity_id, place_id = _resolve_body(sb, args.state, args.place, args.body)
    roster = _roster(sb, entity_id)
    title_forms = _title_forms(sb, place_id)

    docs = (
        sb.table("documents")
        .select("id,document_type,meeting_date")
        .eq("entity_id", entity_id)
        .is_("replaces_id", "null")
        .limit(5000)
        .execute()
        .data
    )
    # A date can carry more than one meeting - a regular session and a special one, or a recessed
    # meeting reconvened the same evening. Pairing by date alone would then read one meeting's
    # roll against another's minutes and manufacture a discrepancy out of two correct records, so
    # an ambiguous date is dropped rather than resolved by guessing which document goes with which.
    by_date: dict[str, dict[str, list[int]]] = defaultdict(lambda: defaultdict(list))
    for doc in docs:
        if doc.get("meeting_date") and doc["document_type"] in ("minutes", "transcript"):
            by_date[doc["meeting_date"]][doc["document_type"]].append(doc["id"])

    skipped: dict[str, int] = defaultdict(int)
    pairs: dict[str, dict[str, int]] = {}
    for date, found in sorted(by_date.items()):
        if len(found) < 2:
            continue  # only one side of the comparison exists
        if any(len(ids) > 1 for ids in found.values()):
            skipped["more than one meeting shares this date"] += 1
            continue
        pairs[date] = {kind: ids[0] for kind, ids in found.items()}

    readings = []
    by_basis: dict[str, int] = defaultdict(int)
    for date, ids in pairs.items():
        # Both readers run, and their answers are unioned. The turn-level reader is the stronger
        # evidence when it fires (a separate voice audibly answering) but usually finds nothing,
        # because the diarizer folds the "here" into the clerk's turn; the text reader sees the
        # words either way. Keeping only the first to succeed would throw away answers the other
        # heard, and a discarded answer is exactly what turns into a false silence.
        turns = _turns(sb, ids["transcript"])
        from_turns = roll_call_attendance(turns, roster) if turns else None
        from_text = roll_call_from_text(
            _chunk_text(sb, ids["transcript"], _TRANSCRIPT_CHUNKS), roster, title_forms
        )
        roll = merge_rolls(from_turns, from_text)
        basis = "+".join(
            name for name, found in (("turns", from_turns), ("text", from_text)) if found
        )
        if roll is None:
            skipped["no roll call detected in transcript"] += 1
            continue
        minutes = parse_minutes_attendance(_chunk_text(sb, ids["minutes"], _MINUTES_CHUNKS))
        if minutes is None:
            skipped["minutes have no attendance block"] += 1
            continue
        by_basis[basis] += 1
        votes = (
            sb.table("votes")
            .select("details")
            .eq("document_id", ids["minutes"])
            .limit(200)
            .execute()
            .data
        )
        for reading in compare_attendance(roll, minutes, roster, votes):
            readings.append(
                {
                    "meeting_date": date,
                    "member": reading.canonical_name,
                    "transcript_quote": reading.transcript_quote,
                    "minutes_quote": reading.minutes_quote,
                    "signals": list(reading.corroborating_signals),
                    "roll_call_basis": basis,
                    "transcript_document_id": ids["transcript"],
                    "minutes_document_id": ids["minutes"],
                }
            )

    comparable = len(pairs) - sum(skipped.values())
    print(f"{args.state}/{args.place}/{args.body}")
    print(f"  meetings with both minutes and transcript : {len(pairs)}")
    print(f"  compared                                  : {comparable}")
    for reason, count in sorted(skipped.items()):
        print(f"  skipped, {reason:40}: {count}")
    # Which reader carried each meeting. Worth printing: if "turns" ever stops appearing, the
    # audit has quietly become text-only and the stronger evidence is no longer being used.
    for basis, count in sorted(by_basis.items()):
        print(f"  read from {basis:39}: {count}")
    print(f"\n  readings (silent at roll, seated by minutes): {len(readings)}")
    for r in readings:
        print(f"\n  {r['meeting_date']}  {r['member']}")
        print(f"    transcript  : {r['transcript_quote']}")
        print(f"    minutes say : {r['minutes_quote'][:160]}")
        print(f"    corroborated: {'; '.join(r['signals'])}")
        print(
            f"    documents   : transcript {r['transcript_document_id']}"
            f" / minutes {r['minutes_document_id']} (read from {r['roll_call_basis']})"
        )

    if args.json_out:
        payload = {"comparable": comparable, "skipped": dict(skipped), "readings": readings}
        with open(args.json_out, "w") as fh:
            json.dump(payload, fh, indent=2)
        print(f"\nwrote {args.json_out}")


if __name__ == "__main__":
    main()
