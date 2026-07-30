"""Audit a body's spoken roll calls against the attendance its minutes record.

Reports members the roll left unanswered whom the minutes nonetheless seat as present, with both
sides quoted so every reading is checkable. Read-only: nothing is written to the database.

    doppler run --project mac --config dev -- uv run python scripts/audit_attendance.py \
        --state mo --place clayton --body council

Coverage is bounded by diarization: a meeting is only readable when its transcript has turns.
The summary prints how many meetings were skipped and why, so a quiet run is never mistaken for
a clean one.
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
    by_date: dict[str, dict[str, int]] = defaultdict(dict)
    for doc in docs:
        if doc.get("meeting_date") and doc["document_type"] in ("minutes", "transcript"):
            by_date[doc["meeting_date"]][doc["document_type"]] = doc["id"]
    pairs = {d: v for d, v in sorted(by_date.items()) if len(v) == 2}

    skipped: dict[str, int] = defaultdict(int)
    readings = []
    for date, ids in pairs.items():
        # Turn-level first: when the diarizer kept the responses in their own turns, a separate
        # voice answering is the stronger evidence. It rarely does - it usually glues the "here"
        # into the clerk's turn - so the same roll is then read from the text.
        turns = _turns(sb, ids["transcript"])
        roll = roll_call_attendance(turns, roster) if turns else None
        basis = "turns"
        if roll is None:
            roll = roll_call_from_text(
                _chunk_text(sb, ids["transcript"], _TRANSCRIPT_CHUNKS), roster, title_forms
            )
            basis = "text"
        if roll is None:
            skipped["no roll call detected in transcript"] += 1
            continue
        minutes = parse_minutes_attendance(_chunk_text(sb, ids["minutes"], _MINUTES_CHUNKS))
        if minutes is None:
            skipped["minutes have no attendance block"] += 1
            continue
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
    print(f"\n  readings (silent at roll, seated by minutes): {len(readings)}")
    for r in readings:
        print(f"\n  {r['meeting_date']}  {r['member']}")
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
