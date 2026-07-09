#!/usr/bin/env python3
"""Measure the speaker-identification headroom in self- and presenter-introductions.

The deterministic resolver (``actalux.identity.resolve``) anchors a diarization cluster
to an official from three spoken-name signals — roll call, self-introduction, presenter
introduction. Two of those (self-intro, presenter-intro) are **roster-gated**: a name is
only turned into an anchor when it matches a pre-existing publishable roster member. A
speaker who identifies themselves — or is introduced on the record — but is NOT on the
roster is *detected* by the same surface patterns and then *discarded*.

This script quantifies what that gate throws away. Over every live (non-superseded)
document with diarization turns, for each public body of a place, it:

  1. finds SELF-INTRODUCTION openings ("my name is X", "I'm X", "I am X", "this is X"),
  2. finds PRESENTER INTRODUCTIONS (an introduce/recognize/welcome/present/invite cue verb
     followed by a capitalized name),
  3. extracts a conservative name candidate (a short capitalized token run, honorifics
     skipped, obvious non-name words rejected),
  4. cross-checks each distinct candidate against the body's roster using the resolver's
     own comparison key (``graph.resolve.normalize_name`` — the same normalizer the seeder
     used to store every alias), bucketing it IN-ROSTER (an anchor the gate KEEPS) or
     NOT-IN-ROSTER (a self-identified/introduced person the gate currently DISCARDS).

The NOT-IN-ROSTER bucket — with a captured role-snippet and the meetings each name appears
in — is the actionable output: the pool of additional identifiable speakers, ready for a
human to triage into official / presenter / protected-staff / public.

This is a measurement of *headroom*, deliberately a bit looser than the resolver on
detection (it does not require the sustained-speech / adjacency structure the resolver uses
to safely ATTRIBUTE an anchor), so its counts are an upper-bound-ish estimate of what the
roster gate discards, not an exact replica of which anchors the resolver would fire.

The name-extraction primitives live in ``actalux.identity.name_extraction`` (shared with
the tier-2 participant-naming module so the two never drift); this script layers the
scan + aggregation + reporting on top.

Read-only. Nothing is written. Jurisdiction-agnostic: place, bodies, roster, and documents
are all resolved from ``--state`` / ``--place`` (bodies are discovered, never hardcoded).

Usage (prefix every invocation with ``doppler run --project mac --config dev --``):
    uv run python scripts/analyze_self_intro_coverage.py --state mo --place clayton
    uv run python scripts/analyze_self_intro_coverage.py --state mo --place clayton --body council
"""

from __future__ import annotations

import argparse
import logging
import os
from dataclasses import dataclass, field
from typing import Any

from supabase import Client

from actalux.config import load_config
from actalux.db import (
    fetch_all_rows,
    get_client,
    get_diarization_turns,
    get_place_by_path,
)
from actalux.diarization.enrollment import superseded_doc_ids
from actalux.graph.resolve import normalize_name
from actalux.identity.name_extraction import (
    STOP_WORDS,
    place_stop_tokens,
    role_snippet,
    roster_keys,
    turn_hits,
)
from actalux.identity.resolve import members_for_entity

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("analyze_self_intro_coverage")

# The cross-meeting threshold that matters for tracked recall (a voiceprint gallery needs
# a speaker across at least this many distinct meetings to calibrate leave-one-out).
DEFAULT_MIN_MEETINGS = 2


# --- aggregation ------------------------------------------------------------------


@dataclass
class Occurrence:
    """One time a candidate name was introduced, with context for triage."""

    document_id: int
    meeting_date: str
    start_seconds: float
    source: str
    cue: str
    role_snippet: str


@dataclass
class Candidate:
    """A distinct introduced person (by normalized-name key) and every occurrence."""

    key: str
    display: str
    occurrences: list[Occurrence] = field(default_factory=list)
    in_roster: bool = False
    subject_ids: set[int] = field(default_factory=set)

    @property
    def meetings(self) -> set[int]:
        return {o.document_id for o in self.occurrences}

    @property
    def sources(self) -> set[str]:
        return {o.source for o in self.occurrences}


def _stamp(seconds: float) -> str:
    """H:MM:SS timestamp for a turn start."""
    s = int(seconds)
    h, rem = divmod(s, 3600)
    m, s = divmod(rem, 60)
    return f"{h}:{m:02d}:{s:02d}"


# --- DB-facing scan ---------------------------------------------------------------


def _service_client() -> Client:
    """A service-key Supabase client (reads below-gate diarization + identity rows)."""
    cfg = load_config()
    key = os.environ.get("ACTALUX_SUPABASE_SERVICE_KEY", "") or cfg.supabase_service_key
    if not key:
        raise SystemExit("ACTALUX_SUPABASE_SERVICE_KEY is required")
    return get_client(cfg.supabase_url, key)


def _live_docs_with_turns(client: Client, entity_id: int) -> list[dict[str, Any]]:
    """Live (non-superseded) documents for a body, metadata only, meeting order."""
    docs = fetch_all_rows(
        lambda: (
            client.table("documents")
            .select("id,replaces_id,meeting_date,meeting_title,video_id")
            .eq("entity_id", entity_id)
        )
    )
    superseded = superseded_doc_ids(docs)
    live = [d for d in docs if d["id"] not in superseded]
    return sorted(live, key=lambda d: (d.get("meeting_date") or "", d["id"]))


def scan_body(
    client: Client, entity_id: int, stops: frozenset[str]
) -> tuple[dict[str, Candidate], int]:
    """Aggregate every introduction candidate for one body -> (candidates, docs_scanned)."""
    members = members_for_entity(client, entity_id)
    keys, key_to_subjects = roster_keys(members)

    candidates: dict[str, Candidate] = {}
    docs_scanned = 0
    for doc in _live_docs_with_turns(client, entity_id):
        turns = get_diarization_turns(client, doc["id"])
        if not turns:
            continue
        docs_scanned += 1
        meeting_date = doc.get("meeting_date") or ""
        for turn in turns:
            text = " ".join(w.get("word", "") for w in (turn.get("words") or []))
            if not text:
                continue
            for hit in turn_hits(text, stops):
                key = normalize_name(hit.name)
                if not key:
                    continue
                cand = candidates.get(key)
                if cand is None:
                    cand = Candidate(key=key, display=hit.name)
                    if key in keys:
                        cand.in_roster = True
                        cand.subject_ids.update(key_to_subjects[key])
                    candidates[key] = cand
                cand.occurrences.append(
                    Occurrence(
                        document_id=doc["id"],
                        meeting_date=meeting_date,
                        start_seconds=float(turn.get("start_seconds") or 0.0),
                        source=hit.source,
                        cue=hit.cue,
                        role_snippet=role_snippet(text, hit.end_index),
                    )
                )
    return candidates, docs_scanned


# --- reporting --------------------------------------------------------------------


@dataclass
class BodyResult:
    body_slug: str
    display_name: str
    docs_scanned: int
    candidates: dict[str, Candidate]

    def in_roster(self) -> list[Candidate]:
        return [c for c in self.candidates.values() if c.in_roster]

    def not_in_roster(self) -> list[Candidate]:
        return [c for c in self.candidates.values() if not c.in_roster]

    def in_roster_subjects(self) -> set[int]:
        subjects: set[int] = set()
        for c in self.in_roster():
            subjects.update(c.subject_ids)
        return subjects


def _print_body(result: BodyResult, min_meetings: int) -> None:
    """Print one body's counts and its full NOT-IN-ROSTER list with role snippets."""
    nir = result.not_in_roster()
    nir_multi = [c for c in nir if len(c.meetings) >= min_meetings]
    print("\n" + "=" * 78)
    print(f"BODY: {result.body_slug}  ({result.display_name})")
    print(f"  documents scanned (live, with turns): {result.docs_scanned}")
    print(f"  distinct introduced people:           {len(result.candidates)}")
    print(
        f"    IN-ROSTER  (anchors the gate keeps):    {len(result.in_roster())} name-keys "
        f"-> {len(result.in_roster_subjects())} distinct officials"
    )
    print(f"    NOT-IN-ROSTER (currently discarded):    {len(nir)}")
    print(f"      of those, in >= {min_meetings} distinct meetings:   {len(nir_multi)}")
    if not nir:
        return
    print("-" * 78)
    print("  NOT-IN-ROSTER (name | #meetings | sources | role-snippet -> doc@stamp):")
    for c in sorted(nir, key=lambda c: (-len(c.meetings), -len(c.occurrences), c.display.lower())):
        occ = sorted(c.occurrences, key=lambda o: (o.meeting_date, o.document_id, o.start_seconds))
        snippet = next((o.role_snippet for o in occ if o.role_snippet), "")
        srcs = "+".join(sorted({"self" if s == "self_intro" else "pres" for s in c.sources}))
        refs = ", ".join(f"doc{o.document_id}@{_stamp(o.start_seconds)}" for o in occ[:4])
        more = f" (+{len(occ) - 4} more)" if len(occ) > 4 else ""
        print(f"    * {c.display}  | {len(c.meetings)}mtg | {srcs}")
        if snippet:
            print(f'        role: "{snippet}"')
        print(f"        seen: {refs}{more}")


def _print_totals(results: list[BodyResult], min_meetings: int) -> None:
    """Cross-body totals."""
    docs = sum(r.docs_scanned for r in results)
    people = sum(len(r.candidates) for r in results)
    in_keys = sum(len(r.in_roster()) for r in results)
    in_subjects = sum(len(r.in_roster_subjects()) for r in results)
    nir = sum(len(r.not_in_roster()) for r in results)
    nir_multi = sum(
        len([c for c in r.not_in_roster() if len(c.meetings) >= min_meetings]) for r in results
    )
    print("\n" + "#" * 78)
    print("CROSS-BODY TOTALS")
    print(f"  documents scanned:                  {docs}")
    print(f"  distinct introduced people:         {people}")
    print(f"  IN-ROSTER name-keys / officials:    {in_keys} / {in_subjects}")
    print(f"  NOT-IN-ROSTER (discarded):          {nir}")
    print(f"    of those, in >= {min_meetings} distinct meetings: {nir_multi}")
    print("#" * 78)


# --- entrypoint -------------------------------------------------------------------


def _bodies_for_place(client: Client, place_id: int, only: str | None) -> list[dict[str, Any]]:
    """Discover the place's public bodies (optionally one), never hardcoded."""
    ents = fetch_all_rows(
        lambda: (
            client.table("entities").select("id,body_slug,display_name").eq("place_id", place_id)
        )
    )
    if only:
        ents = [e for e in ents if e.get("body_slug") == only]
    return sorted(ents, key=lambda e: e.get("body_slug") or "")


def run(state: str, place_slug: str, body: str | None, min_meetings: int) -> None:
    """Scan every (or one) public body of a place and print the coverage report."""
    client = _service_client()
    place = get_place_by_path(client, state, place_slug)
    if not place:
        raise SystemExit(f"no place {state}/{place_slug}")
    stops = STOP_WORDS | place_stop_tokens(place)
    bodies = _bodies_for_place(client, place["id"], body)
    if not bodies:
        raise SystemExit(f"no bodies for {state}/{place_slug}" + (f" body={body}" if body else ""))

    results: list[BodyResult] = []
    for ent in bodies:
        logger.info("scanning body %s (entity %s)", ent.get("body_slug"), ent["id"])
        candidates, docs_scanned = scan_body(client, ent["id"], stops)
        results.append(
            BodyResult(
                body_slug=ent.get("body_slug") or str(ent["id"]),
                display_name=ent.get("display_name") or "",
                docs_scanned=docs_scanned,
                candidates=candidates,
            )
        )

    for result in results:
        _print_body(result, min_meetings)
    _print_totals(results, min_meetings)


def main() -> None:
    summary = (__doc__ or "").splitlines()[0] if __doc__ else None
    parser = argparse.ArgumentParser(description=summary)
    parser.add_argument("--state", default="mo", help="place state slug (default: mo)")
    parser.add_argument("--place", default="clayton", help="place slug (default: clayton)")
    parser.add_argument("--body", default=None, help="restrict to one body slug (default: all)")
    parser.add_argument(
        "--min-meetings",
        type=int,
        default=DEFAULT_MIN_MEETINGS,
        help=f"cross-meeting coverage threshold (default: {DEFAULT_MIN_MEETINGS})",
    )
    args = parser.parse_args()
    run(args.state, args.place, args.body, args.min_meetings)


if __name__ == "__main__":
    main()
