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
import string
from collections import defaultdict
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
from actalux.identity.resolve import RosterMember, members_for_entity

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("analyze_self_intro_coverage")

# --- extraction tunables ----------------------------------------------------------
# A self-intro cue qualifies if it opens the turn (within the first N tokens, so a
# "Hi, my name is ..." greeting prefix still counts) OR begins a fresh sentence
# (pyannote under-segmentation glues a later intro into an ongoing turn — requiring a
# sentence boundary keeps that precise rather than matching a mid-sentence "this is").
SELF_INTRO_HEAD_TOKENS = 6
# A name candidate is at most this many capitalized tokens ("Jane Elizabeth Harris").
MAX_NAME_TOKENS = 3
# Determiner/honorific filler tokens skipped between a cue and the name it introduces
# ("recognize Commissioner Jane Harris", "introduce our Jane Harris"); bounded so the
# name must still follow the cue closely (precision over recall).
MAX_FILLER_SKIP = 3
# Words captured verbatim after the name as the person's self-stated role/affiliation.
SNIPPET_WORDS = 15
# The cross-meeting threshold that matters for tracked recall (a voiceprint gallery needs
# a speaker across at least this many distinct meetings to calibrate leave-one-out).
DEFAULT_MIN_MEETINGS = 2

_SENTENCE_END = frozenset(".?!:")
_EDGE_PUNCT = string.punctuation + "“”‘’—–…"

# Self-intro cue token sequences (matched on the apostrophe/punctuation-stripped lower
# form: "I'm" -> "im", "name's" -> "names"). "this is" is included per the measurement
# spec even though the resolver excludes it as too ambiguous — the name gate carries the
# precision here.
SELF_INTRO_CUES: tuple[tuple[str, ...], ...] = (
    ("my", "name", "is"),
    ("my", "names"),
    ("this", "is"),
    ("i", "am"),
    ("im",),
)
# Presenter/recognition cue verbs (all inflections). Generic English handoff verbs whose
# object is a person; the name gate rejects the non-handoff uses ("present the budget").
PRESENTER_CUES = frozenset(
    """introduce introduces introducing introduced recognize recognizes recognizing
    recognized welcome welcomes welcoming welcomed present presents presenting presented
    invite invites inviting invited""".split()
)

# Filler tokens (determiners + honorifics) that may sit between a cue and the name.
NAME_FILLERS = frozenset(
    """our the a an my your his her their mr mrs ms mx dr doctor prof professor rev
    reverend hon honorable mayor alderman alderwoman alderperson councilmember councilman
    councilwoman commissioner chair chairman chairwoman chairperson president vice
    director superintendent principal secretary treasurer clerk attorney counsel""".split()
)

# Capitalized tokens that are NOT personal names. Kept deliberately broad on the precision
# side (a human triages the NOT-IN-ROSTER output): pronouns/discourse markers, titles/roles,
# procedural + document nouns, address/direction words, days, and months. Place-specific
# tokens (the town and state names) are added at runtime from the resolved place.
_STOP_WORDS = frozenset(
    """i im a an the this that these those we you he she it they them my your our his her
    their and but or so well ok okay now here there yeah yes no not going gonna wanna sorry
    thank thanks hi hello hey good great just actually really also again please welcome glad
    pleased happy honored excited sure right all one first second next last today tonight
    tomorrow morning afternoon evening everyone everybody folks oh any as own both
    someone somebody anybody nobody nothing anything everything youre thats whats heres
    theres lets dont cant wont ive hes shes weve youve maybe okay
    mr mrs ms mx dr doctor prof professor rev reverend hon honorable mayor alderman
    mr mrs ms mx dr doctor prof professor rev reverend hon honorable mayor alderman
    alderwoman alderperson councilmember councilman councilwoman council commissioner
    commission chair chairman chairwoman chairperson president vice director superintendent
    principal secretary treasurer clerk attorney counsel member members board city district
    ward staff department office division bureau agency committee subcommittee
    meeting agenda item items motion motions resolution ordinance proposition minutes budget
    report reports plan plans project projects application applications case cases petition
    presentation number section subsection exhibit page order business hearing session public
    comment comments roll call vote votes aye ayes nay nays abstain present approval consent
    street streets avenue avenues road roads boulevard drive lane court way plaza park
    building buildings floor room north south east west northeast northwest southeast
    southwest
    monday tuesday wednesday thursday friday saturday sunday
    january february march april may june july august september october november december""".split()
)

# --- token helpers ----------------------------------------------------------------


def _edge_strip(token: str) -> str:
    """A token with surrounding punctuation/quotes removed (internal ' and - kept)."""
    return token.strip(_EDGE_PUNCT)


def _cue_norm(token: str) -> str:
    """Lowercase, alphabetic-only form of a token for cue matching ("I'm." -> "im")."""
    return "".join(c for c in token.lower() if c.isalpha())


def _ends_sentence(token: str) -> bool:
    """True if ``token`` ends a sentence (last non-quote char is . ? ! :)."""
    stripped = token.rstrip("\"')]”’")
    return bool(stripped) and stripped[-1] in _SENTENCE_END


def _is_number_token(token: str) -> bool:
    """True if ``token`` is a bare number ("6852", "6674") — a bill/ordinance/item id.

    A name immediately followed by a number is almost never a personal name in this corpus
    ("introduce Bill 6852", "present Ordinance 6674"), so the candidate is rejected.
    """
    bare = _edge_strip(token)
    return bool(bare) and bare.isdigit()


def _is_name_token(token: str) -> bool:
    """True if ``token`` looks like one word of a personal name.

    Title-case (leading uppercase letter, not ALL-CAPS), letters plus internal apostrophe
    or hyphen only (O'Brien, Lyss-Lerman), at least two letters, and not a known non-name
    word. Conservative on purpose — favouring precision over recall.
    """
    bare = _edge_strip(token)
    if len(bare) < 2 or not bare[0].isalpha() or not bare[0].isupper():
        return False
    if bare.isupper():  # ALL-CAPS -> acronym, not a name (OK, TV, LRFMP)
        return False
    core = bare.replace("'", "").replace("’", "").replace("-", "")
    if not core.isalpha() or len(core) < 2:
        return False
    return core.lower() not in _STOP_WORDS


def _extract_name(
    tokens: list[str], start: int, stops: frozenset[str], *, min_tokens: int = 1
) -> tuple[str, int] | None:
    """The name candidate beginning at/after ``start`` -> (display, index-after-name).

    Skips a bounded run of determiner/honorific fillers, then collects up to
    ``MAX_NAME_TOKENS`` consecutive name-like tokens. Returns ``None`` when fewer than
    ``min_tokens`` name-like tokens follow, or when the name is immediately followed by a
    bare number (a bill/ordinance/item id, not a person). ``stops`` augments the static
    stop-list with place-specific tokens.
    """
    i, n, skipped = start, len(tokens), 0
    while i < n and skipped < MAX_FILLER_SKIP and _cue_norm(tokens[i]) in NAME_FILLERS:
        i += 1
        skipped += 1
    name_parts: list[str] = []
    while i < n and len(name_parts) < MAX_NAME_TOKENS and _is_name_token(tokens[i]):
        if _cue_norm(tokens[i]) in stops:  # place token (town/state) mid-name -> stop
            break
        name_parts.append(_edge_strip(tokens[i]))
        i += 1
    if len(name_parts) < min_tokens:
        return None
    if i < n and _is_number_token(tokens[i]):  # "Bill 6852" / "Ordinance 6674" -> not a name
        return None
    return " ".join(name_parts), i


# --- per-turn hit extraction ------------------------------------------------------


@dataclass(frozen=True)
class Hit:
    """One extracted introduction in a turn."""

    source: str  # "self_intro" | "presenter_intro"
    cue: str
    name: str
    end_index: int  # token index immediately after the name (role snippet starts here)


def _self_intro_hits(tokens: list[str], norms: list[str], stops: frozenset[str]) -> list[Hit]:
    """Self-introductions opening the turn or a fresh sentence within it."""
    hits: list[Hit] = []
    for start in range(len(tokens)):
        at_head = start <= SELF_INTRO_HEAD_TOKENS
        if not (at_head or (start > 0 and _ends_sentence(tokens[start - 1]))):
            continue
        for cue in SELF_INTRO_CUES:
            if tuple(norms[start : start + len(cue)]) != cue:
                continue
            found = _extract_name(tokens, start + len(cue), stops)
            if found:
                hits.append(Hit("self_intro", " ".join(cue), found[0], found[1]))
            break  # at most one cue family per starting position
    return hits


def _presenter_hits(tokens: list[str], norms: list[str], stops: frozenset[str]) -> list[Hit]:
    """Presenter/recognition introductions: a cue verb followed by a full (>=2 token) name.

    A full name is required (unlike self-intro, which allows a bare surname) to match the
    resolver's own posture — it anchors a presenter introduction only on a full-name / alias
    span, never a bare token — and because a single capitalized word after a generic cue
    verb ("welcome Everyone", "present Both") is a frequent false positive.
    """
    hits: list[Hit] = []
    for i, norm in enumerate(norms):
        if norm not in PRESENTER_CUES:
            continue
        found = _extract_name(tokens, i + 1, stops, min_tokens=2)
        if found:
            hits.append(Hit("presenter_intro", norm, found[0], found[1]))
    return hits


def turn_hits(text: str, stops: frozenset[str]) -> list[Hit]:
    """Distinct introduction hits in one turn's text (deduped by source+name)."""
    tokens = text.split()
    norms = [_cue_norm(t) for t in tokens]
    seen: set[tuple[str, str]] = set()
    out: list[Hit] = []
    for hit in _self_intro_hits(tokens, norms, stops) + _presenter_hits(tokens, norms, stops):
        key = (hit.source, normalize_name(hit.name))
        if key not in seen:
            seen.add(key)
            out.append(hit)
    return out


def _role_snippet(text: str, end_index: int) -> str:
    """Up to ``SNIPPET_WORDS`` verbatim tokens after the name (the self-stated role)."""
    return " ".join(text.split()[end_index : end_index + SNIPPET_WORDS]).strip()


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


def roster_keys(members: list[RosterMember]) -> tuple[set[str], dict[str, list[int]]]:
    """The body's roster comparison keys and a key -> subject_ids index.

    Keys are ``normalize_name`` outputs: every stored alias (which the seeder already
    normalized, and which includes the canonical name) plus the canonical name normalized
    defensively. The index lets an in-roster hit be attributed to the actual official(s).
    """
    key_to_subjects: dict[str, list[int]] = defaultdict(list)
    for m in members:
        keys = set(m.aliases) | {normalize_name(m.canonical_name)}
        for k in keys:
            if k:
                key_to_subjects[k].append(m.subject_id)
    return set(key_to_subjects), dict(key_to_subjects)


# --- DB-facing scan ---------------------------------------------------------------


def _service_client() -> Client:
    """A service-key Supabase client (reads below-gate diarization + identity rows)."""
    cfg = load_config()
    key = os.environ.get("ACTALUX_SUPABASE_SERVICE_KEY", "") or cfg.supabase_service_key
    if not key:
        raise SystemExit("ACTALUX_SUPABASE_SERVICE_KEY is required")
    return get_client(cfg.supabase_url, key)


def _place_stop_tokens(place: dict[str, Any]) -> frozenset[str]:
    """Place-specific stop tokens (town + state names) so the town name isn't read as a name."""
    parts: list[str] = []
    fields = ("state", "slug", "name", "display_name")
    for value in (place.get(f) for f in fields):
        if isinstance(value, str):
            parts.extend(value.replace("-", " ").split())
    return frozenset(_cue_norm(p) for p in parts if _cue_norm(p))


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
                        role_snippet=_role_snippet(text, hit.end_index),
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
    stops = _STOP_WORDS | _place_stop_tokens(place)
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
