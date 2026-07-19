#!/usr/bin/env python3
"""Operator CLI: confirm or deny hypothesized speaker labels (lever B).

The voiceprint recall gates (labelqa Gate A + nested LOMO) trust human-``confirmed``
speaker labels as a core even when raw coherence fails, so a handful of confirmations
per official is the cheapest way to lift recall. This tool walks the unconfirmed
``speaker_identities`` proposals for a place, shows the operator each cluster's longest
transcript excerpts and a YouTube link cued to the audio, and records the decision:

  * ``y`` -> confidence='confirmed' (a trusted anchor; enrollable + publicly displayable).
    A biometric-``voiceprint`` basis is rewritten to 'manual' so enrollment eligibility
    holds (enrollment.py never trains the gallery on a voiceprint basis); any other basis
    is kept for provenance.
  * ``n`` -> confidence='rejected' (a durable denial: the resolver never re-proposes this
    name for the cluster, and it never enrolls or displays). It stores NOTHING about who
    the voice actually is — the row keeps only the OFFICIAL it was denied under (Option B).

Ordering minimizes operator time to real recall: officials with the fewest confirmed
meetings first, then distinct meetings (Gate A needs confirmed anchors in several meetings
to survive every leave-one-meeting-out fold), then the cluster with the most speech.

This tool ONLY ever touches rows that already hypothesize an official; it never creates a
row, never stores an embedding, and never records a citizen identity.

Usage:
    doppler run --project mac --config dev -- uv run python scripts/confirm_speaker.py \\
        --state mo --place clayton [--body council] [--limit 40]

Spreadsheet round-trip (for large queues): ``--export-csv FILE`` writes the queue with a blank
``decision`` column in the review sheet's reading order (low-margin listen-first block, then
officials by queue size); the operator fills y / n / the correct official's name in any
spreadsheet app; ``--decisions FILE --apply`` records them through the exact same payloads.
A name must resolve unambiguously to a rostered official of the document's body. Typos fail
the whole apply.
"""

from __future__ import annotations

import argparse
import csv
import logging
import os
from collections import defaultdict
from dataclasses import dataclass, field
from itertools import groupby
from pathlib import Path
from typing import Any

from supabase import Client

from actalux.config import load_config
from actalux.db import (
    fetch_all_rows,
    get_client,
    get_diarization_turns,
    get_place_by_path,
)
from actalux.diarization.enrollment import cluster_spans, span_seconds, superseded_doc_ids
from actalux.errors import ActaluxError

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("confirm_speaker")

# Distinct confirmed meetings that let an official survive every leave-one-meeting-out fold:
# Gate A needs >=2 confirmed meetings in each fold's TRAIN split, so with one meeting held out
# the operator targets three. Purely advisory here (progress display + ordering).
CONFIRM_TARGET_MEETINGS = 3
EXCERPTS_PER_CANDIDATE = 3
EXCERPT_MAX_CHARS = 240
# The batch screen packs many clusters onto one page, so it shows fewer, tighter excerpts than the
# clip-by-clip view and caps how many clusters an official gets per pass (the rest surface on a
# later pass). These are display-only knobs; they never affect which rows are confirmable.
BATCH_MAX_CANDIDATES_PER_OFFICIAL = 8
BATCH_EXCERPTS_PER_CANDIDATE = 2
BATCH_EXCERPT_MAX_CHARS = 200
# Gate A survives a leave-one-meeting-out fold only with >=2 confirmed meetings in the TRAIN split
# (see CONFIRM_TARGET_MEETINGS above); the batch summary flags officials that cross this bar.
ENABLEMENT_MIN_MEETINGS = 2
# A locked human decision is never re-shown as a candidate.
LOCKED_TIERS = ("confirmed", "rejected")


@dataclass(frozen=True)
class Candidate:
    """One unconfirmed ``(document, cluster)`` proposal for a specific official."""

    identity_id: int
    document_id: int
    cluster_label: str
    person_id: int  # the official (enrollment/Gate-A key; a person spans boards)
    subject_id: int
    official_name: str
    basis: str | None
    confidence: str
    meeting_key: str  # the video_id — Gate A's leave-one-out unit
    video_id: str
    meeting_title: str
    meeting_date: str
    seconds: float
    excerpts: tuple[tuple[int, str], ...] = ()  # (start_seconds, text), longest first
    roster_title: str | None = None  # the official's memberships.role in this body (display only)


@dataclass
class SessionTally:
    """Per-official confirm/deny/skip counts for the closing summary."""

    confirmed: int = 0
    denied: int = 0
    skipped: int = 0


# --- pure helpers (unit-tested) ---------------------------------------------------


def confirm_payload(candidate: Candidate) -> dict[str, str]:
    """The ``speaker_identities`` update for a confirmation.

    Sets confidence='confirmed'. A biometric ``voiceprint`` basis is rewritten to 'manual'
    (enrollment excludes voiceprint-basis rows even when confirmed, to avoid a poison loop);
    any other basis is kept so the confirmation's provenance is preserved.
    """
    payload = {"confidence": "confirmed"}
    if candidate.basis == "voiceprint":
        payload["basis"] = "manual"
    return payload


def reject_payload(candidate: Candidate) -> dict[str, str]:
    """The ``speaker_identities`` update for a denial: confidence='rejected', basis kept.

    Nothing about the true voice is recorded — the row still carries only the OFFICIAL
    subject it was proposed under, now marked rejected so it is never re-proposed, enrolled,
    or displayed (Option B).
    """
    return {"confidence": "rejected"}


# Spreadsheet review round-trip: --export-csv writes the queue with these columns; the operator
# fills `decision` (y / n / the correct official's name; blank or s = skip) in any spreadsheet
# app; --decisions reads it back and applies through the same payloads as the interactive
# session. The key columns (document_id, cluster_label) drive matching; the rest is context.
CSV_COLUMNS = (
    "decision",
    "flag",
    "official",
    "meeting_date",
    "meeting_title",
    "seconds",
    "listen_url",
    "excerpt",
    "score",
    "margin",
    "runner_up",
    "basis",
    "document_id",
    "cluster_label",
    "identity_id",
)
_SKIP_VALUES = {"", "s", "skip"}
_YES_VALUES = {"y", "yes", "confirm", "confirmed"}
_NO_VALUES = {"n", "no", "reject", "rejected"}
# The review sheet's "listen first" cutoff: a winner this close to the runner-up needs an ear.
LOW_MARGIN_FLAG = 2.0


def parse_decision(raw: str | None) -> tuple[str, str | None]:
    """``(kind, payload)``: confirm / reject / skip, or a NAME attribution.

    Any value that is not a recognized yes/no/skip token is treated as the correct
    official's name ("this voice is actually Chris Win"). Typos are still fail-closed:
    an unresolvable "name" (e.g. "yse") dies in roster resolution, blocking the apply,
    rather than being silently skipped.
    """
    value = (raw or "").strip()
    low = value.lower()
    if low in _YES_VALUES:
        return ("confirm", None)
    if low in _NO_VALUES:
        return ("reject", None)
    if low in _SKIP_VALUES:
        return ("skip", None)
    return ("name", value)


def rename_payload(subject_id: int) -> dict[str, Any]:
    """The update for a human name-attribution: the operator listened and named the voice.

    The row moves to the named official at ``confirmed``, and the basis becomes ``manual``
    regardless of what produced the original hypothesis — keeping a ``discourse``/``voiceprint``
    basis would be false provenance for an attribution that is now entirely the human's, and
    ``manual`` keeps the row enrollment-eligible.
    """
    return {"subject_id": subject_id, "confidence": "confirmed", "basis": "manual"}


@dataclass
class DecisionPlan:
    """What a decisions CSV resolves to against the live candidate queue."""

    confirm: list[Candidate] = field(default_factory=list)
    reject: list[Candidate] = field(default_factory=list)
    # (candidate, resolved subject_id, resolved display name)
    rename: list[tuple[Candidate, int, str]] = field(default_factory=list)
    skipped: int = 0
    unmatched: list[tuple[int, str]] = field(default_factory=list)  # keys not in the queue
    invalid: list[tuple[int, str, str, str]] = field(default_factory=list)  # + reason


def plan_decisions(
    candidates: list[Candidate],
    rows: list[dict[str, str]],
    name_index: dict[int, dict[str, tuple[int, str] | None]],
) -> DecisionPlan:
    """Match CSV rows to live candidates by ``(document_id, cluster_label)``.

    Unmatched keys are reported, not errors — a row can drop out of the queue between
    export and apply (someone confirmed it interactively, or the drafts were regenerated).
    Invalid values MUST block the apply (fail closed on typos).

    A name decision resolves through ``name_index`` (per-document roster of publishable
    officials, from :func:`build_name_index`): unknown names and names that are ambiguous
    within the body (two Chrises — the collision that crossed Win/Tennill) are invalid,
    never guessed. Naming the already-proposed official is just a confirm.
    """
    by_key = {(c.document_id, c.cluster_label): c for c in candidates}
    plan = DecisionPlan()
    for row in rows:
        key = (int(row["document_id"]), row["cluster_label"].strip())
        kind, payload = parse_decision(row.get("decision"))
        if kind == "skip":
            plan.skipped += 1
            continue
        candidate = by_key.get(key)
        if candidate is None:
            plan.unmatched.append(key)
            continue
        if kind == "confirm":
            plan.confirm.append(candidate)
        elif kind == "reject":
            plan.reject.append(candidate)
        else:
            resolved = name_index.get(candidate.document_id, {}).get(
                " ".join(payload.lower().split())
            )
            if resolved is None:
                reason = (
                    "ambiguous in this body — use the full name"
                    if payload
                    and " ".join(payload.lower().split())
                    in name_index.get(candidate.document_id, {})
                    else "no publishable rostered official by this name in the document's body"
                )
                plan.invalid.append((key[0], key[1], payload or "", reason))
            elif resolved[0] == candidate.subject_id:
                plan.confirm.append(candidate)  # naming the proposed official = a confirm
            else:
                plan.rename.append((candidate, resolved[0], resolved[1]))
    return plan


def build_name_index(
    docs_by_id: dict[int, dict[str, Any]],
    subjects_by_id: dict[int, dict[str, Any]],
    members_by_entity: dict[int, dict[int, str | None]],
) -> dict[int, dict[str, tuple[int, str] | None]]:
    """Per-document name -> (subject_id, canonical name) for the body's publishable officials.

    Keys are normalized (lowercase, collapsed whitespace) full canonical names, plus each
    single-token surname WHEN it is unique in the body; a surname shared by two members maps
    to ``None`` so :func:`plan_decisions` reports ambiguity instead of picking one — the
    first-name-collision lesson, applied to this input path too.
    """
    index: dict[int, dict[str, tuple[int, str] | None]] = {}
    by_entity: dict[int, dict[str, tuple[int, str] | None]] = {}
    for entity_id, members in members_by_entity.items():
        names: dict[str, tuple[int, str] | None] = {}
        for subject_id in members:
            subject = subjects_by_id.get(subject_id)
            if not subject or not subject.get("publishable") or subject.get("person_id") is None:
                continue
            canonical = subject["canonical_name"]
            full = " ".join(canonical.lower().split())
            names[full] = (subject_id, canonical)
            surname = full.split()[-1]
            if surname != full:
                names[surname] = None if surname in names else (subject_id, canonical)
        by_entity[entity_id] = names
    for doc_id, doc in docs_by_id.items():
        index[doc_id] = by_entity.get(doc.get("entity_id"), {})
    return index


def youtube_cue_url(video_id: str, start_seconds: int) -> str:
    """A YouTube watch URL cued to ``start_seconds`` (source_pane.html's pattern)."""
    return f"https://www.youtube.com/watch?v={video_id}&t={start_seconds}s"


def _turn_text(turn: dict[str, Any]) -> str:
    """Reconstruct a turn's transcript text from its word tokens (raw ASR)."""
    return " ".join(w.get("word", "") for w in (turn.get("words") or [])).strip()


def cluster_excerpts(
    turns: list[dict[str, Any]], cluster_label: str, *, limit: int = EXCERPTS_PER_CANDIDATE
) -> list[tuple[int, str]]:
    """The cluster's ``limit`` longest transcript excerpts as ``(start_seconds, text)``.

    Longest first (most words = most for the operator to recognize the voice by), each cued
    to its own turn start so the YouTube link jumps to that passage. Ties break on earlier
    start for determinism.
    """
    spoken = [
        (int(float(t["start_seconds"])), _turn_text(t))
        for t in turns
        if t["cluster_label"] == cluster_label
    ]
    spoken = [(start, text) for start, text in spoken if text]
    spoken.sort(key=lambda st: (-len(st[1].split()), st[0]))
    return spoken[:limit]


def order_candidates(
    candidates: list[Candidate], confirmed_meetings: dict[int, set[str]]
) -> list[Candidate]:
    """Order candidates to reach real recall with the fewest operator decisions.

    Officials with the fewest already-confirmed meetings come first (they are furthest from
    the Gate-A coverage an official needs). Within an official, distinct meetings not yet
    confirmed come first — one representative (the most-speech cluster) per meeting, by
    speech descending — because cross-meeting coverage, not a second cluster in the same
    meeting, is what survives the leave-one-meeting-out folds. Deterministic given the
    snapshot; the interactive loop tracks live coverage for display but does not re-sort.
    """
    by_person: dict[int, list[Candidate]] = defaultdict(list)
    for c in candidates:
        by_person[c.person_id].append(c)

    def person_key(person_id: int) -> tuple[int, float, int]:
        coverage = len(confirmed_meetings.get(person_id, set()))
        total_seconds = sum(c.seconds for c in by_person[person_id])
        return (coverage, -total_seconds, person_id)  # fewest confirmed, then most material

    ordered: list[Candidate] = []
    for person_id in sorted(by_person, key=person_key):
        already = confirmed_meetings.get(person_id, set())
        ordered.extend(_order_person_candidates(by_person[person_id], already))
    return ordered


def _order_person_candidates(cands: list[Candidate], already: set[str]) -> list[Candidate]:
    """One official's candidates: new distinct meetings first (top cluster each), then rest."""
    fresh = [c for c in cands if c.meeting_key not in already]
    seen = [c for c in cands if c.meeting_key in already]
    fresh.sort(key=lambda c: (-c.seconds, c.meeting_key, c.cluster_label))
    seen.sort(key=lambda c: (-c.seconds, c.meeting_key, c.cluster_label))
    # Front-load one representative per fresh meeting (distinct-meeting coverage), then the
    # extra clusters from meetings already represented, then clusters in already-confirmed
    # meetings (least useful — that meeting is covered).
    representatives: list[Candidate] = []
    extras: list[Candidate] = []
    represented: set[str] = set()
    for c in fresh:
        if c.meeting_key in represented:
            extras.append(c)
        else:
            represented.add(c.meeting_key)
            representatives.append(c)
    return representatives + extras + seen


# --- batch mode: per-official screens (unit-tested) -------------------------------


@dataclass(frozen=True)
class BatchInput:
    """A parsed response to one official screen. ``confirm``/``reject`` are 1-based positions."""

    action: str  # "apply" | "skip" | "quit"
    confirm: tuple[int, ...] = ()
    reject: tuple[int, ...] = ()


@dataclass(frozen=True)
class OfficialGroup:
    """One official's screen: the capped clusters shown plus how many exist in total."""

    person_id: int
    official_name: str
    roster_title: str | None
    listed: tuple[Candidate, ...]  # capped, most-speech-first — the numbered rows 1..len(listed)
    total: int  # candidates before the per-pass cap (drives the "showing N of M" note)


def _parse_positions(part: str) -> list[int] | None:
    """Parse a comma/space-separated position list; None if any token is not a bare number."""
    tokens = part.replace(",", " ").split()
    out: list[int] = []
    for tok in tokens:
        if not tok.isdigit():
            return None
        out.append(int(tok))
    return out


def parse_batch_input(raw: str, listed_count: int) -> BatchInput | None:
    """Parse one official-screen response into a decision, or None on garbage (caller re-prompts).

    Grammar (numbers are 1-based positions in the displayed list):
      ``""`` / ``s``  -> skip this official (writes nothing)
      ``q``           -> quit with a summary
      ``1,3``         -> confirm positions 1 and 3
      ``a``           -> confirm every listed position
      ``n 2``         -> reject position 2 (no confirmations)
      ``1,3 n 2``     -> confirm 1 and 3, reject 2

    The optional ``n`` clause trails the confirm part; ``a`` cannot be combined with a reject
    clause (that would confirm and reject the same rows), which parses as garbage. Out-of-range
    positions, confirm/reject overlap, and an empty result all return None so no wrong row is ever
    written on a fat-fingered entry.
    """
    text = raw.strip().lower()
    if text in {"", "s"}:
        return BatchInput("skip")
    if text == "q":
        return BatchInput("quit")

    confirm_part, sep, reject_part = text.partition("n")
    confirm_token = confirm_part.replace(",", " ").strip()
    if confirm_token == "a":
        confirm = list(range(1, listed_count + 1))
    else:
        parsed = _parse_positions(confirm_part)
        if parsed is None:
            return None
        confirm = parsed

    if sep == "n":
        rejected = _parse_positions(reject_part)
        if not rejected:  # None (garbage) or empty ("n" with no numbers) -> re-prompt
            return None
        reject = rejected
    else:
        reject = []

    positions = set(confirm) | set(reject)
    if not positions:
        return None
    if any(p < 1 or p > listed_count for p in positions):
        return None
    if set(confirm) & set(reject):
        return None
    return BatchInput("apply", tuple(sorted(set(confirm))), tuple(sorted(set(reject))))


def build_official_groups(
    candidates: list[Candidate],
    confirmed_meetings: dict[int, set[str]],
    *,
    cap: int = BATCH_MAX_CANDIDATES_PER_OFFICIAL,
    done_bar: int = CONFIRM_TARGET_MEETINGS,
) -> list[OfficialGroup]:
    """Group candidates into per-official screens, least-covered official first.

    Officials already spanning ``done_bar`` distinct confirmed meetings are dropped — they have the
    cross-meeting coverage Gate A needs, so more confirmations buy nothing. (Spanning that many
    meetings implies at least that many confirmed rows, so the "N rows across N meetings" bar
    reduces to the distinct-meeting count.) Official order reuses ``order_candidates`` (fewest-
    confirmed first), which emits each official's candidates contiguously; within a screen the
    clusters are ordered by speech seconds (most recognizable voice first) and capped per pass.
    """
    ordered = order_candidates(candidates, confirmed_meetings)
    groups: list[OfficialGroup] = []
    for person_id, run in groupby(ordered, key=lambda c: c.person_id):
        cands = list(run)
        if len(confirmed_meetings.get(person_id, set())) >= done_bar:
            continue
        listed = sorted(cands, key=lambda c: (-c.seconds, c.meeting_key, c.cluster_label))[:cap]
        groups.append(
            OfficialGroup(
                person_id=person_id,
                official_name=listed[0].official_name,
                roster_title=listed[0].roster_title,
                listed=tuple(listed),
                total=len(cands),
            )
        )
    return groups


# --- DB-facing orchestration ------------------------------------------------------


def _service_client() -> Client:
    """A service-key Supabase client (speaker_identities writes are service-only)."""
    cfg = load_config()
    key = os.environ.get("ACTALUX_SUPABASE_SERVICE_KEY", "")
    if not key:
        raise ActaluxError("ACTALUX_SUPABASE_SERVICE_KEY is required")
    return get_client(cfg.supabase_url, key)


def _documents(client: Client, place_id: int, body: str | None) -> dict[int, dict[str, Any]]:
    """Live (non-superseded) documents for a place's entities -> ``{doc_id: doc}``."""
    entities = fetch_all_rows(
        lambda: client.table("entities").select("id,body_slug").eq("place_id", place_id)
    )
    if body:
        entities = [e for e in entities if e.get("body_slug") == body]
    if not entities:
        raise ActaluxError(f"no entities for place {place_id} (body={body!r})")
    entity_ids = [e["id"] for e in entities]
    docs = fetch_all_rows(
        lambda: (
            client.table("documents")
            .select("id,video_id,replaces_id,entity_id,meeting_title,meeting_date")
            .in_("entity_id", entity_ids)
        )
    )
    superseded = superseded_doc_ids(docs)
    return {d["id"]: d for d in docs if d["id"] not in superseded}


def _subjects(client: Client, place_id: int) -> dict[int, dict[str, Any]]:
    """Place-scoped subjects -> ``{subject_id: subject}`` (a cross-place id can't confirm)."""
    return {
        s["id"]: s
        for s in fetch_all_rows(
            lambda: (
                client.table("subjects")
                .select("id,person_id,publishable,canonical_name")
                .eq("place_id", place_id)
            )
        )
    }


def _members_by_entity(client: Client, entity_ids: list[int]) -> dict[int, dict[int, str | None]]:
    """``entity_id -> {subject_id: role}`` roster membership — the authoritative body-official set.

    A subject is an official of a body only if it holds a membership in that body's entity (the
    same roster the resolver proposes from). Being merely publishable makes a subject a public
    figure, not necessarily an official. The ``role`` value is the official's roster title (for
    display only); membership itself is still the set of keys, so the Option-B guard is unchanged.
    """
    if not entity_ids:
        return {}
    rows = fetch_all_rows(
        lambda: (
            client.table("memberships")
            .select("subject_id,entity_id,role")
            .in_("entity_id", entity_ids)
        )
    )
    out: dict[int, dict[int, str | None]] = defaultdict(dict)
    for r in rows:
        out[r["entity_id"]][r["subject_id"]] = r.get("role")
    return out


def _load_candidates(
    client: Client, docs_by_id: dict[int, dict[str, Any]], subjects_by_id: dict[int, dict[str, Any]]
) -> tuple[list[Candidate], dict[int, set[str]]]:
    """Build the candidate queue + each official's already-confirmed meeting set.

    A candidate is a ``speaker_identities`` row that (a) is not already a locked human
    decision, (b) hypothesizes a publishable official — a place subject with a person_id AND a
    roster membership in the document's body — and (c) lives on a video meeting (the leave-one-out
    unit + the only thing the operator can listen to). The membership guard is what enforces the
    Option-B scope structurally: the resolver only ever proposes body members, so it drops no
    legitimate row, but it stops the tool touching a hand-inserted/stale row that points at a
    publishable non-member. Confirmed rows for the same officials seed the coverage counters so
    ordering front-loads the least-covered officials — but only *enrollable* confirmations count
    (see the ``basis == "voiceprint"`` skip below), so coverage matches what Gate A actually trusts.
    """
    doc_ids = sorted(docs_by_id)
    identities = fetch_all_rows(
        lambda: (
            client.table("speaker_identities")
            .select("id,document_id,cluster_label,subject_id,confidence,basis")
            .in_("document_id", doc_ids)
        )
    )
    entity_ids = sorted({d["entity_id"] for d in docs_by_id.values() if d.get("entity_id")})
    members_by_entity = _members_by_entity(client, entity_ids)

    def _official_of_body(subject: dict[str, Any] | None, doc: dict[str, Any] | None) -> bool:
        """The subject is a publishable, person-linked roster member of the document's body."""
        if not subject or not subject.get("publishable") or subject.get("person_id") is None:
            return False
        return bool(doc) and subject["id"] in members_by_entity.get(doc.get("entity_id"), {})

    confirmed_meetings: dict[int, set[str]] = defaultdict(set)
    for row in identities:
        if row.get("confidence") != "confirmed":
            continue
        # enrollment.py drops confirmed voiceprint-basis rows, so they are not real coverage;
        # counting them would let the batch done-gate skip an official who still lacks enrollable
        # meetings (in clip mode it would only skew ordering).
        if row.get("basis") == "voiceprint":
            continue
        subject = subjects_by_id.get(row.get("subject_id"))
        doc = docs_by_id.get(row["document_id"])
        if _official_of_body(subject, doc) and doc.get("video_id"):
            confirmed_meetings[subject["person_id"]].add(doc["video_id"])

    turns_cache: dict[int, list[dict[str, Any]]] = {}
    candidates: list[Candidate] = []
    for row in identities:
        if row.get("confidence") in LOCKED_TIERS:
            continue
        subject = subjects_by_id.get(row.get("subject_id"))
        doc = docs_by_id.get(row["document_id"])
        if not _official_of_body(subject, doc):
            continue  # only a roster member (official) of this body is confirmable (Option B scope)
        if not doc.get("video_id"):
            continue  # need a video meeting to listen + to feed Gate A's video-keyed folds
        turns = turns_cache.get(doc["id"])
        if turns is None:
            turns = get_diarization_turns(client, doc["id"])
            turns_cache[doc["id"]] = turns
        seconds = span_seconds(cluster_spans(turns, row["cluster_label"]))
        candidates.append(
            Candidate(
                identity_id=row["id"],
                document_id=doc["id"],
                cluster_label=row["cluster_label"],
                person_id=subject["person_id"],
                subject_id=subject["id"],
                official_name=subject.get("canonical_name", "?"),
                basis=row.get("basis"),
                confidence=row.get("confidence", "unknown"),
                meeting_key=doc["video_id"],
                video_id=doc["video_id"],
                meeting_title=doc.get("meeting_title", "?"),
                meeting_date=str(doc.get("meeting_date", "")),
                seconds=seconds,
                excerpts=tuple(cluster_excerpts(turns, row["cluster_label"])),
                roster_title=members_by_entity.get(doc.get("entity_id"), {}).get(subject["id"]),
            )
        )
    return candidates, confirmed_meetings


# --- interactive loop -------------------------------------------------------------


def _print_candidate(candidate: Candidate, position: int, total: int, coverage: int) -> None:
    """Show one candidate: official, coverage, meeting, and cued excerpts."""
    print("\n" + "-" * 72)
    print(
        f"[{position}/{total}] {candidate.official_name}  —  "
        f"{coverage} confirmed meeting(s) (target ~{CONFIRM_TARGET_MEETINGS})"
    )
    print(f"  Meeting: {candidate.meeting_title} — {candidate.meeting_date}")
    print(
        f"  Cluster {candidate.cluster_label} · basis={candidate.basis} "
        f"confidence={candidate.confidence} · {candidate.seconds:.0f}s speech"
    )
    if candidate.excerpts:
        print("  Excerpts (open the cue to listen):")
        for n, (start, text) in enumerate(candidate.excerpts, 1):
            snippet = text if len(text) <= EXCERPT_MAX_CHARS else text[:EXCERPT_MAX_CHARS] + "…"
            print(f"   {n}. (t={start}s) {youtube_cue_url(candidate.video_id, start)}")
            print(f'      "{snippet}"')
    else:
        print("  (no transcript excerpts for this cluster)")


def _prompt() -> str:
    """Read one decision: y (confirm) / n (deny) / s (skip) / q (quit)."""
    while True:
        choice = input("  [y]es confirm · [n]o deny · [s]kip · [q]uit > ").strip().lower()
        if choice in {"y", "yes"}:
            return "y"
        if choice in {"n", "no"}:
            return "n"
        if choice in {"s", "skip", ""}:
            return "s"
        if choice in {"q", "quit"}:
            return "q"
        print("  (please answer y, n, s, or q)")


def _apply_decision(client: Client, candidate: Candidate, choice: str) -> None:
    """Persist a confirm/deny (skip is a no-op)."""
    if choice == "y":
        payload = confirm_payload(candidate)
    elif choice == "n":
        payload = reject_payload(candidate)
    else:
        return
    client.table("speaker_identities").update(payload).eq("id", candidate.identity_id).execute()


def _run_session(
    client: Client, queue: list[Candidate], confirmed_meetings: dict[int, set[str]]
) -> dict[str, SessionTally]:
    """Walk the queue interactively; return per-official tallies keyed by official name."""
    live = {pid: set(mtgs) for pid, mtgs in confirmed_meetings.items()}
    tallies: dict[str, SessionTally] = defaultdict(SessionTally)
    total = len(queue)
    for i, candidate in enumerate(queue, 1):
        coverage = len(live.get(candidate.person_id, set()))
        _print_candidate(candidate, i, total, coverage)
        choice = _prompt()
        if choice == "q":
            logger.info("quit at candidate %d/%d", i, total)
            break
        _apply_decision(client, candidate, choice)
        tally = tallies[candidate.official_name]
        if choice == "y":
            tally.confirmed += 1
            live.setdefault(candidate.person_id, set()).add(candidate.meeting_key)
        elif choice == "n":
            tally.denied += 1
        else:
            tally.skipped += 1
    return tallies


def _print_summary(tallies: dict[str, SessionTally]) -> None:
    """Closing per-official confirm/deny/skip summary."""
    print("\n" + "=" * 72)
    print("Session summary (confirmed / denied / skipped):")
    if not tallies:
        print("  (no decisions recorded)")
        return
    totals = SessionTally()
    for name in sorted(tallies):
        t = tallies[name]
        totals.confirmed += t.confirmed
        totals.denied += t.denied
        totals.skipped += t.skipped
        print(f"  {name:<32} {t.confirmed} / {t.denied} / {t.skipped}")
    print("-" * 72)
    print(f"  {'TOTAL':<32} {totals.confirmed} / {totals.denied} / {totals.skipped}")


# --- batch mode: interactive per-official screens ---------------------------------


def _print_official_screen(group: OfficialGroup, coverage: int) -> None:
    """Render one official's screen: header, numbered clusters with excerpts + cue, grammar line."""
    print("\n" + "=" * 72)
    title = group.roster_title or "(no roster title)"
    print(
        f"{group.official_name} — {title}  ·  "
        f"{coverage} confirmed meeting(s) (target ~{CONFIRM_TARGET_MEETINGS})"
    )
    if group.total > len(group.listed):
        print(
            f"Showing the {len(group.listed)} clusters with the most speech of {group.total} "
            "(the rest surface on a later pass)"
        )
    for n, candidate in enumerate(group.listed, 1):
        print("-" * 72)
        print(f"  {n}. {candidate.meeting_date} — {candidate.meeting_title}")
        print(
            f"     cluster {candidate.cluster_label} · basis={candidate.basis} "
            f"confidence={candidate.confidence} · {candidate.seconds:.0f}s speech"
        )
        excerpts = candidate.excerpts[:BATCH_EXCERPTS_PER_CANDIDATE]
        if excerpts:
            print(f"     cue: {youtube_cue_url(candidate.video_id, excerpts[0][0])}")  # to longest
            for _start, text in excerpts:
                trimmed = (
                    text
                    if len(text) <= BATCH_EXCERPT_MAX_CHARS
                    else text[:BATCH_EXCERPT_MAX_CHARS] + "…"
                )
                print(f'       "{trimmed}"')
        else:
            print("     (no transcript excerpts for this cluster)")
    print("-" * 72)


def _prompt_batch(listed_count: int) -> BatchInput:
    """Read one official-screen decision, re-prompting until the grammar parses."""
    while True:
        raw = input(
            "  confirm #s (e.g. 1,3) · a=all · n <#s>=reject (e.g. n 2) · [s]kip · [q]uit > "
        )
        decision = parse_batch_input(raw, listed_count)
        if decision is not None:
            return decision
        print(f"  (couldn't read that — enter 1-{listed_count}, 'a', 'n <#s>', 's', or 'q')")


def _apply_batch_decision(
    client: Client, group: OfficialGroup, decision: BatchInput, live: dict[int, set[str]]
) -> SessionTally:
    """Persist a parsed screen decision via the shared per-clip write path; update live coverage.

    Confirms and rejects go through ``_apply_decision`` exactly as the clip-by-clip mode does, so a
    batch confirm/deny writes byte-for-byte the same payload as its per-clip counterpart. Every
    listed cluster is accounted for: skipped = listed − confirmed − rejected.
    """
    tally = SessionTally()
    for pos in decision.confirm:
        candidate = group.listed[pos - 1]  # 1-based display position -> 0-based list index
        _apply_decision(client, candidate, "y")
        tally.confirmed += 1
        live.setdefault(group.person_id, set()).add(candidate.meeting_key)
    for pos in decision.reject:
        candidate = group.listed[pos - 1]
        _apply_decision(client, candidate, "n")
        tally.denied += 1
    tally.skipped = len(group.listed) - tally.confirmed - tally.denied
    return tally


def _run_batch_session(
    client: Client, groups: list[OfficialGroup], confirmed_meetings: dict[int, set[str]]
) -> tuple[dict[str, SessionTally], list[tuple[str, int, bool]]]:
    """Walk the per-official screens; return tallies and per-official enablement rows.

    Each enablement row is ``(official_name, confirmed_meetings_now, newly_enabled)`` for a
    presented official, where newly_enabled means this session pushed them to the
    >=ENABLEMENT_MIN_MEETINGS bar. A skipped official writes nothing.
    """
    live = {pid: set(mtgs) for pid, mtgs in confirmed_meetings.items()}
    tallies: dict[str, SessionTally] = {}
    enablement: list[tuple[str, int, bool]] = []
    total = len(groups)
    for i, group in enumerate(groups, 1):
        before = len(live.get(group.person_id, set()))
        print(f"\n[official {i}/{total}]")
        _print_official_screen(group, before)
        decision = _prompt_batch(len(group.listed))
        if decision.action == "quit":
            logger.info("quit at official %d/%d", i, total)
            break
        if decision.action == "skip":
            tallies[group.official_name] = SessionTally(skipped=len(group.listed))
            enablement.append((group.official_name, before, False))
            continue
        tallies[group.official_name] = _apply_batch_decision(client, group, decision, live)
        after = len(live.get(group.person_id, set()))
        enablement.append((group.official_name, after, before < ENABLEMENT_MIN_MEETINGS <= after))
    return tallies, enablement


def _print_enablement(enablement: list[tuple[str, int, bool]]) -> None:
    """Report per-official confirmed-meeting coverage and who crossed the enablement bar."""
    print("\n" + "=" * 72)
    print(f"Confirmed-meeting coverage (enablement bar: >={ENABLEMENT_MIN_MEETINGS} meetings):")
    if not enablement:
        print("  (no officials presented)")
        return
    for name, coverage, newly in sorted(enablement):
        meets = "x" if coverage >= ENABLEMENT_MIN_MEETINGS else " "
        flag = "  <- newly enabled" if newly else ""
        print(f"  [{meets}] {name:<32} {coverage} meeting(s){flag}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Confirm/deny speaker labels for a place.")
    parser.add_argument("--state", required=True, help="place state slug, e.g. mo")
    parser.add_argument("--place", required=True, help="place slug, e.g. clayton")
    parser.add_argument("--body", help="restrict to one body_slug; default all bodies")
    parser.add_argument(
        "--limit", type=int, help="cap presented candidates (clip mode) or officials (--batch)"
    )
    parser.add_argument(
        "--batch",
        action="store_true",
        help="one screen per official (decidable from transcript text) instead of clip-by-clip",
    )
    parser.add_argument(
        "--export-csv",
        metavar="FILE",
        help="write the review queue as a spreadsheet (fill `decision` with y/n, then --decisions)",
    )
    parser.add_argument(
        "--decisions",
        metavar="FILE",
        help="apply a filled review CSV: decision = y / n / correct official's name "
        "(dry-run unless --apply)",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="with --decisions: execute the confirms/rejects (default is a dry-run report)",
    )
    args = parser.parse_args()

    client = _service_client()
    place = get_place_by_path(client, args.state, args.place)
    if not place:
        raise ActaluxError(f"no place {args.state}/{args.place}")
    place_id = place["id"]

    docs_by_id = _documents(client, place_id, args.body)
    subjects_by_id = _subjects(client, place_id)
    candidates, confirmed_meetings = _load_candidates(client, docs_by_id, subjects_by_id)
    scope = f"{args.state}/{args.place}" + (f"/{args.body}" if args.body else "")

    if args.export_csv:
        evidence = _evidence_by_key(client, sorted({c.document_id for c in candidates}))
        name_by_person = {
            s["person_id"]: s["canonical_name"]
            for s in subjects_by_id.values()
            if s.get("person_id") is not None
        }
        _export_csv(Path(args.export_csv), candidates, evidence, name_by_person)
        logger.info(
            "%s: exported %d candidate(s) to %s — fill the `decision` column (y/n; blank = "
            "skip), then re-run with --decisions %s --apply",
            scope,
            len(candidates),
            args.export_csv,
            args.export_csv,
        )
        return

    if args.decisions:
        entity_ids = sorted({d["entity_id"] for d in docs_by_id.values() if d.get("entity_id")})
        name_index = build_name_index(
            docs_by_id, subjects_by_id, _members_by_entity(client, entity_ids)
        )
        _run_decisions_mode(client, Path(args.decisions), candidates, name_index, apply=args.apply)
        return

    if args.batch:
        _run_batch_mode(client, candidates, confirmed_meetings, args.limit, scope)
        return

    queue = order_candidates(candidates, confirmed_meetings)
    if args.limit:
        queue = queue[: args.limit]
    logger.info(
        "%s: %d unconfirmed candidate(s) across %d official(s)",
        scope,
        len(queue),
        len({c.person_id for c in queue}),
    )
    if not queue:
        logger.info("nothing to confirm")
        return

    tallies = _run_session(client, queue, confirmed_meetings)
    _print_summary(tallies)


def _evidence_by_key(client: Client, doc_ids: list[int]) -> dict[tuple[int, str], dict[str, Any]]:
    """Voiceprint evidence keyed by ``(document_id, cluster_label)`` (empty for name bases)."""
    if not doc_ids:
        return {}
    rows = fetch_all_rows(
        lambda: (
            client.table("voiceprint_match_evidence")
            .select("document_id,cluster_label,score,margin,alternatives")
            .in_("document_id", doc_ids)
        )
    )
    return {(r["document_id"], r["cluster_label"]): r for r in rows}


def _export_order(
    candidates: list[Candidate], evidence: dict[tuple[int, str], dict[str, Any]]
) -> list[Candidate]:
    """The review sheet's reading order: listen-first block, then officials by queue size.

    Low-margin voice matches (margin < LOW_MARGIN_FLAG) lead, weakest first — that block is
    where the expected errors concentrate. The rest group by official (largest queue first,
    mirroring the HTML sheet's sections), weakest margin first within each; name-evidence
    rows (no margin) follow the voice rows.
    """

    def margin(c: Candidate) -> float:
        ev = evidence.get((c.document_id, c.cluster_label))
        return float(ev["margin"]) if ev else float("inf")

    low = sorted((c for c in candidates if margin(c) < LOW_MARGIN_FLAG), key=margin)
    queue_size: dict[str, int] = defaultdict(int)
    for c in candidates:
        queue_size[c.official_name] += 1
    rest = sorted(
        (c for c in candidates if margin(c) >= LOW_MARGIN_FLAG),
        key=lambda c: (-queue_size[c.official_name], c.official_name, margin(c), -c.seconds),
    )
    return low + rest


def _export_csv(
    path: Path,
    candidates: list[Candidate],
    evidence: dict[tuple[int, str], dict[str, Any]],
    name_by_person: dict[int, str],
) -> None:
    """Write the review queue as a spreadsheet in the review sheet's reading order."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=list(CSV_COLUMNS))
        writer.writeheader()
        for c in _export_order(candidates, evidence):
            ev = evidence.get((c.document_id, c.cluster_label))
            alts = (ev or {}).get("alternatives") or []
            runner = (
                f"{name_by_person.get(alts[0]['person_id'], alts[0]['person_id'])}"
                f" ({alts[0]['score']:.2f})"
                if alts
                else ""
            )
            cue, text = c.excerpts[0] if c.excerpts else (0, "")
            writer.writerow(
                {
                    "decision": "",
                    "flag": (
                        "LOW MARGIN"
                        if ev is not None and float(ev["margin"]) < LOW_MARGIN_FLAG
                        else ""
                    ),
                    "official": c.official_name,
                    "meeting_date": c.meeting_date,
                    "meeting_title": c.meeting_title,
                    "seconds": round(c.seconds),
                    "listen_url": youtube_cue_url(c.video_id, cue),
                    "excerpt": " ".join(text.split())[:240],
                    "score": f"{ev['score']:.2f}" if ev else "",
                    "margin": f"{ev['margin']:.2f}" if ev else "",
                    "runner_up": runner,
                    "basis": c.basis or "",
                    "document_id": c.document_id,
                    "cluster_label": c.cluster_label,
                    "identity_id": c.identity_id,
                }
            )


def _run_decisions_mode(
    client: Client,
    path: Path,
    candidates: list[Candidate],
    name_index: dict[int, dict[str, tuple[int, str] | None]],
    *,
    apply: bool,
) -> None:
    """Apply a filled review CSV through the same payloads as the interactive session."""
    with path.open(newline="") as fh:
        rows = list(csv.DictReader(fh))
    plan = plan_decisions(candidates, rows, name_index)
    logger.info(
        "%s: %d confirm, %d reject, %d rename, %d blank/skip, %d unmatched, %d invalid",
        path.name,
        len(plan.confirm),
        len(plan.reject),
        len(plan.rename),
        plan.skipped,
        len(plan.unmatched),
        len(plan.invalid),
    )
    for candidate, _, new_name in plan.rename:
        logger.info(
            "rename: doc %d %s  %s -> %s",
            candidate.document_id,
            candidate.cluster_label,
            candidate.official_name,
            new_name,
        )
    for doc_id, cluster in plan.unmatched:
        logger.warning(
            "not in the live queue (already decided or regenerated): doc %d %s", doc_id, cluster
        )
    if plan.invalid:
        for doc_id, cluster, raw, reason in plan.invalid:
            logger.error("invalid decision %r at doc %d %s: %s", raw, doc_id, cluster, reason)
        raise ActaluxError("invalid decision values — fix the CSV; nothing was applied")
    if not apply:
        logger.info("dry-run: no writes (re-run with --apply to record these decisions)")
        return
    tallies: dict[str, SessionTally] = defaultdict(SessionTally)
    for candidate, subject_id, new_name in plan.rename:
        client.table("speaker_identities").update(rename_payload(subject_id)).eq(
            "id", candidate.identity_id
        ).execute()
        tallies[new_name].confirmed += 1
    for candidate in plan.confirm:
        _apply_decision(client, candidate, "y")
        tallies[candidate.official_name].confirmed += 1
    for candidate in plan.reject:
        _apply_decision(client, candidate, "n")
        tallies[candidate.official_name].denied += 1
    _print_summary(tallies)


def _run_batch_mode(
    client: Client,
    candidates: list[Candidate],
    confirmed_meetings: dict[int, set[str]],
    limit: int | None,
    scope: str,
) -> None:
    """Drive per-official batch screens: group, present, summarize."""
    groups = build_official_groups(candidates, confirmed_meetings)
    if limit:
        groups = groups[:limit]
    logger.info(
        "%s: %d official screen(s), %d candidate(s)",
        scope,
        len(groups),
        sum(g.total for g in groups),
    )
    if not groups:
        logger.info("nothing to confirm")
        return
    tallies, enablement = _run_batch_session(client, groups, confirmed_meetings)
    _print_summary(tallies)
    _print_enablement(enablement)


if __name__ == "__main__":
    main()
