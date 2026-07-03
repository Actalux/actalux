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
"""

from __future__ import annotations

import argparse
import logging
import os
from collections import defaultdict
from dataclasses import dataclass
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


def _members_by_entity(client: Client, entity_ids: list[int]) -> dict[int, set[int]]:
    """``entity_id -> {subject_id}`` roster membership — the authoritative "official of this body".

    A subject is an official of a body only if it holds a membership in that body's entity (the
    same roster the resolver proposes from). Being merely publishable makes a subject a public
    figure, not necessarily an official.
    """
    if not entity_ids:
        return {}
    rows = fetch_all_rows(
        lambda: (
            client.table("memberships").select("subject_id,entity_id").in_("entity_id", entity_ids)
        )
    )
    out: dict[int, set[int]] = defaultdict(set)
    for r in rows:
        out[r["entity_id"]].add(r["subject_id"])
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
    ordering front-loads the least-covered officials.
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
        return bool(doc) and subject["id"] in members_by_entity.get(doc.get("entity_id"), set())

    confirmed_meetings: dict[int, set[str]] = defaultdict(set)
    for row in identities:
        if row.get("confidence") != "confirmed":
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


def main() -> None:
    parser = argparse.ArgumentParser(description="Confirm/deny speaker labels for a place.")
    parser.add_argument("--state", required=True, help="place state slug, e.g. mo")
    parser.add_argument("--place", required=True, help="place slug, e.g. clayton")
    parser.add_argument("--body", help="restrict to one body_slug; default all bodies")
    parser.add_argument("--limit", type=int, help="cap the number of candidates presented")
    args = parser.parse_args()

    client = _service_client()
    place = get_place_by_path(client, args.state, args.place)
    if not place:
        raise ActaluxError(f"no place {args.state}/{args.place}")
    place_id = place["id"]

    docs_by_id = _documents(client, place_id, args.body)
    subjects_by_id = _subjects(client, place_id)
    candidates, confirmed_meetings = _load_candidates(client, docs_by_id, subjects_by_id)
    queue = order_candidates(candidates, confirmed_meetings)
    if args.limit:
        queue = queue[: args.limit]

    logger.info(
        "%s/%s%s: %d unconfirmed candidate(s) across %d official(s)",
        args.state,
        args.place,
        f"/{args.body}" if args.body else "",
        len(queue),
        len({c.person_id for c in queue}),
    )
    if not queue:
        logger.info("nothing to confirm")
        return

    tallies = _run_session(client, queue, confirmed_meetings)
    _print_summary(tallies)


if __name__ == "__main__":
    main()
