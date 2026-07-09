"""Tier-2 speaker naming: per-document, NON-tracked public-participant names.

The third tier of the speaker-naming policy (see docs/architecture/name-the-public-record.md,
CLAUDE.md Content policy):

  1. TRACKED ENTITY      — ``speaker_identities.subject_id`` (persistent, voiceprinted) — officials.
  2. NAMED-IN-TRANSCRIPT — ``transcript_speaker_names`` (per-document, non-tracked) — public
                           participants who self-identify or are introduced on the record.
  3. ANONYMOUS           — no row — citizens.

This module builds tier 2. **P1 handles self-introductions only** ("my name is X", "I'm X"):
they are ~88% of the headroom and, because the speaker's own cluster states the name, attach
unambiguously to that cluster. Third-party presenter introductions ("please welcome X") are
DEFERRED to a later phase — auto-publishing a non-roster name someone else supplied is the
riskiest path (git preserves the presenter machinery for that phase). Detection reuses the shared
introduction extraction (:mod:`actalux.identity.name_extraction`) — the same surface patterns the
resolver uses — but, unlike the resolver, KEEPS names that are NOT roster members (a roster member
is the tracked path; it is never emitted as a tier-2 name). Each surviving hit becomes a proposal
carrying the extracted name, the verbatim self-ID sentence (the required source cite), and the
timestamp; ``basis`` is recorded as ``self_intro``.

Two safety layers sit above the per-body policy flag:

* **Minor suppression is universal.** A self-ID that reads as a student/minor is never named on
  any body (see :data:`MINOR_CUES`) — self-identification is necessary but never sufficient.
* **Tier 1 wins.** A cluster already named by a tracked ``speaker_identities`` row is an official;
  persistence never also writes a tier-2 name for it.

Persistence honors the per-body ``entities.public_participant_naming`` flag:
``off`` writes nothing, ``auto`` (city bodies) inserts ``approved`` (publicly displayable),
``review`` (schools) inserts ``proposed`` (held below the RLS display gate until a human acts).
The table has NO ``subject_id`` column — the non-tracked guarantee is structural, not procedural.

This is P1: self-introduction detection + minor suppression + schema-aware persistence.
Presenter introductions, display (P2), the schools review queue (P3), and corpus backfill (P4)
are separate later phases.
"""

from __future__ import annotations

import re
from collections import defaultdict
from dataclasses import dataclass
from typing import Any

from supabase import Client

from actalux.db import get_diarization_turns
from actalux.graph.resolve import normalize_name
from actalux.identity.name_extraction import evidence_sentence, roster_keys, turn_hits
from actalux.identity.resolve import RosterMember, members_for_entity

# --- universal minor suppression --------------------------------------------------
# A self-identified minor/student is NEVER named, on any body, above the per-body flag
# (spec §9 decision 4). The cues are matched against the normalized evidence sentence
# (lowercased, apostrophes folded so "I'm" -> "im", other punctuation -> spaces). This
# list is deliberately broad and err-toward-suppression: the cost of over-suppressing an
# adult is a missing name (recoverable); the cost of a miss is naming a minor (a policy
# violation). Each entry documents the cue class it covers. All generic English — no
# jurisdiction wording — so it carries across towns.
MINOR_CUES: tuple[str, ...] = (
    # First-person bare class standing ("I'm a sophomore", "I am a freshman"). "senior"/
    # "junior" are deliberately EXCLUDED here — their common adult senses ("senior architect",
    # "junior associate", "senior partner") are exactly the applicants/developers this feature
    # exists to name. A senior/junior MINOR is still caught by co-occurrence with the student /
    # high-school / grade cues below ("senior at Clayton High School", "junior, 11th grade").
    r"\b(?:im|i am)\s+an?\s+(?:sophomore|freshman)\b",
    # Any student self-identification: "I'm a student", "a student at ...", "student council".
    r"\bstudents?\b",
    # Numeric grade level: "9th grade" ... "12th grade".
    r"\b(?:9|10|11|12)\s*th\s+grade\b",
    # Spelled grade level: "ninth grade" ... "twelfth grade".
    r"\b(?:ninth|tenth|eleventh|twelfth)\s+grade\b",
    # School-level nouns naming a minor's school.
    r"\bhigh\s+school(?:er)?\b",
    r"\bmiddle\s+school(?:er)?\b",
    r"\belementary\s+school\b",
    # Youth governance bodies ("youth council", "youth in government").
    r"\byouth\s+(?:council|government|senate|congress|group|organization|in\s+government)\b",
)
_MINOR_RE = tuple(re.compile(p) for p in MINOR_CUES)


@dataclass(frozen=True)
class ParticipantTurn:
    """One diarization turn reduced to what tier-2 naming needs: who, what, and when."""

    cluster_label: str
    text: str
    start_seconds: float | None = None


@dataclass(frozen=True)
class ParticipantNameProposal:
    """A proposed per-document, non-tracked name for a cluster (a ``transcript_speaker_names`` row).

    There is deliberately no ``subject_id`` — tier 2 is non-tracked by construction.
    ``status`` is decided by the per-body flag at persistence, not carried here.
    """

    cluster_label: str
    display_name: str
    basis: str  # 'self_intro' in P1 (the schema CHECK also allows 'presenter_intro', deferred)
    evidence_quote: str  # the verbatim self-ID sentence (the source cite)
    start_seconds: float | None = None

    def to_row(self, document_id: int, status: str) -> dict[str, Any]:
        """Row for the ``transcript_speaker_names`` table (no subject_id, by design)."""
        return {
            "document_id": document_id,
            "cluster_label": self.cluster_label,
            "display_name": self.display_name,
            "basis": self.basis,
            "evidence_quote": self.evidence_quote,
            "start_seconds": self.start_seconds,
            "status": status,
        }


@dataclass(frozen=True)
class _ClusterHit:
    """An accepted (non-roster, non-minor) self-intro hit awaiting per-cluster arbitration."""

    name_key: str
    display_name: str
    basis: str
    evidence_quote: str
    start_seconds: float | None
    order: int  # turn index; the earliest self-introduction on a cluster wins


def _normalize_for_cue(text: str) -> str:
    """Lowercase, fold apostrophes, drop other punctuation, collapse whitespace.

    Mirrors the resolver's ``_norm_text`` so a self-ID cue matches the same way names do
    ("I'm" -> "im"). Digits are kept so grade-level cues ("9th grade") still match.
    """
    folded = text.lower().replace("'", "").replace("’", "")
    return re.sub(r"\s+", " ", re.sub(r"[^a-z0-9 ]", " ", folded)).strip()


def is_minor_selfid(text: str) -> bool:
    """True if ``text`` (an evidence sentence) reads as a minor/student self-identification.

    Conservative, err-toward-suppression: any :data:`MINOR_CUES` pattern matching the
    normalized text suppresses the name. See the module + :data:`MINOR_CUES` docstrings.
    """
    norm = _normalize_for_cue(text)
    return any(rx.search(norm) for rx in _MINOR_RE)


def detect_participant_names(
    turns: list[ParticipantTurn],
    members: list[RosterMember],
    stops: frozenset[str],
) -> list[ParticipantNameProposal]:
    """Non-roster self-introductions in a document's turns -> tier-2 proposals (P1).

    For every extracted SELF-introduction hit that (a) names someone NOT on the roster (roster
    members are the tracked path) and (b) does not read as a minor/student, a candidate is
    attached to the speaking cluster (the person's own words name that cluster unambiguously).
    Third-party presenter introductions are deferred to a later phase and are ignored here.

    Per cluster: a cluster whose accepted hits name more than one distinct person is ambiguous
    and dropped (precision — never a wrong public name); a cluster with a single named person
    yields one proposal, taken from the earliest self-introduction on that cluster. ``stops``
    augments the static non-name stop-list with place tokens (town/state).
    """
    roster, _ = roster_keys(members)
    by_cluster: dict[str, list[_ClusterHit]] = defaultdict(list)
    for order, turn in enumerate(turns):
        for hit in turn_hits(turn.text, stops):
            if hit.source != "self_intro":
                continue  # P1: presenter introductions are deferred to a later phase
            key = normalize_name(hit.name)
            if not key or key in roster:
                continue  # blank name, or a roster official (tracked path) -> never tier 2
            quote = evidence_sentence(turn.text, hit.start_index, hit.end_index)
            if is_minor_selfid(quote):
                continue  # universal minor suppression, above the per-body flag
            by_cluster[turn.cluster_label].append(
                _ClusterHit(key, hit.name, hit.source, quote, turn.start_seconds, order)
            )

    proposals: list[ParticipantNameProposal] = []
    for cluster, hits in by_cluster.items():
        if len({h.name_key for h in hits}) != 1:
            continue  # cluster claims >1 distinct person -> ambiguous -> drop
        best = min(hits, key=lambda h: h.order)  # earliest self-introduction on the cluster
        proposals.append(
            ParticipantNameProposal(
                cluster_label=cluster,
                display_name=best.display_name,
                basis=best.basis,
                evidence_quote=best.evidence_quote,
                start_seconds=best.start_seconds,
            )
        )
    return sorted(proposals, key=lambda p: p.cluster_label)


# --- DB-facing helpers ------------------------------------------------------------


def turns_for_participant_naming(client: Client, document_id: int) -> list[ParticipantTurn]:
    """The document's diarization turns as ``ParticipantTurn``\\ s (verbatim, uncanonicalized).

    Detection runs on the RAW ASR text so the evidence quote is verbatim as recorded and the
    roster comparison mirrors the headroom scan. A tracked official whose ASR-mangled name
    slips past the raw roster compare is still caught at persistence by the tier-1 skip.
    """
    turns: list[ParticipantTurn] = []
    for row in get_diarization_turns(client, document_id):
        text = " ".join(w.get("word", "") for w in (row.get("words") or []))
        start = row.get("start_seconds")
        turns.append(
            ParticipantTurn(
                cluster_label=row["cluster_label"],
                text=text,
                start_seconds=float(start) if start is not None else None,
            )
        )
    return turns


def _naming_flag(service_client: Client, entity_id: int) -> str:
    """The body's ``public_participant_naming`` policy, defaulting to the safe ``off``."""
    data = (
        service_client.table("entities")
        .select("public_participant_naming")
        .eq("id", entity_id)
        .limit(1)
        .execute()
        .data
    )
    flag = data[0].get("public_participant_naming") if data else None
    return flag if flag in ("auto", "review", "off") else "off"


def _tracked_clusters(service_client: Client, document_id: int) -> set[str]:
    """Clusters already named by a tracked ``speaker_identities`` row (tier 1 wins)."""
    rows = (
        service_client.table("speaker_identities")
        .select("cluster_label,subject_id")
        .eq("document_id", document_id)
        .execute()
        .data
        or []
    )
    return {r["cluster_label"] for r in rows if r.get("subject_id") is not None}


def persist_participant_names(
    service_client: Client,
    document_id: int,
    entity_id: int,
    proposals: list[ParticipantNameProposal],
) -> int:
    """Write tier-2 names for a document under its body's policy; return rows upserted.

    * ``off``   -> writes nothing (returns early).
    * ``auto``  -> upserts each proposal ``status='approved'`` (publicly displayable).
    * ``review``-> upserts each proposal ``status='proposed'`` (below the RLS display gate).

    Two things are never written:

    * a cluster already named by a tracked ``speaker_identities`` row (tier 1 wins), and
    * a row a human has touched — a ``rejected`` "do not name", or a status that differs from
      what this body's flag writes (e.g. a schools row a human ``approved``): both are left
      as-is. The guard is status-based: an existing row is overwritten only when its status
      equals the machine status for this body.

    Upserts on ``UNIQUE(document_id, cluster_label)``. Never writes a ``subject_id`` (the
    table has none). Retraction of a no-longer-proposed row is out of scope for P1.
    """
    flag = _naming_flag(service_client, entity_id)
    if flag == "off":
        return 0
    status = "approved" if flag == "auto" else "proposed"  # 'review' -> proposed

    tracked = _tracked_clusters(service_client, document_id)
    existing = {
        r["cluster_label"]: r
        for r in (
            service_client.table("transcript_speaker_names")
            .select("cluster_label,status")
            .eq("document_id", document_id)
            .execute()
            .data
            or []
        )
    }

    rows: list[dict[str, Any]] = []
    for p in proposals:
        if p.cluster_label in tracked:
            continue  # a tracked official already names this cluster -> tier 1 wins
        prior = existing.get(p.cluster_label)
        if prior is not None and prior.get("status") != status:
            continue  # human-touched (rejected, or approved on a review body) -> leave it
        rows.append(p.to_row(document_id, status))
    if rows:
        service_client.table("transcript_speaker_names").upsert(
            rows, on_conflict="document_id,cluster_label"
        ).execute()
    return len(rows)


def name_participants_document(
    client: Client,
    service_client: Client,
    document_id: int,
    entity_id: int,
    stops: frozenset[str],
    members: list[RosterMember] | None = None,
) -> list[ParticipantNameProposal]:
    """Detect + persist tier-2 names for one transcript; return the proposals.

    ``members`` (the body roster) may be passed to avoid re-fetching it per document in a
    batch; otherwise it is loaded for ``entity_id``. Persistence applies the body flag and
    the tier-1 / human-touched guards.
    """
    if members is None:
        members = members_for_entity(client, entity_id)
    turns = turns_for_participant_naming(client, document_id)
    proposals = detect_participant_names(turns, members, stops)
    persist_participant_names(service_client, document_id, entity_id, proposals)
    return proposals
