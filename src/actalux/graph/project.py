"""Project structured votes into citation-backed graph edges (Phase 1).

Pure projection: a document's votes + the curated roster -> typed edges (each
member's recorded aye/no/abstain, plus who moved/seconded a motion) and
resolution-queue rows for names the roster could not resolve. No DB access here —
scripts/project_member_votes.py does the I/O; this module is the deterministic
core, so the edge taxonomy and citation plumbing are unit-testable without a
database. Every edge is ``status='cited'`` and carries the vote's durable
identity ``(vote_document_id, vote_ref)`` plus its citation_id/source_quote, so
nothing is asserted without a verbatim source (connections-graph §2, §4).
"""

from __future__ import annotations

import hashlib
from datetime import date

from actalux.graph.resolve import Roster, normalize_name

# quote_hash uses the SAME normalization chunks are hashed with, so a persisted
# edge can later re-resolve its citation against chunk text (connections-graph §4.4).
from actalux.ingest.hashing import _normalize_for_citation

# A roll-call member's recorded vote -> the edge asserting it. absent/present (or
# any other value) carry no edge: a non-vote is not a fact about how they voted.
_VOTE_EDGE_TYPE = {"aye": "voted_aye_on", "no": "voted_no_on", "abstain": "voted_abstain_on"}
_VOTE_OUTCOME_TYPES = frozenset(_VOTE_EDGE_TYPE.values())
_ROLE_FIELDS = (("moved_by", "moved"), ("seconded_by", "seconded"))


def quote_hash(quote: str) -> str:
    """sha256 hex of the citation-normalized quote (casefold + whitespace-collapse)."""
    return hashlib.sha256(_normalize_for_citation(quote or "").encode()).hexdigest()


def _as_date(value: str | date) -> date:
    return value if isinstance(value, date) else date.fromisoformat(value)


def _edge_row(subject_id: int, edge_type: str, vote: dict, qhash: str) -> dict:
    """An edges-table row for one resolved (member, vote, edge_type)."""
    return {
        "from_subject": subject_id,
        "vote_document_id": vote["document_id"],
        "vote_ref": vote["vote_ref"],
        "source_document_id": vote["document_id"],  # provenance == the vote's document
        "type": edge_type,
        "status": "cited",
        "chunk_id": vote.get("chunk_id"),  # best-effort; nulls on re-ingest
        "citation_id": vote.get("citation_id"),  # durable citation link
        "source_quote": vote.get("source_quote"),
        "quote_hash": qhash,
        "as_of_date": vote.get("meeting_date"),
        "as_of_date_source": "vote",
        "projection_complete": True,
    }


def _queue_row(name: str, reason: str, vote: dict) -> dict:
    """A subject_resolution_queue row for a name the roster could not resolve."""
    return {
        "raw_alias": name,
        "normalized_alias": normalize_name(name),
        "entity_id": vote["entity_id"],
        "meeting_date": vote.get("meeting_date"),
        "document_id": vote["document_id"],
        "vote_ref": vote["vote_ref"],
        "reason": reason,
        "status": "open",
    }


def _edge_key(edge: dict) -> tuple:
    """The uniqueness signature an edge occupies, mirroring migrate_029's partial
    unique indexes, so a name repeated in one roll call (e.g. an OCR double) collapses
    before insert instead of tripping the index. Outcome edges key without the type
    (a member cannot be both aye and no on one vote); role edges include the type
    (a member may move AND vote)."""
    if edge["type"] in _VOTE_OUTCOME_TYPES:
        return ("outcome", edge["vote_document_id"], edge["from_subject"], edge["vote_ref"])
    return ("role", edge["vote_document_id"], edge["from_subject"], edge["type"], edge["vote_ref"])


def _vote_targets(vote: dict) -> list[tuple[str, str]]:
    """(name, edge_type) pairs a vote asserts: roll-call outcomes + mover/seconder."""
    details = vote.get("details") or {}
    targets: list[tuple[str, str]] = []
    for member in details.get("members") or []:
        edge_type = _VOTE_EDGE_TYPE.get(member.get("vote"))
        if edge_type and member.get("name"):
            targets.append((member["name"], edge_type))
    for field, edge_type in _ROLE_FIELDS:
        if details.get(field):
            targets.append((details[field], edge_type))
    return targets


def derive_document_edges(votes: list[dict], roster: Roster) -> tuple[list[dict], list[dict]]:
    """Edges + resolution-queue rows for one document's votes.

    Each vote must carry ``document_id``, ``vote_ref``, ``entity_id``,
    ``meeting_date`` and (for citation) ``citation_id``/``source_quote``; the
    projector attaches entity_id/meeting_date from the owning document. A vote with
    no ``vote_ref`` is skipped (it has no durable identity to anchor an edge to —
    this should not happen after the migrate_028 backfill). Returns
    ``(edges, queue_rows)``; duplicate edges and duplicate queue rows within the
    document are collapsed.
    """
    edges: list[dict] = []
    queue: list[dict] = []
    seen_edges: set[tuple] = set()
    seen_queue: set[tuple] = set()
    for vote in votes:
        if not vote.get("vote_ref"):
            continue
        qhash = quote_hash(vote.get("source_quote") or "")
        meeting_date = _as_date(vote["meeting_date"])
        for name, edge_type in _vote_targets(vote):
            res = roster.resolve(name, vote["entity_id"], meeting_date)
            if res.status == "resolved":
                edge = _edge_row(res.subject_id, edge_type, vote, qhash)
                key = _edge_key(edge)
                if key not in seen_edges:
                    seen_edges.add(key)
                    edges.append(edge)
            else:
                qkey = (normalize_name(name), vote["vote_ref"])
                if qkey not in seen_queue:
                    seen_queue.add(qkey)
                    queue.append(_queue_row(name, res.reason, vote))
    return edges, queue
