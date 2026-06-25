"""Persistence for the connections graph: load the roster, write projected edges.

Thin DB layer between the pure projector (graph.project) and Supabase. Reads build
a ``Roster`` from the seeded subjects/memberships/aliases; writes rebuild a
document's edges + resolution-queue rows idempotently (delete-then-insert per
document), and a prune step drops any edges left on a superseded document so the
read gate (``projection_complete = true AND documents.replaces_id IS NULL``) never
sees stale graph rows (connections-graph §4.3, §4.5).
"""

from __future__ import annotations

from datetime import date
from typing import Any

from supabase import Client

from actalux.db import fetch_all_rows
from actalux.graph.resolve import Membership, Roster, RosterSubject

# A document's votes carry everything an edge needs except the body it belongs to,
# which lives on the document; the projector attaches entity_id from there.
_VOTE_FIELDS = "id,document_id,vote_ref,citation_id,source_quote,chunk_id,details,meeting_date"


def _to_date(value: str | None) -> date | None:
    return date.fromisoformat(value) if value else None


def load_roster(client: Client, place_id: int) -> Roster:
    """Build the resolver's roster from the seeded subjects for one place.

    Loads every ``person`` subject in the place with its normalized aliases and
    membership windows. Only publishable subjects are loaded — an unpublishable
    person is not a roster member to attribute votes to.
    """
    subjects = fetch_all_rows(
        lambda: (
            client.table("subjects")
            .select("id")
            .eq("place_id", place_id)
            .eq("type", "person")
            .eq("publishable", True)
        )
    )
    subject_ids = {s["id"] for s in subjects}
    if not subject_ids:
        return Roster([])

    aliases = fetch_all_rows(
        lambda: client.table("subject_aliases").select("subject_id,normalized_alias")
    )
    memberships = fetch_all_rows(
        lambda: client.table("memberships").select("subject_id,entity_id,start_date,end_date")
    )

    by_subject_aliases: dict[int, set[str]] = {sid: set() for sid in subject_ids}
    for a in aliases:
        if a["subject_id"] in by_subject_aliases:
            by_subject_aliases[a["subject_id"]].add(a["normalized_alias"])
    by_subject_memberships: dict[int, list[Membership]] = {sid: [] for sid in subject_ids}
    for m in memberships:
        if m["subject_id"] in by_subject_memberships:
            by_subject_memberships[m["subject_id"]].append(
                Membership(m["entity_id"], _to_date(m["start_date"]), _to_date(m["end_date"]))
            )

    return Roster(
        RosterSubject(
            subject_id=sid,
            aliases=frozenset(by_subject_aliases[sid]),
            memberships=tuple(by_subject_memberships[sid]),
        )
        for sid in subject_ids
    )


def current_minutes(client: Client, entity_ids: list[int]) -> list[dict[str, Any]]:
    """Current (non-superseded) minutes documents for the given bodies."""
    return fetch_all_rows(
        lambda: (
            client.table("documents")
            .select("id,entity_id,meeting_date")
            .eq("document_type", "minutes")
            .is_("replaces_id", "null")
            .in_("entity_id", entity_ids)
        )
    )


def document_votes(client: Client, doc_id: int, entity_id: int) -> list[dict[str, Any]]:
    """One document's votes, each tagged with its body (entity_id) for resolution."""
    rows = client.table("votes").select(_VOTE_FIELDS).eq("document_id", doc_id).execute().data
    for row in rows:
        row["entity_id"] = entity_id
    return rows


def replace_document_graph(
    client: Client, doc_id: int, edges: list[dict], queue: list[dict]
) -> None:
    """Idempotently rebuild one document's edges + queue rows (delete-then-insert).

    Scoped to ``source_document_id`` (edges) / ``document_id`` (queue), so a re-run
    reproduces exactly the current derivation for this document and nothing else.
    """
    client.table("edges").delete().eq("source_document_id", doc_id).execute()
    client.table("subject_resolution_queue").delete().eq("document_id", doc_id).execute()
    if edges:
        client.table("edges").insert(edges).execute()
    if queue:
        client.table("subject_resolution_queue").insert(queue).execute()


def body_members(client: Client, entity_id: int) -> list[dict[str, Any]]:
    """Publishable members of a body with their term window + role/ward metadata.

    Joins memberships to subjects in Python (small, and avoids PostgREST embedded-
    filter quirks). Drops any membership whose subject is not publishable.
    """
    memberships = (
        client.table("memberships")
        .select("subject_id,role,start_date,end_date")
        .eq("entity_id", entity_id)
        .execute()
        .data
    )
    subject_ids = [m["subject_id"] for m in memberships]
    if not subject_ids:
        return []
    subjects = (
        client.table("subjects")
        .select("id,slug,canonical_name,metadata")
        .in_("id", subject_ids)
        .eq("publishable", True)
        .execute()
        .data
    )
    by_id = {s["id"]: s for s in subjects}
    members: list[dict[str, Any]] = []
    for m in memberships:
        subject = by_id.get(m["subject_id"])
        if subject is None:
            continue
        members.append({**subject, "start_date": m["start_date"], "end_date": m["end_date"]})
    return members


def member_by_slug(client: Client, place_id: int, slug: str, entity_id: int) -> dict | None:
    """One publishable member of a body by slug, with its term window, or None."""
    rows = (
        client.table("subjects")
        .select("id,slug,canonical_name,metadata")
        .eq("place_id", place_id)
        .eq("slug", slug)
        .eq("type", "person")
        .eq("publishable", True)
        .limit(1)
        .execute()
        .data
    )
    if not rows:
        return None
    subject = rows[0]
    membership = (
        client.table("memberships")
        .select("role,start_date,end_date")
        .eq("subject_id", subject["id"])
        .eq("entity_id", entity_id)
        .limit(1)
        .execute()
        .data
    )
    if not membership:
        return None  # publishable subject, but not a member of THIS body
    return {**subject, **membership[0]}


def member_records(client: Client, subject_id: int, entity_id: int) -> list[dict[str, Any]]:
    """A member's full cited voting record (via the member_vote_records view).

    Paged past the row cap (a long-serving member can exceed 1000 edges across
    voted/moved/seconded), ordered by the view's edge_id for stable paging.
    """
    return fetch_all_rows(
        lambda: (
            client.table("member_vote_records")
            .select("*")
            .eq("subject_id", subject_id)
            .eq("entity_id", entity_id)
        ),
        order="edge_id",
    )


def prune_stale_graph(client: Client, current_doc_ids: set[int]) -> int:
    """Delete edges/queue rows that reference a document no longer current.

    Enforces the §4.5 postcondition (zero graph rows on a superseded document)
    after a full rebuild: when a minutes doc re-versions, its old edges (keyed on
    the old source_document_id) would otherwise linger, invisible to reads but
    violating the invariant. Returns the number of edges pruned.
    """
    edge_docs = {
        r["source_document_id"]
        for r in fetch_all_rows(lambda: client.table("edges").select("source_document_id"))
        if r["source_document_id"] is not None
    }
    stale = edge_docs - current_doc_ids
    for doc_id in stale:
        client.table("edges").delete().eq("source_document_id", doc_id).execute()
        client.table("subject_resolution_queue").delete().eq("document_id", doc_id).execute()
    return len(stale)
