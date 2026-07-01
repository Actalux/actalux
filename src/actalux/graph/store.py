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
# which lives on the document; the projector attaches entity_id from there. ``motion``
# is the verbatim text the matter projector reads bill/resolution numbers from.
_VOTE_FIELDS = (
    "id,document_id,vote_ref,citation_id,source_quote,chunk_id,details,meeting_date,motion"
)


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


def current_documents(client: Client, entity_ids: list[int]) -> list[dict[str, Any]]:
    """Current (non-superseded) documents of ANY type for the given bodies.

    Broader than ``current_minutes``: matter mentions scan agendas, staff reports, and
    transcripts too (a bill is discussed in the agenda and the meeting, not only voted
    on in the minutes), so the mention projector iterates all current documents.
    ``document_type`` is included so minting can restrict its source to the authoritative
    doc types (agenda/minutes) while mentions still scan everything.
    """
    return fetch_all_rows(
        lambda: (
            client.table("documents")
            .select("id,entity_id,meeting_date,document_type")
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
        .select("id,slug,canonical_name,metadata,person_id")
        .in_("id", subject_ids)
        .eq("publishable", True)
        .execute()
        .data
    )
    by_id = {s["id"]: s for s in subjects}
    # The member link/URL uses the public persons.slug, not the internal subject slug
    # (a non-primary board's subject slug is '{slug}--{body_slug}'). Map each subject's
    # person to its public slug; fall back to the subject slug when unlinked (a person
    # row not yet backfilled).
    person_ids = [s["person_id"] for s in subjects if s.get("person_id")]
    public_slug_by_person: dict[int, str] = {}
    if person_ids:
        persons = client.table("persons").select("id,slug").in_("id", person_ids).execute().data
        public_slug_by_person = {p["id"]: p["slug"] for p in persons}
    members: list[dict[str, Any]] = []
    for m in memberships:
        subject = by_id.get(m["subject_id"])
        if subject is None:
            continue
        public_slug = public_slug_by_person.get(subject.get("person_id")) or subject["slug"]
        # role from the membership (per body) — a cross-body member's role differs
        # between, say, council and the Plan Commission.
        members.append(
            {
                **subject,
                "slug": public_slug,
                "role": m["role"],
                "start_date": m["start_date"],
                "end_date": m["end_date"],
            }
        )
    return members


def member_by_slug(client: Client, place_id: int, slug: str, entity_id: int) -> dict | None:
    """One publishable member of a body by slug, with its term window, or None.

    The public identity is ``persons.slug`` (Model B): a person on N bodies has one
    ``persons`` row and one per-board ``subjects`` row each, so the member-in-body page
    resolves ``persons.slug`` + the body to that board's subject. Falls back to a
    ``subjects.slug`` match when no person resolves — which keeps every member URL
    working before the per-board migration (``persons`` not yet populated) and resolves
    any legacy/internal subject-slug link afterward. The returned ``slug`` is always the
    public one; ``id`` is the per-board subject id (the vote record keys on it).
    """
    subject: Any = None
    public_slug = slug
    person = (
        client.table("persons")
        .select("id,slug")
        .eq("slug", slug)
        .eq("publishable", True)
        .limit(1)
        .execute()
        .data
    )
    if person:
        rows = (
            client.table("subjects")
            .select("id,slug,canonical_name,metadata")
            .eq("person_id", person[0]["id"])
            .eq("entity_id", entity_id)
            .eq("type", "person")
            .eq("publishable", True)
            .limit(1)
            .execute()
            .data
        )
        if rows:
            subject = rows[0]
            public_slug = person[0]["slug"]
    if subject is None:
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
        public_slug = rows[0]["slug"]
    assert subject is not None  # set by the person or fallback branch, else returned above
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
    return {**subject, **membership[0], "slug": public_slug}


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


def _member_record_summary(client: Client, subject_id: int, entity_id: int) -> dict[str, Any]:
    """Cited-action count + meeting-date span for one (member, body) — two light reads.

    PostgREST's exact count is the total matching set (not the limited page), so the
    ascending query yields the count and the earliest date together; one more read
    gives the latest. Avoids paging a long-serving member's whole record just to
    summarize it on the person spine.
    """
    asc = (
        client.table("member_vote_records")
        .select("meeting_date", count="exact")
        .eq("subject_id", subject_id)
        .eq("entity_id", entity_id)
        .order("meeting_date")
        .limit(1)
        .execute()
    )
    desc = (
        client.table("member_vote_records")
        .select("meeting_date")
        .eq("subject_id", subject_id)
        .eq("entity_id", entity_id)
        .order("meeting_date", desc=True)
        .limit(1)
        .execute()
        .data
    )
    return {
        "count": asc.count or 0,
        "first_date": asc.data[0]["meeting_date"] if asc.data else None,
        "last_date": desc[0]["meeting_date"] if desc else None,
    }


def person_dossier(client: Client, slug: str) -> dict | None:
    """One publishable person with a per-body tenure summary, or None.

    The /people/{slug} spine (Model B): the global person identity plus one entry per
    governing body they serve(d) — each per-board subject's role, term window, cited-
    action count, and the span of that cited record. Returns DATA only; the caller
    assembles URLs/labels per tenure from ``entity_id`` (presentation stays in the web
    layer). A non-publishable / unknown slug returns None.
    """
    person_rows = (
        client.table("persons")
        .select("id,slug,canonical_name")
        .eq("slug", slug)
        .eq("publishable", True)
        .limit(1)
        .execute()
        .data
    )
    if not person_rows:
        return None
    person = person_rows[0]

    subjects = (
        client.table("subjects")
        .select("id,entity_id,metadata")
        .eq("person_id", person["id"])
        .eq("type", "person")
        .eq("publishable", True)
        .execute()
        .data
    )
    subject_ids = [s["id"] for s in subjects if s.get("entity_id") is not None]
    memberships = (
        client.table("memberships")
        .select("subject_id,role,start_date,end_date")
        .in_("subject_id", subject_ids)
        .execute()
        .data
        if subject_ids
        else []
    )
    mem_by_subject = {m["subject_id"]: m for m in memberships}

    tenures: list[dict[str, Any]] = []
    for s in subjects:
        entity_id = s.get("entity_id")
        if entity_id is None:
            continue  # an unsplit/legacy subject carries no single body; skip it
        m = mem_by_subject.get(s["id"], {})
        summary = _member_record_summary(client, s["id"], entity_id)
        tenures.append(
            {
                "subject_id": s["id"],
                "entity_id": entity_id,
                "role": m.get("role") or (s.get("metadata") or {}).get("role"),
                "start_date": m.get("start_date"),
                "end_date": m.get("end_date"),
                "actions": summary["count"],
                "first_date": summary["first_date"],
                "last_date": summary["last_date"],
            }
        )
    return {
        "person": {"slug": person["slug"], "canonical_name": person["canonical_name"]},
        "tenures": tenures,
    }


def publishable_person_slugs(client: Client) -> list[str]:
    """Every publishable person's slug, for the sitemap's ``/people/{slug}`` spine.

    Global (not place-scoped): the person route aggregates a person's per-body
    subjects across jurisdictions, so the canonical URL carries no place segment.
    """
    rows = fetch_all_rows(
        lambda: client.table("persons").select("slug").eq("publishable", True),
        order="slug",
    )
    return [r["slug"] for r in rows if r.get("slug")]


def place_lexicon(client: Client, place_id: int) -> list[dict[str, Any]]:
    """Every publishable person in a place with its name variants and memberships.

    The place-level roll-up behind GET /api/v1/{state}/{place}/lexicon: a person on
    two bodies is ONE entry carrying both memberships, so a downstream consumer
    maintains canonical official spellings in one place. Reads only publishable
    subjects through the anon RLS path (migration 032 opened aliases for them).
    Mirrors load_roster's fetch-all-then-group (single place, small tables).
    """
    subjects = fetch_all_rows(
        lambda: (
            client.table("subjects")
            .select("id,slug,canonical_name,metadata,person_id,entity_id")
            .eq("place_id", place_id)
            .eq("type", "person")
            .eq("publishable", True)
        )
    )
    if not subjects:
        return []
    subject_ids = {s["id"] for s in subjects}

    aliases = fetch_all_rows(
        lambda: client.table("subject_aliases").select(
            "subject_id,raw_alias,normalized_alias,source"
        )
    )
    memberships = fetch_all_rows(
        lambda: client.table("memberships").select("subject_id,entity_id,role,start_date,end_date")
    )
    entities = client.table("entities").select("id,body_slug").eq("place_id", place_id).execute()
    body_by_eid = {e["id"]: e["body_slug"] for e in entities.data}
    person_ids = [s["person_id"] for s in subjects if s.get("person_id")]
    persons = (
        client.table("persons")
        .select("id,slug,canonical_name")
        .in_("id", person_ids)
        .execute()
        .data
        if person_ids
        else []
    )
    person_by_id = {p["id"]: p for p in persons}

    by_aliases: dict[int, list[dict]] = {sid: [] for sid in subject_ids}
    for a in aliases:
        if a["subject_id"] in by_aliases:
            by_aliases[a["subject_id"]].append(a)
    by_memberships: dict[int, list[dict]] = {sid: [] for sid in subject_ids}
    for m in memberships:
        if m["subject_id"] in by_memberships and m["entity_id"] in body_by_eid:
            by_memberships[m["subject_id"]].append(m)

    # Group a person's per-board subjects (Model B) back into ONE lexicon entry keyed by
    # the public persons.slug, so a cross-body person is one entry carrying both bodies —
    # the pre-split contract the downstream consumers (ledger glossary, speaker
    # attribution) depend on. A subject not yet linked to a person stands alone.
    groups: dict[Any, list[dict]] = {}
    for s in subjects:
        key = s["person_id"] if s.get("person_id") else ("subject", s["id"])
        groups.setdefault(key, []).append(s)

    def _entity_order(subject: dict) -> int:
        return subject["entity_id"] if subject.get("entity_id") is not None else 1_000_000

    lexicon: list[dict[str, Any]] = []
    for key, subs in groups.items():
        primary = min(subs, key=_entity_order)  # lowest entity_id keeps role/identity
        person = person_by_id.get(key) if isinstance(key, int) else None
        slug = person["slug"] if person else primary["slug"]
        canonical = person["canonical_name"] if person else primary["canonical_name"]

        mships = [m for s in subs for m in by_memberships[s["id"]]]
        bodies = sorted(
            (
                {
                    "body_slug": body_by_eid[m["entity_id"]],
                    "role": m["role"],
                    "start_date": m["start_date"],
                    "end_date": m["end_date"],
                }
                for m in mships
            ),
            key=lambda b: b["body_slug"],
        )
        # Union the per-board subjects' aliases (each board carries the same copied set).
        alias_by_norm: dict[str, dict] = {}
        for s in subs:
            for a in by_aliases[s["id"]]:
                alias_by_norm.setdefault(
                    a["normalized_alias"],
                    {
                        "raw": a["raw_alias"],
                        "normalized": a["normalized_alias"],
                        "source": a["source"],
                    },
                )
        lexicon.append(
            {
                "slug": slug,
                "canonical_name": canonical,
                "kind": "person",
                "role": (primary.get("metadata") or {}).get("role"),
                # still seated on at least one body (a NULL end_date = open term)
                "current": any(m["end_date"] is None for m in mships),
                "bodies": bodies,
                "aliases": sorted(
                    alias_by_norm.values(), key=lambda a: a["raw"] or a["normalized"] or ""
                ),
            }
        )
    return sorted(lexicon, key=lambda e: e["canonical_name"])


def upsert_matters(client: Client, place_id: int, matters: dict[str, Any]) -> dict[str, int]:
    """Idempotently upsert matter subjects; return slug -> subject_id.

    ``matters`` maps slug -> MatterRef. A matter is publishable on its own (the
    minting trigger gates only persons), so it is written publishable in one upsert.
    Batched: a single upsert returns every row's id.
    """
    if not matters:
        return {}
    rows = [
        {
            "type": "matter",
            "subject_role": "matter",
            "canonical_name": ref.canonical,
            "slug": slug,
            "place_id": place_id,
            "minting_basis": "regex_number",
            "publishable": True,
            "metadata": {"kind": ref.kind, "number": ref.number, "title": ref.title},
        }
        for slug, ref in matters.items()
    ]
    result = client.table("subjects").upsert(rows, on_conflict="place_id,type,slug").execute()
    return {r["slug"]: r["id"] for r in result.data}


def matter_mention_rollup(client: Client, entity_id: int) -> dict[int, dict[str, Any]]:
    """Per-matter mention count + latest mention date for one body, keyed by subject_id.

    Rolls up the ``mentions`` table (cited occurrences of a matter across a body's
    documents) so a matter referenced in agendas/discussion but never voted still has a
    presence to surface. Each mention is scoped to the body via its document. Returns
    ``{subject_id: {"references": int, "latest_date": str | None}}``.
    """
    mentions = fetch_all_rows(
        lambda: client.table("mentions").select("subject_id,document_id"), order="id"
    )
    if not mentions:
        return {}
    doc_ids = sorted({m["document_id"] for m in mentions})
    dates: dict[int, str | None] = {}
    for i in range(0, len(doc_ids), 200):
        rows = (
            client.table("documents")
            .select("id,meeting_date")
            .in_("id", doc_ids[i : i + 200])
            .eq("entity_id", entity_id)
            .execute()
            .data
        )
        for d in rows:
            dates[d["id"]] = d.get("meeting_date")
    rollup: dict[int, dict[str, Any]] = {}
    for m in mentions:
        if m["document_id"] not in dates:
            continue  # mention on another body's document
        r = rollup.setdefault(m["subject_id"], {"references": 0, "latest_date": None})
        r["references"] += 1
        md = dates[m["document_id"]]
        if (md or "") > (r["latest_date"] or ""):
            r["latest_date"] = md
    return rollup


def body_matters(client: Client, entity_id: int) -> list[dict[str, Any]]:
    """Matters with a cited presence in a body: action count, reference count, latest date.

    Unions two sources so a bill is listed whether it was voted or only scheduled:
    the matter_vote_records view (matters acted on) and the mentions rollup (matters
    referenced in agendas/discussion). A never-voted matter carries ``actions == 0`` and
    ``references > 0``. Sorted newest first (latest action or mention).
    """
    rows = fetch_all_rows(
        lambda: (
            client.table("matter_vote_records")
            .select("subject_id,subject_slug,subject_name,subject_metadata,meeting_date")
            .eq("entity_id", entity_id)
        ),
        order="edge_id",
    )
    by_id: dict[int, dict[str, Any]] = {}
    for r in rows:
        m = by_id.get(r["subject_id"])
        if m is None:
            by_id[r["subject_id"]] = {
                "subject_id": r["subject_id"],
                "slug": r["subject_slug"],
                "canonical_name": r["subject_name"],
                "metadata": r["subject_metadata"] or {},
                "actions": 1,
                "latest_date": r["meeting_date"],
            }
        else:
            m["actions"] += 1
            if (r["meeting_date"] or "") > (m["latest_date"] or ""):
                m["latest_date"] = r["meeting_date"]

    mentions = matter_mention_rollup(client, entity_id)
    mention_only = [sid for sid in mentions if sid not in by_id]
    for i in range(0, len(mention_only), 200):
        subs = (
            client.table("subjects")
            .select("id,slug,canonical_name,metadata")
            .in_("id", mention_only[i : i + 200])
            .eq("type", "matter")
            .eq("publishable", True)
            .execute()
            .data
        )
        for s in subs:
            mr = mentions[s["id"]]
            by_id[s["id"]] = {
                "subject_id": s["id"],
                "slug": s["slug"],
                "canonical_name": s["canonical_name"],
                "metadata": s.get("metadata") or {},
                "actions": 0,
                "references": mr["references"],
                "latest_date": mr["latest_date"],
            }
    for sid, m in by_id.items():
        m.setdefault("references", mentions.get(sid, {}).get("references", 0))
    return sorted(by_id.values(), key=lambda m: m["latest_date"] or "", reverse=True)


def matter_by_slug(client: Client, place_id: int, slug: str) -> dict | None:
    """One publishable matter subject by slug, or None."""
    rows = (
        client.table("subjects")
        .select("id,slug,canonical_name,metadata")
        .eq("place_id", place_id)
        .eq("slug", slug)
        .eq("type", "matter")
        .eq("publishable", True)
        .limit(1)
        .execute()
        .data
    )
    return rows[0] if rows else None


def matter_records(client: Client, subject_id: int, entity_id: int) -> list[dict[str, Any]]:
    """A matter's full cited timeline (via the matter_vote_records view), paged."""
    return fetch_all_rows(
        lambda: (
            client.table("matter_vote_records")
            .select("*")
            .eq("subject_id", subject_id)
            .eq("entity_id", entity_id)
        ),
        order="edge_id",
    )


# ---- Cross-links (member <-> matter), keyed on vote_id ---------------------
#
# In both read views vote_id = votes.id (migrate_030/031 join votes on the durable
# pair document_id + vote_ref), so a member edge and a matter's 'considered' edge on
# the SAME vote share vote_id. That shared key is what lets a member's vote link to
# the bill it was on, and a matter's action list the members who acted on it — from
# the existing edges, with no schema change.


def matters_by_vote(client: Client, entity_id: int) -> dict[int, list[dict[str, str]]]:
    """Map each vote_id to the matter(s) it concerned, for one body.

    A vote almost always concerns one matter, but a combined motion can be linked to
    several ('considered' edges are unique per (vote, matter), not per vote), so the
    value is a list. Bounded by the body's matter-action count, so one paged read
    backs a whole member page.
    """
    rows = fetch_all_rows(
        lambda: (
            client.table("matter_vote_records")
            .select("vote_id,subject_slug,subject_name")
            .eq("entity_id", entity_id)
        ),
        order="edge_id",
    )
    out: dict[int, list[dict[str, str]]] = {}
    for r in rows:
        vid, slug = r.get("vote_id"), r.get("subject_slug")
        if vid is None or not slug:
            continue
        out.setdefault(vid, []).append({"slug": slug, "name": r["subject_name"]})
    return out


def members_by_vote(
    client: Client, entity_id: int, vote_ids: list[int | None]
) -> dict[int, list[dict[str, str]]]:
    """Map each vote_id to the members who acted on it (voted/moved/seconded).

    Scoped to ``vote_ids`` (a single matter's actions), so the IN list stays small.
    Each entry carries the member slug/name + the edge_type (aye/no/abstain/moved/
    seconded) so the caller can label the role.
    """
    ids = [v for v in dict.fromkeys(vote_ids) if v is not None]
    if not ids:
        return {}
    rows = fetch_all_rows(
        lambda: (
            client.table("member_vote_records")
            .select("vote_id,subject_slug,subject_name,edge_type")
            .eq("entity_id", entity_id)
            .in_("vote_id", ids)
        ),
        order="edge_id",
    )
    out: dict[int, list[dict[str, str]]] = {}
    for r in rows:
        vid, slug = r.get("vote_id"), r.get("subject_slug")
        if vid is None or not slug:
            continue
        out.setdefault(vid, []).append(
            {"slug": slug, "name": r["subject_name"], "edge_type": r["edge_type"]}
        )
    return out


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


def replace_document_mentions(client: Client, doc_id: int, mentions: list[dict]) -> None:
    """Idempotently rebuild one document's matter mentions (delete-then-insert).

    Scoped to ``document_id`` so a re-run reproduces exactly the current derivation for
    this document and nothing else — the same contract as ``replace_document_graph``.
    """
    client.table("mentions").delete().eq("document_id", doc_id).execute()
    if mentions:
        client.table("mentions").insert(mentions).execute()


def prune_stale_mentions(client: Client, current_doc_ids: set[int]) -> int:
    """Delete mentions on a document no longer in the projected set (superseded or out
    of scope), mirroring ``prune_stale_graph`` for edges. ``current_doc_ids`` is every
    document scanned for mentions this run. Returns the number of documents pruned.
    """
    mention_docs = {
        r["document_id"]
        for r in fetch_all_rows(lambda: client.table("mentions").select("document_id"))
        if r["document_id"] is not None
    }
    stale = mention_docs - current_doc_ids
    for doc_id in stale:
        client.table("mentions").delete().eq("document_id", doc_id).execute()
    return len(stale)


def matter_mention_records(client: Client, subject_id: int) -> list[dict[str, Any]]:
    """Cited mentions of a matter, enriched with the referencing document's date/title/
    type, newest first.

    Anon-safe: the ``mentions`` RLS policy gates to publishable subjects and documents
    are anon-readable, so this serves the matter page's "Also referenced in" section.
    Returns DATA only — the caller assembles labels/links (display stays in the web
    layer).
    """
    rows = fetch_all_rows(
        lambda: (
            client.table("mentions")
            .select("document_id,chunk_id,citation_id,source_quote")
            .eq("subject_id", subject_id)
        ),
        order="id",
    )
    if not rows:
        return []
    doc_ids = sorted({r["document_id"] for r in rows})
    docs = {
        d["id"]: d
        for d in fetch_all_rows(
            lambda: (
                client.table("documents")
                .select("id,meeting_date,meeting_title,document_type")
                .in_("id", doc_ids)
            )
        )
    }
    enriched = []
    for r in rows:
        doc = docs.get(r["document_id"]) or {}
        enriched.append(
            {
                **r,
                "meeting_date": doc.get("meeting_date"),
                "meeting_title": doc.get("meeting_title"),
                "document_type": doc.get("document_type"),
            }
        )
    enriched.sort(key=lambda r: r.get("meeting_date") or "", reverse=True)
    return enriched
