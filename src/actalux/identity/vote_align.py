"""Vote-sequence alignment labeler: name a cluster from the clerk-call roll-call structure.

A second, independent evidence family alongside the deterministic name-anchor resolver
(:mod:`actalux.identity.resolve`) and the LLM discourse labeler (:mod:`actalux.identity`
``.discourse``). Where the resolver reads a *single* name+response adjacency and the discourse
labeler reads *addressing semantics*, this reads the *whole roll-call sequence* as one structured
object and aligns it against the structured vote record — a genuinely different failure mode, so
its agreement with the other families is worth something the adjacency families' mutual agreement
is not (design: docs/architecture/voiceprint-scale-design.md, family 2).

The signal is a clerk reading a roll: "Smith" -> (a different voice) "here", "Jones" -> "here",
... . Four properties make a wrong label structurally hard, not merely unlikely:

* **Whole-sequence consistency.** Names are aligned to responses by a monotone DP over time
  (a response for a called name falls between that call and the next), not one adjacency at a
  time — a stray "here" cannot anchor a name unless the surrounding sequence agrees.
* **Clerk exclusion.** The cluster doing the calling (>= two name-calls in the region) is the
  clerk and is ineligible to be a *responder* — so the clerk is never labeled as a member.
* **1:1 matching.** Within a meeting the assignment is a matching: no cluster maps to two
  members and no member to two clusters; any conflict rejects rather than guesses.
* **Count check against the DB.** The correlated vote record's member set (immune to ASR
  mangling) bounds how many distinct voices may respond — a voice-vote chorus (many voices,
  no per-name calls) fails the count and is rejected wholesale.

Measured reality (Clayton): a *precision* play, not a coverage play. Only a few dozen transcripts
have a cleanly separable clerk-call sequence (pyannote often glues the member's "here" into the
clerk's next-name turn, which this treats as an alignment gap, never a mislabel). Its real value
is retiring the poisoned single-adjacency roll-call heuristic (``resolve._rollcall_hits``) as the
source of truth on those transcripts and seeding the cleanest voiceprint-gallery anchors.

Option B throughout: only roster members are ever labeled; a private citizen (never in the
roster) is structurally unlabelable. Jurisdiction-agnostic: no town's wording appears here — the
affirmative vocabulary is generic parliamentary English and the member set comes from the DB.
"""

from __future__ import annotations

import logging
from collections import Counter, defaultdict
from dataclasses import dataclass, replace

from supabase import Client

from actalux.db import fetch_all_rows
from actalux.identity.resolve import (
    _AFFIRMATIVE_RESPONSES,
    _CONFIDENCE_RANK,
    IdentityProposal,
    ResolverTurn,
    RosterMember,
    _name_index,
    _name_only_match,
    _norm_text,
)

logger = logging.getLogger(__name__)

# The recorded schema basis for a vote-alignment anchor. Already admitted by the
# speaker_identities.basis / subject_voiceprints.source_basis CHECK constraints (reserved in
# migrate_034, kept through migrate_042/044), so no migration is needed to persist or enroll it.
VOTE_ANCHOR_BASIS = "vote_anchor"

# --- region detection --------------------------------------------------------------------
# A clerk reading a roll names members in quick succession. A region is a run of name-call turns
# whose successive gaps stay small; K distinct members must be named for it to be a real roll
# (not a couple of incidental name mentions). K=4 keeps precision high on the multi-member bodies
# this reaches (council-heavy; a 3-member board that roll-calls is missed — acceptable for a
# precision play), while acceptance below still tolerates one absent/glued member (>=3 bindings).
_MIN_REGION_CALLS = 4
# Max turns between two successive name-calls to stay one region. Between "Smith" and "Jones" sit
# Smith's "here" (and perhaps a short fragment); a real debate stretch exceeds this and correctly
# splits two separate roll calls apart. Generous enough to survive diarization fragmenting a reply.
_MAX_CALL_GAP = 8
# A cluster emitting this many name-calls within a region is the clerk (reading the roll). The
# clerk contributes the *calls* and is excluded from the *responders* — so it is never labeled.
_CLERK_MIN_CALLS = 2
# Turns past the last call still scanned for that last member's response (the response trails the
# call by a turn or three). Bounds the region's tail so unrelated later speech is not swept in.
_RESPONSE_TAIL = 4

# --- acceptance + tiering ----------------------------------------------------------------
# Minimum clean call<->response bindings to accept a region at all. Below this the alignment is
# too thin to be a roll call. Combined with K=4 region calls, this tolerates exactly one gap.
_MIN_CLEAN_BINDINGS = 3
# The margin that WOULD support a publishable (inferred_high) tier: this many clean bindings, zero
# gaps, and a correlated DB vote record. See _PUBLISHABLE_TIER_ENABLED for why it is not emitted.
_HIGH_MIN_BINDINGS = 5
# Whether a large-margin alignment may PUBLISH (inferred_high, above the public-display gate).
# OFF by design: the aligner proves the called NAME is a roster member, but text alone cannot
# prove the responding VOICE is that member — a citizen's short reply filling a glued member's
# window would bind the wrong cluster to a real official's name (codex review 2026-07). A wrong
# PUBLIC name is the fatal error class, so every vote_anchor stays inferred_medium (enrollable,
# below the public gate, exactly like presenter_intro / discourse); the acoustic gates on the
# voiceprint gallery then verify the voice. The publishable tier is deferred to the Phase C
# acoustic consensus gate (Gate A, docs/architecture/voiceprint-scale-design.md), which checks
# family agreement in embedding space; flip this to True only once that gate is the read path.
_PUBLISHABLE_TIER_ENABLED = False
# Distinct responders may exceed the authoritative member count by at most this much before the
# region is rejected as a chorus/crowd. Small: it only absorbs diarization splitting one member
# into two clusters, never a room of voices answering "aye" at once (which would blow past it).
_RESPONDER_SLACK = 2

# The affirmative vocabulary of a roll-call response: the resolver's here/present set plus the
# unambiguous parliamentary aye-forms. Deliberately EXCLUDES the collision-prone bare tokens a
# citizen or ASR noise could emit — "i"/"im" (the ASR of "aye", but also the most common English
# word), and the dissent tokens "no"/"nay"/"abstain". A member who votes no or is absent simply
# becomes an alignment gap (safe), never a mislabel; the tighter set keeps a stray one-word turn
# from ever standing in as a response (codex review 2026-07). Every entry is a whole-turn match.
_VOTE_AFFIRMATIVES = _AFFIRMATIVE_RESPONSES | frozenset({"aye", "ayes", "yea", "yeas"})


@dataclass(frozen=True)
class VoteReference:
    """The correlated vote record's authoritative member set for a meeting.

    Built only when a minutes document with per-member roll-call votes correlates to the
    transcript by ``(entity_id, meeting_date)``; its presence therefore *is* the "the DB
    corroborates a roll call here" signal (a precondition for the publishable tier). The set is
    ASR-immune (it comes from parsed structured votes, not the audio), so it bounds the responder
    count and confirms the meeting truly held a roll call.
    """

    member_ids: frozenset[int]


@dataclass(frozen=True)
class _Call:
    """One clerk name-call: the roster member named, anchored at its turn index (= time order)."""

    turn_index: int
    subject_id: int


@dataclass(frozen=True)
class _Response:
    """One short affirmative response turn from a non-clerk cluster, at its turn index."""

    turn_index: int
    cluster_label: str


@dataclass(frozen=True)
class _Region:
    """A detected clerk-call roll-call region: the clerk cluster(s), its calls and responses.

    ``end`` is the exclusive upper turn bound (last call + tail); it is also the response-window
    ceiling for the last call in the DP.
    """

    start: int
    end: int
    clerk_clusters: frozenset[str]
    calls: tuple[_Call, ...]
    responses: tuple[_Response, ...]


def _name_call_turns(
    turns: list[ResolverTurn], strong: dict[str, int], surname: dict[str, int]
) -> list[tuple[int, int, str]]:
    """``(turn_index, subject_id, cluster_label)`` for turns that ARE exactly one member's name.

    Reuses the resolver's exact name-only match (honorifics stripped; a turn that merely *contains*
    a name, or whose text is a longer name, does not match), so a call is only taken from a clean
    clerk read like "Smith" / "Councilmember Smith". A glued "Smith. Here." is not a name-only
    turn and is deliberately missed (precision over recall).
    """
    out: list[tuple[int, int, str]] = []
    for idx, turn in enumerate(turns):
        hit = _name_only_match(turn.text, strong, surname)
        if hit is not None:
            out.append((idx, hit[0], turn.cluster_label))
    return out


def _group_by_gap(
    items: list[tuple[int, int, str]], max_gap: int
) -> list[list[tuple[int, int, str]]]:
    """Split time-ordered ``(turn_index, ...)`` items into runs broken by a gap over ``max_gap``."""
    groups: list[list[tuple[int, int, str]]] = []
    current: list[tuple[int, int, str]] = []
    for item in items:
        if current and item[0] - current[-1][0] > max_gap:
            groups.append(current)
            current = []
        current.append(item)
    if current:
        groups.append(current)
    return groups


def _detect_regions(
    turns: list[ResolverTurn], strong: dict[str, int], surname: dict[str, int]
) -> list[_Region]:
    """Find clerk-call roll-call regions in a transcript's turns.

    A region is a run of name-calls (clustered by :data:`_MAX_CALL_GAP`) in which some cluster
    calls at least :data:`_CLERK_MIN_CALLS` names (the clerk) and the clerk's calls name at least
    :data:`_MIN_REGION_CALLS` distinct members. Only the clerk's calls become reference positions;
    responders are the short affirmatives from every OTHER cluster in the region's turn span.
    """
    regions: list[_Region] = []
    for group in _group_by_gap(_name_call_turns(turns, strong, surname), _MAX_CALL_GAP):
        caller_counts = Counter(cluster for _, _, cluster in group)
        clerk = frozenset(c for c, n in caller_counts.items() if n >= _CLERK_MIN_CALLS)
        if not clerk:
            continue  # no cluster reads two+ names -> not a clerk-read roll call
        clerk_calls = [(idx, sid) for idx, sid, cluster in group if cluster in clerk]
        if len({sid for _, sid in clerk_calls}) < _MIN_REGION_CALLS:
            continue  # too few distinct members named to be a roll
        start = clerk_calls[0][0]
        end = min(len(turns), clerk_calls[-1][0] + 1 + _RESPONSE_TAIL)
        calls = tuple(_Call(idx, sid) for idx, sid in clerk_calls)
        responses = tuple(
            _Response(i, turns[i].cluster_label)
            for i in range(start, end)
            if turns[i].cluster_label not in clerk
            and _norm_text(turns[i].text) in _VOTE_AFFIRMATIVES
        )
        regions.append(_Region(start, end, clerk, calls, responses))
    return regions


def _align_calls_responses(region: _Region) -> list[tuple[_Call, _Response]]:
    """Monotone (Needleman-Wunsch) alignment of the call sequence to the response sequence.

    A call may bind a response only when the response falls strictly between that call and the
    next call in time (``call_i < resp_j < next_call``), so a response is attributed to the name
    it immediately follows. The DP maximizes the number of such bindings; skipping a call (an
    absent member or a response pyannote glued into the clerk's turn) or a response is free, so
    maximizing matches is equivalently minimizing gaps — the "gaps allowed with penalty" of the
    design, where the penalty is the unit reward forgone. Backtracking prefers a valid match, then
    skipping a call, then skipping a response, so the recovered alignment is deterministic.
    """
    calls, responses = region.calls, region.responses
    m, n = len(calls), len(responses)
    # The exclusive time ceiling for a response bound to call i: the next call, or the region end.
    next_time = [calls[i + 1].turn_index if i + 1 < m else region.end for i in range(m)]

    def _valid(i: int, j: int) -> bool:
        return calls[i].turn_index < responses[j].turn_index < next_time[i]

    # dp[i][j] = max bindings achievable over calls[i:] against responses[j:].
    dp = [[0] * (n + 1) for _ in range(m + 1)]
    for i in range(m - 1, -1, -1):
        for j in range(n - 1, -1, -1):
            best = max(dp[i + 1][j], dp[i][j + 1])  # skip this call / skip this response
            if _valid(i, j):
                best = max(best, 1 + dp[i + 1][j + 1])
            dp[i][j] = best

    bindings: list[tuple[_Call, _Response]] = []
    i = j = 0
    while i < m and j < n:
        if _valid(i, j) and dp[i][j] == 1 + dp[i + 1][j + 1]:
            bindings.append((calls[i], responses[j]))
            i, j = i + 1, j + 1
        elif dp[i][j] == dp[i + 1][j]:
            i += 1
        else:
            j += 1
    return bindings


def _accept_region(
    region: _Region, vote_ref: VoteReference | None
) -> list[tuple[str, int, str]] | None:
    """Accept a region and return its ``(cluster, subject_id, confidence)`` anchors, or ``None``.

    Rejects (labels nothing) unless the alignment binds at least :data:`_MIN_CLEAN_BINDINGS`
    positions, the binding is a 1:1 matching (no cluster to two members, no member to two
    clusters), AND the distinct-responder count is consistent with the authoritative member count
    (a voice-vote chorus fails this). Every accepted anchor is inferred_medium (enrollable, below
    the public-display gate); the large-margin publishable tier is withheld — see
    :data:`_PUBLISHABLE_TIER_ENABLED` for why text evidence alone does not publish a name.
    """
    bindings = _align_calls_responses(region)
    if len(bindings) < _MIN_CLEAN_BINDINGS:
        return None
    cluster_members: dict[str, set[int]] = defaultdict(set)
    member_clusters: dict[int, set[str]] = defaultdict(set)
    for call, resp in bindings:
        cluster_members[resp.cluster_label].add(call.subject_id)
        member_clusters[call.subject_id].add(resp.cluster_label)
    if any(len(v) > 1 for v in cluster_members.values()) or any(
        len(v) > 1 for v in member_clusters.values()
    ):
        return None  # a cluster answered to two names (or vice versa) -> reject, do not guess
    # The DB member set (ASR-immune) is authoritative for "how many voices should answer"; without
    # it, fall back to the count of distinct called members. A chorus blows past this bound.
    n_members = len(vote_ref.member_ids) if vote_ref else len({c.subject_id for c in region.calls})
    if len({r.cluster_label for r in region.responses}) > n_members + _RESPONDER_SLACK:
        return None  # too many distinct voices for a per-name roll -> chorus/crowd, reject
    zero_gaps = len(bindings) == len(region.calls)
    large_margin = vote_ref is not None and len(bindings) >= _HIGH_MIN_BINDINGS and zero_gaps
    confidence = (
        "inferred_high" if (large_margin and _PUBLISHABLE_TIER_ENABLED) else "inferred_medium"
    )
    return [(resp.cluster_label, call.subject_id, confidence) for call, resp in bindings]


def align_votes(
    turns: list[ResolverTurn],
    members: list[RosterMember],
    vote_ref: VoteReference | None,
) -> list[IdentityProposal]:
    """Vote-alignment cluster -> member proposals for one transcript (pure; no DB, no I/O).

    Detects every clerk-call region, accepts each independently (:func:`_accept_region`), then
    enforces the MEETING-level matching across regions: a cluster bound to two different members
    (or a member to two clusters) anywhere in the meeting is dropped as ambiguous; a cluster bound
    to the SAME member in more than one region is corroborated and kept at its best tier. Nothing
    is invented — every subject is a roster member — and a meeting with no clean roll call yields
    no proposals.
    """
    if not turns or not members:
        return []
    strong, surname = _name_index(members)
    members_by_id = {m.subject_id: m for m in members}

    accepted: list[tuple[str, int, str]] = []
    for region in _detect_regions(turns, strong, surname):
        anchors = _accept_region(region, vote_ref)
        if anchors:
            accepted.extend(anchors)

    cluster_members: dict[str, set[int]] = defaultdict(set)
    member_clusters: dict[int, set[str]] = defaultdict(set)
    best_conf: dict[tuple[str, int], str] = {}
    for cluster, sid, conf in accepted:
        cluster_members[cluster].add(sid)
        member_clusters[sid].add(cluster)
        key = (cluster, sid)
        if key not in best_conf or _CONFIDENCE_RANK[conf] > _CONFIDENCE_RANK[best_conf[key]]:
            best_conf[key] = conf

    proposals: list[IdentityProposal] = []
    for (cluster, sid), conf in best_conf.items():
        if len(cluster_members[cluster]) > 1 or len(member_clusters[sid]) > 1:
            continue  # meeting-level conflict across regions -> ambiguous, drop
        proposals.append(
            IdentityProposal(cluster, sid, members_by_id[sid].slug, conf, VOTE_ANCHOR_BASIS)
        )
    return sorted(proposals, key=lambda p: p.cluster_label)


def _demote_contested(proposals: list[IdentityProposal]) -> list[IdentityProposal]:
    """Demote to inferred_low any member claimed by more than one cluster (a cross-family contest).

    Preserves the invariant the resolver already enforces within its own family: a single member
    is never published on two clusters in one document. When merging vote_anchor with the
    deterministic anchors introduces such a contest, both claimants drop to review rather than one
    being guessed — precision over recall.
    """
    clusters_by_subject: dict[int, set[str]] = defaultdict(set)
    for p in proposals:
        clusters_by_subject[p.subject_id].add(p.cluster_label)
    out: list[IdentityProposal] = []
    for p in proposals:
        if len(clusters_by_subject[p.subject_id]) > 1 and p.confidence != "inferred_low":
            out.append(replace(p, confidence="inferred_low"))
        else:
            out.append(p)
    return out


def merge_vote_anchor(
    resolver_proposals: list[IdentityProposal],
    vote_proposals: list[IdentityProposal],
) -> list[IdentityProposal]:
    """Merge vote_anchor proposals into the deterministic resolver proposals under precedence.

    The design-reviewed precedence between the poisoned single-adjacency roll-call heuristic
    (``resolve._rollcall_hits``) and this whole-sequence family, resolved per (document, cluster):

    * **Same cluster, agree** (same member): keep the anchor at the higher confidence but record
      ``basis='vote_anchor'`` — the structural evidence supersedes the poisoned rollcall label.
    * **Same cluster, disagree**: vote_anchor wins ONLY when it is inferred_high AND the resolver
      row it displaces is the poisoned ``rollcall`` (the case this override exists for). A high
      vote_anchor does NOT override a conflicting ``self_intro`` / ``presenter_intro`` (a spoken
      self-declaration outranks an inferred alignment) and a medium vote_anchor overrides nothing —
      in both, the resolver row stays. (While ``_PUBLISHABLE_TIER_ENABLED`` is off the aligner
      emits only medium, so the high branch is dormant until the Phase C acoustic gate lands.)
    * **New cluster** (no resolver claim): the vote_anchor proposal is added.

    Doing this in memory before a single :func:`~actalux.identity.resolve.persist_identities` call
    (both families in ``RESOLVER_BASES``) makes the outcome deterministic and independent of DB
    write order — unlike routing vote_anchor through the cross-family tie-break, which keeps
    whichever row was written first on an equal-tier tie (so an old rollcall row would always win).
    A final :func:`_demote_contested` pass sends any residual member-on-two-clusters contest to
    review, preserving the never-publish-one-member-twice invariant.
    """
    by_cluster = {p.cluster_label: p for p in resolver_proposals}
    for vp in vote_proposals:
        existing = by_cluster.get(vp.cluster_label)
        if existing is None:
            by_cluster[vp.cluster_label] = vp
        elif existing.subject_id == vp.subject_id:
            higher = (
                vp.confidence
                if _CONFIDENCE_RANK[vp.confidence] >= _CONFIDENCE_RANK[existing.confidence]
                else existing.confidence
            )
            by_cluster[vp.cluster_label] = replace(vp, confidence=higher)
        elif vp.confidence == "inferred_high" and existing.basis == "rollcall":
            by_cluster[vp.cluster_label] = vp  # high vote_anchor retires ONLY a poisoned rollcall
        # else: keep the resolver row — a medium vote_anchor overrides nothing, and a high one does
        # not displace a self_intro / presenter_intro (a spoken self-declaration outranks alignment)
    merged = _demote_contested(list(by_cluster.values()))
    return sorted(merged, key=lambda p: p.cluster_label)


# --- DB-facing correlation --------------------------------------------------------------


def vote_reference_for_document(
    client: Client, document_id: int, entity_id: int
) -> VoteReference | None:
    """The correlated vote record's member set for a transcript, or ``None`` if none correlates.

    Correlation is by ``(entity_id, meeting_date)``: the transcript's meeting date is read from its
    document row, then the ``member_vote_records`` view (already publishable- and membership-gated)
    yields every member who cast a per-member vote in that body on that date. The union of those
    subject ids is the authoritative, ASR-immune member set. A transcript with no meeting date, or
    a meeting with no parsed per-member votes, has no reference (the aligner falls back to the
    audible called-surname count).
    """
    doc = (
        client.table("documents")
        .select("meeting_date")
        .eq("id", document_id)
        .limit(1)
        .execute()
        .data
    )
    if not doc or not doc[0].get("meeting_date"):
        return None
    meeting_date = doc[0]["meeting_date"]
    rows = fetch_all_rows(
        lambda: (
            client.table("member_vote_records")
            .select("subject_id")
            .eq("entity_id", entity_id)
            .eq("meeting_date", meeting_date)
        ),
        # The view has no ``id``; ``edge_id`` is its stable unique key for paging (see
        # graph/store.member_records) — the default ``order='id'`` would 42703 here.
        order="edge_id",
    )
    member_ids = frozenset(r["subject_id"] for r in rows if r.get("subject_id") is not None)
    return VoteReference(member_ids=member_ids) if member_ids else None
