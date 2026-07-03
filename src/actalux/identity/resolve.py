"""Deterministic speaker-identity resolution: anonymous cluster -> a known official.

Diarization gives anonymous clusters (``SPEAKER_00``); this maps a cluster to a
knowledge-graph ``subject`` using deterministic, high-precision signals only (no LLM —
the locked decision). Cardinals: never invent a name (a proposal's subject is always a
roster member), precision over recall (an unresolved or ambiguous cluster stays
anonymous), and only a clean, unambiguous anchor reaches the public display bar.

Three signals, all grounded in spoken-name anchors and all with a valid schema
``basis``:

* **roll call** — a turn that names exactly one member, immediately followed by a short
  "here / present" response from a *different* cluster, anchors that responding cluster
  to the named member (the clerk-reads-name -> member-answers pattern).
* **self-introduction** — "I'm <member>" / "this is <member>" / "my name is <member>"
  within a cluster anchors that cluster to the named member.
* **presenter introduction** — a turn that introduces exactly one member in handoff
  position (near its end), immediately followed by a *different* cluster taking the floor
  with sustained speech, anchors that speaking cluster to the introduced member. This is
  the only signal that reaches an official who speaks at length but is never in a roll
  call (an appointed director, the counsel, a staff presenter with a seat).

Confidence is assigned conservatively:

* a clean **1:1** roll-call / self-intro map (one cluster <-> one member, the member
  claimed by no other cluster) -> ``inferred_high`` (publishable),
* a clean **presenter-introduction** map -> ``inferred_medium`` — deliberately held BELOW
  the public-display gate. A handoff is inferred from free ASR text, so unlike a
  name+response roll call or a self-declaration it can misread ("...thank you to Jane
  Harris"); keeping it non-public means a misread never mislabels a speaker on the page.
  It still reaches the review queue and is eligible to seed the voiceprint gallery, whose
  own gates (label/purity + nested-LOMO calibration) contain any error,
* a **contested** member (claimed by more than one cluster) -> ``inferred_low`` for both
  (the review queue disambiguates),
* an **ambiguous** cluster (more than one candidate member) -> no proposal at all
  (stays anonymous; the review queue surfaces it as an unresolved cluster).

The "vote roll-call" and voiceprint anchors in the design need data this pass doesn't
have (vote timestamps, a voice gallery) and are left for later phases.
"""

from __future__ import annotations

import re
from collections import defaultdict
from dataclasses import dataclass
from typing import Any

from supabase import Client

from actalux.db import fetch_all_rows, get_diarization_turns, get_name_corrections
from actalux.glossary.canonicalize import CorrectionRule, build_rules, canonicalize_text
from actalux.graph.store import place_lexicon

# A roll-call response is one of a small set of exact affirmative forms. Anything else
# ("she is here", "I am here to present the budget", "not present") is ordinary speech,
# not a response, and is matched on the punctuation-folded normalized text.
_AFFIRMATIVE_RESPONSES = frozenset(
    {
        "here",
        "present",
        "present and voting",
        "im here",
        "i am here",
        "yes",
        "yes here",
        "yes present",
        "here present",
    }
)
# A self-introduction OPENS the turn: an optional short greeting, a first-person lead-in,
# then the member's name immediately. Anchored at the start so a mid-sentence "this is
# Bob Stevens" (third-person) is not mistaken for one; "this is" is excluded entirely as
# too ambiguous. Matched on punctuation-folded text, so "I'm" -> "im".
_GREETING_RE = r"(?:hi|hello|hey|good morning|good afternoon|good evening|thanks|thank you)"
_INTRO_RE = re.compile(rf"^(?:{_GREETING_RE}\s+)?(?:i am|im|my name is)\s+")
# Leading honorifics/titles stripped before checking a roll-call turn IS a member name.
_HONORIFICS = frozenset(
    """mr mrs ms mx dr mayor alderman alderwoman councilmember councilman councilwoman
    council member commissioner president vice chair chairman chairwoman""".split()
)
# Words that may legitimately FOLLOW a name in a self-intro (connectors + roles). Any
# OTHER token right after a matched name means the name is being extended into a
# different, longer name (e.g. "Jane Harris Smith"), so the match is not trusted.
_TAIL_WORDS = frozenset(
    """the a an and of for with from to on at in representing here im i am my our we
    speaking calling presenting mayor alderman alderwoman councilmember councilman
    councilwoman council member members commissioner president vice chair chairman
    chairwoman director treasurer secretary resident applicant attorney clerk
    superintendent principal ward district board""".split()
)
# A surname is used as a match key only when long enough to be low-collision; shorter
# surnames are still matched via the full name. A surname-only match (vs a full
# name / curated alias) is never trusted enough to publish — it stays review-only.
_MIN_SURNAME = 4

# --- presenter-introduction anchor: a member is introduced and handed the floor -------
# A handoff both SHOWS a cue and NAMES the incoming speaker at the END of the introducer's
# turn ("...I'd like to introduce Jane Harris"). The name is matched only as a full-name
# suffix, AND a handoff cue must sit in the connector run immediately before it (below) —
# position alone would let a gratitude closing ("...thank you to Jane Harris") anchor the
# next speaker. So a name dropped mid-turn, a trailing thank-you, an unrelated earlier cue,
# or a longer name that contains a member's ("Jane Harris Smith", "Mary Jane Harris") fail.
# The cues are generic English introduction/recognition verbs whose object is a PERSON
# (introduce, recognize, welcome, invite, yield) — NONE is jurisdiction-specific, so the
# signal carries no town's wording. Person-directed verbs are chosen deliberately: they
# rarely take a thing as object, so "<verb> Jane Harris" really is a handoff. The
# transfer/particle and noun-colliding candidates are all excluded because they mis-read
# object transfers and noun uses as handoffs — "over" ("sent the packet over to Jane"),
# bare "turn"/"hand" ("your turn", "a hand for"), "call" (phone/roll call), "floor" ("the
# floor"). Precision over recall: a handoff phrased without one of these is missed, never
# mis-anchored (so "turn it over to <name>", which collides with transfers, is not covered).
_HANDOFF_CUES = frozenset(
    """introduce introduces introducing introduced recognize recognizes recognized
    welcome welcomes welcomed welcoming invite invites invited inviting yield yields
    yielded""".split()
)
# A negation directly governing the cue flips it ("do not recognize Jane Harris"), so a cue
# whose run is broken on the left by one of these is not a handoff. (Apostrophes are folded
# by _norm_text, so "don't" -> "dont".) Non-adjacent negation is left to the review queue —
# presenter_intro is non-public and gate-contained, so this need only catch the clean cases.
_NEGATIONS = frozenset("not never no cannot cant dont doesnt didnt wont nor".split())
# Turns after the handoff over which the incoming cluster's speech is summed — diarization
# fragments a monologue into several turns and brief interjections can interleave.
_SUSTAINED_WINDOW_TURNS = 6
# Words the incoming cluster must speak within that window to count as taking the floor to
# present — well above a one- or two-sentence reply, so a brief answer never anchors. Word
# count is a self-contained proxy for sustained speech (no per-turn timing needed here);
# the recalibration harness measures precision/recall to tune it.
_MIN_SUSTAINED_WORDS = 60
# Which anchor wins the recorded ``basis`` when one cluster is reached by more than one
# signal of the SAME tier (below): a name+response roll call and a self-introduction both
# outrank being introduced by someone else.
_BASIS_RANK = {"rollcall": 3, "self_intro": 2, "presenter_intro": 1}
# The publishable tier a single anchor supports, highest first. A bare surname never
# publishes (review only), whatever its basis; a presenter introduction is held below the
# public bar (non-public but enrollable); only a strong roll call / self-introduction
# publishes. A cluster's confidence is the MAX tier of its evidence — never borrowed across
# pieces, so a surname roll call does not become public just because a presenter
# introduction for the same person happens to be strong.
_TIER_HIGH, _TIER_MEDIUM, _TIER_LOW = 3, 2, 1
_TIER_CONFIDENCE = {
    _TIER_HIGH: "inferred_high",
    _TIER_MEDIUM: "inferred_medium",
    _TIER_LOW: "inferred_low",
}


@dataclass(frozen=True)
class RosterMember:
    """A body member the resolver may attribute a cluster to."""

    subject_id: int
    slug: str
    canonical_name: str
    aliases: frozenset[str]  # normalized alias keys


@dataclass(frozen=True)
class ResolverTurn:
    """One diarization turn reduced to what resolution needs: who (cluster) said what."""

    cluster_label: str
    text: str


@dataclass(frozen=True)
class IdentityProposal:
    """A proposed ``cluster -> subject`` mapping with its confidence + basis."""

    cluster_label: str
    subject_id: int
    slug: str  # for review/logging; not a DB column
    confidence: str  # inferred_high | inferred_medium | inferred_low
    basis: str  # rollcall | self_intro | presenter_intro

    def to_row(self, document_id: int) -> dict[str, Any]:
        """Row for the ``speaker_identities`` table."""
        return {
            "document_id": document_id,
            "cluster_label": self.cluster_label,
            "subject_id": self.subject_id,
            "confidence": self.confidence,
            "basis": self.basis,
        }


def _norm_text(text: str) -> str:
    """Lowercase, fold apostrophes, drop other punctuation, collapse whitespace.

    Apostrophes are removed (not split) so "I'm" -> "im" and "O'Brien" -> "obrien" —
    matching the glossary's normalization, so a member's name and the spoken form align.
    """
    folded = text.lower().replace("'", "").replace("’", "")
    return re.sub(r"\s+", " ", re.sub(r"[^a-z0-9 ]", " ", folded)).strip()


def _name_index(members: list[RosterMember]) -> tuple[dict[str, int], dict[str, int]]:
    """Two phrase -> subject_id indexes: ``strong`` (full names + aliases) and ``surname``.

    A phrase owned by more than one member is ambiguous and dropped from both, so it can
    never produce a false attribution. The split lets the caller trust a full-name /
    curated-alias hit (``strong``) but treat a bare-surname hit as review-only — a roster
    surname can collide with a *non-roster* person of the same surname, which the
    shared-phrase drop can't catch.
    """
    strong_owners: dict[str, set[int]] = defaultdict(set)
    surname_owners: dict[str, set[int]] = defaultdict(set)

    def _place(key: str, sid: int) -> None:
        # A multi-token key is a full name (trustworthy); a single token — whether a
        # bare surname or a one-word alias — carries the surname collision risk, so it
        # is review-only.
        if " " in key:
            strong_owners[key].add(sid)
        elif len(key) >= _MIN_SURNAME:
            surname_owners[key].add(sid)

    for m in members:
        for alias in m.aliases:
            _place(_norm_text(alias), m.subject_id)
        full = _norm_text(m.canonical_name)
        if full:
            _place(full, m.subject_id)
            _place(full.split()[-1], m.subject_id)  # bare surname -> surname index
    strong = {k: next(iter(v)) for k, v in strong_owners.items() if len(v) == 1}
    surname = {k: next(iter(v)) for k, v in surname_owners.items() if len(v) == 1}
    return strong, surname


def _strip_honorifics(tokens: list[str]) -> list[str]:
    """Drop leading honorifics/titles ("council member harris" -> "harris")."""
    i = 0
    while i < len(tokens) and tokens[i] in _HONORIFICS:
        i += 1
    return tokens[i:]


def _name_only_match(
    text: str, strong: dict[str, int], surname: dict[str, int]
) -> tuple[int, str] | None:
    """A turn that IS a member's name (after honorifics) -> (subject_id, strength).

    EXACT match, not substring: the cleaned tokens must equal a full name (``strong``)
    or be a single bare surname (``surname``). Arbitrary speech that merely contains a
    name ("I spoke with Jane Harris") and a longer name ("Jane Harris Smith") both fail,
    so a roll-call anchor is only taken from a turn that reads exactly one member's name.
    """
    tokens = _strip_honorifics(_norm_text(text).split())
    if not tokens:
        return None
    phrase = " ".join(tokens)
    if phrase in strong:
        return strong[phrase], "strong"
    if len(tokens) == 1 and tokens[0] in surname:
        return surname[tokens[0]], "surname"
    return None


def _leading_member(
    remainder: str, strong: dict[str, int], surname: dict[str, int]
) -> tuple[int, str] | None:
    """The member whose COMPLETE name ``remainder`` starts with (longest strong first).

    The token right after the matched name must be end-of-turn or a connector/role
    (``_TAIL_WORDS``); any other token means the name is extended into a different,
    longer name ("Jane Harris Smith"), so that match is rejected.
    """

    def _complete(phrase: str) -> bool:
        m = re.match(rf"{re.escape(phrase)}(?:$|\s+(\S+))", remainder)
        return bool(m) and (m.group(1) is None or m.group(1) in _TAIL_WORDS)

    for phrase, sid in sorted(strong.items(), key=lambda kv: -len(kv[0])):
        if _complete(phrase):
            return sid, "strong"
    for phrase, sid in surname.items():
        if _complete(phrase):
            return sid, "surname"
    return None


def _rollcall_hits(
    turns: list[ResolverTurn], strong: dict[str, int], surname: dict[str, int]
) -> list[tuple[str, int, str]]:
    """(cluster, subject_id, strength) anchored by the roll-call response pattern.

    A name-only turn (the clerk reading one member) immediately followed by an exact
    affirmative response from a different cluster anchors that cluster to the member.
    """
    out: list[tuple[str, int, str]] = []
    for prev, nxt in zip(turns, turns[1:]):
        hit = _name_only_match(prev.text, strong, surname)
        if hit is None or nxt.cluster_label == prev.cluster_label:
            continue
        if _norm_text(nxt.text) not in _AFFIRMATIVE_RESPONSES:
            continue
        out.append((nxt.cluster_label, hit[0], hit[1]))
    return out


def _selfintro_hits(
    turns: list[ResolverTurn], strong: dict[str, int], surname: dict[str, int]
) -> list[tuple[str, int, str]]:
    """(cluster, subject_id, strength) anchored by a self-introduction opening the turn."""
    out: list[tuple[str, int, str]] = []
    for turn in turns:
        norm = _norm_text(turn.text)
        match = _INTRO_RE.match(norm)
        if not match:
            continue
        hit = _leading_member(norm[match.end() :], strong, surname)
        if hit:
            out.append((turn.cluster_label, hit[0], hit[1]))
    return out


def _distinct_members_named(
    tokens: list[str], strong: dict[str, int], surname: dict[str, int]
) -> set[int]:
    """Distinct roster members named anywhere in ``tokens`` (an ambiguity probe).

    Deliberately LIBERAL — a longest-match left-to-right scan over full names / aliases
    (``strong``, always multi-token) plus bare surnames. Used only to *reject* a handoff
    when a second member is named, so over-matching can only make the caller skip (safe).
    The anchor itself is taken by ``_trailing_member``, which is strict about boundaries.
    """
    max_len = max((k.count(" ") + 1 for k in strong), default=0)
    named: set[int] = set()
    i, n = 0, len(tokens)
    while i < n:
        hit_len = 0
        for length in range(min(max_len, n - i), 1, -1):  # strong keys are multi-token
            sid = strong.get(" ".join(tokens[i : i + length]))
            if sid is not None:
                named.add(sid)
                hit_len = length
                break
        if hit_len:
            i += hit_len
            continue
        if len(tokens[i]) >= _MIN_SURNAME and tokens[i] in surname:
            named.add(surname[tokens[i]])
        i += 1
    return named


def _trailing_member(tokens: list[str], strong: dict[str, int]) -> int | None:
    """The member introduced at the handoff position of ``tokens``, or ``None``.

    Requires a full-name / alias suffix of the turn (after any trailing role/connector run,
    "...Jane Harris, councilmember") whose handoff cue is bound LOCALLY to the name: reading
    left from the name, the contiguous run of connectors/honorifics must contain a cue
    (``_HANDOFF_CUES``) before any other word breaks it. That locality is what separates a
    real handoff ("turn it over to Jane Harris") from a gratitude closing that merely ends
    with a name ("...thank you to Jane Harris"), a longer name that contains a member's
    ("Mary Jane Harris" — "mary" breaks the run with no cue), and an unrelated earlier cue
    ("Welcome everyone. ...thank you to Jane Harris"). Only a full name / curated alias
    anchors — a bare surname is too collision-prone to launch enrollment on.
    """
    end = len(tokens)
    while end > 0 and tokens[end - 1] in _TAIL_WORDS:
        end -= 1  # strip a trailing role/connector run so the name can still be the suffix
    max_len = max((k.count(" ") + 1 for k in strong), default=0)
    for length in range(min(max_len, end), 1, -1):  # longest suffix name first
        start = end - length
        sid = strong.get(" ".join(tokens[start:end]))
        if sid is None:
            continue
        # Walk the connector/honorific/cue run immediately left of the name; a cue must
        # appear before any other word (a name part like "mary", a gratitude "you") ends it.
        j, saw_cue = start, False
        while j > 0 and (
            tokens[j - 1] in _TAIL_WORDS
            or tokens[j - 1] in _HONORIFICS
            or tokens[j - 1] in _HANDOFF_CUES
        ):
            saw_cue = saw_cue or tokens[j - 1] in _HANDOFF_CUES
            j -= 1
        if j > 0 and tokens[j - 1] in _NEGATIONS:
            return None  # the run's cue is negated ("do not recognize Jane Harris")
        return sid if saw_cue else None
    return None


def _presenter_intro_hits(
    turns: list[ResolverTurn], strong: dict[str, int], surname: dict[str, int]
) -> list[tuple[str, int, str]]:
    """(cluster, subject_id, strength) anchored by an introduction/handoff to a presenter.

    Cluster P names exactly one roster member in handoff position (the full-name suffix of
    P's turn — see ``_trailing_member``), a DIFFERENT cluster Q immediately follows, and Q
    holds the floor with sustained speech in the window after the handoff. That anchors Q
    to the introduced member. Precision is deliberate over recall — the guards below prefer
    missing a handoff to attributing the wrong voice:

    * a second distinct member named in P's turn -> ambiguous -> skip;
    * P (or nothing) continuing rather than a new cluster -> no handoff happened -> skip;
    * the introduced name is not a clean full-name suffix -> skip;
    * any other cluster matching or out-speaking Q in the window -> Q isn't clearly the
      presenter -> skip.
    """
    out: list[tuple[str, int, str]] = []
    for i, prev in enumerate(turns):
        if i + 1 >= len(turns) or turns[i + 1].cluster_label == prev.cluster_label:
            continue  # same cluster continues (or the turn is last) -> no handoff
        tokens = _norm_text(prev.text).split()
        named = _distinct_members_named(tokens, strong, surname)
        if len(named) != 1:
            continue  # zero named, or two+ members named (ambiguous) -> not a clean handoff
        sid = _trailing_member(tokens, strong)
        if sid is None or sid not in named:
            continue  # the sole named member is not the clean handoff suffix
        q = turns[i + 1].cluster_label
        words: dict[str, int] = defaultdict(int)
        for turn in turns[i + 1 : i + 1 + _SUSTAINED_WINDOW_TURNS]:
            if turn.cluster_label != prev.cluster_label:  # the introducer's own words don't count
                words[turn.cluster_label] += len(_norm_text(turn.text).split())
        # Q must clear the sustained floor AND strictly out-speak every other non-introducer
        # cluster in the window — a brief reply, a tie, or a different presenter never anchors.
        other_max = max((w for c, w in words.items() if c != q), default=0)
        if words[q] >= _MIN_SUSTAINED_WORDS and words[q] > other_max:
            out.append((q, sid, "strong"))
    return out


def _anchor_tier(basis: str, strength: str) -> int:
    """The publishable tier one anchor supports (see ``_TIER_*``)."""
    if strength != "strong":
        return _TIER_LOW  # a bare-surname match is review-only whatever the basis
    return _TIER_MEDIUM if basis == "presenter_intro" else _TIER_HIGH


def resolve_identities(
    turns: list[ResolverTurn], members: list[RosterMember]
) -> list[IdentityProposal]:
    """Deterministic cluster -> subject proposals from name-anchor signals.

    A cluster's confidence is the highest tier its evidence supports (``_anchor_tier``);
    among equal-tier evidence the higher-ranked basis (``_BASIS_RANK``) is recorded. See the
    module docstring for the tier rules; nothing is invented and ambiguous clusters get no
    proposal.
    """
    if not turns or not members:
        return []
    strong, surname = _name_index(members)
    by_subject = {m.subject_id: m for m in members}

    # cluster -> subject_id -> {"tier", "basis"} of the single best supporting anchor.
    # The evidence is selected as a unit (tier first, then basis rank), so confidence and
    # basis always come from the SAME anchor — strength is never borrowed across pieces.
    acc: dict[str, dict[int, dict[str, int | str]]] = defaultdict(dict)

    def _add(cluster: str, sid: int, basis: str, strength: str) -> None:
        tier = _anchor_tier(basis, strength)
        cur = acc[cluster].get(sid)
        cand = (tier, _BASIS_RANK[basis])
        if cur is None or cand > (cur["tier"], _BASIS_RANK[cur["basis"]]):
            acc[cluster][sid] = {"tier": tier, "basis": basis}

    for cluster, sid, strength in _rollcall_hits(turns, strong, surname):
        _add(cluster, sid, "rollcall", strength)
    for cluster, sid, strength in _selfintro_hits(turns, strong, surname):
        _add(cluster, sid, "self_intro", strength)
    for cluster, sid, strength in _presenter_intro_hits(turns, strong, surname):
        _add(cluster, sid, "presenter_intro", strength)

    subject_clusters: dict[int, set[str]] = defaultdict(set)
    for cluster, sdict in acc.items():
        for sid in sdict:
            subject_clusters[sid].add(cluster)

    proposals: list[IdentityProposal] = []
    for cluster, sdict in acc.items():
        if len(sdict) != 1:
            continue  # ambiguous cluster -> stays anonymous (review queue surfaces it)
        sid, info = next(iter(sdict.items()))
        # A member claimed by more than one cluster is contested -> review, whatever the
        # tier (the review queue disambiguates). Otherwise confidence is the evidence tier.
        tier = _TIER_LOW if len(subject_clusters[sid]) > 1 else info["tier"]
        proposals.append(
            IdentityProposal(
                cluster, sid, by_subject[sid].slug, _TIER_CONFIDENCE[tier], info["basis"]
            )
        )
    return sorted(proposals, key=lambda p: p.cluster_label)


# --- DB-facing orchestration ------------------------------------------------------


def members_for_entity(client: Client, entity_id: int) -> list[RosterMember]:
    """Publishable members of one body, with their normalized aliases (the candidate set)."""
    mems = (
        client.table("memberships").select("subject_id").eq("entity_id", entity_id).execute().data
    )
    subject_ids = {m["subject_id"] for m in mems}
    if not subject_ids:
        return []
    subjects = (
        client.table("subjects")
        .select("id,slug,canonical_name")
        .in_("id", list(subject_ids))
        .eq("publishable", True)
        .execute()
        .data
    )
    aliases = fetch_all_rows(
        lambda: client.table("subject_aliases").select("subject_id,normalized_alias")
    )
    by_alias: dict[int, set[str]] = defaultdict(set)
    for a in aliases:
        if a["subject_id"] in subject_ids:
            by_alias[a["subject_id"]].add(a["normalized_alias"])
    return [
        RosterMember(
            subject_id=s["id"],
            slug=s["slug"],
            canonical_name=s["canonical_name"],
            aliases=frozenset(by_alias.get(s["id"], set())),
        )
        for s in subjects
    ]


def _rows_to_turns(
    rows: list[dict[str, Any]], rules: list[CorrectionRule] | None = None
) -> list[ResolverTurn]:
    """Diarization-turn rows -> ``ResolverTurn``\\ s, optionally name-canonicalized.

    Resolution matches the roster's canonical names, but the stored turn words are raw
    ASR (so the clerk's "York" never matches roster "Yorg"). Applying the place's
    canonical name-corrections to each turn's text first lets a known mangling resolve.
    """
    turns: list[ResolverTurn] = []
    for row in rows:
        text = " ".join(w.get("word", "") for w in (row.get("words") or []))
        if rules:
            text = canonicalize_text(text, rules)[0]
        turns.append(ResolverTurn(cluster_label=row["cluster_label"], text=text))
    return turns


def turns_for_document(
    client: Client, document_id: int, rules: list[CorrectionRule] | None = None
) -> list[ResolverTurn]:
    """The document's diarization turns reduced to ``(cluster, text)`` for resolution."""
    return _rows_to_turns(get_diarization_turns(client, document_id), rules)


def persist_identities(
    service_client: Client, document_id: int, proposals: list[IdentityProposal]
) -> int:
    """Reconcile a document's resolved identities via the service client; return rows written.

    ``proposals`` must be the COMPLETE current proposal set for the document. This:
      * never touches a human-decided row — ``confirmed`` (manual gold) OR ``rejected`` (a
        denied hypothesis): a locked cluster is neither retracted nor re-proposed, so a
        confirmed name is preserved and a denied name is never re-attached on re-pass,
      * retracts stale auto rows — a previously-published cluster the resolver no longer
        proposes (e.g. after a roster/alias change) is deleted, so a wrong public
        identity can't linger,
      * upserts the current proposals on ``(document_id, cluster_label)``.

    Caveat: not atomic — a human could decide a row between the read and the upsert and
    have it overwritten. The durable fix is the DB trigger (migrate_035/043) that skips any
    update moving a confirmed/rejected row off its tier; in practice the auto pass is a
    non-concurrent batch. The trigger guards the UPDATE (upsert) path; this read-then-write
    guard also covers the DELETE (retract) path, which the BEFORE UPDATE trigger cannot.
    """
    existing = (
        service_client.table("speaker_identities")
        .select("cluster_label,confidence")
        .eq("document_id", document_id)
        .execute()
        .data
        or []
    )
    # A human decision locks a cluster in both directions: 'confirmed' keeps its name,
    # 'rejected' keeps a denied name from ever being re-proposed (Option B — the row carries
    # no citizen identity, only the official it was denied under).
    locked = {
        r["cluster_label"] for r in existing if r.get("confidence") in ("confirmed", "rejected")
    }
    proposed = {p.cluster_label for p in proposals}
    table = service_client.table("speaker_identities")

    for row in existing:
        cluster = row["cluster_label"]
        if cluster not in proposed and cluster not in locked:
            table.delete().eq("document_id", document_id).eq("cluster_label", cluster).execute()

    rows = [p.to_row(document_id) for p in proposals if p.cluster_label not in locked]
    if rows:
        table.upsert(rows, on_conflict="document_id,cluster_label").execute()
    return len(rows)


def _place_canonical_rules(client: Client, entity_id: int) -> list[CorrectionRule]:
    """The place's canonical name-correction rules for the body's entity, or []."""
    row = client.table("entities").select("place_id").eq("id", entity_id).limit(1).execute().data
    if not row:
        return []
    place_id = row[0]["place_id"]
    return build_rules(get_name_corrections(client, place_id), place_lexicon(client, place_id))


def resolve_document(
    client: Client, service_client: Client, document_id: int, entity_id: int
) -> list[IdentityProposal]:
    """Resolve + persist identities for one transcript; return the proposals.

    Turn text is name-canonicalized first (so a known mangling like "York" resolves to
    roster "Jeffery Yorg") before matching against the body roster.
    """
    members = members_for_entity(client, entity_id)
    rules = _place_canonical_rules(client, entity_id)
    turns = turns_for_document(client, document_id, rules)
    proposals = resolve_identities(turns, members)
    persist_identities(service_client, document_id, proposals)
    return proposals
