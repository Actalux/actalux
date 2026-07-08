#!/usr/bin/env python3
"""Operator tool: a BLIND speaker-ID sheet for one public body.

The voiceprint gates trust human-``confirmed`` speaker labels as a core; the fastest way
to seed real recall is a short, honest confirmation pass. This builds a self-contained HTML
sheet the operator opens locally: for each of ~20-30 clips they listen to a 12-second cued
YouTube embed and pick who is speaking from the body's roster dropdown. It is BLIND — the
machine's own hypothesis for a clip is NEVER written into the HTML (no name, no id, no data
attribute, no comment), so the operator's ear is the only input and a wrong machine label
surfaces as a disagreement instead of being confirmed by suggestion. The hypothesis lives
only in the sibling manifest JSON, which ``--apply`` joins against the pasted answers.

Two clip families (the clever part is the selection):

  * HYPOTHESIS clips — for each official the resolver has proposed (a non-confirmed,
    non-rejected identity row), up to 3 clusters from 3 DISTINCT meetings (>=2 required), so
    a confirmation lands cross-meeting coverage (what the leave-one-meeting-out gate needs).
  * UNKNOWN clips — the largest clusters that carry NO identity row at all, up to a handful,
    each from a different meeting: net-new voices the operator can name (or mark a citizen).

Answers come back as a compact token string (``clip07=jane-harris``); ``--apply`` writes them
through the SAME guarded path scripts/confirm_speaker.py uses: a person answer -> a
``confirmed`` row (basis kept for an existing hypothesis row, ``basis='manual'`` for a de-novo
naming — the human basis, matching confirm_speaker's voiceprint->manual rewrite and
enrollment.py's "a human-confirmed row may carry no basis; 'manual' is the honest label"); a
"citizen" answer -> the ``rejected`` denial path (Option B); "not sure" / "someone else" are
skipped. Writes are idempotent and every decision is logged.

Nothing here is Clayton-specific: the place/body, roster, and documents are all resolved from
the request, and the same command builds a sheet for any jurisdiction.

Usage (prefix every invocation with ``doppler run --project mac --config dev --``):
    uv run python scripts/build_id_sheet.py --state mo --place clayton --body schools
    uv run python scripts/build_id_sheet.py --apply answers.txt \\
        --manifest data/id_sheets/mo_clayton_schools_<ts>.manifest.json
"""

from __future__ import annotations

import argparse
import hashlib
import html
import json
import logging
import os
import random
import re
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from supabase import Client

from actalux.config import load_config
from actalux.db import (
    fetch_all_rows,
    get_client,
    get_diarization_turns,
    get_entity_by_path,
)
from actalux.diarization.enrollment import cluster_spans, span_seconds, superseded_doc_ids
from actalux.errors import ActaluxError

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("build_id_sheet")

# Selection knobs. A hypothesis subject must appear across at least this many distinct
# meetings to be worth confirming (cross-meeting coverage is what the leave-one-meeting-out
# gate needs); we take at most one representative cluster per meeting up to the cap.
HYP_MIN_MEETINGS = 2
HYP_MAX_CLIPS_PER_SUBJECT = 3
# A cluster with at least this much speech is a "prefer" pick — enough audio to recognize a
# voice by. Below it a cluster can still be chosen (it is a preference, not a floor).
HYP_PREFER_MIN_SECONDS = 60.0
MAX_UNKNOWN_CLIPS = 8
MAX_TOTAL_CLIPS = 30
CLIP_WINDOW_SECONDS = 12  # the YouTube embed plays [start, start + this]

# Non-locked (still-open) speaker-identity tiers a hypothesis clip may be drawn from.
INFERRED_TIERS = frozenset({"inferred_low", "inferred_medium", "inferred_high"})
# The de-novo human basis for a confirmed row with no prior name-anchor evidence. Matches
# confirm_speaker.confirm_payload (voiceprint -> 'manual') and enrollment.select_enrollable
# ("a human-confirmed row may carry no basis; 'manual' is the honest label"); it is in the
# speaker_identities basis CHECK (migrate_044) and is enrollable (subject_voiceprints CHECK).
HUMAN_BASIS = "manual"

# The fixed non-roster dropdown choices, shared by every clip (value -> label). Kept identical
# across clips so the dropdown itself leaks nothing about which clip is whom.
FIXED_CHOICES: tuple[tuple[str, str], ...] = (
    ("other", "Someone else (not on this list)"),
    ("citizen", "Citizen / member of the public"),
    ("unsure", "Not sure"),
)
_FIXED_VALUES = frozenset(v for v, _ in FIXED_CHOICES)


@dataclass(frozen=True)
class RosterEntry:
    """One body official the operator can pick (dropdown row + write target)."""

    subject_id: int
    slug: str  # body-scoped, unique per place — the answer token and the join key
    canonical_name: str
    person_id: int | None
    publishable: bool


@dataclass(frozen=True)
class Clip:
    """One selected 12-second clip. ``hypothesis_subject_id`` is manifest-only (never HTML)."""

    document_id: int
    cluster_label: str
    video_id: str
    start_seconds: int
    end_seconds: int
    kind: str  # "hypothesis" | "unknown"
    hypothesis_subject_id: int | None  # the resolver's guess (unknown clips: None)


@dataclass(frozen=True)
class _ClusterCand:
    """A candidate cluster during selection (before the window is cut)."""

    document_id: int
    cluster_label: str
    video_id: str
    seconds: float
    subject_id: int | None  # the inferred subject for a hypothesis cand; None for unknown


# --- pure helpers (unit-tested) ---------------------------------------------------


def seed_from_path(entity_path: str) -> int:
    """A stable 64-bit seed from the entity path (NOT Python's salted ``hash``).

    Deterministic across processes so the same body always shuffles to the same sheet order
    for a given clip set — reproducibility the built-in ``hash`` cannot give.
    """
    return int.from_bytes(hashlib.sha256(entity_path.encode("utf-8")).digest()[:8], "big")


def longest_turn_window(
    turns: list[dict[str, Any]], cluster_label: str, *, window: int = CLIP_WINDOW_SECONDS
) -> tuple[int, int] | None:
    """The clip window for a cluster: its LONGEST single turn's start, ``window`` seconds long.

    Integer seconds (the YouTube embed takes integer ``start``/``end``). ``None`` if the cluster
    has no turn. Ties break on the earlier start for determinism.
    """
    spans = [
        (float(t["start_seconds"]), float(t["end_seconds"]))
        for t in turns
        if t["cluster_label"] == cluster_label
    ]
    if not spans:
        return None
    start, _end = max(spans, key=lambda s: (s[1] - s[0], -s[0]))
    s = int(start)
    return s, s + window


def pick_subject_meeting_reps(cands: list[_ClusterCand]) -> list[_ClusterCand]:
    """One representative cluster per distinct meeting for a subject, best meetings first.

    The representative of a meeting is its highest-speech cluster (ties -> lower
    document_id/cluster_label). Meetings are ordered so a ``>= HYP_PREFER_MIN_SECONDS``
    representative comes first, then by speech descending, then video_id for determinism, and
    the top ``HYP_MAX_CLIPS_PER_SUBJECT`` are returned. Returns ``[]`` if the subject spans
    fewer than ``HYP_MIN_MEETINGS`` distinct meetings (too little cross-meeting coverage).
    """

    def _beats(c: _ClusterCand, cur: _ClusterCand) -> bool:
        # higher speech wins; tie -> lower (document_id, cluster_label) for determinism.
        if c.seconds != cur.seconds:
            return c.seconds > cur.seconds
        return (c.document_id, c.cluster_label) < (cur.document_id, cur.cluster_label)

    by_meeting: dict[str, _ClusterCand] = {}
    for c in cands:
        cur = by_meeting.get(c.video_id)
        if cur is None or _beats(c, cur):
            by_meeting[c.video_id] = c
    if len(by_meeting) < HYP_MIN_MEETINGS:
        return []
    reps = sorted(
        by_meeting.values(),
        key=lambda c: (-(c.seconds >= HYP_PREFER_MIN_SECONDS), -c.seconds, c.video_id),
    )
    return reps[:HYP_MAX_CLIPS_PER_SUBJECT]


def pick_unknown_reps(
    cands: list[_ClusterCand], *, limit: int = MAX_UNKNOWN_CLIPS
) -> list[_ClusterCand]:
    """The largest unknown clusters, each from a different meeting where possible, up to ``limit``.

    Speech descending; a meeting already represented is skipped on the first pass so the picks
    spread across meetings, then any remainder backfills once every meeting is used.
    """
    ordered = sorted(cands, key=lambda c: (-c.seconds, c.document_id, c.cluster_label))
    picked: list[_ClusterCand] = []
    used_videos: set[str] = set()
    for c in ordered:
        if len(picked) >= limit:
            break
        if c.video_id not in used_videos:
            picked.append(c)
            used_videos.add(c.video_id)
    if len(picked) < limit:  # backfill with extra clusters from already-used meetings
        for c in ordered:
            if len(picked) >= limit:
                break
            if c not in picked:
                picked.append(c)
    return picked


def assemble_clips(
    hypothesis_sets: list[list[Clip]],
    unknown_clips: list[Clip],
    *,
    max_total: int = MAX_TOTAL_CLIPS,
    max_unknown: int = MAX_UNKNOWN_CLIPS,
) -> list[Clip]:
    """Combine the per-subject hypothesis sets and unknown clips under the caps.

    Unknown clips are capped at ``max_unknown``; the rest of ``max_total`` is the hypothesis
    budget. A subject's clip set is admitted whole (never split — a shown official keeps full
    multi-meeting coverage) in the order given, skipping a set that would overflow the budget
    so a later smaller set can still fit. Order here is provisional; the caller shuffles.
    """
    unknown = unknown_clips[:max_unknown]
    hyp_budget = max(0, max_total - len(unknown))
    chosen: list[Clip] = []
    for clip_set in hypothesis_sets:
        if len(chosen) + len(clip_set) <= hyp_budget:
            chosen.extend(clip_set)
    return chosen + unknown


def deterministic_order(clips: list[Clip], seed: int) -> list[Clip]:
    """Shuffle deterministically, then spread so no two adjacent clips share a subject.

    Two hypothesis clips for the SAME official placed next to each other would leak the grouping
    and partly un-blind the sheet, so the order interleaves subjects. A seeded shuffle fixes the
    result for a given clip set; then the standard "reorganize so equal keys are >=2 apart"
    greedy runs: at each step place a clip from the group with the MOST remaining clips whose key
    differs from the one just placed (ties break on earliest appearance in the shuffled order).
    That greedy succeeds whenever it is possible (a subject's count <= ceil(n/2)); with at most
    three clips per official it always is. Unknown clips carry no subject, so each is its own
    singleton group that never conflicts (two unknowns may sit adjacent — they leak nothing).
    """
    pool = list(clips)
    random.Random(seed).shuffle(pool)
    groups: dict[Any, list[Clip]] = {}
    appearance: dict[Any, int] = {}  # first index in the shuffled order (deterministic tie-break)
    for idx, c in enumerate(pool):
        # unknown clips get a unique per-clip key so they never block each other or a subject.
        if c.hypothesis_subject_id is not None:
            key: Any = ("subject", c.hypothesis_subject_id)
        else:
            key = ("unknown", idx)
        groups.setdefault(key, []).append(c)
        appearance.setdefault(key, idx)

    out: list[Clip] = []
    last_key: Any = None
    while any(groups.values()):
        keys = [k for k, v in groups.items() if v and k != last_key]
        if not keys:  # only the last-placed group has clips left -> adjacency is unavoidable
            keys = [k for k, v in groups.items() if v]
        key = min(keys, key=lambda k: (-len(groups[k]), appearance[k]))
        out.append(groups[key].pop(0))
        last_key = key
    return out


def clip_id_for(position: int) -> str:
    """The stable ``clipNN`` id for a 1-based display position."""
    return f"clip{position:02d}"


_TOKEN_RE = re.compile(r"^clip(\d+)=([a-z0-9][a-z0-9-]*)$")


def parse_answers(text: str) -> dict[str, str]:
    """Parse the pasted answer string into ``{clipNN: value}``; raise on junk or a duplicate.

    A token is ``clip<N>=<value>`` where value is a roster slug or a fixed choice
    (``other``/``citizen``/``unsure``). Whitespace-separated (newlines or spaces). A malformed
    token or a repeated clip id raises ``ValueError`` so a fat-fingered paste never writes a
    wrong row silently — the operator fixes it and re-applies. Clip numbers are normalized
    (``clip7`` -> ``clip07``) so numbering style can't cause a silent miss.
    """
    answers: dict[str, str] = {}
    bad: list[str] = []
    for tok in text.split():
        m = _TOKEN_RE.match(tok)
        if not m:
            bad.append(tok)
            continue
        cid = clip_id_for(int(m.group(1)))
        if cid in answers:
            raise ValueError(f"duplicate clip id in answers: {cid}")
        answers[cid] = m.group(2)
    if bad:
        raise ValueError(f"unparseable answer token(s): {', '.join(bad)}")
    return answers


def confirm_update(existing_basis: str | None) -> dict[str, str]:
    """The update payload confirming an EXISTING hypothesis row (mirrors confirm_speaker).

    Sets confidence='confirmed', keeping the row's basis — except a biometric ``voiceprint``
    basis is rewritten to ``manual`` (enrollment never trains the gallery on a voiceprint
    basis, so keeping it would strand the confirmation). Byte-identical to
    confirm_speaker.confirm_payload.
    """
    payload = {"confidence": "confirmed"}
    if existing_basis == "voiceprint":
        payload["basis"] = HUMAN_BASIS
    return payload


def denovo_confirm_row(document_id: int, cluster_label: str, subject_id: int) -> dict[str, Any]:
    """A confirmed ``speaker_identities`` insert for a cluster that had no identity row.

    basis=``manual`` is the honest human basis (see ``HUMAN_BASIS``); confidence='confirmed'
    with a non-null subject satisfies the "a displayable row must name a subject" CHECK.
    """
    return {
        "document_id": document_id,
        "cluster_label": cluster_label,
        "subject_id": subject_id,
        "confidence": "confirmed",
        "basis": HUMAN_BASIS,
    }


def render_html(clips: list[Clip], roster: list[RosterEntry], *, title: str, subtitle: str) -> str:
    """The self-contained blind sheet. Names/slugs appear ONLY inside the shared dropdown.

    Every clip gets the identical roster ``<select>`` (so the dropdown reveals nothing), a
    YouTube embed cued to the clip, and its ``clipNN`` id. The footer's textarea live-builds
    the answer string. No hypothesis is emitted anywhere — not as text, attribute, or comment.
    """
    options = "".join(
        f'<option value="{html.escape(r.slug)}">{html.escape(r.canonical_name)}</option>'
        for r in roster
    )
    options += "".join(
        f'<option value="{html.escape(v)}">{html.escape(label)}</option>'
        for v, label in FIXED_CHOICES
    )

    cards: list[str] = []
    for i, c in enumerate(clips, 1):
        cid = clip_id_for(i)
        # Embeds are unusable here: the source channels disable embedding (YouTube error 153),
        # and the error screen's "Watch on YouTube" link drops the timestamp. A cued watch
        # link is the reliable path — it opens YouTube at the clip's exact second.
        watch = f"https://www.youtube.com/watch?v={html.escape(c.video_id)}&t={c.start_seconds}s"
        m, s = divmod(c.start_seconds, 60)
        h, m = divmod(m, 60)
        stamp = f"{h}:{m:02d}:{s:02d}" if h else f"{m}:{s:02d}"
        cards.append(
            f'<section class="clip">'
            f'<div class="chead"><span class="num">Clip {i}</span>'
            f'<span class="cid">{cid}</span></div>'
            f'<a class="play" href="{watch}" target="_blank" rel="noopener">'
            f"&#9654; Play clip {i} on YouTube (opens at {stamp} &mdash; listen ~"
            f"{CLIP_WINDOW_SECONDS}s)</a>"
            f'<label class="pick">Who is speaking?'
            f'<select data-clip="{cid}"><option value="">— pick speaker —</option>'
            f"{options}</select></label>"
            f"</section>"
        )

    total = len(clips)
    return _HTML_TEMPLATE.format(
        title=html.escape(title),
        subtitle=html.escape(subtitle),
        total=total,
        cards="\n".join(cards),
    )


def build_manifest(
    clips: list[Clip],
    roster: list[RosterEntry],
    *,
    state: str,
    place: str,
    body: str,
    entity_id: int,
    seed: int,
) -> dict[str, Any]:
    """The sibling manifest ``--apply`` reads: per-clip provenance + the roster snapshot.

    The hypothesis lives HERE, never in the HTML. The roster snapshot lets ``--apply`` join a
    slug answer to a subject_id even if the roster later changes (writes still re-validate
    against the live DB).
    """
    return {
        "generated_at": datetime.now(UTC).isoformat(),
        "entity_path": f"{state}/{place}/{body}",
        "state": state,
        "place": place,
        "body": body,
        "entity_id": entity_id,
        "seed": seed,
        "counts": {
            "total": len(clips),
            "hypothesis": sum(1 for c in clips if c.kind == "hypothesis"),
            "unknown": sum(1 for c in clips if c.kind == "unknown"),
        },
        "roster": {
            r.slug: {
                "subject_id": r.subject_id,
                "person_id": r.person_id,
                "canonical_name": r.canonical_name,
                "publishable": r.publishable,
            }
            for r in roster
        },
        "clips": {
            clip_id_for(i): {
                "document_id": c.document_id,
                "cluster_label": c.cluster_label,
                "video_id": c.video_id,
                "start_seconds": c.start_seconds,
                "end_seconds": c.end_seconds,
                "kind": c.kind,
                "hypothesis_subject_id": c.hypothesis_subject_id,
            }
            for i, c in enumerate(clips, 1)
        },
    }


# --- DB-facing selection ----------------------------------------------------------


def _service_client() -> Client:
    """A service-key Supabase client (reads below-gate rows + writes speaker_identities)."""
    cfg = load_config()
    key = os.environ.get("ACTALUX_SUPABASE_SERVICE_KEY", "") or cfg.supabase_service_key
    if not key:
        raise ActaluxError("ACTALUX_SUPABASE_SERVICE_KEY is required")
    return get_client(cfg.supabase_url, key)


def _video_documents(client: Client, entity_id: int) -> dict[int, dict[str, Any]]:
    """Live (non-superseded) documents with a video for one body -> ``{doc_id: doc}``."""
    docs = fetch_all_rows(
        lambda: (
            client.table("documents")
            .select("id,video_id,replaces_id,entity_id,meeting_title,meeting_date")
            .eq("entity_id", entity_id)
        )
    )
    superseded = superseded_doc_ids(docs)
    return {d["id"]: d for d in docs if d["id"] not in superseded and d.get("video_id")}


def _roster(client: Client, entity_id: int, place_id: int) -> list[RosterEntry]:
    """Every official with a membership in this body, sorted by name (the dropdown + targets)."""
    mems = fetch_all_rows(
        lambda: client.table("memberships").select("subject_id").eq("entity_id", entity_id)
    )
    subject_ids = sorted({m["subject_id"] for m in mems})
    if not subject_ids:
        return []
    subjects = fetch_all_rows(
        lambda: (
            client.table("subjects")
            .select("id,slug,canonical_name,person_id,publishable")
            .eq("place_id", place_id)
        )
    )
    by_id = {s["id"]: s for s in subjects}
    roster = [
        RosterEntry(
            subject_id=s["id"],
            slug=s["slug"],
            canonical_name=s.get("canonical_name", "?"),
            person_id=s.get("person_id"),
            publishable=bool(s.get("publishable")),
        )
        for sid in subject_ids
        if (s := by_id.get(sid)) is not None
    ]
    return sorted(roster, key=lambda r: (r.canonical_name.lower(), r.subject_id))


def _gather_clip_candidates(
    client: Client, docs_by_id: dict[int, dict[str, Any]]
) -> tuple[dict[int, list[_ClusterCand]], list[_ClusterCand], dict[int, list[dict[str, Any]]]]:
    """Scan a body's video documents into (hypothesis cands per subject, unknown cands, turns).

    One diarization read per document (cached and returned so the caller can cut clip windows
    without re-reading). A cluster with an open inferred identity row becomes a hypothesis
    candidate for that subject; a cluster with NO identity row at all becomes an unknown
    candidate. Locked rows (confirmed/rejected) are neither — they are already decided.
    """
    doc_ids = sorted(docs_by_id)
    idents = (
        fetch_all_rows(
            lambda: (
                client.table("speaker_identities")
                .select("document_id,cluster_label,subject_id,confidence")
                .in_("document_id", doc_ids)
            )
        )
        if doc_ids
        else []
    )
    # (document_id, cluster_label) -> the inferred subject, plus the set of clusters that have
    # ANY row (so an unknown cluster is one absent from this set).
    inferred_subject: dict[tuple[int, str], int] = {}
    has_row: set[tuple[int, str]] = set()
    for r in idents:
        key = (r["document_id"], r["cluster_label"])
        has_row.add(key)
        if r.get("confidence") in INFERRED_TIERS and r.get("subject_id") is not None:
            inferred_subject[key] = r["subject_id"]

    turns_by_doc: dict[int, list[dict[str, Any]]] = {}
    hyp_by_subject: dict[int, list[_ClusterCand]] = {}
    unknown: list[_ClusterCand] = []
    for doc_id, doc in docs_by_id.items():
        turns = get_diarization_turns(client, doc_id)
        turns_by_doc[doc_id] = turns
        video_id = doc["video_id"]
        for cluster_label in {t["cluster_label"] for t in turns}:
            seconds = span_seconds(cluster_spans(turns, cluster_label))
            key = (doc_id, cluster_label)
            subject_id = inferred_subject.get(key)
            if subject_id is not None:
                hyp_by_subject.setdefault(subject_id, []).append(
                    _ClusterCand(doc_id, cluster_label, video_id, seconds, subject_id)
                )
            elif key not in has_row:
                unknown.append(_ClusterCand(doc_id, cluster_label, video_id, seconds, None))
    return hyp_by_subject, unknown, turns_by_doc


def _cand_to_clip(
    cand: _ClusterCand, turns_by_doc: dict[int, list[dict[str, Any]]], kind: str
) -> Clip | None:
    """Cut a candidate cluster into a ``Clip`` at its longest-turn window (or ``None``)."""
    window = longest_turn_window(turns_by_doc[cand.document_id], cand.cluster_label)
    if window is None:
        return None
    start, end = window
    return Clip(
        document_id=cand.document_id,
        cluster_label=cand.cluster_label,
        video_id=cand.video_id,
        start_seconds=start,
        end_seconds=end,
        kind=kind,
        hypothesis_subject_id=cand.subject_id,
    )


def select_clips(
    hyp_by_subject: dict[int, list[_ClusterCand]],
    unknown: list[_ClusterCand],
    turns_by_doc: dict[int, list[dict[str, Any]]],
) -> list[Clip]:
    """Turn candidates into the final ordered clip list (selection + caps + shuffle-free order).

    Per subject: representative cluster per distinct meeting (>= HYP_MIN_MEETINGS). Subjects
    are ranked by total inferred speech (most material — the highest-value labels to verify —
    first) so the cap admits the strongest officials in full. Unknown clusters fill the rest.
    Ordering/shuffle is applied by the caller.
    """
    subject_totals = {sid: sum(c.seconds for c in cands) for sid, cands in hyp_by_subject.items()}
    hypothesis_sets: list[list[Clip]] = []
    for sid in sorted(hyp_by_subject, key=lambda s: (-subject_totals[s], s)):
        reps = pick_subject_meeting_reps(hyp_by_subject[sid])
        clips = [c for c in (_cand_to_clip(r, turns_by_doc, "hypothesis") for r in reps) if c]
        if len(clips) >= HYP_MIN_MEETINGS:
            hypothesis_sets.append(clips)

    unknown_reps = pick_unknown_reps(unknown)
    unknown_clips = [
        c for c in (_cand_to_clip(r, turns_by_doc, "unknown") for r in unknown_reps) if c
    ]
    return assemble_clips(hypothesis_sets, unknown_clips)


# --- build command ----------------------------------------------------------------


def _resolve_entity(client: Client, state: str, place: str, body: str) -> dict[str, Any]:
    """Resolve the body via its URL parts (place + body_slug), the canonical seam."""
    entity = get_entity_by_path(client, state, place, body)
    if not entity:
        raise ActaluxError(f"no body {state}/{place}/{body}")
    return entity


def run_build(state: str, place: str, body: str, out_dir: Path) -> tuple[Path, Path]:
    """Build the sheet + manifest for one body; return (html_path, manifest_path)."""
    client = _service_client()
    entity = _resolve_entity(client, state, place, body)
    entity_id, place_id = entity["id"], entity["place_id"]
    entity_path = f"{state}/{place}/{body}"

    roster = _roster(client, entity_id, place_id)
    if not roster:
        raise ActaluxError(f"{entity_path}: no roster members (nothing to pick from)")
    docs_by_id = _video_documents(client, entity_id)
    if not docs_by_id:
        raise ActaluxError(f"{entity_path}: no video documents to clip")

    hyp_by_subject, unknown, turns_by_doc = _gather_clip_candidates(client, docs_by_id)
    clips = select_clips(hyp_by_subject, unknown, turns_by_doc)
    if not clips:
        raise ActaluxError(f"{entity_path}: no clips selected (no inferred or unknown clusters)")
    seed = seed_from_path(entity_path)
    clips = deterministic_order(clips, seed)

    display = entity.get("display_name") or entity_path
    html_doc = render_html(
        clips,
        roster,
        title=f"Blind speaker-ID sheet — {display}",
        subtitle=f"{entity_path} · {len(clips)} clips · listen and pick who speaks",
    )
    manifest = build_manifest(
        clips, roster, state=state, place=place, body=body, entity_id=entity_id, seed=seed
    )

    out_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(UTC).strftime("%Y%m%d-%H%M%S")
    base = f"{state}_{place}_{body}_{stamp}"
    html_path = out_dir / f"{base}.html"
    manifest_path = out_dir / f"{base}.manifest.json"
    html_path.write_text(html_doc, encoding="utf-8")
    manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")

    n_hyp = manifest["counts"]["hypothesis"]
    n_unk = manifest["counts"]["unknown"]
    n_meetings = len({c.video_id for c in clips})
    logger.info(
        "%s: %d clips (%d hypothesis / %d unknown) across %d meetings; roster=%d",
        entity_path,
        len(clips),
        n_hyp,
        n_unk,
        n_meetings,
        len(roster),
    )
    logger.info("HTML:     %s", html_path)
    logger.info("Manifest: %s", manifest_path)
    return html_path, manifest_path


# --- apply command ----------------------------------------------------------------


@dataclass
class ApplyTally:
    """Counts for the ``--apply`` summary (every answered clip lands in exactly one bucket)."""

    inserted: int = 0  # de-novo confirmed row written
    confirmed_in_place: int = 0  # existing hypothesis row moved to confirmed
    already_confirmed: int = 0  # row already confirmed for this subject (idempotent no-op)
    rejected: int = 0  # citizen denial written (existing official row -> rejected)
    already_rejected: int = 0  # row already rejected (idempotent no-op)
    citizen_anonymous: int = 0  # citizen on a cluster with no row (already anonymous; no-op)
    disagreement: int = 0  # operator names a DIFFERENT official than the row -> flagged, no write
    conflict: int = 0  # operator says citizen but the row is human-confirmed -> flagged, no write
    unknown_slug: int = 0  # answer slug not in the roster snapshot -> flagged, no write
    not_confirmable: int = 0  # target subject not publishable/person-linked -> flagged, no write
    unknown_clip: int = 0  # answer references a clip id absent from the manifest
    skipped_unsure_other: int = 0  # "not sure" / "someone else" -> intentionally no write


def _existing_identity(
    client: Client, document_id: int, cluster_label: str
) -> dict[str, Any] | None:
    """The current speaker_identities row for a cluster, or ``None``."""
    rows = (
        client.table("speaker_identities")
        .select("id,subject_id,confidence,basis")
        .eq("document_id", document_id)
        .eq("cluster_label", cluster_label)
        .limit(1)
        .execute()
        .data
    )
    return rows[0] if rows else None


def _apply_person(  # noqa: PLR0913 - one guarded write path; splitting would hide the branches
    client: Client,
    cid: str,
    entry: dict[str, Any],
    target: RosterEntry,
    tally: ApplyTally,
    flags: list[str],
    *,
    write: bool,
) -> None:
    """Apply one person answer: confirm in place, insert de-novo, or flag a conflict.

    ``target`` is already a LIVE publishable, person-linked member of this body (the caller
    re-validates against the DB, not the manifest snapshot). The write is guarded so a wrong
    ``confirmed`` row is never produced:
      * no existing row       -> INSERT a de-novo confirmed row (basis='manual');
      * existing is ``rejected`` -> a prior HUMAN denial of a name for this cluster; a confirm
        contradicts it, so NEVER write (the DB trigger blocks the downgrade anyway) — flag it;
      * existing names the SAME subject -> confirm in place (idempotent if already confirmed);
      * existing names a DIFFERENT subject -> a disagreement: NEVER overwrite/confirm it here
        (that would confirm a contradicted label or destroy a machine label on a possible
        mis-click). Flagged for the operator to resolve deliberately via confirm_speaker.
    """
    doc_id, cluster = entry["document_id"], entry["cluster_label"]
    existing = _existing_identity(client, doc_id, cluster)
    if existing is None:
        if write:
            client.table("speaker_identities").insert(
                denovo_confirm_row(doc_id, cluster, target.subject_id)
            ).execute()
        tally.inserted += 1
        return
    if existing["confidence"] == "rejected":
        tally.conflict += 1
        flags.append(
            f"{cid}: CONFLICT — operator picked {target.slug!r} but this cluster was "
            f"human-REJECTED under subject_id={existing['subject_id']}; left for manual review"
        )
        return
    if existing["subject_id"] == target.subject_id:
        if existing["confidence"] == "confirmed":
            tally.already_confirmed += 1
            return
        if write:
            client.table("speaker_identities").update(confirm_update(existing.get("basis"))).eq(
                "id", existing["id"]
            ).execute()
        tally.confirmed_in_place += 1
        return
    tally.disagreement += 1
    flags.append(
        f"{cid}: DISAGREEMENT — operator picked {target.slug!r} but the row names "
        f"subject_id={existing['subject_id']} ({existing['confidence']}); left for manual review"
    )


def _apply_citizen(
    client: Client,
    cid: str,
    entry: dict[str, Any],
    tally: ApplyTally,
    flags: list[str],
    *,
    write: bool,
) -> None:
    """Apply one citizen answer: deny an official hypothesis, or note an already-anonymous cluster.

    An existing inferred official row -> ``rejected`` (Option B: keeps only the official it was
    denied under). A cluster with no row is already anonymous — correct for a citizen — so
    nothing is written. A human-confirmed row is a conflict (the operator contradicts a prior
    confirmation) and is flagged, never rejected.
    """
    doc_id, cluster = entry["document_id"], entry["cluster_label"]
    existing = _existing_identity(client, doc_id, cluster)
    if existing is None:
        tally.citizen_anonymous += 1
        return
    if existing["confidence"] == "confirmed":
        tally.conflict += 1
        flags.append(
            f"{cid}: CONFLICT — operator says citizen but the row is human-confirmed "
            f"(subject_id={existing['subject_id']}); left for manual review"
        )
        return
    if existing["confidence"] == "rejected":
        tally.already_rejected += 1
        return
    if write:
        client.table("speaker_identities").update({"confidence": "rejected"}).eq(
            "id", existing["id"]
        ).execute()
    tally.rejected += 1


def _agreement_label(operator_subject_id: int, hypothesis_subject_id: int | None) -> str:
    """How the operator's pick relates to the manifest hypothesis (report only)."""
    if hypothesis_subject_id is None:
        return "net-new"
    return "agree" if operator_subject_id == hypothesis_subject_id else "disagree"


def _live_confirmable(client: Client, manifest: dict[str, Any]) -> dict[int, RosterEntry]:
    """The body's CURRENT confirmable officials, ``{subject_id: RosterEntry}`` (live DB).

    A stale or hand-edited manifest must not confirm a name onto a cluster if that subject is no
    longer a publishable, person-linked member of THIS body — the same Option-B scope guard
    confirm_speaker enforces. The manifest's ``entity_id`` is re-resolved from its state/place/body
    and must still match, so a manifest can't be applied against a different body/DB.
    """
    entity = get_entity_by_path(client, manifest["state"], manifest["place"], manifest["body"])
    if not entity or entity["id"] != manifest["entity_id"]:
        raise ActaluxError(
            f"manifest entity_id={manifest['entity_id']} no longer resolves to "
            f"{manifest['entity_path']!r}; refusing to apply a mismatched manifest"
        )
    return {
        r.subject_id: r
        for r in _roster(client, entity["id"], entity["place_id"])
        if r.publishable and r.person_id is not None
    }


def run_apply(answers_text: str, manifest_path: Path, *, write: bool) -> None:
    """Join answers to the manifest and apply them through the guarded write path."""
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    clips = manifest["clips"]
    snapshot_by_slug = {
        slug: info["subject_id"] for slug, info in manifest["roster"].items()
    }  # slug -> subject_id join (subject_id is an immutable PK, safe to take from the snapshot)
    answers = parse_answers(answers_text)

    client = _service_client()
    live_confirmable = _live_confirmable(client, manifest)
    tally = ApplyTally()
    flags: list[str] = []
    agreements: dict[str, int] = {"agree": 0, "disagree": 0, "net-new": 0}
    mode = "APPLY" if write else "DRY-RUN"
    logger.info("%s: %d answered clip(s) against %s", mode, len(answers), manifest["entity_path"])

    for cid, value in sorted(answers.items()):
        entry = clips.get(cid)
        if entry is None:
            tally.unknown_clip += 1
            flags.append(f"{cid}: not in the manifest (stale or wrong sheet)")
            continue
        if value in _FIXED_VALUES:
            if value == "citizen":
                _apply_citizen(client, cid, entry, tally, flags, write=write)
            else:  # other / unsure
                tally.skipped_unsure_other += 1
            continue
        subject_id = snapshot_by_slug.get(value)
        if subject_id is None:
            tally.unknown_slug += 1
            flags.append(f"{cid}: answer slug {value!r} is not in the roster snapshot")
            continue
        agreements[_agreement_label(subject_id, entry.get("hypothesis_subject_id"))] += 1
        target = live_confirmable.get(subject_id)
        if target is None:  # re-validated against the LIVE roster, not the snapshot
            tally.not_confirmable += 1
            flags.append(
                f"{cid}: {value!r} (subject_id={subject_id}) is no longer a live publishable, "
                f"person-linked member of {manifest['entity_path']}; left for manual review"
            )
            continue
        _apply_person(client, cid, entry, target, tally, flags, write=write)

    _print_apply_report(tally, agreements, flags, write=write)


def _print_apply_report(
    tally: ApplyTally, agreements: dict[str, int], flags: list[str], *, write: bool
) -> None:
    """Print the write counts, the agree/disagree/net-new breakdown, and every flag."""
    print("\n" + "=" * 72)
    print(f"{'APPLIED' if write else 'DRY-RUN (no writes)'} — speaker-identity decisions")
    print("-" * 72)
    print(f"  inserted (de-novo confirmed)   {tally.inserted}")
    print(f"  confirmed in place             {tally.confirmed_in_place}")
    print(f"  already confirmed (no-op)      {tally.already_confirmed}")
    print(f"  rejected (citizen denial)      {tally.rejected}")
    print(f"  already rejected (no-op)       {tally.already_rejected}")
    print(f"  citizen, already anonymous     {tally.citizen_anonymous}")
    print(f"  skipped (not sure / someone else) {tally.skipped_unsure_other}")
    print("-" * 72)
    print(
        "  agreement vs machine hypothesis: "
        f"agree={agreements['agree']} disagree={agreements['disagree']} "
        f"net-new={agreements['net-new']}"
    )
    print(
        "  NOT written (need review): "
        f"disagreements={tally.disagreement} conflicts={tally.conflict} "
        f"unknown-slug={tally.unknown_slug} not-confirmable={tally.not_confirmable} "
        f"unknown-clip={tally.unknown_clip}"
    )
    if flags:
        print("-" * 72)
        print("  Flags (review these):")
        for f in flags:
            print(f"    * {f}")
    print("=" * 72)


_HTML_TEMPLATE = """<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>{title}</title>
<style>
  * {{ box-sizing: border-box; }}
  body {{ margin: 0; font: 15px/1.5 -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
         color: #1a1a1a; background: #f7f5f0; padding: 0 0 200px; }}
  header {{ padding: 20px 24px; border-bottom: 1px solid #ddd; background: #fff; }}
  header h1 {{ margin: 0 0 4px; font-size: 20px; }}
  header p {{ margin: 0; color: #555; }}
  .grid {{ display: grid; grid-template-columns: repeat(auto-fill, minmax(320px, 1fr));
          gap: 16px; padding: 24px; }}
  .clip {{ background: #fff; border: 1px solid #ddd; padding: 12px; }}
  .chead {{ display: flex; justify-content: space-between; align-items: baseline;
           margin-bottom: 8px; }}
  .num {{ font-weight: 600; }}
  .cid {{ font: 12px "SF Mono", ui-monospace, monospace; color: #888; }}
  .play {{ display: block; padding: 14px 12px; margin: 0 0 4px;
    background: #1a1a1a; color: #fff; text-decoration: none; font-size: 15px; }}
  .play:hover {{ background: #333; }}
  .pick {{ display: block; margin-top: 10px; font-size: 13px; color: #444; }}
  .pick select {{ display: block; width: 100%; margin-top: 4px; padding: 6px;
                 font-size: 14px; }}
  footer {{ position: fixed; bottom: 0; left: 0; right: 0; background: #fff;
           border-top: 1px solid #ccc; padding: 12px 24px;
           box-shadow: 0 -2px 8px rgba(0,0,0,.06); }}
  footer .row {{ display: flex; gap: 12px; align-items: center; margin-bottom: 6px; }}
  footer button {{ padding: 8px 16px; font-size: 14px; cursor: pointer; }}
  #answers {{ width: 100%; height: 80px; font: 13px "SF Mono", ui-monospace, monospace; }}
  #count {{ color: #555; }}
</style>
</head>
<body>
<header>
  <h1>{title}</h1>
  <p>{subtitle}</p>
</header>
<main class="grid">
{cards}
</main>
<footer>
  <div class="row">
    <button id="copy" type="button">Copy answers</button>
    <span id="count">0 / {total} answered</span>
  </div>
  <textarea id="answers" readonly placeholder="Answers appear here as you pick."></textarea>
</footer>
<script>
  var selects = document.querySelectorAll('select[data-clip]');
  var out = document.getElementById('answers');
  var count = document.getElementById('count');
  function rebuild() {{
    var lines = [], n = 0;
    selects.forEach(function (s) {{
      if (s.value) {{ lines.push(s.dataset.clip + '=' + s.value); n++; }}
    }});
    out.value = lines.join('\\n');
    count.textContent = n + ' / {total} answered';
  }}
  selects.forEach(function (s) {{ s.addEventListener('change', rebuild); }});
  document.getElementById('copy').addEventListener('click', function () {{
    out.select();
    if (navigator.clipboard) {{ navigator.clipboard.writeText(out.value); }}
    else {{ document.execCommand('copy'); }}
  }});
</script>
</body>
</html>
"""


def main() -> None:
    parser = argparse.ArgumentParser(description="Build (or apply) a blind speaker-ID sheet.")
    parser.add_argument("--state", help="place state slug, e.g. mo")
    parser.add_argument("--place", help="place slug, e.g. clayton")
    parser.add_argument("--body", help="body slug, e.g. schools")
    parser.add_argument(
        "--out-dir", type=Path, default=Path("data/id_sheets"), help="output dir (gitignored)"
    )
    parser.add_argument(
        "--apply",
        metavar="ANSWERS",
        help="apply mode: a path to the answer string, or '-' to read stdin",
    )
    parser.add_argument("--manifest", type=Path, help="manifest JSON (required with --apply)")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="with --apply: report the decisions without writing",
    )
    args = parser.parse_args()

    if args.apply is not None:
        if not args.manifest:
            parser.error("--apply requires --manifest")
        if args.apply == "-":
            import sys

            answers_text = sys.stdin.read()
        else:
            answers_text = Path(args.apply).read_text(encoding="utf-8")
        run_apply(answers_text, args.manifest, write=not args.dry_run)
        return

    if not (args.state and args.place and args.body):
        parser.error("--state, --place and --body are required to build a sheet")
    run_build(args.state, args.place, args.body, args.out_dir)


if __name__ == "__main__":
    main()
