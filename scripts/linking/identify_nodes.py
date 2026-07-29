"""Identify unanchored recurring voice-nodes from evidence, without listening.

Input: the ``--dump-nodes`` JSON from propose_identities (anonymous voice-nodes the
linker flagged as recurring but could not name). For each node this tool gathers two
independent, jurisdiction-agnostic signals and reports them side by side:

1. **Gallery similarity** — cosine of the node's cached cluster embeddings against each
   official's centroid of confirmed clusters (mean/min across the node, plus the margin
   to the runner-up official). A conservative complete-linkage merge can strand a whole
   island of a known official's voice (one weak pair, or one cannot-link edge, blocks the
   merge), so a node can sit unanchored while scoring same-voice-high against a gallery.
2. **Minutes attribution** — motion-shaped utterances in the node's own turns ("I move
   that the Board…") fuzzy-matched against the already-extracted ``votes`` rows for the
   same body and meeting date, whose ``details.moved_by`` carries the minutes' verbatim
   attribution ("Motion by Gary Pierson"). A match means the written record names the
   voice — the same cross-reference that identified a rejected cluster as Pierson by
   three anchors, mechanized.

A second input mode, ``--manifest``, takes a transcription-pipeline manifest instead of a
node dump: every UNCLAIMED cluster (no identity row of any tier) of the manifest's live
documents becomes its own single-cluster node. This is the nightly "morning-after" scope —
score a fresh meeting's unknown voices against the galleries the moment diarization lands,
before any minutes exist. (It generalizes the single-cluster island scan first improvised
for the council/PC evidence sheets.)

A third mode, ``--audit-anchors``, turns the minutes signal on the anchors themselves:
every cluster the linker would treat as ground truth (``select_enrollable``, the exact
filter the embedding-cache builder uses) is cross-checked against the minutes' motion
attributions, and each match is judged roster-relatively — "agree" only when the minutes
attribution carries a token unique to the claimed official among the body's roster,
"contradict" when it uniquely names someone else. This matters for bodies whose anchors
are mostly unreviewed text inferences (council: 79% discourse-basis) — a wrong anchor
poisons both the measured frontier and every name the proposer propagates from it. Audit
mode needs no embedding cache, so it runs before the all-cluster embed exists.

Report-only in both modes: no identity rows, no entities, no voiceprints are written.
Attribution remains a human decision recorded via attribute_speaker.py. This is the
replicable form of the island-rescue + minutes-trick analyses first run on
mo/clayton/schools.

Usage:
    doppler run --project mac --config dev -- uv run python scripts/linking/identify_nodes.py \
        --state mo --place clayton --body schools \
        --nodes nodes.json --out identified.json
    doppler run --project mac --config dev -- uv run python scripts/linking/identify_nodes.py \
        --state mo --place clayton --body council \
        --audit-anchors --out anchor_audit.json
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import re
import sys
from collections import Counter
from pathlib import Path
from typing import Any

import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent / "src"))

from supabase import Client  # noqa: E402

from actalux.config import load_config  # noqa: E402
from actalux.db import fetch_all_rows, get_client, get_entity_by_path  # noqa: E402
from actalux.diarization.enrollment import select_enrollable, superseded_doc_ids  # noqa: E402
from actalux.diarization.linking.cache import MODE_ALL, cache_dir, require_mode  # noqa: E402
from actalux.diarization.linking.observations import load_observation_dir  # noqa: E402
from actalux.errors import ActaluxError  # noqa: E402

# The motion primitives are shared with the automatic resolver family (vote_align.align_motions
# writes what this tool reports); single source keeps their behavior identical.
from actalux.identity.vote_align import (  # noqa: E402
    MOTION_MATCH_THRESHOLD,  # noqa: F401  (re-exported: tests + report consumers key off it)
    match_motion,
    spoken_motions,
)

logger = logging.getLogger("identify_nodes")

# An official's gallery needs a few confirmed clusters before its centroid is stable.
_MIN_GALLERY_CLUSTERS = 3


def name_tokens(name: str) -> set[str]:
    """Lowercased letter-runs of a name — the unit of roster-relative comparison."""
    return {t for t in re.split(r"[^a-zA-Z]+", (name or "").lower()) if t}


def audit_verdict(claimed: str, moved_by: str, roster_names: list[str]) -> str:
    """Judge a minutes attribution against the claimed official, relative to the roster.

    Mirrors the discourse labeler's ambiguity gate: a token supports a member only if no
    other roster member shares it. "agree" iff ``moved_by`` carries a token unique to the
    claimed member; "contradict" iff it carries a token unique to a DIFFERENT member (and
    none unique to the claimed one); "unclear" otherwise — shared tokens only (two Susans)
    or no overlap at all identify nobody.
    """
    per = {n: name_tokens(n) for n in roster_names}
    counts = Counter(t for ts in per.values() for t in ts)
    moved = name_tokens(moved_by)
    claimed_tokens = per.get(claimed, name_tokens(claimed))
    if moved & {t for t in claimed_tokens if counts[t] <= 1}:
        return "agree"
    for name, tokens in per.items():
        if name != claimed and moved & {t for t in tokens if counts[t] == 1}:
            return "contradict"
    return "unclear"


def _service_client() -> Client:
    cfg = load_config()
    key = os.environ.get("ACTALUX_SUPABASE_SERVICE_KEY", "")
    if not key:
        raise ActaluxError("ACTALUX_SUPABASE_SERVICE_KEY is required")
    return get_client(cfg.supabase_url, key)


def _gallery_centroids(
    client: Client, entity_id: int, emb_by_key: dict[tuple[int, str], np.ndarray]
) -> dict[str, tuple[np.ndarray, int]]:
    """Per-official unit centroids from confirmed clusters present in the cache."""
    docs = fetch_all_rows(lambda: client.table("documents").select("id").eq("entity_id", entity_id))
    doc_ids = [d["id"] for d in docs]
    confirmed: list[dict[str, Any]] = []
    for i in range(0, len(doc_ids), 200):
        confirmed += (
            client.table("speaker_identities")
            .select("document_id,cluster_label,subject_id")
            .eq("confidence", "confirmed")
            .in_("document_id", doc_ids[i : i + 200])
            .execute()
            .data
        )
    names = {
        s["id"]: s["canonical_name"]
        for s in client.table("subjects")
        .select("id,canonical_name")
        .in_("id", sorted({r["subject_id"] for r in confirmed}))
        .execute()
        .data
    }
    by_official: dict[str, list[np.ndarray]] = {}
    for r in confirmed:
        v = emb_by_key.get((r["document_id"], r["cluster_label"]))
        if v is not None:
            by_official.setdefault(names[r["subject_id"]], []).append(v)
    centroids: dict[str, tuple[np.ndarray, int]] = {}
    for name, vecs in by_official.items():
        if len(vecs) >= _MIN_GALLERY_CLUSTERS:
            c = np.mean(vecs, axis=0)
            centroids[name] = (c / np.linalg.norm(c), len(vecs))
    return centroids


def _node_gallery_score(
    node: dict[str, Any],
    centroids: dict[str, tuple[np.ndarray, int]],
    emb_by_key: dict[tuple[int, str], np.ndarray],
) -> dict[str, Any] | None:
    vecs = [
        v
        for m in node["members"]
        if (v := emb_by_key.get((m["document_id"], m["cluster_label"]))) is not None
    ]
    if not vecs or not centroids:
        return None
    scored = []
    for name, (cent, k) in centroids.items():
        sims = np.array([float(v @ cent) for v in vecs])
        scored.append((float(sims.mean()), float(sims.min()), name, k))
    scored.sort(reverse=True)
    top = scored[0]
    margin = top[0] - scored[1][0] if len(scored) > 1 else top[0]
    return {
        "best_official": top[2],
        "mean": round(top[0], 3),
        "min": round(top[1], 3),
        "margin": round(margin, 3),
        "runner_up": scored[1][2] if len(scored) > 1 else None,
        "gallery_clusters": top[3],
        "scored_members": len(vecs),
    }


def _turn_texts(client: Client, document_id: int, cluster_label: str) -> list[str]:
    turns = (
        client.table("diarization_turns")
        .select("words")
        .eq("document_id", document_id)
        .eq("cluster_label", cluster_label)
        .execute()
        .data
    )
    out = []
    for t in turns:
        text = " ".join(w["word"] for w in (t["words"] or [])).strip()
        if text:
            out.append(text)
    return out


def _node_minutes_evidence(
    client: Client,
    node: dict[str, Any],
    meeting_dates: dict[int, str],
    votes_by_date: dict[str, list[dict[str, Any]]] | None = None,
) -> list[dict[str, Any]]:
    """Spoken-motion -> minutes-attribution matches for every member of one node.

    ``meeting_dates`` (doc id -> date) doubles as the body's document universe for the
    votes lookup. Pass a shared ``votes_by_date`` dict to reuse fetches across calls (the
    anchor audit hits the same meeting many times); each call otherwise caches locally.
    """
    evidence: list[dict[str, Any]] = []
    if votes_by_date is None:
        votes_by_date = {}
    body_doc_ids = list(meeting_dates)
    for member in node["members"]:
        doc_id = member["document_id"]
        date = meeting_dates.get(doc_id)
        if not date:
            continue
        if date not in votes_by_date:
            votes_by_date[date] = (
                client.table("votes")
                .select("document_id,motion,details")
                .eq("meeting_date", date)
                .in_("document_id", body_doc_ids)
                .execute()
                .data
            )
        votes = votes_by_date[date]
        if not votes:
            continue
        for spoken in spoken_motions(_turn_texts(client, doc_id, member["cluster_label"])):
            hit = match_motion(spoken, votes)
            if hit:
                vote, score = hit
                evidence.append(
                    {
                        "document_id": doc_id,
                        "cluster_label": member["cluster_label"],
                        "spoken": spoken[:200],
                        "minutes_motion": (vote.get("motion") or "")[:200],
                        "moved_by": (vote.get("details") or {}).get("moved_by"),
                        "match_score": round(score, 1),
                    }
                )
    return evidence


def _audit_anchors(client: Client, entity_id: int, out_path: str | None) -> None:
    """Cross-check every linking anchor of one body against the minutes' motion attributions."""
    docs = fetch_all_rows(
        lambda: client.table("documents").select("id,meeting_date").eq("entity_id", entity_id)
    )
    meeting_dates = {d["id"]: d["meeting_date"] for d in docs}
    doc_ids = list(meeting_dates)
    identities: list[dict[str, Any]] = []
    for i in range(0, len(doc_ids), 200):
        identities += (
            client.table("speaker_identities")
            .select("id,document_id,cluster_label,subject_id,confidence,basis")
            .in_("document_id", doc_ids[i : i + 200])
            .execute()
            .data
        )
    subject_ids = sorted({r["subject_id"] for r in identities if r.get("subject_id") is not None})
    subjects_by_id = {
        s["id"]: s
        for s in client.table("subjects")
        .select("id,person_id,publishable,canonical_name")
        .in_("id", subject_ids)
        .execute()
        .data
    }
    anchors = select_enrollable(identities, subjects_by_id, confirmed_only=False)
    member_ids = fetch_all_rows(
        lambda: client.table("memberships").select("subject_id").eq("entity_id", entity_id)
    )
    roster_names = [
        s["canonical_name"]
        for s in client.table("subjects")
        .select("canonical_name")
        .in_("id", sorted({m["subject_id"] for m in member_ids}))
        .execute()
        .data
    ]
    logger.info("%d anchor(s) to audit, %d roster names", len(anchors), len(roster_names))

    votes_by_date: dict[str, list[dict[str, Any]]] = {}
    report, tallies = [], Counter()
    for a in anchors:
        node = {"members": [{"document_id": a.document_id, "cluster_label": a.cluster_label}]}
        evidence = _node_minutes_evidence(client, node, meeting_dates, votes_by_date)
        if not evidence:
            tallies["no_signal"] += 1
            continue
        verdicts = [audit_verdict(a.canonical_name, e["moved_by"], roster_names) for e in evidence]
        # one contradiction taints the anchor; agreement needs no dissent
        overall = (
            "contradict"
            if "contradict" in verdicts
            else ("agree" if "agree" in verdicts else "unclear")
        )
        tallies[overall] += 1
        report.append(
            {
                "document_id": a.document_id,
                "cluster_label": a.cluster_label,
                "claimed": a.canonical_name,
                "confidence": a.confidence,
                "basis": a.source_basis,
                "verdict": overall,
                "evidence": [dict(e, verdict=v) for e, v in zip(evidence, verdicts)],
            }
        )
        if overall == "contradict":
            logger.warning(
                "CONTRADICTED anchor: doc %d %s claimed %s (%s/%s) but minutes say %s",
                a.document_id,
                a.cluster_label,
                a.canonical_name,
                a.confidence,
                a.source_basis,
                sorted({e["moved_by"] for e in evidence}),
            )
    logger.info(
        "audit: %d anchors — agree %d / contradict %d / unclear %d / no minutes signal %d",
        len(anchors),
        tallies["agree"],
        tallies["contradict"],
        tallies["unclear"],
        tallies["no_signal"],
    )
    if out_path:
        Path(out_path).write_text(json.dumps(report, indent=1))
        logger.info("audit report written to %s", out_path)


def manifest_nodes(
    client: Client, entity_id: int, manifest_path: Path, observations: list
) -> list[dict[str, Any]]:
    """One single-cluster node per UNCLAIMED cluster of the manifest's live documents.

    "Unclaimed" = no ``speaker_identities`` row of any tier — the fresh meeting's genuinely
    unknown voices, scored the morning after diarization lands (no minutes exist yet, so the
    gallery signal usually carries this pass alone).
    """
    entries = json.loads(Path(manifest_path).read_text())
    video_ids = {e["video_id"] for e in entries if e.get("video_id")}
    if not video_ids:
        return []
    docs = fetch_all_rows(
        lambda: (
            client.table("documents").select("id,video_id,replaces_id").eq("entity_id", entity_id)
        )
    )
    superseded = superseded_doc_ids(docs)
    targets = {
        d["id"] for d in docs if d.get("video_id") in video_ids and d["id"] not in superseded
    }
    claimed: set[tuple[int, str]] = set()
    for doc_id in sorted(targets):
        for r in (
            client.table("speaker_identities")
            .select("cluster_label")
            .eq("document_id", doc_id)
            .execute()
            .data
        ):
            claimed.add((doc_id, r["cluster_label"]))
    nodes = []
    for i, o in enumerate(observations):
        if o.document_id not in targets or (o.document_id, o.cluster_label) in claimed:
            continue
        nodes.append(
            {
                "node_id": f"solo{i}",
                "n_meetings": 1,
                "total_seconds": o.speech_seconds,
                "members": [
                    {
                        "document_id": o.document_id,
                        "cluster_label": o.cluster_label,
                        "speech_seconds": o.speech_seconds,
                    }
                ],
            }
        )
    return nodes


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--state", required=True)
    parser.add_argument("--place", required=True)
    parser.add_argument("--body", required=True)
    parser.add_argument("--nodes", help="node JSON from --dump-nodes")
    parser.add_argument(
        "--manifest",
        help="transcription manifest; score every unclaimed cluster of its live documents "
        "(the nightly morning-after scope)",
    )
    parser.add_argument(
        "--audit-anchors",
        action="store_true",
        help="audit the body's linking anchors against minutes attributions (no cache needed)",
    )
    parser.add_argument("--cache-dir", default="data/linking_cache")
    parser.add_argument("--out", help="write the full report JSON here")
    args = parser.parse_args()
    if sum([bool(args.nodes), bool(args.manifest), args.audit_anchors]) != 1:
        parser.error("exactly one of --nodes / --manifest / --audit-anchors is required")

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    client = _service_client()
    entity = get_entity_by_path(client, args.state, args.place, args.body)
    if entity is None:
        raise ActaluxError(f"no entity for {args.state}/{args.place}/{args.body}")

    if args.audit_anchors:
        _audit_anchors(client, entity["id"], args.out)
        return

    cache_path = cache_dir(args.cache_dir, args.state, args.place, args.body, mode=MODE_ALL)
    require_mode(cache_path, MODE_ALL)
    obs = load_observation_dir(cache_path)
    emb_by_key: dict[tuple[int, str], np.ndarray] = {}
    for o in obs:
        v = np.asarray(o.embedding, dtype=np.float64)
        emb_by_key[(o.document_id, o.cluster_label)] = v / np.linalg.norm(v)

    nodes = (
        manifest_nodes(client, entity["id"], Path(args.manifest), obs)
        if args.manifest
        else json.loads(Path(args.nodes).read_text())
    )
    docs = fetch_all_rows(
        lambda: client.table("documents").select("id,meeting_date").eq("entity_id", entity["id"])
    )
    meeting_dates = {d["id"]: d["meeting_date"] for d in docs}
    centroids = _gallery_centroids(client, entity["id"], emb_by_key)
    logger.info(
        "%d node(s), %d official galleries (>=%d confirmed clusters each)",
        len(nodes),
        len(centroids),
        _MIN_GALLERY_CLUSTERS,
    )

    report = []
    for node in nodes:
        gallery = _node_gallery_score(node, centroids, emb_by_key)
        minutes = _node_minutes_evidence(client, node, meeting_dates)
        movers = sorted({e["moved_by"] for e in minutes if e.get("moved_by")})
        report.append(
            {
                "node_id": node["node_id"],
                "n_meetings": node["n_meetings"],
                "total_seconds": node["total_seconds"],
                "members": node["members"],
                "gallery": gallery,
                "minutes_movers": movers,
                "minutes_evidence": minutes,
            }
        )
        g = gallery or {}
        logger.info(
            "node %s (%d mtgs, %.1fh): gallery=%s mean=%s min=%s margin=%s | minutes movers=%s "
            "(%d motion match(es))",
            node["node_id"],
            node["n_meetings"],
            node["total_seconds"] / 3600,
            g.get("best_official"),
            g.get("mean"),
            g.get("min"),
            g.get("margin"),
            movers or "-",
            len(minutes),
        )

    if args.out:
        Path(args.out).write_text(json.dumps(report, indent=1))
        logger.info("report written to %s", args.out)


if __name__ == "__main__":
    main()
