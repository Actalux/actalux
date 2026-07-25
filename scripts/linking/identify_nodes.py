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

Report-only: no identity rows, no entities, no voiceprints are written. Attribution
remains a human decision recorded via attribute_speaker.py. This is the replicable form
of the island-rescue + minutes-trick analyses first run on mo/clayton/schools.

Usage:
    doppler run --project mac --config dev -- uv run python scripts/linking/identify_nodes.py \
        --state mo --place clayton --body schools \
        --nodes nodes.json --out identified.json
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import re
import sys
from pathlib import Path
from typing import Any

import numpy as np
from rapidfuzz import fuzz

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent / "src"))

from supabase import Client  # noqa: E402

from actalux.config import load_config  # noqa: E402
from actalux.db import fetch_all_rows, get_client, get_entity_by_path  # noqa: E402
from actalux.diarization.linking.cache import MODE_ALL, cache_dir, require_mode  # noqa: E402
from actalux.diarization.linking.observations import load_observation_dir  # noqa: E402
from actalux.errors import ActaluxError  # noqa: E402

logger = logging.getLogger("identify_nodes")

# A spoken motion long enough to fuzzy-match against a minutes motion; shorter
# fragments ("I move") match everything.
_MIN_MOTION_CHARS = 25
# token_set_ratio threshold for "the spoken motion is the minutes motion". Minutes
# paraphrase lightly (tense, "that the Board of Education"), so exact matching fails;
# below ~70 unrelated motions start colliding.
_MOTION_MATCH_THRESHOLD = 70.0
# An official's gallery needs a few confirmed clusters before its centroid is stable.
_MIN_GALLERY_CLUSTERS = 3

# First-person declarative motion-making ONLY. The first live run showed why: a chair's
# REQUEST ("May I have a motion to approve the agenda, please?") contains the motion's
# words and fuzzy-matches the minutes motion — which the minutes then attribute to
# whoever answered the request, not to the requesting voice. Bare "motion to" /
# "move that" without a first-person subject is exactly that request/paraphrase shape,
# so only "I move…", "I make a motion…", "so moved", and the read-aloud "Moved that…"
# opening count as the speaker making the motion themselves.
_MOTION_CUE = re.compile(
    r"\b(?:i (?:will |'ll |would like to )?move|i make a motion|so moved)\b|^moved that\b",
    re.IGNORECASE,
)


def spoken_motions(texts: list[str]) -> list[str]:
    """First-person motion-making utterances from a cluster's turn texts.

    Splits each turn on sentence-ish boundaries and keeps declarative sentences carrying
    a first-person motion cue, long enough to be matchable. Questions are dropped — a
    sentence ending in "?" is a chair soliciting a motion, not making one. Remaining
    caveat for consumers: a single 2-3 second match can still be diarization boundary
    bleed from an adjacent speaker; treat repeated matches to the SAME mover across
    meetings as evidence, a lone hit as noise.
    """
    out: list[str] = []
    for text in texts:
        for sentence in re.split(r"(?<=[.?!])\s+", text):
            if sentence.rstrip().endswith("?"):
                continue
            m = _MOTION_CUE.search(sentence)
            if not m:
                continue
            candidate = sentence[m.start() :].strip()
            if len(candidate) >= _MIN_MOTION_CHARS:
                out.append(candidate)
    return out


def match_motion(spoken: str, votes: list[dict[str, Any]]) -> tuple[dict[str, Any], float] | None:
    """The vote row whose motion text best matches a spoken motion, if above threshold.

    Only votes carrying a minutes attribution (``details.moved_by``) are candidates —
    an unattributed match identifies nothing.
    """
    best: tuple[dict[str, Any], float] | None = None
    for vote in votes:
        moved_by = (vote.get("details") or {}).get("moved_by")
        if not moved_by:
            continue
        score = fuzz.token_set_ratio(spoken.lower(), (vote.get("motion") or "").lower())
        if score >= _MOTION_MATCH_THRESHOLD and (best is None or score > best[1]):
            best = (vote, score)
    return best


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
    entity_id: int,
    node: dict[str, Any],
    meeting_dates: dict[int, str],
) -> list[dict[str, Any]]:
    """Spoken-motion -> minutes-attribution matches for every member of one node."""
    evidence: list[dict[str, Any]] = []
    votes_by_date: dict[str, list[dict[str, Any]]] = {}
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
                .in_(
                    "document_id",
                    [
                        d["id"]
                        for d in fetch_all_rows(
                            lambda: (
                                client.table("documents").select("id").eq("entity_id", entity_id)
                            )
                        )
                    ],
                )
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


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--state", required=True)
    parser.add_argument("--place", required=True)
    parser.add_argument("--body", required=True)
    parser.add_argument("--nodes", required=True, help="node JSON from --dump-nodes")
    parser.add_argument("--cache-dir", default="data/linking_cache")
    parser.add_argument("--out", help="write the full report JSON here")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    client = _service_client()
    entity = get_entity_by_path(client, args.state, args.place, args.body)
    if entity is None:
        raise ActaluxError(f"no entity for {args.state}/{args.place}/{args.body}")

    cache_path = cache_dir(args.cache_dir, args.state, args.place, args.body, mode=MODE_ALL)
    require_mode(cache_path, MODE_ALL)
    obs = load_observation_dir(cache_path)
    emb_by_key: dict[tuple[int, str], np.ndarray] = {}
    for o in obs:
        v = np.asarray(o.embedding, dtype=np.float64)
        emb_by_key[(o.document_id, o.cluster_label)] = v / np.linalg.norm(v)

    nodes = json.loads(Path(args.nodes).read_text())
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
        minutes = _node_minutes_evidence(client, entity["id"], node, meeting_dates)
        movers = sorted({e["moved_by"] for e in minutes if e.get("moved_by")})
        report.append(
            {
                "node_id": node["node_id"],
                "n_meetings": node["n_meetings"],
                "total_seconds": node["total_seconds"],
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
