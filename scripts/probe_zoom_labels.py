#!/usr/bin/env python3
"""Z1 probe: read the Zoom-rendered speaker name off a cluster's video frames.

READ-ONLY evidence gathering (docs/architecture/zoom-name-extraction.md, phase
Z1). For each target diarization cluster this samples a few of the cluster's turn
midpoints, grabs the frame Zoom recorded at that instant, reads the active-tile
name label (actalux.diarization.zoomlabels), and fuzzy-matches it to the body's
roster. It writes an evidence JSON plus the grabbed frames as receipts and prints
a per-cluster verdict table. It makes NO database writes and never touches
``speaker_identities`` — the operator reviews the receipts and decides what, if
anything, to enroll.

Frames are pulled without downloading whole videos: one ``yt-dlp -g`` per video
for the CDN stream URL, then ``ffmpeg -ss`` ranged single-frame grabs. Jurisdiction
is resolved from --state/--place/--body exactly like the sibling diarization
scripts; nothing here is Clayton-specific.

Run under Doppler for the Supabase service key (below-gate identities are
service-only), e.g.::

    doppler run --project mac --config dev -- \\
      uv run python scripts/probe_zoom_labels.py --state mo --place clayton \\
      --body schools --doc-id 2531 --clusters SPEAKER_09
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
import time
from dataclasses import asdict
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from PIL import Image

from actalux import db
from actalux.config import load_config
from actalux.diarization import zoomlabels
from actalux.errors import ActaluxError, IngestError
from actalux.identity.resolve import members_active_on, members_for_entity
from actalux.ingest.youtube import PLAYER_CLIENT

DEFAULT_FORMAT = "b[height<=720][ext=mp4]/b[height<=720]/b"
DEFAULT_FRAMES_PER_CLUSTER = 4
DEFAULT_OUT_DIR = Path("data/zoom_receipts")
DEFAULT_FLOOR = 85  # rapidfuzz floor; matches zoomlabels.match_roster's default
DEFAULT_MIN_AGREE = 2  # frames that must agree; matches zoomlabels.cluster_verdict
# A slug that wins more than this many clusters in one meeting is a room-feed/account
# label (Zoom names the streaming account, not the room speaker) — matches
# zoomlabels.feed_label_slugs's default.
DEFAULT_MAX_CLUSTERS_PER_SLUG = 2
# A cluster with no identity row is worth a frame grab only if it actually speaks
# for a while; a few seconds of backchannel is not.
DEFAULT_MIN_SPEECH_SECONDS = 60.0
# Full-frame OCR shorter than this many letters is discarded as noise (room-camera
# frames, blank strips) before roster matching.
MIN_FULLFRAME_ALPHA = 3
# A corpus sweep runs for hours; one document's transient failure (a Supabase
# ConnectTimeout, a CDN hiccup) must not abort it. Retry briefly, then skip.
DOC_RETRIES = 2
RETRY_BACKOFF_SECONDS = 20.0
# Confidences whose clusters are settled and not re-probed.
SETTLED_CONFIDENCES = frozenset({"confirmed", "rejected"})
YTDLP_TIMEOUT = 90
FFMPEG_TIMEOUT = 90
TRANSCRIPT_TYPE = "transcript"
_DOC_COLUMNS = "id,video_id,meeting_date,meeting_title,source_url"


def _midpoint(turn: dict[str, Any]) -> float:
    return (turn["start_seconds"] + turn["end_seconds"]) / 2.0


def _duration(turn: dict[str, Any]) -> float:
    return turn["end_seconds"] - turn["start_seconds"]


def cluster_speech_seconds(turns: list[dict[str, Any]]) -> dict[str, float]:
    """Total spoken seconds per cluster across a document's turns."""
    totals: dict[str, float] = {}
    for turn in turns:
        totals[turn["cluster_label"]] = totals.get(turn["cluster_label"], 0.0) + _duration(turn)
    return totals


def group_turns(turns: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    """Group a document's turns by cluster_label."""
    grouped: dict[str, list[dict[str, Any]]] = {}
    for turn in turns:
        grouped.setdefault(turn["cluster_label"], []).append(turn)
    return grouped


def select_target_clusters(
    turns: list[dict[str, Any]],
    identities: list[dict[str, Any]],
    *,
    min_speech: float = DEFAULT_MIN_SPEECH_SECONDS,
) -> list[str]:
    """Clusters worth a Zoom-label probe (pure; the DB read happens in the caller).

    A cluster is a target when it carries a non-settled identity row (a proposal to
    confirm or correct) OR it is unlabeled but speaks for at least ``min_speech``
    seconds. Settled rows (confirmed/rejected) and brief unlabeled backchannel are
    skipped.
    """
    confidence_by_cluster = {row["cluster_label"]: row["confidence"] for row in identities}
    speech = cluster_speech_seconds(turns)
    targets: list[str] = []
    for cluster in sorted(speech):
        confidence = confidence_by_cluster.get(cluster)
        if confidence in SETTLED_CONFIDENCES:
            continue
        if confidence is not None or speech[cluster] >= min_speech:
            targets.append(cluster)
    return targets


def choose_turn_midpoints(turns: list[dict[str, Any]], frames_per_cluster: int) -> list[float]:
    """Midpoints of ``frames_per_cluster`` long turns spread across the meeting.

    Split the cluster's time-ordered turns into that many contiguous groups and take
    the longest turn in each, so the sampled frames are spread over the meeting
    (guarding against a highlight that lingers on one exchange) yet each lands on a
    substantial turn (better OCR than a one-word interjection). If the cluster has
    fewer turns than frames requested, every turn is sampled.
    """
    ordered = sorted(turns, key=lambda turn: turn["start_seconds"])
    if frames_per_cluster <= 0 or not ordered:
        return []
    if len(ordered) <= frames_per_cluster:
        return [_midpoint(turn) for turn in ordered]
    midpoints: list[float] = []
    for i in range(frames_per_cluster):
        lo = i * len(ordered) // frames_per_cluster
        hi = (i + 1) * len(ordered) // frames_per_cluster
        longest = max(ordered[lo:hi], key=_duration)
        midpoints.append(_midpoint(longest))
    return midpoints


def build_alias_map(members: list[Any]) -> dict[str, str]:
    """Map every roster spelling (canonical name + aliases) to its person slug.

    Keys are folded through the same ``normalize_display_name`` the OCR path uses so
    an informal Zoom display name and a roster alias meet on common ground.
    """
    aliases: dict[str, str] = {}
    for member in members:
        for raw in (member.canonical_name, *member.aliases):
            key = zoomlabels.normalize_display_name(raw)
            if key:
                aliases[key] = member.slug
    return aliases


def _extractor_args(proxy: str | None) -> list[str]:
    """yt-dlp args for the android player client (and WARP proxy when given).

    The android client is the one that resolves a downloadable YouTube format; see
    actalux.ingest.youtube for the full recipe. Reusing ``PLAYER_CLIENT`` keeps this
    probe on the same client the transcription path is proven against.
    """
    args = ["--extractor-args", f"youtube:player_client={PLAYER_CLIENT}"]
    if proxy:
        args += ["--proxy", proxy]
    return args


def resolve_stream_url(video_id: str, fmt: str, proxy: str | None) -> str:
    """The direct CDN stream URL for a video via ``yt-dlp -g`` (one call per video)."""
    url = f"https://www.youtube.com/watch?v={video_id}"
    cmd = ["yt-dlp", "-g", *_extractor_args(proxy), "-f", fmt, url]
    try:
        result = subprocess.run(
            cmd, capture_output=True, text=True, timeout=YTDLP_TIMEOUT, check=True
        )
    except (subprocess.CalledProcessError, subprocess.TimeoutExpired) as exc:
        detail = (getattr(exc, "stderr", "") or str(exc))[-300:]
        raise IngestError(f"yt-dlp -g failed for {video_id}: {detail}") from exc
    streams = [line for line in result.stdout.splitlines() if line.strip()]
    if not streams:
        raise IngestError(f"yt-dlp -g returned no stream URL for {video_id}")
    return streams[0]  # first line is the video stream (audio-only is a separate line)


def grab_frame(stream_url: str, t_seconds: float, out_path: Path) -> bool:
    """Grab one frame at ``t_seconds`` via a ranged ffmpeg seek; True on success.

    ``-ss`` before ``-i`` seeks by keyframe before decode, so only a small ranged
    slice of the CDN stream is fetched (no whole-video download). A failure is
    reported (not raised) so one bad grab does not abort the run.
    """
    out_path.parent.mkdir(parents=True, exist_ok=True)
    cmd = [
        "ffmpeg", "-nostdin", "-y",
        "-ss", f"{t_seconds:.2f}", "-i", stream_url,
        "-frames:v", "1", "-q:v", "2", str(out_path),
    ]  # fmt: skip
    try:
        subprocess.run(cmd, capture_output=True, text=True, timeout=FFMPEG_TIMEOUT, check=True)
    except (subprocess.CalledProcessError, subprocess.TimeoutExpired):
        return False
    return out_path.exists()


def _match_label(raw: str, aliases: dict[str, str], floor: int) -> tuple[str | None, int]:
    """Roster-match an OCR'd label, normalizing the no-match case to ``(None, 0)``."""
    match = zoomlabels.match_roster(raw, aliases, floor=floor)
    return match if match else (None, 0)


def read_frame_evidence(
    frame_path: Path, t_seconds: float, aliases: dict[str, str], floor: int
) -> zoomlabels.FrameEvidence:
    """Read one saved frame: try the gallery active-speaker tile, else the full-frame label.

    A gallery frame with no detected highlight yields no evidence — reading its bottom-left
    would pick up a non-speaking tile's label. Full-frame OCR shorter than
    ``MIN_FULLFRAME_ALPHA`` letters is discarded as noise (e.g. a room camera).
    """
    with Image.open(frame_path) as opened:
        img = opened.convert("RGB")
    path = str(frame_path)
    tile = zoomlabels.detect_active_tile(img)
    if tile is not None:
        raw = zoomlabels.read_tile_label(img, tile)
        slug, score = _match_label(raw, aliases, floor)
        return zoomlabels.FrameEvidence(t_seconds, path, tile, raw, slug, score, mode="tile")
    if zoomlabels.looks_like_gallery(img):
        return zoomlabels.FrameEvidence(t_seconds, path, None, "", None, 0, mode="none")
    raw = zoomlabels.read_fullframe_label(img)
    if sum(ch.isalpha() for ch in raw) < MIN_FULLFRAME_ALPHA:
        return zoomlabels.FrameEvidence(t_seconds, path, None, raw, None, 0, mode="none")
    slug, score = _match_label(raw, aliases, floor)
    return zoomlabels.FrameEvidence(t_seconds, path, None, raw, slug, score, mode="fullframe")


def probe_cluster(
    stream_url: str,
    turns: list[dict[str, Any]],
    aliases: dict[str, str],
    args: argparse.Namespace,
    receipt_dir: Path,
    video_id: str,
) -> tuple[list[zoomlabels.FrameEvidence], tuple[str, list[str]] | None]:
    """Grab and read one cluster's sampled frames, then take a cluster verdict.

    Frames already on disk are reused (an interrupted run resumes without
    re-fetching); a corrupt cached frame is dropped so it re-grabs next run.
    """
    frames: list[zoomlabels.FrameEvidence] = []
    for t_seconds in choose_turn_midpoints(turns, args.frames_per_cluster):
        frame_path = receipt_dir / f"{video_id}_{int(round(t_seconds))}.jpg"
        cached = frame_path.is_file() and frame_path.stat().st_size > 0
        if cached or grab_frame(stream_url, t_seconds, frame_path):
            try:
                frames.append(read_frame_evidence(frame_path, t_seconds, aliases, args.floor))
                continue
            except OSError:
                frame_path.unlink(missing_ok=True)
        frames.append(zoomlabels.FrameEvidence(t_seconds, str(frame_path), None, "", None, 0))
    verdict = zoomlabels.cluster_verdict(frames, min_agree=args.min_agree)
    return frames, verdict


def probe_document(
    client: Any,
    doc: dict[str, Any],
    members: list[Any],
    args: argparse.Namespace,
    receipt_dir: Path,
    requested_clusters: list[str] | None,
) -> dict[str, Any]:
    """Probe every target cluster of one transcript; returns the evidence record."""
    result: dict[str, Any] = {
        "doc_id": doc["id"],
        "video_id": doc.get("video_id"),
        "meeting_date": doc.get("meeting_date"),
        "clusters": [],
    }
    if not doc.get("video_id"):
        result["skipped"] = "no video_id"
        return result

    turns = db.get_diarization_turns(client, doc["id"])
    identities = db.get_speaker_identities(client, doc["id"])
    aliases = build_alias_map(members_active_on(members, doc.get("meeting_date")))
    targets = requested_clusters or select_target_clusters(
        turns, identities, min_speech=args.min_speech
    )
    turns_by_cluster = group_turns(turns)

    try:
        stream_url = resolve_stream_url(doc["video_id"], args.format, args.proxy)
    except IngestError as exc:
        result["skipped"] = str(exc)
        return result

    for cluster in targets:
        cluster_turns = turns_by_cluster.get(cluster, [])
        if not cluster_turns:
            continue
        frames, verdict = probe_cluster(
            stream_url, cluster_turns, aliases, args, receipt_dir, doc["video_id"]
        )
        result["clusters"].append(_cluster_record(cluster, frames, verdict))
    _flag_feed_labels(result["clusters"], args.max_clusters_per_slug)
    return result


def _probe_with_retry(
    client: Any,
    doc: dict[str, Any],
    members: list[Any],
    args: argparse.Namespace,
    receipt_dir: Path,
    requested_clusters: list[str] | None,
) -> dict[str, Any]:
    """Probe one document, riding out transient failures; a persistent one skips the doc.

    The failure universe spans httpx timeouts, postgrest errors, and stream/OCR
    surprises — any of them on ONE document must not abort an hours-long sweep,
    so this deliberately catches everything and records the error as evidence.
    """
    for attempt in range(DOC_RETRIES + 1):
        try:
            return probe_document(client, doc, members, args, receipt_dir, requested_clusters)
        except Exception as exc:  # noqa: BLE001
            if attempt == DOC_RETRIES:
                return {
                    "doc_id": doc["id"],
                    "video_id": doc.get("video_id"),
                    "meeting_date": doc.get("meeting_date"),
                    "clusters": [],
                    "skipped": f"error after {DOC_RETRIES + 1} attempts: {exc}",
                }
            time.sleep(RETRY_BACKOFF_SECONDS * (attempt + 1))
    raise AssertionError("unreachable")


def _cluster_record(
    cluster: str,
    frames: list[zoomlabels.FrameEvidence],
    verdict: tuple[str, list[str]] | None,
) -> dict[str, Any]:
    return {
        "cluster_label": cluster,
        "verdict": verdict[0] if verdict else None,
        "supporting_frames": verdict[1] if verdict else [],
        "feed_label": False,
        "frames": [asdict(frame) for frame in frames],
    }


def _flag_feed_labels(cluster_records: list[dict[str, Any]], max_clusters_per_slug: int) -> None:
    """Mark clusters whose verdict is a document-wide feed/account label (not a speaker)."""
    verdict_map = {record["cluster_label"]: record["verdict"] for record in cluster_records}
    feed = zoomlabels.feed_label_slugs(verdict_map, max_clusters_per_slug=max_clusters_per_slug)
    for record in cluster_records:
        record["feed_label"] = record["verdict"] in feed


def _verdict_label(cluster_record: dict[str, Any]) -> str:
    """Terse status for the stdout table: a slug, or why there is no verdict."""
    if cluster_record["verdict"]:
        if cluster_record.get("feed_label"):
            return f"FEED({cluster_record['verdict']})"  # streaming account, not the speaker
        return cluster_record["verdict"]
    frames = cluster_record["frames"]
    matched = {frame["matched_slug"] for frame in frames if frame["matched_slug"]}
    if len(matched) > 1:
        return "CONFLICT"
    if matched:
        return "NONE"
    if not any(frame["mode"] != "none" for frame in frames):
        return "no-read"  # nothing legible: no tile in gallery frames, or blank OCR
    return "NONE"


def _print_document_table(result: dict[str, Any]) -> None:
    date = result.get("meeting_date") or "?"
    print(f"\ndoc {result['doc_id']} ({date}) video={result.get('video_id')}")
    if result.get("skipped"):
        print(f"  skipped: {result['skipped']}")
        return
    if not result["clusters"]:
        print("  (no target clusters)")
        return
    for record in result["clusters"]:
        frames = record["frames"]
        tiles = sum(1 for frame in frames if frame["mode"] == "tile")
        fullframe = sum(1 for frame in frames if frame["mode"] == "fullframe")
        matched = sum(1 for frame in frames if frame["matched_slug"])
        label = _verdict_label(record)
        print(
            f"  {record['cluster_label']:<12} {label:<20} "
            f"tile={tiles} ff={fullframe} matched={matched}/{len(frames)}"
        )


def load_documents(client: Any, entity_id: int, args: argparse.Namespace) -> list[dict[str, Any]]:
    """Target transcript documents for the run (from --doc-id or --all-video-docs)."""
    if args.doc_id:
        return (
            client.table("documents").select(_DOC_COLUMNS).in_("id", args.doc_id).execute().data
            or []
        )
    return db.fetch_all_rows(
        lambda: (
            client.table("documents")
            .select(_DOC_COLUMNS)
            .eq("entity_id", entity_id)
            .eq("document_type", TRANSCRIPT_TYPE)
            .not_.is_("video_id", "null")
        )
    )


def _config_snapshot(args: argparse.Namespace, entity_id: int) -> dict[str, Any]:
    return {
        "state": args.state,
        "place": args.place,
        "body": args.body,
        "entity_id": entity_id,
        "frames_per_cluster": args.frames_per_cluster,
        "format": args.format,
        "floor": args.floor,
        "min_agree": args.min_agree,
        "min_speech": args.min_speech,
        "max_clusters_per_slug": args.max_clusters_per_slug,
        "clusters": args.clusters,
    }


def _parse_cluster_filter(raw: str | None) -> list[str] | None:
    if not raw:
        return None
    return [label.strip() for label in raw.split(",") if label.strip()]


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Read Zoom on-screen speaker names off meeting-video frames (read-only).",
    )
    parser.add_argument("--state", required=True, help="place state slug, e.g. mo")
    parser.add_argument("--place", required=True, help="place slug, e.g. clayton")
    parser.add_argument("--body", required=True, help="body_slug, e.g. schools")
    docs = parser.add_mutually_exclusive_group(required=True)
    docs.add_argument(
        "--doc-id", type=int, action="append", dest="doc_id", help="document id (repeatable)"
    )
    docs.add_argument(
        "--all-video-docs",
        action="store_true",
        help="probe every transcript for the body that has a video_id",
    )
    parser.add_argument(
        "--clusters", help="comma-separated cluster filter, e.g. SPEAKER_04,SPEAKER_07"
    )
    parser.add_argument("--frames-per-cluster", type=int, default=DEFAULT_FRAMES_PER_CLUSTER)
    parser.add_argument("--format", default=DEFAULT_FORMAT, help="yt-dlp -f expression")
    parser.add_argument("--out-dir", type=Path, default=DEFAULT_OUT_DIR)
    parser.add_argument("--proxy", help="SOCKS proxy for yt-dlp (WARP egress in CI)")
    parser.add_argument(
        "--floor", type=int, default=DEFAULT_FLOOR, help="rapidfuzz match floor 0-100"
    )
    parser.add_argument("--min-agree", type=int, default=DEFAULT_MIN_AGREE)
    parser.add_argument("--min-speech", type=float, default=DEFAULT_MIN_SPEECH_SECONDS)
    parser.add_argument(
        "--max-clusters-per-slug",
        type=int,
        default=DEFAULT_MAX_CLUSTERS_PER_SLUG,
        help="a slug winning more than this many clusters is flagged as a room-feed label",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    config = load_config()
    if not config.supabase_service_key:
        raise ActaluxError(
            "ACTALUX_SUPABASE_SERVICE_KEY is required (below-gate identities are service-only)"
        )
    client = db.get_client(config.supabase_url, config.supabase_service_key)

    entity = db.get_entity_by_path(client, args.state, args.place, args.body)
    if entity is None:
        raise ActaluxError(f"no such body: {args.state}/{args.place}/{args.body}")
    entity_id = entity["id"]
    members = members_for_entity(client, entity_id)
    documents = load_documents(client, entity_id, args)
    if not documents:
        print("no target documents", file=sys.stderr)
        return 1

    receipt_dir = args.out_dir / f"{args.state}_{args.place}_{args.body}"
    receipt_dir.mkdir(parents=True, exist_ok=True)
    evidence_path = receipt_dir / f"evidence_{datetime.now(UTC).strftime('%Y%m%dT%H%M%SZ')}.json"
    requested_clusters = _parse_cluster_filter(args.clusters)
    run: dict[str, Any] = {"config": _config_snapshot(args, entity_id), "documents": []}
    for doc in documents:
        result = _probe_with_retry(client, doc, members, args, receipt_dir, requested_clusters)
        run["documents"].append(result)
        _print_document_table(result)
        # Checkpoint after every document so a crash mid-sweep loses nothing.
        evidence_path.write_text(json.dumps(run, indent=2))

    print(f"\nevidence -> {evidence_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
