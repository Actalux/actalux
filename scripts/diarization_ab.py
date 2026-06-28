#!/usr/bin/env python3
"""A/B harness for speaker attribution over meeting transcripts.

Two candidate paths, judged on the same meetings:

  Arm O (overlay): keep the existing ingested Whisper transcript (verbatim,
    name-biased) and attach a speaker cluster to each chunk by diarization
    overlap. Zero corpus disruption — text and chunk ids are untouched.

  Arm X (WhisperX): re-transcribe + align + diarize from scratch (word-level
    speaker labels, but new text). Adopting it means re-ingesting the corpus.
    [added in a follow-up — this run does Arm O.]

For each meeting it downloads the audio once, runs the requested arm(s), and
writes a per-meeting markdown report plus a metrics JSON under
``results/diarization_ab/`` so the speaker labels can be eyeballed against the
actual video before deciding the going-forward path.

Run (DB creds from Doppler ``mac``; Modal tokens injected from ``actalux``). The
``$(doppler secrets get ...)`` form keeps the token values out of the transcript
(flags inline — zsh doesn't word-split an unquoted flags variable):

    MODAL_TOKEN_ID="$(doppler secrets get MODAL_TOKEN_ID --plain --project actalux --config dev)" \
    MODAL_TOKEN_SECRET="$(doppler secrets get MODAL_TOKEN_SECRET --plain --project actalux --config dev)" \
    doppler run --project mac --config dev -- \
      uv run --group diarization python scripts/diarization_ab.py \
      --doc-ids 769,665,1898,1519,1780
"""  # noqa: E501

from __future__ import annotations

import argparse
import json
import logging
from dataclasses import dataclass
from pathlib import Path

from actalux.config import load_config
from actalux.db import get_client
from actalux.diarization.align import assign_clusters, chunk_windows
from actalux.diarization.backend import SpeakerTimeline
from actalux.diarization.modal_runner import ModalRunner
from actalux.ingest.youtube import download_audio

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

OUTPUT_DIR = Path("results/diarization_ab")
AUDIO_DIR = Path("data/audio")  # transient; audio is deleted after each meeting


@dataclass(frozen=True)
class OverlayResult:
    """Arm O output for one meeting: the diarization plus the per-chunk labels."""

    timeline: SpeakerTimeline
    chunk_speaker: dict[int, str]  # chunk_id -> cluster label (omits unattributed)
    n_chunks: int


def fetch_document(client, doc_id: int) -> dict:
    """The transcript document row, or raise if it isn't a usable YouTube transcript."""
    rows = (
        client.table("documents")
        .select("id, video_id, meeting_date, meeting_title, source_portal, document_type")
        .eq("id", doc_id)
        .execute()
        .data
    )
    if not rows:
        raise SystemExit(f"doc {doc_id} not found")
    doc = rows[0]
    if not doc.get("video_id"):
        raise SystemExit(f"doc {doc_id} has no video_id; cannot fetch audio")
    return doc


def fetch_chunks(client, doc_id: int) -> list[dict]:
    """A document's chunks in order: ``id, content, chunk_index, start_seconds``."""
    return (
        client.table("chunks")
        .select("id, content, chunk_index, start_seconds")
        .eq("document_id", doc_id)
        .order("chunk_index")
        .execute()
        .data
    )


def run_overlay(audio_path: Path, chunks: list[dict]) -> OverlayResult:
    """Diarize the audio and assign each timed chunk its max-overlap speaker cluster."""
    timeline = ModalRunner().run(str(audio_path))
    windows = chunk_windows(chunks)
    chunk_speaker = assign_clusters(timeline, windows)
    return OverlayResult(timeline=timeline, chunk_speaker=chunk_speaker, n_chunks=len(chunks))


def _fmt_ts(seconds: float | int | None) -> str:
    """``mm:ss`` from a second offset (``--`` when untimed)."""
    if seconds is None:
        return "  --  "
    s = int(seconds)
    return f"{s // 60:02d}:{s % 60:02d}"


def render_overlay_md(doc: dict, chunks: list[dict], result: OverlayResult) -> str:
    """A reviewable markdown report: metrics header + speaker-annotated transcript."""
    n_attr = len(result.chunk_speaker)
    pct = (100 * n_attr / result.n_chunks) if result.n_chunks else 0.0
    lines = [
        f"# Arm O (Whisper+pyannote overlay) — doc {doc['id']}",
        "",
        f"- **Meeting:** {doc.get('meeting_title') or '(untitled)'} ({doc.get('meeting_date')})",
        f"- **Video:** https://www.youtube.com/watch?v={doc['video_id']}",
        f"- **Speakers detected:** {result.timeline.num_speakers}",
        f"- **Diarization turns:** {len(result.timeline.turns)}",
        f"- **Chunks attributed:** {n_attr}/{result.n_chunks} ({pct:.0f}%)",
        f"- **Model:** {result.timeline.source_model}",
        "",
        "_Verbatim transcript text below is unchanged; only a speaker label is overlaid._",
        "",
        "---",
        "",
    ]
    for c in chunks:
        label = result.chunk_speaker.get(c["id"], "—")
        ts = _fmt_ts(c.get("start_seconds"))
        lines.append(f"**[{label}]** `{ts}` {(c.get('content') or '').strip()}")
        lines.append("")
    return "\n".join(lines)


def process(client, doc_id: int) -> dict:
    """Run Arm O for one meeting; write its report + return its metrics row."""
    doc = fetch_document(client, doc_id)
    chunks = fetch_chunks(client, doc_id)
    logger.info("doc %s: %s (%s) — %d chunks", doc_id, doc.get("meeting_title"),
                doc.get("meeting_date"), len(chunks))  # fmt: skip

    audio = download_audio(doc["video_id"], AUDIO_DIR)
    try:
        result = run_overlay(audio, chunks)
    finally:
        audio.unlink(missing_ok=True)

    md = render_overlay_md(doc, chunks, result)
    stem = f"{doc_id}_{doc.get('meeting_date') or 'undated'}_overlay"
    (OUTPUT_DIR / f"{stem}.md").write_text(md)
    n_attr = len(result.chunk_speaker)
    logger.info("  -> %d speakers, %d/%d chunks attributed",
                result.timeline.num_speakers, n_attr, result.n_chunks)  # fmt: skip
    return {
        "doc_id": doc_id,
        "meeting_date": doc.get("meeting_date"),
        "meeting_title": doc.get("meeting_title"),
        "video_id": doc["video_id"],
        "num_speakers": result.timeline.num_speakers,
        "turns": len(result.timeline.turns),
        "n_chunks": result.n_chunks,
        "n_attributed": n_attr,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Speaker-attribution A/B (Arm O: overlay).")
    parser.add_argument(
        "--doc-ids",
        required=True,
        help="comma-separated transcript document ids (e.g. 769,665,1898,1519,1780)",
    )
    args = parser.parse_args()
    doc_ids = [int(x) for x in args.doc_ids.split(",") if x.strip()]

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    AUDIO_DIR.mkdir(parents=True, exist_ok=True)
    cfg = load_config()
    client = get_client(cfg.supabase_url, cfg.supabase_key)

    metrics: list[dict] = []
    for doc_id in doc_ids:
        metrics.append(process(client, doc_id))

    (OUTPUT_DIR / "summary.json").write_text(json.dumps(metrics, indent=2))
    logger.info("wrote %d report(s) + summary.json to %s", len(metrics), OUTPUT_DIR)
    print(f"\n{'doc':>5} {'date':<11} {'spk':>3} {'turns':>5} {'attributed':>12}  title")
    for m in metrics:
        print(f"{m['doc_id']:>5} {str(m['meeting_date']):<11} {m['num_speakers']:>3} "
              f"{m['turns']:>5} {m['n_attributed']:>5}/{m['n_chunks']:<6} "
              f"{(m['meeting_title'] or '')[:38]}")  # fmt: skip


if __name__ == "__main__":
    main()
