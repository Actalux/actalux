#!/usr/bin/env python3
"""Head-to-head A/B for the going-forward speaker-attribution pipeline.

Both paths attribute at speaker-turn granularity (chunk-level overlay was shown
too coarse) and share the SAME pyannote turns (``actalux-diarization``), so the
only variable is the transcription engine + timestamp granularity:

  Path A — Groq ``whisper-large-v3`` (hosted, free, name-prompted) → segment-level
    timestamps. Each Whisper segment is assigned to its max-overlap pyannote turn,
    then consecutive same-speaker segments merge into speaker blocks.

  Path B — WhisperX (faster-whisper ``large-v3`` + wav2vec2 forced alignment, same
    name prompt) → word-level timestamps. Each WORD is assigned to its max-overlap
    turn, then consecutive same-speaker words merge into speaker blocks.

Per meeting it writes a markdown report (metrics + name-fidelity table + both
speaker-labeled transcripts) under ``results/diarization_headtohead/`` so the two
can be judged against the video before picking the going-forward path.

Prereqs: both Modal apps deployed (``modal deploy src/actalux/diarization/modal_runner.py``
and ``modal deploy scripts/whisperx_modal.py``).

Run (DB creds from Doppler ``mac``; Modal tokens injected from ``actalux``):

    MODAL_TOKEN_ID="$(doppler secrets get MODAL_TOKEN_ID --plain --project actalux --config dev)" \
    MODAL_TOKEN_SECRET="$(doppler secrets get MODAL_TOKEN_SECRET --plain --project actalux --config dev)" \
    doppler run --project mac --config dev -- \
      uv run --group diarization python scripts/diarization_headtohead.py --doc-ids 769,665
"""  # noqa: E501

from __future__ import annotations

import argparse
import json
import logging
import re
from pathlib import Path

import modal

from actalux.config import load_config
from actalux.db import get_client
from actalux.diarization.backend import SpeakerTurn
from actalux.diarization.modal_runner import ModalRunner
from actalux.graph.store import place_lexicon
from actalux.ingest.bodies import BODIES
from actalux.ingest.transcribe import transcribe_audio
from actalux.ingest.youtube import download_audio

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

OUTPUT_DIR = Path("results/diarization_headtohead")
AUDIO_DIR = Path("data/audio")
WHISPERX_APP = "actalux-whisperx-eval"


def fetch_document(client, doc_id: int) -> dict:
    """The transcript document row; abort if not a usable YouTube transcript."""
    rows = (
        client.table("documents")
        .select("id, video_id, meeting_date, meeting_title, entity_id")
        .eq("id", doc_id)
        .execute()
        .data
    )
    if not rows or not rows[0].get("video_id"):
        raise SystemExit(f"doc {doc_id} missing or has no video_id")
    return rows[0]


def resolve_prompt_and_place(client, entity_id: int) -> tuple[str, int | None]:
    """Map a doc's entity to its body name-prompt and place_id (no hardcoded place)."""
    ent = client.table("entities").select("body_slug, place_id").eq("id", entity_id).execute().data
    if not ent:
        return "", None
    place_id = ent[0]["place_id"]
    pl = client.table("places").select("state, slug").eq("id", place_id).execute().data
    if not pl:
        return "", place_id
    path = f"{pl[0]['state']}/{pl[0]['slug']}/{ent[0]['body_slug']}"
    for body in BODIES.values():
        if body.entity_path == path:
            return body.transcribe_prompt, place_id
    return "", place_id


def label_units(
    units: list[tuple[float, float, str]], turns: list[SpeakerTurn]
) -> list[dict]:
    """Assign each (start, end, text) unit to its max-overlap turn, then merge runs.

    Consecutive units sharing a cluster collapse into one speaker block; this is the
    verbatim-preserving re-segmentation (text is concatenated, never altered).
    """
    blocks: list[dict] = []
    for start, end, text in units:
        best_label: str | None = None
        best_overlap = 0.0
        for t in turns:
            overlap = min(end, t.end_s) - max(start, t.start_s)
            if overlap > best_overlap:
                best_overlap = overlap
                best_label = t.cluster_label
        text = text.strip()
        if not text:
            continue
        if blocks and blocks[-1]["cluster"] == best_label:
            blocks[-1]["end"] = end
            blocks[-1]["text"] = f"{blocks[-1]['text']} {text}".strip()
        else:
            blocks.append({"cluster": best_label, "start": start, "end": end, "text": text})
    return blocks


def surnames_from_lexicon(client, place_id: int | None) -> list[str]:
    """Surnames of publishable officials for the place (name-fidelity probe terms)."""
    if not place_id:
        return []
    names: set[str] = set()
    for entry in place_lexicon(client, place_id):
        parts = (entry.get("canonical_name") or "").split()
        if parts:
            names.add(parts[-1])
    return sorted(names)


def count_names(text: str, names: list[str]) -> dict[str, int]:
    """Whole-word, case-insensitive occurrence count of each name in the text."""
    low = text.lower()
    return {n: len(re.findall(rf"\b{re.escape(n.lower())}\b", low)) for n in names}


def _ts(seconds: float) -> str:
    s = int(seconds)
    return f"{s // 60:02d}:{s % 60:02d}"


def _render_blocks(blocks: list[dict]) -> list[str]:
    return [f"**[{b['cluster']}]** `{_ts(b['start'])}` {b['text']}\n" for b in blocks]


def render_report(
    doc: dict,
    turns: list[SpeakerTurn],
    a_blocks: list[dict],
    b_blocks: list[dict],
    a_text: str,
    b_text: str,
    names: list[str],
) -> str:
    n_clusters = len({t.cluster_label for t in turns})
    a_counts = count_names(a_text, names)
    b_counts = count_names(b_text, names)
    lines = [
        f"# A/B: Path A (Groq+pyannote) vs Path B (WhisperX+pyannote) — doc {doc['id']}",
        "",
        f"- **Meeting:** {doc.get('meeting_title')} ({doc.get('meeting_date')})",
        f"- **Video:** https://www.youtube.com/watch?v={doc['video_id']}",
        f"- **pyannote turns:** {len(turns)} across {n_clusters} clusters (shared by both)",
        f"- **Path A speaker blocks:** {len(a_blocks)} · **chars:** {len(a_text)}",
        f"- **Path B speaker blocks:** {len(b_blocks)} · **chars:** {len(b_text)}",
        "",
        "## Name fidelity (official surnames, whole-word counts)",
        "",
        "| surname | Path A (Groq) | Path B (WhisperX) |",
        "|---|---:|---:|",
    ]
    for n in names:
        if a_counts[n] or b_counts[n]:
            lines.append(f"| {n} | {a_counts[n]} | {b_counts[n]} |")
    lines += [
        f"| **total** | **{sum(a_counts.values())}** | **{sum(b_counts.values())}** |",
        "",
        "_Higher isn't automatically better (a name may be correctly absent); read with",
        "the transcripts. Same words, same pyannote turns — differences are the engine._",
        "",
        "---",
        "",
        "## Path A — Groq Whisper, segment-level turns",
        "",
    ]
    lines += _render_blocks(a_blocks)
    lines += ["", "---", "", "## Path B — WhisperX, word-level turns", ""]
    lines += _render_blocks(b_blocks)
    return "\n".join(lines)


def process(client, doc_id: int, whisperx_fn, names_cache: dict) -> dict:
    """Run both paths for one meeting; write the report; return metrics."""
    doc = fetch_document(client, doc_id)
    prompt, place_id = resolve_prompt_and_place(client, doc["entity_id"])
    if place_id not in names_cache:
        names_cache[place_id] = surnames_from_lexicon(client, place_id)
    names = names_cache[place_id]
    logger.info("doc %s: %s (%s)", doc_id, doc.get("meeting_title"), doc.get("meeting_date"))

    audio = download_audio(doc["video_id"], AUDIO_DIR)
    try:
        cfg = load_config()
        # Shared pyannote turns.
        turns = ModalRunner().run(str(audio)).turns
        # Path A: Groq segments.
        transcript = transcribe_audio(
            audio, cfg.groq_api_key, model=cfg.transcribe_model,
            base_url=cfg.transcribe_base_url, prompt=prompt,
        )  # fmt: skip
        a_units = [(s.start, s.end, s.text) for s in transcript.segments]
        a_text = transcript.text
        # Path B: WhisperX words.
        wx = whisperx_fn.remote(audio.read_bytes(), prompt)
        b_units = [
            (w["start"], w["end"], w["word"])
            for s in wx["segments"]
            for w in s["words"]
        ]
        b_text = " ".join(s["text"] for s in wx["segments"]).strip()
    finally:
        audio.unlink(missing_ok=True)

    a_blocks = label_units(a_units, turns)
    b_blocks = label_units(b_units, turns)
    report = render_report(doc, turns, a_blocks, b_blocks, a_text, b_text, names)
    stem = f"{doc_id}_{doc.get('meeting_date') or 'undated'}_ab"
    (OUTPUT_DIR / f"{stem}.md").write_text(report)
    a_counts, b_counts = count_names(a_text, names), count_names(b_text, names)
    logger.info(
        "  A: %d blocks, %d chars, %d names | B: %d blocks, %d chars, %d names",
        len(a_blocks), len(a_text), sum(a_counts.values()),
        len(b_blocks), len(b_text), sum(b_counts.values()),
    )  # fmt: skip
    return {
        "doc_id": doc_id,
        "meeting_date": doc.get("meeting_date"),
        "meeting_title": doc.get("meeting_title"),
        "clusters": len({t.cluster_label for t in turns}),
        "a_blocks": len(a_blocks),
        "b_blocks": len(b_blocks),
        "a_chars": len(a_text),
        "b_chars": len(b_text),
        "a_name_hits": sum(a_counts.values()),
        "b_name_hits": sum(b_counts.values()),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Speaker-attribution A/B: Groq vs WhisperX.")
    parser.add_argument("--doc-ids", required=True, help="comma-separated transcript doc ids")
    args = parser.parse_args()
    doc_ids = [int(x) for x in args.doc_ids.split(",") if x.strip()]

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    AUDIO_DIR.mkdir(parents=True, exist_ok=True)
    cfg = load_config()
    client = get_client(cfg.supabase_url, cfg.supabase_key)
    whisperx_fn = modal.Function.from_name(WHISPERX_APP, "transcribe_align_remote")

    names_cache: dict = {}
    metrics = [process(client, d, whisperx_fn, names_cache) for d in doc_ids]
    (OUTPUT_DIR / "summary.json").write_text(json.dumps(metrics, indent=2))
    logger.info("wrote %d report(s) + summary.json to %s", len(metrics), OUTPUT_DIR)
    hdr = f"\n{'doc':>5} {'date':<11} {'clu':>3} {'A blk':>6} {'B blk':>6} {'A nm':>5} {'B nm':>5}"
    print(hdr)
    for m in metrics:
        print(
            f"{m['doc_id']:>5} {str(m['meeting_date']):<11} {m['clusters']:>3} "
            f"{m['a_blocks']:>6} {m['b_blocks']:>6} {m['a_name_hits']:>5} {m['b_name_hits']:>5}"
        )


if __name__ == "__main__":
    main()
