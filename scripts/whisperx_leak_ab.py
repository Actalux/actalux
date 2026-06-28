#!/usr/bin/env python3
"""WhisperX prompt-leakage A/B: can we keep word-level timestamps without the bias
text echoing into the transcript?

For each meeting, transcribe the SAME audio under several WhisperX configs and
measure, per config: bias-echo count (how often the bias string regurgitates into
the output), name fidelity (official surnames from the lexicon), and word count.
The whole point is verbatim integrity for a citation-first record, so the winner
is the config with ZERO echo that still captures names acceptably.

Configs (per codex's analysis — see docs/architecture/speaker-attribution.md):
  B0_prompted  full-sentence initial_prompt (current/leaky baseline)
  B2_clean     no bias; no_repeat_ngram_size=5, repetition_penalty=1.05
  B3_hotwords  short proper-noun hotword list (no initial_prompt); same repeat curbs

Writes per-meeting transcripts + a metrics table under results/whisperx_leak_ab/
for eyeballing. Does NOT touch the production transcription pipeline.

Run (DB creds from Doppler ``mac``; Modal tokens injected from ``actalux``):

    MODAL_TOKEN_ID="$(doppler secrets get MODAL_TOKEN_ID --plain --project actalux --config dev)" \
    MODAL_TOKEN_SECRET="$(doppler secrets get MODAL_TOKEN_SECRET --plain --project actalux --config dev)" \
    doppler run --project mac --config dev -- \
      uv run --group diarization python scripts/whisperx_leak_ab.py --doc-ids 769,665
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
from actalux.graph.store import place_lexicon
from actalux.ingest.bodies import BODIES
from actalux.ingest.youtube import download_audio

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

OUTPUT_DIR = Path("results/whisperx_leak_ab")
AUDIO_DIR = Path("data/audio")
WHISPERX_APP = "actalux-whisperx-eval"


def fetch_document(client, doc_id: int) -> dict:
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


def resolve_body_and_place(client, entity_id: int) -> tuple[str, int | None]:
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


def lexicon_names(client, place_id: int | None) -> tuple[list[str], list[str]]:
    """(full canonical names, surnames) of publishable officials for the place."""
    if not place_id:
        return [], []
    full, surnames = [], set()
    for entry in place_lexicon(client, place_id):
        name = (entry.get("canonical_name") or "").strip()
        if not name:
            continue
        full.append(name)
        surnames.add(name.split()[-1])
    return full, sorted(surnames)


def _norm(s: str) -> str:
    return re.sub(r"\s+", " ", re.sub(r"[^a-z0-9 ]", " ", s.lower())).strip()


def echo_count(text: str, bias_text: str) -> int:
    """How often the bias string regurgitates into the output.

    The echo lands as either the head or tail of the bias string repeated verbatim,
    so we probe both distinctive fragments and take the larger count.
    """
    if not bias_text:
        return 0
    nt = _norm(text)
    words = _norm(bias_text).split()
    if not words:
        return 0
    frags = {" ".join(words[:4]), " ".join(words[-4:])}
    return max(nt.count(f) for f in frags)


def count_names(text: str, surnames: list[str]) -> int:
    low = text.lower()
    return sum(len(re.findall(rf"\b{re.escape(n.lower())}\b", low)) for n in surnames)


def run_config(whisperx_fn, audio_bytes: bytes, cfg: dict) -> dict:
    """One WhisperX pass; return its text + word count for scoring."""
    out = whisperx_fn.remote(
        audio_bytes, cfg["initial_prompt"], cfg["hotwords"], cfg["nrns"], cfg["rp"]
    )
    text = " ".join(s["text"] for s in out["segments"]).strip()
    n_words = sum(len(s["words"]) for s in out["segments"])
    return {"text": text, "words": n_words, "segments": len(out["segments"])}


def process(client, doc_id: int, whisperx_fn) -> list[dict]:
    doc = fetch_document(client, doc_id)
    prompt, place_id = resolve_body_and_place(client, doc["entity_id"])
    full_names, surnames = lexicon_names(client, place_id)
    hotwords = ", ".join(full_names)
    configs = [
        {"key": "B0_prompted", "initial_prompt": prompt, "hotwords": "", "nrns": 0, "rp": 1.0},
        {"key": "B2_clean", "initial_prompt": "", "hotwords": "", "nrns": 5, "rp": 1.05},
        {"key": "B3_hotwords", "initial_prompt": "", "hotwords": hotwords, "nrns": 5, "rp": 1.05},
    ]
    logger.info("doc %s: %s (%s)", doc_id, doc.get("meeting_title"), doc.get("meeting_date"))

    audio = download_audio(doc["video_id"], AUDIO_DIR)
    try:
        audio_bytes = audio.read_bytes()
        results = []
        for cfg in configs:
            try:
                r = run_config(whisperx_fn, audio_bytes, cfg)
            except Exception:
                logger.exception("  %-12s FAILED", cfg["key"])
                results.append({"key": cfg["key"], "text": "", "words": 0,
                                "echo": -1, "name_hits": -1})  # fmt: skip
                continue
            bias = cfg["initial_prompt"] or cfg["hotwords"]
            r |= {
                "key": cfg["key"],
                "echo": echo_count(r["text"], bias),
                "name_hits": count_names(r["text"], surnames),
            }
            results.append(r)
            logger.info("  %-12s echo=%d names=%d words=%d",
                        cfg["key"], r["echo"], r["name_hits"], r["words"])  # fmt: skip
    finally:
        audio.unlink(missing_ok=True)

    _write_report(doc, results)
    return [{"doc_id": doc_id, "meeting_date": doc.get("meeting_date"), **{
        r["key"]: {"echo": r["echo"], "name_hits": r["name_hits"], "words": r["words"]}
        for r in results
    }}]


def _write_report(doc: dict, results: list[dict]) -> None:
    lines = [
        f"# WhisperX leakage A/B — doc {doc['id']}",
        "",
        f"- **Meeting:** {doc.get('meeting_title')} ({doc.get('meeting_date')})",
        f"- **Video:** https://www.youtube.com/watch?v={doc['video_id']}",
        "",
        "| config | bias echoes | name hits | words |",
        "|---|---:|---:|---:|",
    ]
    for r in results:
        lines.append(f"| {r['key']} | {r['echo']} | {r['name_hits']} | {r['words']} |")
    lines += ["", "_bias echoes = times the prompt/hotword string regurgitated into the",
              "transcript (0 = clean verbatim record). Read names with the transcripts._", ""]
    for r in results:
        lines += ["", "---", "", f"## {r['key']} — transcript", "", r["text"], ""]
    (OUTPUT_DIR / f"{doc['id']}_{doc.get('meeting_date') or 'undated'}_leak.md").write_text(
        "\n".join(lines)
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="WhisperX prompt-leakage A/B.")
    parser.add_argument("--doc-ids", required=True, help="comma-separated transcript doc ids")
    args = parser.parse_args()
    doc_ids = [int(x) for x in args.doc_ids.split(",") if x.strip()]

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    AUDIO_DIR.mkdir(parents=True, exist_ok=True)
    cfg = load_config()
    client = get_client(cfg.supabase_url, cfg.supabase_key)
    whisperx_fn = modal.Function.from_name(WHISPERX_APP, "transcribe_align_remote")

    metrics: list[dict] = []
    for doc_id in doc_ids:
        metrics.extend(process(client, doc_id, whisperx_fn))
    (OUTPUT_DIR / "summary.json").write_text(json.dumps(metrics, indent=2))
    logger.info("wrote %d report(s) + summary.json to %s", len(metrics), OUTPUT_DIR)


if __name__ == "__main__":
    main()
