"""Discover proper-name manglings in Whisper transcripts and propose corrections.

For each transcript (a ``documents`` row, source_portal='youtube'), this aligns the ASR
text against the authoritative record for the *same* meeting — that meeting's agenda and
minutes plus the place's roster/lexicon of officials — and proposes ``mangled ->
canonical`` corrections. Every proposed canonical is a name that appears verbatim in one
of those sources (we never invent a spelling), and every mangled form appears verbatim in
the transcript. See ``src/actalux/glossary/discovery.py`` for the matcher.

Output is a review file (the human gate): a clear ``high`` bucket and a ``review`` bucket,
each row carrying its evidence (score, the meetings it was seen in, transcript snippets,
and where the canonical is documented). Nothing is written to the DB unless ``--apply``,
which inserts only the ``high`` bucket with provenance='auto' (distinct from the curated
'asr' rows, so they are never blind-merged and a curated re-seed leaves them intact).

Run (prefix with `doppler run --project mac --config dev --`):
  uv run python scripts/discover_corrections.py --backfill          # all transcripts -> review file
  uv run python scripts/discover_corrections.py --video-id <ID>     # one meeting
  uv run python scripts/discover_corrections.py --backfill --apply  # also write the 'high' bucket
  uv run python scripts/discover_corrections.py --manifest <m> --apply  # new only
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from collections import defaultdict
from datetime import date
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parent))

from actalux.config import load_config  # noqa: E402
from actalux.db import (  # noqa: E402
    fetch_all_rows,
    get_client,
    get_document_chunks,
    get_meeting_records,
    get_name_corrections,
    get_place_by_path,
    insert_rows_resilient,
)
from actalux.glossary.discovery import (  # noqa: E402
    build_vocabulary,
    context_snippet,
    find_manglings,
    norm_key,
)
from actalux.graph.store import place_lexicon  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

DEFAULT_PLACE = "mo/clayton"
AUTH_TYPES = ["agenda", "minutes"]


def _doc_text(client, doc: dict[str, Any]) -> str:
    """Full text of a document: the stored ``content``, or its chunks reassembled."""
    content = (doc.get("content") or "").strip()
    if content:
        return content
    chunks = get_document_chunks(client, doc["id"])
    return "\n\n".join(c["content"] for c in chunks)


def _manifest_video_ids(path: Path) -> set[str]:
    """Video ids in a transcription manifest — the meetings a pipeline run just produced."""
    entries = json.loads(path.read_text(encoding="utf-8"))
    return {e["video_id"] for e in entries if e.get("video_id")}


def _aggregate(per_meeting: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Merge per-transcript manglings into one proposal each, gathering cross-meeting evidence.

    A mangling seen across several meetings with one consistent canonical is the strongest
    signal; a mangling that maps to *different* canonicals in different meetings is
    ambiguous and forced to ``review`` rather than guessing which spelling wins.
    """
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for hit in per_meeting:
        grouped[hit["mangled"]].append(hit)

    proposals: list[dict[str, Any]] = []
    for mangled, hits in grouped.items():
        canonicals = {h["canonical"] for h in hits}
        best = max(hits, key=lambda h: h["score"])
        consistent = len(canonicals) == 1
        any_high = any(h["confidence"] == "high" for h in hits)
        confidence = "high" if (any_high and consistent) else "review"
        proposals.append(
            {
                "mangled": mangled,
                "canonical": best["canonical"],
                "category": best["category"],
                "confidence": confidence,
                "score": round(best["score"], 3),
                "meetings": len({h["video_id"] or h["meeting_date"] for h in hits}),
                "occurrences": sum(h["occurrences"] for h in hits),
                "source": best["source"],
                "rival_canonicals": sorted(canonicals - {best["canonical"]}) or None,
                "evidence": [
                    {
                        "video_id": h["video_id"],
                        "meeting_date": h["meeting_date"],
                        "body": h["body"],
                        "snippet": h["snippet"],
                    }
                    for h in sorted(hits, key=lambda h: h["score"], reverse=True)[:3]
                ],
            }
        )
    proposals.sort(key=lambda p: (p["confidence"] != "high", -p["score"]))
    return proposals


def _select_transcripts(client, entity_ids: list[int], args) -> list[dict[str, Any]]:
    def query():
        q = (
            client.table("documents")
            .select("id,entity_id,meeting_date,video_id,content")
            .eq("document_type", "transcript")
            .is_("replaces_id", "null")
            .in_("entity_id", entity_ids)
        )
        if args.video_id:
            q = q.eq("video_id", args.video_id)
        if args.meeting_date:
            q = q.eq("meeting_date", args.meeting_date)
        return q

    rows = fetch_all_rows(query)
    if args.manifest:
        ids = _manifest_video_ids(args.manifest)
        rows = [r for r in rows if r.get("video_id") in ids]
    if args.limit:
        rows = rows[: args.limit]
    return rows


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--place", default=DEFAULT_PLACE, help="state/place, e.g. mo/clayton")
    parser.add_argument(
        "--backfill", action="store_true", help="scan every transcript for the place"
    )
    parser.add_argument("--video-id", help="scan one meeting by YouTube video id")
    parser.add_argument("--meeting-date", help="scan one meeting date (YYYY-MM-DD)")
    parser.add_argument(
        "--manifest", type=Path, help="scan only the meetings in a transcription manifest"
    )
    parser.add_argument("--body", help="restrict to one body_slug (e.g. council)")
    parser.add_argument("--limit", type=int, help="cap transcripts scanned")
    parser.add_argument(
        "--out",
        type=Path,
        help="proposals JSON path (default: scripts/corrections/proposals_<state>_<place>.json)",
    )
    parser.add_argument(
        "--apply", action="store_true", help="insert the 'high' bucket (provenance='auto')"
    )
    args = parser.parse_args()

    if not (args.backfill or args.video_id or args.meeting_date or args.manifest):
        raise SystemExit("Choose a scope: --backfill, --video-id, --meeting-date, or --manifest.")

    config = load_config()
    if not config.supabase_service_key:
        logger.warning("No ACTALUX_SUPABASE_SERVICE_KEY; reading via the anon key may under-read.")
    if args.apply and not config.supabase_service_key:
        raise SystemExit("ACTALUX_SUPABASE_SERVICE_KEY is required to --apply.")
    client = get_client(config.supabase_url, config.supabase_service_key or config.supabase_key)

    state, place_slug = args.place.split("/")
    place = get_place_by_path(client, state, place_slug)
    if not place:
        raise SystemExit(f"Unknown place {args.place!r}.")
    place_id = place["id"]

    entities = (
        client.table("entities").select("id,body_slug").eq("place_id", place_id).execute().data
    )
    body_of = {e["id"]: e["body_slug"] for e in entities}
    entity_ids = [e["id"] for e in entities]
    if args.body:
        entity_ids = [e["id"] for e in entities if e["body_slug"] == args.body]
        if not entity_ids:
            raise SystemExit(f"No body {args.body!r} in {args.place}.")

    # Dedup against everything already logged for the place (every provenance), keyed the
    # same way the matcher keys a mangling, so re-runs only surface genuinely new forms.
    existing = get_name_corrections(client, place_id)
    existing_norm = frozenset(norm_key(r["mangled"]) for r in existing if r.get("mangled"))

    lexicon = place_lexicon(client, place_id)  # place-scoped: same for every body

    transcripts = _select_transcripts(client, entity_ids, args)
    logger.info("Scanning %d transcript(s) for %s …", len(transcripts), args.place)

    per_meeting: list[dict[str, Any]] = []
    scanned = with_auth = 0
    for i, t in enumerate(transcripts, 1):
        text = _doc_text(client, t)
        if not text.strip():
            continue
        scanned += 1
        auth_rows = get_meeting_records(client, t["entity_id"], t["meeting_date"], AUTH_TYPES)
        if auth_rows:
            with_auth += 1
        auth_docs = [
            {
                "id": d["id"],
                "document_type": d["document_type"],
                "meeting_date": d["meeting_date"],
                "text": _doc_text(client, d),
            }
            for d in auth_rows
        ]
        vocab = build_vocabulary(lexicon, auth_docs)
        for m in find_manglings(text, vocab, existing_norm=existing_norm):
            per_meeting.append(
                {
                    "mangled": m.mangled,
                    "canonical": m.canonical,
                    "category": m.category,
                    "confidence": m.confidence,
                    "score": m.score,
                    "occurrences": m.occurrences,
                    "source": m.source,
                    "video_id": t.get("video_id") or "",
                    "meeting_date": str(t["meeting_date"]),
                    "body": body_of.get(t["entity_id"], "?"),
                    "snippet": context_snippet(text, m.surface),
                }
            )
        if i % 25 == 0:
            logger.info("  … %d/%d scanned", i, len(transcripts))

    proposals = _aggregate(per_meeting)
    high = [p for p in proposals if p["confidence"] == "high"]
    review = [p for p in proposals if p["confidence"] == "review"]

    out_path = args.out or (
        Path(__file__).resolve().parent / "corrections" / f"proposals_{state}_{place_slug}.json"
    )
    payload = {
        "place": args.place,
        "generated_on": date.today().isoformat(),
        "transcripts_scanned": scanned,
        "transcripts_with_agenda_or_minutes": with_auth,
        "existing_corrections": len(existing),
        "counts": {"high": len(high), "review": len(review)},
        "high": high,
        "review": review,
    }
    out_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    logger.info(
        "Wrote %d proposals (%d high, %d review) to %s",
        len(proposals),
        len(high),
        len(review),
        out_path,
    )

    if args.apply and high:
        # Additive: 'auto' rows are owned by this tool, separate from curated 'asr' rows.
        live = {norm_key(r["mangled"]) for r in get_name_corrections(client, place_id)}
        rows = [
            {
                "place_id": place_id,
                "mangled": p["mangled"],
                "canonical": p["canonical"],
                "category": p["category"],
                "provenance": "auto",
                "active": True,
            }
            for p in high
            if p["mangled"] not in live
        ]
        if rows:
            insert_rows_resilient(client, "name_corrections", rows)
        logger.info(
            "Inserted %d new 'auto' corrections (skipped %d already present).",
            len(rows),
            len(high) - len(rows),
        )
    elif not args.apply:
        logger.info(
            "Review file only. After review, merge accepted rows into the corrections "
            "file and re-seed, or re-run with --apply to write the 'high' bucket."
        )
    return 0


if __name__ == "__main__":
    sys.exit(main())
