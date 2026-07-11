#!/usr/bin/env python3
"""Z2 writer: turn a Z1 evidence JSON's cluster verdicts into speaker_identities anchors.

Applies the write policy in docs/architecture/zoom-name-extraction.md (Z2):

- basis='screen_name', tier decided per verdict by rendering mode — inferred_high when
  every supporting frame is a gallery active-speaker tile, inferred_medium when any is a
  full-frame speaker-view read (the mode subject to the account-feed trap);
- clusters the probe flagged as feed labels are never written, and the zero-diversity
  full-frame guard (zoomlabels.uniform_fullframe_slugs) is re-applied here so evidence
  produced before that guard existed cannot write the account's name as a speaker;
- one anchor per (document, cluster) — Postgres enforces it. An existing anchor naming
  the SAME person is an agreement: no write (independent-family corroboration happens
  across meetings, never by stacking rows on one voice sample). A DIFFERENT person is a
  conflict: no write, surfaced for review. A rejected row is sticky: no write, surfaced.
- non-roster names never reach this script (the probe only emits roster-matched slugs).

Dry-run by default — prints the full plan; --apply executes the inserts and verifies.

    doppler run --project mac --config dev -- uv run python scripts/apply_zoom_verdicts.py \\
      data/zoom_receipts/mo_clayton_plan-commission/evidence_20260711T031509Z.json
    ... --apply   # after reviewing the plan
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

from actalux import db
from actalux.config import load_config
from actalux.diarization import zoomlabels
from actalux.errors import ActaluxError

TIER_TILE = "inferred_high"
TIER_FULLFRAME = "inferred_medium"


def verdict_tier(record: dict[str, Any]) -> str:
    """Tile-only support earns inferred_high; any full-frame read holds it at medium."""
    modes = {
        frame["mode"] for frame in record["frames"] if frame["matched_slug"] == record["verdict"]
    }
    return TIER_TILE if modes == {"tile"} else TIER_FULLFRAME


def frames_for_guard(doc: dict[str, Any]) -> dict[str, list[zoomlabels.FrameEvidence]]:
    """Rebuild just enough FrameEvidence for the zero-diversity guard (mode + slug)."""
    out: dict[str, list[zoomlabels.FrameEvidence]] = {}
    for record in doc["clusters"]:
        out[record["cluster_label"]] = [
            zoomlabels.FrameEvidence(
                t_seconds=frame["t_seconds"],
                frame_path=frame["frame_path"],
                tile=None,
                ocr_raw=frame["ocr_raw"],
                matched_slug=frame["matched_slug"],
                match_score=frame["match_score"],
                mode=frame["mode"],
            )
            for frame in record["frames"]
        ]
    return out


def plan_document(
    doc: dict[str, Any], existing: dict[tuple[int, str], dict[str, Any]], subjects: dict[str, int]
) -> list[dict[str, Any]]:
    """One document's verdicts -> planned actions (insert / agree / conflict / guarded)."""
    actions: list[dict[str, Any]] = []
    feed_slugs = zoomlabels.uniform_fullframe_slugs(frames_for_guard(doc))
    for record in doc["clusters"]:
        slug = record["verdict"]
        if not slug or record.get("feed_label"):
            continue
        base = {
            "doc_id": doc["doc_id"],
            "cluster": record["cluster_label"],
            "slug": slug,
            "date": doc.get("meeting_date"),
        }
        if slug in feed_slugs:
            actions.append({**base, "action": "GUARDED", "why": "uniform full-frame label"})
            continue
        if slug not in subjects:
            actions.append({**base, "action": "NO_SUBJECT", "why": "slug not in subjects"})
            continue
        row = existing.get((doc["doc_id"], record["cluster_label"]))
        tier = verdict_tier(record)
        if row is None:
            actions.append({**base, "action": "INSERT", "tier": tier})
        elif row["confidence"] == "rejected":
            why = "rejected row is sticky" + (" (SAME label!)" if row["slug"] == slug else "")
            actions.append({**base, "action": "BLOCKED", "why": why, "anchor": row["slug"]})
        elif row["slug"] == slug:
            anchor = f"{row['confidence']}/{row['basis']}"
            actions.append({**base, "action": "AGREE", "anchor": anchor})
        else:
            anchor = f"{row['slug']} ({row['confidence']}/{row['basis']})"
            actions.append({**base, "action": "CONFLICT", "anchor": anchor})
    return actions


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("evidence", type=Path, help="Z1 evidence JSON from probe_zoom_labels.py")
    parser.add_argument("--apply", action="store_true", help="execute the planned inserts")
    args = parser.parse_args(argv)

    config = load_config()
    if not config.supabase_service_key:
        raise ActaluxError("ACTALUX_SUPABASE_SERVICE_KEY is required")
    client = db.get_client(config.supabase_url, config.supabase_service_key)

    run = json.loads(args.evidence.read_text())
    docs = [d for d in run["documents"] if d.get("clusters")]
    doc_ids = [d["doc_id"] for d in docs]

    subject_rows = client.table("subjects").select("id,slug").execute().data
    subjects = {r["slug"]: r["id"] for r in subject_rows}
    slug_of = {r["id"]: r["slug"] for r in subject_rows}
    identity_rows = (
        client.table("speaker_identities")
        .select("document_id,cluster_label,subject_id,confidence,basis")
        .in_("document_id", doc_ids)
        .execute()
        .data
    )
    existing = {
        (r["document_id"], r["cluster_label"]): {
            "slug": slug_of.get(r["subject_id"]),
            "confidence": r["confidence"],
            "basis": r["basis"],
        }
        for r in identity_rows
    }

    actions = [a for doc in docs for a in plan_document(doc, existing, subjects)]
    by_kind: dict[str, list[dict[str, Any]]] = {}
    for action in actions:
        by_kind.setdefault(action["action"], []).append(action)

    for kind in ("INSERT", "AGREE", "CONFLICT", "BLOCKED", "GUARDED", "NO_SUBJECT"):
        rows = by_kind.get(kind, [])
        if not rows:
            continue
        print(f"\n{kind} ({len(rows)}):")
        for a in rows:
            extra = a.get("tier") or a.get("anchor") or a.get("why") or ""
            print(f"  doc {a['doc_id']} ({a['date']}) {a['cluster']} -> {a['slug']}  {extra}")

    inserts = by_kind.get("INSERT", [])
    if not args.apply:
        print(f"\ndry run: {len(inserts)} inserts planned; re-run with --apply to write")
        return 0
    if not inserts:
        print("\nnothing to write")
        return 0
    payload = [
        {
            "document_id": a["doc_id"],
            "cluster_label": a["cluster"],
            "subject_id": subjects[a["slug"]],
            "confidence": a["tier"],
            "basis": "screen_name",
        }
        for a in inserts
    ]
    written = client.table("speaker_identities").insert(payload).execute().data
    print(f"\nwrote {len(written)} screen_name anchors")
    if len(written) != len(inserts):
        print("MISMATCH: planned != written — inspect", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
