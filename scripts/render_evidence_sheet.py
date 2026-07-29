"""Render an identify_nodes evidence report as an operator review sheet (HTML + summary).

The presentation layer of the evidence loop: identify_nodes gathers the signals
(gallery similarity + minutes attribution) and this renders them for ratification —
minutes-named voices first, then gallery matches at the operator-approved batch bar,
with a cued ``youtu.be/<id>?t=<sec>`` thumbnail per card (opened in a real browser;
never an iframe). The optional markdown summary is sized for a GitHub issue body, which
is how the nightly pipeline notifies the operator that a fresh meeting has nameable
voices. Report-only, like everything upstream: ratification happens in chat / via the
guarded attribution tools, never here.

Run:
    doppler run --project mac --config dev -- uv run python scripts/render_evidence_sheet.py \
        --state mo --place clayton --body council \
        --report identified.json --out-html sheet.html --out-summary summary.md
"""

from __future__ import annotations

import argparse
import html
import json
import logging
import os
import sys
from collections.abc import Callable
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from supabase import Client  # noqa: E402

from actalux.config import load_config  # noqa: E402
from actalux.db import get_client, get_entity_by_path  # noqa: E402
from actalux.errors import ActaluxError  # noqa: E402
from actalux.identity.resolve import _name_index, members_for_entity  # noqa: E402
from actalux.identity.vote_align import _resolve_mover  # noqa: E402

logger = logging.getLogger("render_evidence_sheet")

# The gallery batch bar: mean cosine vs the best official's centroid, and the margin to the
# runner-up. The operator-approved island-rescue threshold (schools 2026-07-25; council S2
# sheet) — matches at or above it have batch-approval precedent, below it is never proposed.
GALLERY_STRONG_MEAN = 0.85
GALLERY_STRONG_MARGIN = 0.15


def classify_rows(
    rows: list[dict[str, Any]], resolve_mover: Callable[[str], str | None]
) -> dict[str, list[dict[str, Any]]]:
    """Split report rows into sheet sections (pure).

    ``minutes``: every matched motion resolves to ONE member (carries ``resolved_mover``).
    ``gallery``: no usable minutes signal, gallery at/above the batch bar.
    ``conflict``: both signals present and disagreeing — listed, never proposed.
    Everything else is silent (no evidence worth an operator's minute).
    """
    out: dict[str, list[dict[str, Any]]] = {"minutes": [], "gallery": [], "conflict": []}
    for row in rows:
        gallery = row.get("gallery")
        movers = {resolve_mover(m) for m in row.get("minutes_movers") or []}
        named = movers.pop() if len(movers) == 1 and None not in movers else None
        strong = bool(
            gallery
            and gallery["mean"] >= GALLERY_STRONG_MEAN
            and gallery["margin"] >= GALLERY_STRONG_MARGIN
        )
        if named and strong and gallery["best_official"] != named:
            out["conflict"].append(row)
        elif named:
            out["minutes"].append(dict(row, resolved_mover=named))
        elif strong:
            out["gallery"].append(row)
    return out


def _cue_seconds(client: Client, doc_id: int, cluster: str, fragment: str | None) -> int:
    turns = (
        client.table("diarization_turns")
        .select("start_seconds,end_seconds,words")
        .eq("document_id", doc_id)
        .eq("cluster_label", cluster)
        .order("start_seconds")
        .execute()
        .data
    )
    if not turns:
        return 0
    if fragment:
        head = " ".join(fragment.split()[:6]).lower()
        for t in turns:
            if head in " ".join(w["word"] for w in (t["words"] or [])).lower():
                return max(0, int(t["start_seconds"]) - 4)
    longest = max(turns, key=lambda t: t["end_seconds"] - t["start_seconds"])
    return max(0, int(longest["start_seconds"]) - 2)


def _card(
    client: Client,
    docs: dict[int, dict[str, Any]],
    key: str,
    row: dict[str, Any],
    headline: str,
    action: str,
    color: str,
) -> str:
    member = row["members"][0]
    evidence = row.get("minutes_evidence") or []
    fragment = evidence[0]["spoken"] if evidence else None
    doc = docs[member["document_id"]]
    sec = _cue_seconds(client, member["document_id"], member["cluster_label"], fragment)
    vid = doc.get("video_id") or ""
    link = f"https://youtu.be/{vid}?t={sec}" if vid else "#"
    thumb = f"https://i.ytimg.com/vi/{vid}/mqdefault.jpg" if vid else ""
    ev_html = "".join(
        f"<div style='margin:6px 0;padding:6px;background:#f6f4ef;border-left:3px solid #999'>"
        f"<div><b>voice:</b> {html.escape(e['spoken'][:200])}</div>"
        f"<div><b>minutes:</b> {html.escape(e['minutes_motion'][:200])} "
        f"<i>(by {html.escape(str(e['moved_by']))}, match {e['match_score']})</i></div></div>"
        for e in evidence[:2]
    )
    g = row.get("gallery")
    g_html = (
        f"<div style='font-size:13px;color:#555'>gallery: {html.escape(g['best_official'])} "
        f"mean {g['mean']:.2f} / margin {g['margin']:.2f} "
        f"(runner-up {html.escape(str(g['runner_up']))})</div>"
        if g
        else ""
    )
    return (
        f"<div style='border:1px solid #ccc;border-radius:6px;padding:12px;margin:12px 0'>"
        f"<div style='display:flex;gap:14px;align-items:flex-start'>"
        f"<a href='{link}' target='_blank'>"
        f"<img src='{thumb}' width='160' style='border-radius:4px'>"
        f"<div style='font-size:12px'>▶ cue {sec // 60}:{sec % 60:02d}</div></a>"
        f"<div><b>{key}</b> <span style='background:{color};color:#fff;padding:2px 8px;"
        f"border-radius:3px'>{headline}</span> doc {member['document_id']} "
        f"{member['cluster_label']} — {html.escape(doc.get('meeting_title') or '')} "
        f"({doc.get('meeting_date')}, {int(row['total_seconds'])}s)<br>"
        f"proposed: <b>{html.escape(action)}</b>{g_html}{ev_html}</div></div></div>"
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--state", required=True)
    parser.add_argument("--place", required=True)
    parser.add_argument("--body", required=True)
    parser.add_argument("--report", required=True, help="identify_nodes --out JSON")
    parser.add_argument("--title", default="", help="sheet title suffix (e.g. the meeting date)")
    parser.add_argument("--out-html", required=True)
    parser.add_argument("--out-summary", help="markdown summary (e.g. a GitHub issue body)")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    cfg = load_config()
    key = os.environ.get("ACTALUX_SUPABASE_SERVICE_KEY", "")
    if not key:
        raise ActaluxError("ACTALUX_SUPABASE_SERVICE_KEY is required")
    client = get_client(cfg.supabase_url, key)
    entity = get_entity_by_path(client, args.state, args.place, args.body)
    if entity is None:
        raise ActaluxError(f"no entity for {args.state}/{args.place}/{args.body}")

    rows = json.loads(Path(args.report).read_text())
    strong, surname = _name_index(members_for_entity(client, entity["id"]))
    members_by_id = {m.subject_id: m for m in members_for_entity(client, entity["id"])}

    def resolve_mover(moved_by: str) -> str | None:
        sid = _resolve_mover(moved_by, strong, surname)
        return members_by_id[sid].canonical_name if sid is not None else None

    sections = classify_rows(rows, resolve_mover)
    doc_ids = sorted({m["document_id"] for r in rows for m in r["members"]})
    docs: dict[int, dict[str, Any]] = {}
    for i in range(0, len(doc_ids), 150):
        for d in (
            client.table("documents")
            .select("id,video_id,meeting_date,meeting_title")
            .in_("id", doc_ids[i : i + 150])
            .execute()
            .data
        ):
            docs[d["id"]] = d

    title = f"{args.state}/{args.place}/{args.body} evidence review {args.title}".strip()
    parts = [
        f"<!doctype html><meta charset='utf-8'><title>{html.escape(title)}</title>"
        "<body style='font-family:system-ui;max-width:920px;margin:24px auto;padding:0 12px'>"
        f"<h2>{html.escape(title)}</h2>"
        "<p>Reply in chat to ratify: <code>M all, G all</code>, or per-item by key. "
        "Thumbnails open the cued moment on YouTube.</p>"
    ]
    parts.append("<h3>Named by the minutes</h3>" if sections["minutes"] else "")
    for i, row in enumerate(sections["minutes"], 1):
        parts.append(
            _card(
                client,
                docs,
                f"M{i}",
                row,
                "minutes-named",
                f"attribute {row['resolved_mover']}",
                "#1565c0",
            )
        )
    parts.append("<h3>Gallery matches (batch bar)</h3>" if sections["gallery"] else "")
    for i, row in enumerate(sections["gallery"], 1):
        parts.append(
            _card(
                client,
                docs,
                f"G{i}",
                row,
                "gallery-strong",
                f"attribute {row['gallery']['best_official']}",
                "#2e7d32",
            )
        )
    if sections["conflict"]:
        parts.append(
            f"<h3>Held back — signals disagree ({len(sections['conflict'])})</h3>"
            "<p>Gallery and minutes name different people; never proposed.</p>"
        )
    if not (sections["minutes"] or sections["gallery"]):
        parts.append("<p><b>No nameable voices found</b> — no evidence cleared the bars.</p>")
    Path(args.out_html).write_text("".join(parts))
    logger.info(
        "sheet: %d minutes-named, %d gallery, %d conflicts -> %s",
        len(sections["minutes"]),
        len(sections["gallery"]),
        len(sections["conflict"]),
        args.out_html,
    )

    if args.out_summary:
        lines = [f"## {title}", ""]
        for i, row in enumerate(sections["minutes"], 1):
            m = row["members"][0]
            lines.append(
                f"- **M{i}** doc {m['document_id']} {m['cluster_label']} -> "
                f"**{row['resolved_mover']}** (minutes-named, {int(row['total_seconds'])}s)"
            )
        for i, row in enumerate(sections["gallery"], 1):
            m = row["members"][0]
            g = row["gallery"]
            lines.append(
                f"- **G{i}** doc {m['document_id']} {m['cluster_label']} -> "
                f"**{g['best_official']}** (gallery {g['mean']:.2f}/{g['margin']:.2f}, "
                f"{int(row['total_seconds'])}s)"
            )
        if sections["conflict"]:
            lines.append(f"- {len(sections['conflict'])} conflict(s) held back")
        if len(lines) == 2:
            lines.append("- no nameable voices found")
        lines += [
            "",
            "Full sheet: see the run's `evidence-sheet` artifact. "
            "Ratify in chat; writes go through the guarded attribution tools.",
        ]
        Path(args.out_summary).write_text("\n".join(lines))


if __name__ == "__main__":
    main()
