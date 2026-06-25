#!/usr/bin/env python3
"""Flag meetings whose minutes are missing from the archive, for every body.

Minutes post about one meeting cycle late (a meeting's minutes are approved at the
*next* meeting, then published), so a freshly held meeting legitimately has no
minutes yet. This check therefore looks only at meetings that are (a) recent enough
to be worth chasing and (b) old enough that minutes should exist by now, and reports
the ones still missing minutes. Run daily by ``coverage_check.yml``; when gaps exist
it upserts a single GitHub issue (created/updated/closed in place, never spammed).

A "meeting" on a date = the body has an agenda, transcript, or minutes for it; a gap
= such a date in the window with no minutes document.

Usage:
  doppler run --project mac --config dev -- uv run python scripts/check_minutes_coverage.py
  # In CI, GITHUB_TOKEN + GITHUB_REPOSITORY drive the issue upsert; without them it
  # just prints the report (local dry run).
"""

from __future__ import annotations

import logging
import os
from collections import defaultdict
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta

import httpx

from actalux.config import load_config
from actalux.db import fetch_all_rows, get_client

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# A meeting's minutes are approved at the following meeting, so allow at least one
# cycle plus publishing slack before calling minutes "missing".
LAG_DAYS = 35
# Only chase the recent backlog; older gaps are often genuine source-side holes
# (scanned-only PDFs, minutes the city never posted) and would just add noise.
WINDOW_DAYS = 180

ISSUE_TITLE = "Minutes coverage gaps"
_MEETING_TYPES = ("agenda", "transcript", "minutes")


@dataclass(frozen=True)
class BodyGaps:
    entity_id: int
    body: str
    missing: list[str]  # ISO meeting dates with no minutes, newest first


def find_coverage_gaps(
    rows: list[dict], names: dict[int, str], today: date, *, lag_days: int, window_days: int
) -> list[BodyGaps]:
    """Per body, the recent meeting dates that lack a minutes document.

    ``rows`` are live documents with ``entity_id``, ``document_type``, ``meeting_date``.
    Pure function (no I/O) so the windowing logic is unit-testable.
    """
    meetings: dict[int, set[str]] = defaultdict(set)
    have_minutes: dict[int, set[str]] = defaultdict(set)
    for r in rows:
        md = r.get("meeting_date")
        if not md or r.get("document_type") not in _MEETING_TYPES:
            continue
        meetings[r["entity_id"]].add(md)
        if r["document_type"] == "minutes":
            have_minutes[r["entity_id"]].add(md)

    newest = today - timedelta(days=lag_days)
    oldest = today - timedelta(days=window_days)
    out: list[BodyGaps] = []
    for entity_id, dates in sorted(meetings.items()):
        missing = sorted(
            (
                d
                for d in dates
                if oldest <= date.fromisoformat(d) <= newest and d not in have_minutes[entity_id]
            ),
            reverse=True,
        )
        if missing:
            out.append(BodyGaps(entity_id, names.get(entity_id, f"entity {entity_id}"), missing))
    return out


def render_markdown(gaps: list[BodyGaps], today: date) -> str:
    total = sum(len(g.missing) for g in gaps)
    lines = [
        f"_As of {today.isoformat()}._ "
        f"{total} recent meeting(s) across {len(gaps)} body(ies) have an agenda or "
        f"transcript but **no minutes document** in the archive.",
        "",
        f"Window: meetings between {(today - timedelta(days=WINDOW_DAYS)).isoformat()} "
        f"and {(today - timedelta(days=LAG_DAYS)).isoformat()} "
        f"(minutes lag ~{LAG_DAYS}d after a meeting; older backlog excluded).",
        "",
        "Most should fill in once the city publishes them — the daily CivicPlus crawl "
        "picks them up. Persistent entries may be work sessions with no separate "
        "minutes, or a source-side gap to chase.",
        "",
    ]
    for g in gaps:
        lines.append(f"### {g.body}")
        lines += [f"- {d}" for d in g.missing]
        lines.append("")
    lines.append("<!-- actalux:minutes-coverage -->")  # stable marker for upsert
    return "\n".join(lines)


def _gh(method: str, path: str, token: str, **kw) -> httpx.Response:
    return httpx.request(
        method,
        f"https://api.github.com{path}",
        headers={"Authorization": f"Bearer {token}", "Accept": "application/vnd.github+json"},
        timeout=30,
        **kw,
    )


def upsert_issue(repo: str, token: str, title: str, body: str, *, has_gaps: bool) -> None:
    """Create/update one open issue when gaps exist; close it when they clear."""
    existing = _gh("GET", f"/repos/{repo}/issues?state=open&per_page=100", token).json()
    match = next((i for i in existing if i.get("title") == title and "pull_request" not in i), None)
    if has_gaps:
        if match:
            _gh("PATCH", f"/repos/{repo}/issues/{match['number']}", token, json={"body": body})
            logger.info("Updated issue #%s", match["number"])
        else:
            r = _gh("POST", f"/repos/{repo}/issues", token, json={"title": title, "body": body})
            logger.info("Opened issue #%s", r.json().get("number"))
    elif match:
        _gh(
            "POST",
            f"/repos/{repo}/issues/{match['number']}/comments",
            token,
            json={"body": "All tracked meetings now have minutes. Closing."},
        )
        _gh("PATCH", f"/repos/{repo}/issues/{match['number']}", token, json={"state": "closed"})
        logger.info("Closed issue #%s (no gaps)", match["number"])


def main() -> None:
    cfg = load_config()
    db = get_client(cfg.supabase_url, cfg.supabase_service_key)
    rows = fetch_all_rows(
        lambda: (
            db.table("documents")
            .select("entity_id,document_type,meeting_date,replaces_id")
            .in_("document_type", list(_MEETING_TYPES))
        )
    )
    rows = [r for r in rows if r.get("replaces_id") is None]
    entity_rows = db.table("entities").select("id,display_name").execute().data
    names = {e["id"]: e["display_name"] for e in entity_rows}

    today = datetime.now(UTC).date()
    gaps = find_coverage_gaps(rows, names, today, lag_days=LAG_DAYS, window_days=WINDOW_DAYS)
    report = render_markdown(gaps, today)
    print(report)

    token, repo = os.environ.get("GITHUB_TOKEN"), os.environ.get("GITHUB_REPOSITORY")
    if token and repo:
        upsert_issue(repo, token, ISSUE_TITLE, report, has_gaps=bool(gaps))
    else:
        logger.info("No GITHUB_TOKEN/GITHUB_REPOSITORY; printed report only (local dry run).")


if __name__ == "__main__":
    main()
