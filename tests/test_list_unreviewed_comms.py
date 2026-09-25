"""Tests for the district-news review list (scripts/list_unreviewed_comms.py)."""

from __future__ import annotations

import json
from pathlib import Path

import scripts.list_unreviewed_comms as review


def _post(slug: str, date: str, title: str = "t") -> dict:
    return {
        "source_file": f"comms_{slug}.html",
        "source_url": f"https://x/post-details/~board/district-news/post/{slug}",
        "meeting_date": date,
        "meeting_title": title,
    }


def test_only_posts_neither_ingested_nor_excluded_are_waiting() -> None:
    posts = [
        _post("new-policy", "2026-09-20"),
        _post("award", "2026-09-19"),
        _post("old", "2026-09-01"),
    ]
    waiting = review.awaiting_review(
        posts, ingested_files={"comms_old.html"}, excluded_slugs={"award"}
    )
    assert [p["source_file"] for p in waiting] == ["comms_new-policy.html"]


def test_newest_first_and_empty_when_nothing_waits() -> None:
    posts = [_post("a", "2026-09-01"), _post("b", "2026-09-20")]
    assert [p["meeting_date"] for p in review.awaiting_review(posts, set(), set())] == [
        "2026-09-20",
        "2026-09-01",
    ]
    assert review.render([], "scripts/comms/x.json") == ""


def test_render_names_the_post_and_its_slug() -> None:
    md = review.render([_post("new-policy", "2026-09-20", "New Policy")], "scripts/comms/x.json")
    assert "1 district-news post(s)" in md
    assert "[New Policy](https://x/post-details/~board/district-news/post/new-policy)" in md
    assert "`new-policy`" in md and "scripts/comms/x.json" in md


def test_the_committed_review_record_is_well_formed() -> None:
    path = Path(review.__file__).resolve().parent / "comms" / "mo_clayton_reviewed.json"
    data = json.loads(path.read_text())
    assert data["place"] == "mo/clayton"
    slugs = data["excluded_slugs"]
    assert slugs == sorted(set(slugs)) and all("/" not in s for s in slugs)
