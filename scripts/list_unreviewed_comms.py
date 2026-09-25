"""List district-news posts that nobody has reviewed yet.

The nightly comms crawl ingests facilities posts automatically but holds district
news for review, because that stream mixes governance posts (ingested) with
recognition, PR and event posts the content filter excludes. A post is awaiting
review when it is neither ingested nor recorded as excluded in
``scripts/comms/<state>_<place>_reviewed.json``.

Writes a Markdown list (empty when nothing is waiting) for the workflow to post
as a standing GitHub issue. Read-only against the database.

Run (prefix with `doppler run --project actalux --config dev --`):
  uv run python scripts/list_unreviewed_comms.py --place mo/clayton --out awaiting.md
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from actalux.config import load_config  # noqa: E402
from actalux.db import fetch_all_rows, get_client  # noqa: E402

NEWS_MANIFEST = Path("data/documents/comms_news_manifest.json")
REVIEWED_DIR = Path(__file__).resolve().parent / "comms"


def slug_of(url: str) -> str:
    return url.rstrip("/").rsplit("/", 1)[-1]


def awaiting_review(
    posts: list[dict], ingested_files: set[str], excluded_slugs: set[str]
) -> list[dict]:
    """Posts neither ingested nor already excluded, newest first."""
    waiting = [
        p
        for p in posts
        if p["source_file"] not in ingested_files and slug_of(p["source_url"]) not in excluded_slugs
    ]
    return sorted(waiting, key=lambda p: p["meeting_date"], reverse=True)


def render(waiting: list[dict], reviewed_path: str) -> str:
    if not waiting:
        return ""
    lines = [
        f"{len(waiting)} district-news post(s) are waiting for an include/exclude decision.",
        "",
        "- **Include** (policy, finance, governance, administration appointments): ingest it.",
        f"- **Exclude** (recognition, PR, events, enrollment): add its slug to `{reviewed_path}`.",
        "",
    ]
    for p in waiting:
        lines.append(
            f"- {p['meeting_date']} — [{p['meeting_title']}]({p['source_url']})  "
            f"`{slug_of(p['source_url'])}`"
        )
    return "\n".join(lines) + "\n"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--place", required=True, help="state/place, e.g. mo/clayton")
    parser.add_argument("--out", required=True, help="Markdown file to write")
    args = parser.parse_args()

    state, place = args.place.split("/")
    reviewed_file = REVIEWED_DIR / f"{state}_{place}_reviewed.json"
    excluded = set(json.loads(reviewed_file.read_text())["excluded_slugs"])
    posts = json.loads(NEWS_MANIFEST.read_text()) if NEWS_MANIFEST.exists() else []

    cfg = load_config()
    client = get_client(cfg.supabase_url, cfg.supabase_service_key or cfg.supabase_key)
    ingested = {
        r["source_file"]
        for r in fetch_all_rows(
            lambda: (
                client.table("documents")
                .select("source_file")
                .eq("source_portal", "claytonschools")
            )
        )
    }

    waiting = awaiting_review(posts, ingested, excluded)
    rel = reviewed_file.relative_to(Path(__file__).resolve().parent.parent).as_posix()
    Path(args.out).write_text(render(waiting, rel), encoding="utf-8")
    print(f"{len(posts)} district-news post(s) crawled; {len(waiting)} awaiting review")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
