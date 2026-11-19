#!/usr/bin/env python3
"""Return whether an open PR already covers a routine refactor fingerprint.

The weekly ``abstraction-police`` Cursor Cloud Agent (branches
``routine/abstraction-police-YYYY-MM-DD``) opens pull requests when it finds a
safe deduplication. Without a guard, the same unmerged refactor (e.g. DESE money
cells) gets proposed on every run.

Run this **before** creating a branch or PR for a known fingerprint. Exit 0 with
a short message when a duplicate open PR exists (caller should skip opening
another). Exit 1 when no duplicate is found (safe to proceed).

Usage:
  GITHUB_REPOSITORY=Actalux/actalux GITHUB_TOKEN=... \\
    uv run python scripts/routine/find_open_duplicate_pr.py --fingerprint dese-ssrs-money-cell
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
from pathlib import Path

import httpx

_FINGERPRINTS = Path(__file__).with_name("fingerprints.json")
_ROUTINE_BRANCH = re.compile(r"^routine/abstraction-police-\d{4}-\d{2}-\d{2}$", re.I)


def _gh_get(path: str, token: str, params: dict | None = None) -> list | dict:
    r = httpx.get(
        f"https://api.github.com{path}",
        headers={"Authorization": f"Bearer {token}", "Accept": "application/vnd.github+json"},
        params=params or {},
        timeout=30,
    )
    r.raise_for_status()
    return r.json()


def _open_pulls(repo: str, token: str) -> list[dict]:
    pulls: list[dict] = []
    page = 1
    while True:
        batch = _gh_get(
            f"/repos/{repo}/pulls",
            token,
            {"state": "open", "per_page": 100, "page": page},
        )
        if not isinstance(batch, list) or not batch:
            break
        pulls.extend(batch)
        if len(batch) < 100:
            break
        page += 1
    return pulls


def _pr_files(repo: str, token: str, number: int) -> list[str]:
    files: list[str] = []
    page = 1
    while True:
        batch = _gh_get(
            f"/repos/{repo}/pulls/{number}/files",
            token,
            {"per_page": 100, "page": page},
        )
        if not isinstance(batch, list) or not batch:
            break
        files.extend(f["filename"] for f in batch)
        if len(batch) < 100:
            break
        page += 1
    return files


def _matches_fingerprint(pull: dict, fp: dict, repo: str, token: str) -> bool:
    head_ref = (pull.get("head") or {}).get("ref") or ""
    title = (pull.get("title") or "").lower()
    body = (pull.get("body") or "").lower()
    want_paths = set(fp.get("paths") or [])
    title_bits = [s.lower() for s in fp.get("title_substrings") or []]

    on_routine_branch = bool(_ROUTINE_BRANCH.match(head_ref))
    title_hit = any(bit in title for bit in title_bits) or any(bit in body for bit in title_bits)

    if not on_routine_branch and not title_hit:
        return False

    number = int(pull["number"])
    touched = set(_pr_files(repo, token, number))
    if not want_paths:
        return on_routine_branch or title_hit
    overlap = want_paths & touched
    if len(overlap) >= 2:
        return True
    # Single shared module + one parser is still the same refactor.
    shared = {
        "src/actalux/ingest/ssrs_xml.py",
        "src/actalux/ingest/money.py",
        "src/actalux/ingest/dese_money.py",
    }
    if overlap & shared and any(p.endswith("_xml.py") for p in overlap):
        return True
    return on_routine_branch and title_hit and bool(overlap)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--fingerprint",
        required=True,
        help="Key in scripts/routine/fingerprints.json",
    )
    args = parser.parse_args()

    token = os.environ.get("GITHUB_TOKEN")
    repo = os.environ.get("GITHUB_REPOSITORY")
    if not token or not repo:
        print("GITHUB_TOKEN and GITHUB_REPOSITORY are required", file=sys.stderr)
        sys.exit(2)

    catalog = json.loads(_FINGERPRINTS.read_text())
    if args.fingerprint not in catalog:
        print(f"Unknown fingerprint {args.fingerprint!r}", file=sys.stderr)
        sys.exit(2)
    fp = catalog[args.fingerprint]

    for pull in _open_pulls(repo, token):
        if _matches_fingerprint(pull, fp, repo, token):
            num = pull["number"]
            url = pull.get("html_url") or f"https://github.com/{repo}/pull/{num}"
            print(f"duplicate_open_pr={url}")
            sys.exit(0)

    print("no_duplicate_open_pr")
    sys.exit(1)


if __name__ == "__main__":
    main()
