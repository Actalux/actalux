#!/usr/bin/env python3
"""Reconcile the Diligent portal's renamed-but-identical minutes files.

The district periodically re-standardises filenames on the Diligent portal
(e.g. "October 9 2024 Meeting Minutes.pdf" -> "October 9, 2024 BOE Meeting
Minutes.pdf") without changing the document itself. Because ingest dedup keys on
``source_file`` (the filename), a re-crawl downloads the renamed copy as a brand
new file and would insert a duplicate row for a meeting we already have.

This reconciles the known renames by updating the *existing* row's
``source_file`` to the portal's current name, so the next ingest recognises it
(content hash matches -> SKIP) instead of duplicating it. A rename is applied
ONLY when the new-named file on disk is byte-for-byte content-identical to the
stored row (verified via the same parse+hash path as ingest). If the content
differs, this refuses to rename — that case is a genuine new version and must go
through the normal ingest version mechanism, not a silent rename.

The OLD->NEW pairs below were confirmed content-identical on 2026-06-14 by
comparing ``content_hash`` of the freshly-downloaded file against the stored row.

Conservative by design:
  * Renames only the listed pairs; nothing else is touched.
  * Refuses any pair whose on-disk content hash no longer matches the stored row.
  * Idempotent: once a row carries the new name, the old name is absent and the
    pair is reported as already applied.

Dry-run by default; --apply writes (needs ACTALUX_SUPABASE_SERVICE_KEY).

Usage:
  doppler run --project mac --config dev -- uv run python scripts/fix_portal_renames.py
  doppler run --project mac --config dev -- uv run python scripts/fix_portal_renames.py --apply
"""

from __future__ import annotations

import argparse
import logging
import os
from pathlib import Path

from actalux.db import get_client
from actalux.ingest.hashing import content_hash
from actalux.ingest.parser import parse_file

logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger(__name__)

DOCS_DIR = Path("data/documents")

# old source_file (in DB) -> new canonical source_file (current portal name).
# Confirmed content-identical 2026-06-14; see module docstring.
RENAMES: dict[str, str] = {
    "August 14 2024 Meeting Minutes.pdf": "August 14, 2024 BOE Meeting Minutes.pdf",
    "September 4 2024 Joint Board of Education-Board of Aldermen Meeting Minutes.pdf": (
        "September 4, 2024 Joint Board of Education-Board of Aldermen Meeting Minutes.pdf"
    ),
    "September 25 2024 Meeting Minutes.pdf": "September 25, 2024 BOE Meeting Minutes.pdf",
    "October 9 2024 Meeting Minutes.pdf": "October 9, 2024 BOE Meeting Minutes.pdf",
    "October 30 2024 Meeting Minutes.pdf": "October 30, 2024 BOE Meeting Minutes.pdf",
    "March 11, 2026 BOE Meerting Minutes .pdf": "March 11, 2026 BOE Meeting Minutes .pdf",
}


def plan_renames(client) -> tuple[list[dict], list[str], list[str]]:
    """Return (to_apply, already_done, refused).

    ``to_apply`` items are {id, old, new}. A pair is refused when the new-named
    file is missing on disk or its content hash differs from the stored row.
    """
    to_apply: list[dict] = []
    already: list[str] = []
    refused: list[str] = []

    for old, new in RENAMES.items():
        rows = (
            client.table("documents")
            .select("id, source_file, content_hash")
            .eq("source_file", old)
            .is_("replaces_id", "null")
            .execute()
        ).data or []
        if not rows:
            already.append(old)
            continue

        new_path = DOCS_DIR / new
        if not new_path.exists():
            refused.append(f"{old!r}: new file not on disk ({new!r})")
            continue
        disk_hash = content_hash(parse_file(new_path))
        for r in rows:
            if r["content_hash"] != disk_hash:
                refused.append(f"{old!r}: content differs from {new!r} — not a rename, skipping")
                continue
            to_apply.append({"id": r["id"], "old": old, "new": new})

    return to_apply, already, refused


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--apply", action="store_true", help="Write changes (default: dry-run).")
    args = parser.parse_args()

    url = os.environ["ACTALUX_SUPABASE_URL"]
    key_var = "ACTALUX_SUPABASE_SERVICE_KEY" if args.apply else "ACTALUX_SUPABASE_KEY"
    try:
        key = os.environ[key_var]
    except KeyError as exc:
        raise SystemExit(
            f"Missing {exc}; run under doppler run --project mac --config dev -- ..."
        ) from exc

    client = get_client(url, key)
    to_apply, already, refused = plan_renames(client)

    logger.info(
        "%d rename(s) to apply, %d already applied, %d refused.",
        len(to_apply),
        len(already),
        len(refused),
    )
    for c in to_apply:
        logger.info("   #%s  %r\n        -> %r", c["id"], c["old"], c["new"])
    for a in already:
        logger.info("   (already applied) %r", a)
    for r in refused:
        logger.warning("   REFUSED %s", r)

    if not args.apply:
        logger.info("\nDRY RUN — no changes written. Re-run with --apply to write.")
        return 0

    for c in to_apply:
        client.table("documents").update({"source_file": c["new"]}).eq("id", c["id"]).execute()
    logger.info("\nApplied %d rename(s).", len(to_apply))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
