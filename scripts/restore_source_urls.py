#!/usr/bin/env python3
"""Restore ``documents.source_url`` to the real origin URL — conservatively.

Background: an earlier ``upload_to_storage.py`` overwrote every document's
``source_url`` with a raw Supabase storage-bucket URL, destroying the canonical
origin (Diligent ``…/document/{guid}``, the district website, etc.). The crawler
manifests under ``data/documents/*.json`` still carry the origin URLs, so we can
restore them — but only where it is unambiguous.

The manifests contain DUPLICATE ``source_file`` keys mapping to DIFFERENT origin
URLs (e.g. two distinct Diligent GUIDs for re-uploaded minutes). Restoring such a
row would be a guess, so this script restores ``source_url`` ONLY for a DB row
whose ``source_file`` is UNIQUE across all manifests AND whose ``source_portal``
matches AND whose stored ``content_hash`` matches the hash of the local source
file (recomputed via the same parse+hash path ingest used). Both signals must
agree. Rows keyed on a duplicate ``source_file`` — or that fail either
corroboration check — are written to a review CSV for manual resolution. Rows
with no manifest origin are left unchanged (never pointed back at the raw file).

Only rows whose current ``source_url`` is empty or a storage-bucket URL are
candidates; rows that already hold a real origin are left alone. Idempotent.

Dry-run by default (prints a diff); ``--apply`` writes via the service key.

Usage:
  doppler run --project mac --config dev -- uv run python scripts/restore_source_urls.py
  doppler run --project mac --config dev -- uv run python scripts/restore_source_urls.py --apply
"""

from __future__ import annotations

import argparse
import csv
import json
import logging
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from actalux.db import get_client
from actalux.ingest.hashing import content_hash
from actalux.ingest.parser import parse_file

logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger(__name__)

DOCS_DIR = Path("data/documents")
REVIEW_CSV = Path("data/restore_source_urls_review.csv")

# Storage public-bucket URL segment that marks a source_url as a raw-file link
# (an overwrite victim) rather than the canonical origin. Matches the marker
# used by scripts/audit_corpus.py.
_BUCKET_URL_MARKER = "/storage/v1/object/public/documents/"

# DB columns needed to plan the restore.
_SELECT_COLS = "id, source_file, source_url, source_portal, content_hash"


@dataclass(frozen=True)
class ManifestEntry:
    """One origin record drawn from a crawler manifest.

    Manifests carry no content hash, so the corroborating hash is computed from
    the local source file (see ``compute_local_hashes``), not stored here.
    """

    source_file: str
    source_url: str
    source_portal: str


def load_manifest_entries(docs_dir: Path) -> list[ManifestEntry]:
    """Load every ``*.json`` manifest under ``docs_dir`` into ManifestEntry rows.

    Entries missing a ``source_file`` or ``source_url`` are skipped (they cannot
    contribute an origin mapping).
    """
    entries: list[ManifestEntry] = []
    for path in sorted(docs_dir.glob("*.json")):
        raw = json.loads(path.read_text())
        if not isinstance(raw, list):
            continue
        for item in raw:
            if not isinstance(item, dict):
                continue
            source_file = item.get("source_file") or ""
            source_url = item.get("source_url") or ""
            if not source_file or not source_url:
                continue
            entries.append(
                ManifestEntry(
                    source_file=source_file,
                    source_url=source_url,
                    source_portal=item.get("source_portal") or "",
                )
            )
    return entries


def build_unique_origin_map(entries: list[ManifestEntry]) -> dict[str, ManifestEntry]:
    """Map ``source_file`` -> its single origin entry, for unique keys only.

    A ``source_file`` that appears more than once across manifests is excluded:
    its origin is ambiguous, so it is left for manual review rather than guessed.
    """
    by_file: dict[str, list[ManifestEntry]] = {}
    for e in entries:
        by_file.setdefault(e.source_file, []).append(e)
    return {sf: lst[0] for sf, lst in by_file.items() if len(lst) == 1}


def compute_local_hashes(source_files: set[str], docs_dir: Path) -> dict[str, str]:
    """Hash the on-disk source file for each ``source_file`` (same path as ingest).

    The manifests carry no ``content_hash``, so the corroborating hash is derived
    from the local source file via ``parse_file`` + ``content_hash`` — the exact
    pipeline ingest used to compute the stored ``documents.content_hash``. A file
    absent on disk is simply omitted (its row then fails corroboration).

    Parameters
    ----------
    source_files
        The ``source_file`` keys to hash (the unique candidates).
    docs_dir
        Directory holding the downloaded source files.

    Returns
    -------
    dict[str, str]
        ``source_file`` -> content hash, only for files present on disk.
    """
    hashes: dict[str, str] = {}
    for source_file in source_files:
        path = docs_dir / source_file
        if path.exists():
            hashes[source_file] = content_hash(parse_file(path))
    return hashes


def _needs_restore(current_url: str) -> bool:
    """A row is a restore candidate only if its source_url is empty/bucket-url."""
    return (not current_url) or (_BUCKET_URL_MARKER in current_url)


def _corroboration_status(
    row: dict[str, Any], origin: ManifestEntry, local_hashes: dict[str, str]
) -> str:
    """Classify how well the manifest origin corroborates the DB row.

    Both signals must agree before a restore is allowed: the portal must match,
    and the local file's content hash must equal the row's stored content hash.
    Returns ``"ok"`` only when both hold; otherwise a reason string naming the
    failed check (so the row is routed to review, never silently restored).
    """
    if (row.get("source_portal") or "") != origin.source_portal:
        return "portal-mismatch"
    local_hash = local_hashes.get(origin.source_file)
    if not local_hash:
        return "local-file-missing"
    if (row.get("content_hash") or "") != local_hash:
        return "content-hash-mismatch"
    return "ok"


@dataclass(frozen=True)
class RestorePlan:
    """Outcome of planning a restore over the DB rows."""

    to_apply: list[dict[str, str]]  # {id, source_file, old_url, new_url, portal}
    review: list[dict[str, str]]  # ambiguous / uncorroborated rows
    unchanged: list[dict[str, str]]  # already-has-origin / no manifest origin


def plan_restore(
    rows: list[dict[str, Any]],
    entries: list[ManifestEntry],
    local_hashes: dict[str, str],
) -> RestorePlan:
    """Decide, per DB row, whether to restore source_url, review, or leave it.

    Restores only when the row's ``source_file`` is unique across manifests AND
    both corroboration signals agree (portal match + local-file content-hash match
    against the row). Duplicate-key and uncorroborated rows go to review; rows that
    already hold a real origin or have no manifest origin are left unchanged.

    Parameters
    ----------
    rows
        DB document rows (must include the columns in ``_SELECT_COLS``).
    entries
        All manifest origin entries.
    local_hashes
        ``source_file`` -> on-disk content hash (from ``compute_local_hashes``).

    Returns
    -------
    RestorePlan
        The partition of rows into ``to_apply`` / ``review`` / ``unchanged``.
    """
    unique_origins = build_unique_origin_map(entries)
    duplicate_files = {e.source_file for e in entries} - set(
        unique_origins
    )  # files that appear >1× across manifests

    to_apply: list[dict[str, str]] = []
    review: list[dict[str, str]] = []
    unchanged: list[dict[str, str]] = []

    for row in rows:
        source_file = row.get("source_file") or ""
        current_url = row.get("source_url") or ""
        record = {
            "id": str(row.get("id", "")),
            "source_file": source_file,
            "old_url": current_url,
            "portal": row.get("source_portal") or "",
        }

        if not _needs_restore(current_url):
            # Already holds a real origin; nothing to do.
            unchanged.append({**record, "reason": "already-has-origin"})
            continue

        origin = unique_origins.get(source_file)
        if origin is not None:
            status = _corroboration_status(row, origin, local_hashes)
            if status == "ok":
                to_apply.append({**record, "new_url": origin.source_url})
            else:
                # Unique key but corroboration failed — do not guess.
                review.append({**record, "reason": status})
        elif source_file in duplicate_files:
            review.append({**record, "reason": "duplicate-manifest-key"})
        else:
            unchanged.append({**record, "reason": "no-manifest-origin"})

    return RestorePlan(to_apply=to_apply, review=review, unchanged=unchanged)


def write_review_csv(review: list[dict[str, str]], path: Path) -> None:
    """Write ambiguous rows to a CSV for manual resolution (empty file if none)."""
    fields = ["id", "source_file", "old_url", "portal", "reason"]
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fields)
        writer.writeheader()
        for r in review:
            writer.writerow({k: r.get(k, "") for k in fields})


def _print_diff(plan: RestorePlan) -> None:
    """Log the dry-run / apply diff for human inspection."""
    logger.info(
        "%d to restore, %d for review, %d unchanged.",
        len(plan.to_apply),
        len(plan.review),
        len(plan.unchanged),
    )
    for c in plan.to_apply:
        logger.info(
            "   #%s  %s\n        %r -> %r",
            c["id"],
            c["source_file"],
            c["old_url"],
            c["new_url"],
        )
    for r in plan.review:
        logger.info("   REVIEW  #%s  %s  (%s)", r["id"], r["source_file"], r["reason"])


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
    entries = load_manifest_entries(DOCS_DIR)
    logger.info("Loaded %d origin entries from manifests.", len(entries))

    # Hash only the unique-key candidates: those are the only files a restore can
    # corroborate against, so hashing the rest would be wasted work.
    unique_files = set(build_unique_origin_map(entries))
    local_hashes = compute_local_hashes(unique_files, DOCS_DIR)
    logger.info(
        "Hashed %d of %d unique-key source files present on disk.",
        len(local_hashes),
        len(unique_files),
    )

    rows = (client.table("documents").select(_SELECT_COLS).execute()).data or []
    logger.info("Fetched %d document rows.", len(rows))

    plan = plan_restore(rows, entries, local_hashes)
    _print_diff(plan)

    write_review_csv(plan.review, REVIEW_CSV)
    logger.info("Wrote %d review row(s) to %s", len(plan.review), REVIEW_CSV)

    if not args.apply:
        logger.info("\nDRY RUN — no changes written. Re-run with --apply to write.")
        return 0

    for c in plan.to_apply:
        client.table("documents").update({"source_url": c["new_url"]}).eq("id", c["id"]).execute()
    logger.info("\nRestored source_url on %d row(s).", len(plan.to_apply))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
