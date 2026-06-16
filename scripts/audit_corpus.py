#!/usr/bin/env python3
"""Read-only corpus-quality auditor.

Scans the documents table and reports issues in five categories:

  * suspected-default-date  — meeting_date appears to be an ingest-day fallback
  * bucket-url              — source_url points at the raw Supabase storage bucket
                             (not the canonical origin) or is empty
  * duplicate-candidates    — docs that cluster as likely twins (same entity/date/
                             type + normalised title, or content_hash collision)
  * extraction-health       — control-character noise or exotic-character (mojibake)
                             ratio above threshold
  * classification-anomaly  — document_type vs source_portal mismatch

Detection functions are PURE (no DB access) so they are unit-testable with
synthetic rows. The DB read at runtime uses the anon key (ACTALUX_SUPABASE_URL /
ACTALUX_SUPABASE_KEY); this script never writes.

Usage (dry — read only, no doppler mutation):
  doppler run --project mac --config dev -- \\
    uv run python scripts/audit_corpus.py
  doppler run --project mac --config dev -- \\
    uv run python scripts/audit_corpus.py --json
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import re
import sys
from collections import defaultdict
from typing import Any

from actalux.ingest.parser import exotic_char_ratio, strip_control_chars

logging.basicConfig(level=logging.WARNING, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)

# --- constants ------------------------------------------------------------

# Supabase storage public-bucket URL segment that signals a raw-bucket link
# rather than the canonical origin URL.
_BUCKET_URL_MARKER = "/storage/v1/object/public/documents/"

# exotic_char_ratio threshold above which a document's content is flagged as
# potential mojibake from broken-font PDF extraction.
_EXOTIC_CHAR_THRESHOLD = 0.05

# Control characters beyond tab/newline/CR (the same set stripped by
# strip_control_chars in parser.py). Detecting their presence signals that
# strip_control_chars was not applied at ingest time, or new content was added
# raw.
_CONTROL_CHARS_RE = re.compile(r"[\x00-\x08\x0b\x0c\x0e-\x1f\x7f-\x9f]")

# Known valid (portal, document_type) pairs that are never anomalous.
# The classifier produces these combinations from real documents; adding an
# entry here suppresses an otherwise-correct flag.
_VALID_PORTAL_TYPES: dict[str, set[str]] = {
    "diligent": {
        "agenda",
        "minutes",
        "packet",
        "resolution",
        "audit",
        "budget",
        "per_pupil",
        "warrants",
        "expenditure_summary",
        "revenue_summary",
        "presentation",
        "ballot",
        "schedule",
        "other",
        "governance",
        "facilities_plan",
        "strategic_plan",
    },
    "claytonschools": {
        "curriculum",
        "curriculum_map",
        "facilities_plan",
        "strategic_plan",
        "assessment",
        "governance",
        "presentation",
        "other",
        "budget",
    },
    "youtube": {"transcript"},
    "manual": set(),  # manual docs accept any type
    "sunshine": {
        "governance",
        "resolution",
        "other",
        "presentation",
    },
    "dese": {"budget", "per_pupil", "expenditure_summary", "revenue_summary", "other"},
}

# Types that are anomalous for a youtube doc regardless of the above table.
_YOUTUBE_NON_TRANSCRIPT = frozenset({"minutes", "agenda", "packet", "budget", "curriculum"})


# --- pure detection functions (no DB access, unit-testable) ---------------


def is_suspected_default_date(row: dict[str, Any]) -> bool:
    """True when the stored meeting_date looks like an ingest-day fallback.

    Two signals, either sufficient:
      1. date_source == 'default' (when the A3 migration column exists).
      2. meeting_date's date portion equals created_at's date portion — the classic
         "fell back to date.today() at ingest time" pattern; absent the column we
         use this heuristic.

    Tolerates missing/null columns gracefully so it works before the A3
    migration is applied.
    """
    date_source = row.get("date_source")
    if date_source == "default":
        return True
    # 'unknown' is the column default for rows ingested before A3: treat it the
    # same as a missing value — fall through to the heuristic rather than
    # accepting it as trusted provenance.  Only verified values ('filename',
    # 'content', 'manual') suppress the heuristic.
    trusted = {"filename", "content", "manual"}
    if date_source is not None and date_source in trusted:
        return False

    meeting_date = row.get("meeting_date")
    created_at = row.get("created_at")
    if not meeting_date or not created_at:
        return False

    # Both are ISO strings: "2026-04-11" and "2026-04-11T14:23:07.123Z". Compare
    # only the date portion (first 10 characters). Fires for pre-A3 rows and for
    # rows whose date_source is 'unknown' (legacy/unverified).
    return str(meeting_date)[:10] == str(created_at)[:10]


def is_bucket_url_issue(row: dict[str, Any]) -> str | None:
    """Return a short reason string if source_url is problematic, else None.

    Two bad states:
      * Empty string — the original URL was lost; "Open original" link broken.
      * Points at the raw Supabase storage bucket — not the canonical origin.
    """
    url = row.get("source_url") or ""
    if not url:
        return "empty"
    if _BUCKET_URL_MARKER in url:
        return "storage-bucket-url"
    return None


def dedup_cluster_key(row: dict[str, Any]) -> str:
    """Stable clustering key for duplicate-candidate detection.

    Docs with the same key are candidate twins even if they have different
    source_files (e.g. a PDF and HTML version of the same resolution).

    Key components (all normalised to lower-case with whitespace collapsed):
      entity_id | meeting_date | document_type | normalised-title-stem

    The title stem strips the file extension and common suffixes so
    "2024-2025 Budget.pdf" and "2024-2025 School District of Clayton Budget.html"
    both reduce to a comparable base.
    """
    entity_id = str(row.get("entity_id") or "")
    meeting_date = str(row.get("meeting_date") or "")[:10]
    doc_type = (row.get("document_type") or "").lower().strip()

    raw_title = row.get("meeting_title") or row.get("source_file") or ""
    # Strip file extension and common admin suffixes before collapsing.
    stem = re.sub(r"\.(pdf|html?|txt|md|docx?)$", "", raw_title, flags=re.I)
    stem = re.sub(r"\b(signed|final|draft|approved|v\d+|rev\d+)\b", "", stem, flags=re.I)
    stem = re.sub(r"\s+", " ", stem).lower().strip()
    # Collapse punctuation so minor differences don't split a cluster.
    stem = re.sub(r"[^a-z0-9 ]", "", stem)
    # Truncate to 60 chars — two titles that differ past that are probably
    # different documents even if they share a prefix.
    stem = stem[:60].strip()

    return f"{entity_id}|{meeting_date}|{doc_type}|{stem}"


def find_duplicate_candidates(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Group docs into candidate-twin clusters; return one entry per cluster with >1 member.

    Two independent signals, each generating its own cluster entries:
      1. Same dedup_cluster_key (entity + date + type + normalised title).
      2. Identical content_hash (byte-for-byte duplicate content across different rows).

    Returns a list of cluster dicts:
      {"reason": str, "doc_ids": [int, ...], "key": str}
    """
    clusters: list[dict[str, Any]] = []

    # Signal 1: clustering-key collisions.
    by_key: dict[str, list[int]] = defaultdict(list)
    for row in rows:
        doc_id = row.get("id")
        if doc_id is None:
            continue
        by_key[dedup_cluster_key(row)].append(doc_id)
    for key, ids in by_key.items():
        if len(ids) > 1:
            clusters.append({"reason": "cluster-key", "doc_ids": sorted(ids), "key": key})

    # Signal 2: content_hash collisions (different rows, same hash).
    by_hash: dict[str, list[int]] = defaultdict(list)
    for row in rows:
        h = row.get("content_hash") or ""
        doc_id = row.get("id")
        if h and doc_id is not None:
            by_hash[h].append(doc_id)
    for h, ids in by_hash.items():
        if len(ids) > 1:
            clusters.append(
                {"reason": "content-hash-collision", "doc_ids": sorted(ids), "key": h[:16]}
            )

    return clusters


def check_extraction_health(row: dict[str, Any]) -> list[str]:
    """Return a list of health issues for this row's content field.

    Checks (independent; both may apply):
      * control-char-noise  — raw control characters beyond tab/newline/CR
      * exotic-char-ratio   — mojibake signal above threshold (broken-font PDF)

    Returns an empty list when the content looks healthy. Only checks rows
    where ``content`` is a non-empty string; skips otherwise.
    """
    content = row.get("content") or ""
    if not content:
        return []

    issues: list[str] = []
    if _CONTROL_CHARS_RE.search(content):
        issues.append("control-char-noise")

    # exotic_char_ratio counts codepoints ≥ U+0250 outside General Punctuation.
    # Stripping control chars first (as the ingest pipeline does) avoids
    # double-counting with the control-char check above.
    ratio = exotic_char_ratio(strip_control_chars(content))
    if ratio > _EXOTIC_CHAR_THRESHOLD:
        issues.append(f"exotic-char-ratio={ratio:.3f}")

    return issues


def check_classification_anomaly(row: dict[str, Any]) -> str | None:
    """Return a short reason string if doc_type vs source_portal looks wrong, else None.

    Uses _VALID_PORTAL_TYPES to check known portals. Portals with an allow-list
    entry flag any type outside that set. The ``manual`` portal is deliberately
    open (accepts any type). Unknown portal values are tolerated so new portals
    can be added without updating this function.

    A separate hard check catches the ``youtube`` case where a specific set of
    types are structurally impossible (the YouTube crawler only ever produces
    transcripts).
    """
    portal = (row.get("source_portal") or "").lower()
    doc_type = (row.get("document_type") or "").lower()

    if not portal or not doc_type:
        return None

    # manual portal is intentionally open-typed.
    if portal == "manual":
        return None

    # youtube docs must be transcripts. The VALID_PORTAL_TYPES set is also
    # {"transcript"}, but the specific message is more actionable.
    if portal == "youtube" and doc_type in _YOUTUBE_NON_TRANSCRIPT:
        return f"youtube doc typed as '{doc_type}' (expected 'transcript')"

    # For portals in the allow-list, flag any type not in the set.
    valid_types = _VALID_PORTAL_TYPES.get(portal)
    if valid_types is not None and doc_type not in valid_types:
        return f"{portal!r} doc typed as '{doc_type}' (not in expected set for this portal)"

    return None


# --- audit runner (calls DB once, then pure functions) --------------------


_BASE_SELECT = (
    "id, meeting_date, meeting_title, document_type, source_url, "
    "source_file, content_hash, source_portal, entity_id, created_at, content"
)


def _fetch_rows(client: Any) -> list[dict[str, Any]]:
    """Fetch all live document rows (replaces_id IS NULL) for the audit.

    Attempts to include ``date_source`` (added by the A3 migration). If
    PostgREST rejects the query because the column does not yet exist,
    retries without it. In that case, ``is_suspected_default_date`` falls
    back to the meeting_date == created_at heuristic.

    NOTE: Supabase's default page limit is 1000 rows. For corpora larger than
    that, add pagination using .range(start, end). Not needed for the current
    corpus size.
    """
    # supabase-py requires .select() before filters like .is_().
    try:
        result = (
            client.table("documents")
            .select(f"{_BASE_SELECT}, date_source")
            .is_("replaces_id", "null")
            .execute()
        )
        # If date_source is absent from the schema PostgREST returns an error
        # in result.data (or raises). Validate that at least one known column
        # is present to confirm success.
        if result.data and "id" in result.data[0]:
            return result.data
        # Empty corpus is fine; fall through only on error-shaped responses.
        if not result.data:
            return []
    except Exception:
        pass

    # Fallback: omit date_source (pre-A3 schema). The date heuristic will use
    # the meeting_date == created_at comparison instead.
    logger.info("date_source column not available; falling back to base select")
    result = client.table("documents").select(_BASE_SELECT).is_("replaces_id", "null").execute()
    return result.data or []


def run_audit(rows: list[dict[str, Any]]) -> dict[str, Any]:
    """Execute all checks on a list of document rows. Pure — no DB access.

    Returns a structured report dict suitable for JSON serialisation or
    pretty-printing. Schema::

        {
          "doc_count": int,
          "suspected_default_dates": [{"id": int, "meeting_date": str,
                                       "created_at": str, "date_source": str|None},
                                      ...],
          "bucket_url_issues": [{"id": int, "reason": str, "source_url": str}, ...],
          "duplicate_clusters": [{"reason": str, "doc_ids": [int], "key": str}, ...],
          "extraction_issues": [{"id": int, "issues": [str]}, ...],
          "classification_anomalies": [{"id": int, "portal": str, "doc_type": str,
                                        "reason": str}, ...],
        }
    """
    report: dict[str, Any] = {
        "doc_count": len(rows),
        "suspected_default_dates": [],
        "bucket_url_issues": [],
        "duplicate_clusters": [],
        "extraction_issues": [],
        "classification_anomalies": [],
    }

    for row in rows:
        doc_id = row.get("id")

        if is_suspected_default_date(row):
            report["suspected_default_dates"].append(
                {
                    "id": doc_id,
                    "meeting_date": str(row.get("meeting_date") or ""),
                    "created_at": str(row.get("created_at") or ""),
                    "date_source": row.get("date_source"),
                }
            )

        url_issue = is_bucket_url_issue(row)
        if url_issue:
            report["bucket_url_issues"].append(
                {
                    "id": doc_id,
                    "reason": url_issue,
                    "source_url": (row.get("source_url") or "")[:120],
                }
            )

        health_issues = check_extraction_health(row)
        if health_issues:
            report["extraction_issues"].append({"id": doc_id, "issues": health_issues})

        anomaly = check_classification_anomaly(row)
        if anomaly:
            report["classification_anomalies"].append(
                {
                    "id": doc_id,
                    "portal": row.get("source_portal") or "",
                    "doc_type": row.get("document_type") or "",
                    "reason": anomaly,
                }
            )

    report["duplicate_clusters"] = find_duplicate_candidates(rows)

    return report


def _print_report(report: dict[str, Any]) -> None:
    """Print a human-readable summary of the audit report to stdout."""
    total = report["doc_count"]
    print(f"\nCorpus audit  ({total} live documents)\n{'=' * 50}")

    def section(title: str, items: list[Any], fmt_fn: Any) -> None:
        print(f"\n{title} ({len(items)})")
        if not items:
            print("  (none)")
        else:
            for item in items[:50]:  # cap long lists in readable output
                print(f"  {fmt_fn(item)}")
            if len(items) > 50:
                print(f"  ... and {len(items) - 50} more")

    section(
        "Suspected default dates",
        report["suspected_default_dates"],
        lambda r: (
            f"id={r['id']:>5}  date={r['meeting_date']}  "
            f"created={str(r['created_at'])[:10]}  "
            f"date_source={r['date_source']}"
        ),
    )

    section(
        "Source-URL issues",
        report["bucket_url_issues"],
        lambda r: f"id={r['id']:>5}  {r['reason']:<24}  {r['source_url'][:60]}",
    )

    section(
        "Duplicate candidate clusters",
        report["duplicate_clusters"],
        lambda r: f"[{r['reason']}] ids={r['doc_ids']}  key={r['key'][:60]}",
    )

    section(
        "Extraction health issues",
        report["extraction_issues"],
        lambda r: f"id={r['id']:>5}  {', '.join(r['issues'])}",
    )

    section(
        "Classification anomalies",
        report["classification_anomalies"],
        lambda r: f"id={r['id']:>5}  portal={r['portal']:<15}  {r['reason']}",
    )

    print(f"\n{'=' * 50}")
    totals = [
        ("suspected-default-dates", len(report["suspected_default_dates"])),
        ("bucket-url-issues", len(report["bucket_url_issues"])),
        ("duplicate-clusters", len(report["duplicate_clusters"])),
        ("extraction-issues", len(report["extraction_issues"])),
        ("classification-anomalies", len(report["classification_anomalies"])),
    ]
    for label, n in totals:
        flag = " *" if n else ""
        print(f"  {label:<32} {n}{flag}")
    print()


def main() -> int:
    """Entry point: read documents from DB, run audit, print or emit JSON."""
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Emit JSON to stdout instead of the readable summary.",
    )
    args = parser.parse_args()

    url = os.environ.get("ACTALUX_SUPABASE_URL")
    key = os.environ.get("ACTALUX_SUPABASE_KEY")
    if not url or not key:
        print(
            "Missing ACTALUX_SUPABASE_URL or ACTALUX_SUPABASE_KEY.\n"
            "Run via: doppler run --project mac --config dev -- "
            "uv run python scripts/audit_corpus.py",
            file=sys.stderr,
        )
        return 1

    from actalux.db import get_client

    client = get_client(url, key)
    rows = _fetch_rows(client)
    report = run_audit(rows)

    if args.json:
        json.dump(report, sys.stdout, indent=2, default=str)
        print()
    else:
        _print_report(report)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
