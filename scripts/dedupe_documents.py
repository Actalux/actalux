#!/usr/bin/env python3
"""Canonicalise duplicate documents (PDF/HTML twins) — conservatively.

Background: the same record was sometimes ingested twice — once as a PDF and once
as an HTML page — producing two live rows (both ``replaces_id IS NULL``). One row
embeds well (the PDF); the twin falls back to links. Filename dedup never caught
this because the twins have *different* stems
(``2024-2025 Budget.pdf`` vs ``2024-2025 School District of Clayton Budget.html``).

This script clusters likely-duplicate rows CONSERVATIVELY and, per cluster, picks
a canonical row (preferring the embeddable PDF). On ``--apply`` it would point the
non-canonical rows' ``replaces_id`` at the canonical id — it NEVER deletes a row.
Every cluster is also written to a CSV for human review before any apply.

Clustering signals (a row pair clusters when ANY holds):
  * same non-empty ``source_ref`` within the same portal — the stable origin id;
  * same non-empty ``content_hash`` — byte-identical content;
  * high text overlap AND same ``entity_id`` + ``meeting_date`` + ``document_type``
    — the twin case where bytes differ (PDF vs HTML extraction) but it is plainly
    the same record. Stem equality alone is NOT used (twins differ by stem).

The clustering and canonical-pick logic are PURE (no DB access) and unit-tested on
synthetic rows. The DB read uses the anon key for dry-run; ``--apply`` requires the
service key (RLS lets only the service key write ``replaces_id``).

Usage (dry — read only, no mutation):
  doppler run --project mac --config dev -- \\
    uv run python scripts/dedupe_documents.py
Apply (DESTRUCTIVE to version chain — human-reviewed CSV first; not run here):
  doppler run --project mac --config dev -- \\
    uv run python scripts/dedupe_documents.py --apply
"""

from __future__ import annotations

import argparse
import csv
import logging
import os
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger(__name__)

REVIEW_CSV = Path("data/dedupe_documents_review.csv")

# DB columns needed to cluster and pick a canonical row.
_SELECT_COLS = (
    "id, source_file, source_url, source_portal, source_ref, content_hash, "
    "content, entity_id, meeting_date, document_type, meeting_title, video_id"
)

# Word-token Jaccard at or above this counts as "high text overlap" for the
# twin signal. High by design: a twin's PDF and HTML extractions of the same
# record share almost all word tokens, while distinct records do not. Combined
# with same entity+date+type, so it cannot cluster unrelated docs.
_TEXT_OVERLAP_THRESHOLD = 0.85

# Below this token count the Jaccard ratio is noisy (a few shared stop-words can
# clear the threshold), so the text-overlap signal is not trusted; such rows can
# still cluster via source_ref or content_hash.
_MIN_TOKENS_FOR_OVERLAP = 30

_WORD_RE = re.compile(r"[a-z0-9]+")


def _tokens(text: str) -> set[str]:
    """Lowercase word-token set used for text-overlap (Jaccard) comparison."""
    return set(_WORD_RE.findall((text or "").lower()))


def text_overlap(a: str, b: str) -> float:
    """Jaccard similarity of the two texts' word-token sets (0.0–1.0).

    Returns 0.0 when either side is empty. Order-independent and robust to the
    cosmetic reflow differences between a PDF extraction and an HTML extraction
    of the same record.
    """
    ta, tb = _tokens(a), _tokens(b)
    if not ta or not tb:
        return 0.0
    inter = len(ta & tb)
    union = len(ta | tb)
    return inter / union if union else 0.0


def _twin_key(row: dict[str, Any]) -> tuple[str, str, str]:
    """The (entity, meeting_date, document_type) bucket the text-overlap signal is scoped to."""
    return (
        str(row.get("entity_id") or ""),
        str(row.get("meeting_date") or "")[:10],
        (row.get("document_type") or "").lower().strip(),
    )


def _twin_key_complete(key: tuple[str, str, str]) -> bool:
    """True only when ALL three identity fields are present (non-empty).

    The text-overlap twin signal requires same entity_id AND meeting_date AND
    document_type. If any is blank the bucket is too coarse to trust text overlap
    alone (two records with a matching entity but no date/type are not twins), so
    such rows can only cluster via source_ref or content_hash. Conservative by
    design — see the A2 plan.
    """
    return all(part != "" for part in key)


def _is_twin_pair(a: dict[str, Any], b: dict[str, Any], threshold: float) -> bool:
    """True when two rows are same entity+date+type AND their content overlaps highly.

    The identity bucket must be fully populated (all of entity_id/meeting_date/
    document_type present and equal), and both texts must clear
    ``_MIN_TOKENS_FOR_OVERLAP`` so the Jaccard ratio is meaningful (a couple of
    shared stop-words must not be enough).
    """
    key = _twin_key(a)
    if key != _twin_key(b):
        return False
    if not _twin_key_complete(key):
        # An incomplete identity bucket is too coarse to twin on text alone.
        return False
    if len(_tokens(a.get("content") or "")) < _MIN_TOKENS_FOR_OVERLAP:
        return False
    if len(_tokens(b.get("content") or "")) < _MIN_TOKENS_FOR_OVERLAP:
        return False
    return text_overlap(a.get("content") or "", b.get("content") or "") >= threshold


@dataclass
class _DSU:
    """Disjoint-set union over row indices, for merging pairwise cluster signals."""

    parent: dict[int, int] = field(default_factory=dict)

    def find(self, x: int) -> int:
        self.parent.setdefault(x, x)
        root = x
        while self.parent[root] != root:
            root = self.parent[root]
        # Path compression.
        while self.parent[x] != root:
            self.parent[x], x = root, self.parent[x]
        return root

    def union(self, a: int, b: int) -> None:
        ra, rb = self.find(a), self.find(b)
        if ra != rb:
            self.parent[max(ra, rb)] = min(ra, rb)


@dataclass(frozen=True)
class Cluster:
    """A group of likely-duplicate document rows plus why they clustered."""

    rows: list[dict[str, Any]]
    reasons: list[str]


def cluster_documents(
    rows: list[dict[str, Any]],
    *,
    text_overlap_threshold: float = _TEXT_OVERLAP_THRESHOLD,
) -> list[Cluster]:
    """Group rows into duplicate-candidate clusters (size > 1). PURE — no DB access.

    Three pairwise signals are merged transitively via union-find, so a PDF, an
    HTML twin, and a byte-identical re-upload all land in one cluster:
      1. same (portal, non-empty source_ref);
      2. same non-empty content_hash;
      3. twin pair (same entity+date+type AND text overlap >= threshold).

    Parameters
    ----------
    rows
        Document rows (need the columns in ``_SELECT_COLS``). Rows without an
        ``id`` are skipped.
    text_overlap_threshold
        Jaccard threshold for the twin signal (default ``_TEXT_OVERLAP_THRESHOLD``).

    Returns
    -------
    list[Cluster]
        One entry per cluster with more than one member; singletons are omitted.
    """
    indexed = [(i, r) for i, r in enumerate(rows) if r.get("id") is not None]
    dsu = _DSU()
    reasons_by_pair: dict[tuple[int, int], set[str]] = {}

    def mark(i: int, j: int, reason: str) -> None:
        dsu.union(i, j)
        key = (min(i, j), max(i, j))
        reasons_by_pair.setdefault(key, set()).add(reason)

    # Signal 1: (portal, source_ref) collisions.
    by_ref: dict[tuple[str, str], list[int]] = {}
    for i, r in indexed:
        ref = r.get("source_ref") or ""
        if ref:
            by_ref.setdefault(((r.get("source_portal") or ""), ref), []).append(i)
    for members in by_ref.values():
        for k in range(1, len(members)):
            mark(members[0], members[k], "source_ref")

    # Signal 2: content_hash collisions.
    by_hash: dict[str, list[int]] = {}
    for i, r in indexed:
        h = r.get("content_hash") or ""
        if h:
            by_hash.setdefault(h, []).append(i)
    for members in by_hash.values():
        for k in range(1, len(members)):
            mark(members[0], members[k], "content_hash")

    # Signal 3: twin pairs (same entity+date+type bucket, then text overlap).
    by_bucket: dict[tuple[str, str, str], list[int]] = {}
    for i, r in indexed:
        by_bucket.setdefault(_twin_key(r), []).append(i)
    for bucket_key, members in by_bucket.items():
        if not _twin_key_complete(bucket_key) or len(members) < 2:
            continue
        for a in range(len(members)):
            for b in range(a + 1, len(members)):
                ia, ib = members[a], members[b]
                if _is_twin_pair(rows[ia], rows[ib], text_overlap_threshold):
                    mark(ia, ib, "text-overlap")

    # Assemble clusters from the union-find roots.
    groups: dict[int, list[int]] = {}
    for i, _ in indexed:
        groups.setdefault(dsu.find(i), []).append(i)

    clusters: list[Cluster] = []
    for members in groups.values():
        if len(members) < 2:
            continue
        member_set = set(members)
        reasons: set[str] = set()
        for (i, j), rs in reasons_by_pair.items():
            if i in member_set and j in member_set:
                reasons |= rs
        cluster_rows = sorted((rows[i] for i in members), key=lambda r: r["id"])
        clusters.append(Cluster(rows=cluster_rows, reasons=sorted(reasons)))

    clusters.sort(key=lambda c: c.rows[0]["id"])
    return clusters


# Filename markers that signal a provisional/secondary copy that should lose the
# canonical pick to its sibling: a working "draft" (vs the adopted/final version)
# and a "Copy of ..." export (vs the original). Checked on the filename so a
# longer draft never beats the shorter final on content length alone.
_PROVISIONAL_RE = re.compile(r"\b(draft|copy of)\b")


def _canonical_rank(row: dict[str, Any]) -> tuple[int, int, int, int]:
    """Sort key for canonical preference within a cluster (lower = more canonical).

    Preference order, most-preferred first:
      1. embeddable copy — a PDF source_file, or a doc with a video_id (both
         render in-window, unlike an HTML/text twin that falls back to links);
      2. not a provisional copy — a "draft" or "Copy of ..." twin yields to its
         final/original sibling even when it happens to carry more text;
      3. richer content (longer extracted text = more searchable);
      4. lower id (older, stable) as the final deterministic tie-break.
    """
    source_file = (row.get("source_file") or "").lower()
    is_embeddable = source_file.endswith(".pdf") or bool(row.get("video_id"))
    is_provisional = bool(_PROVISIONAL_RE.search(source_file))
    return (
        0 if is_embeddable else 1,
        1 if is_provisional else 0,
        -len(row.get("content") or ""),
        row.get("id", 0),
    )


def pick_canonical(cluster: Cluster) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    """Choose the canonical row and the non-canonical rows for one cluster. PURE.

    Returns ``(canonical_row, [non_canonical_rows...])``. The non-canonical rows
    are the ones whose ``replaces_id`` would be set to the canonical id on apply.
    """
    ordered = sorted(cluster.rows, key=_canonical_rank)
    return ordered[0], ordered[1:]


@dataclass(frozen=True)
class DedupePlan:
    """Outcome of planning supersession over the clustered rows."""

    # {non_canonical_id, canonical_id, reason, cluster_ids} — the rows that would
    # get replaces_id set on apply.
    to_supersede: list[dict[str, Any]]
    # The full clusters, for the human-review CSV.
    clusters: list[Cluster]


def plan_dedupe(
    rows: list[dict[str, Any]],
    *,
    text_overlap_threshold: float = _TEXT_OVERLAP_THRESHOLD,
) -> DedupePlan:
    """Cluster rows and decide each cluster's canonical + supersession edges. PURE.

    Only currently-live rows (``replaces_id IS NULL``) should be passed in;
    already-superseded rows have no place in a fresh canonicalisation.
    """
    clusters = cluster_documents(rows, text_overlap_threshold=text_overlap_threshold)
    to_supersede: list[dict[str, Any]] = []
    for cluster in clusters:
        canonical, others = pick_canonical(cluster)
        cluster_ids = sorted(r["id"] for r in cluster.rows)
        for row in others:
            to_supersede.append(
                {
                    "non_canonical_id": row["id"],
                    "canonical_id": canonical["id"],
                    "reason": ",".join(cluster.reasons),
                    "cluster_ids": cluster_ids,
                }
            )
    return DedupePlan(to_supersede=to_supersede, clusters=clusters)


def write_review_csv(plan: DedupePlan, path: Path) -> None:
    """Write every cluster member to a CSV for human review (one row per document)."""
    fields = [
        "cluster_ids",
        "reasons",
        "id",
        "role",
        "source_portal",
        "document_type",
        "meeting_date",
        "source_file",
        "source_ref",
        "content_hash",
    ]
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fields)
        writer.writeheader()
        for cluster in plan.clusters:
            canonical, _others = pick_canonical(cluster)
            cluster_ids = ",".join(str(r["id"]) for r in cluster.rows)
            reasons = ",".join(cluster.reasons)
            for row in cluster.rows:
                role = "canonical" if row["id"] == canonical["id"] else "superseded-candidate"
                writer.writerow(
                    {
                        "cluster_ids": cluster_ids,
                        "reasons": reasons,
                        "id": row["id"],
                        "role": role,
                        "source_portal": row.get("source_portal") or "",
                        "document_type": row.get("document_type") or "",
                        "meeting_date": str(row.get("meeting_date") or "")[:10],
                        "source_file": row.get("source_file") or "",
                        "source_ref": row.get("source_ref") or "",
                        "content_hash": (row.get("content_hash") or "")[:16],
                    }
                )


def _print_plan(plan: DedupePlan) -> None:
    """Log the dry-run / apply plan for human inspection."""
    logger.info(
        "%d cluster(s); %d row(s) would be superseded.",
        len(plan.clusters),
        len(plan.to_supersede),
    )
    for cluster in plan.clusters:
        canonical, others = pick_canonical(cluster)
        logger.info(
            "  cluster %s  [%s]  canonical=#%s (%s)",
            [r["id"] for r in cluster.rows],
            ",".join(cluster.reasons),
            canonical["id"],
            canonical.get("source_file") or "",
        )
        for row in others:
            logger.info(
                "      supersede #%s (%s) -> #%s",
                row["id"],
                row.get("source_file") or "",
                canonical["id"],
            )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Set replaces_id on non-canonical rows (default: dry-run). Review the CSV first.",
    )
    args = parser.parse_args()

    url = os.environ["ACTALUX_SUPABASE_URL"]
    key_var = "ACTALUX_SUPABASE_SERVICE_KEY" if args.apply else "ACTALUX_SUPABASE_KEY"
    try:
        key = os.environ[key_var]
    except KeyError as exc:
        raise SystemExit(
            f"Missing {exc}; run under doppler run --project mac --config dev -- ..."
        ) from exc

    from actalux.db import get_client

    client = get_client(url, key)
    # Only live rows are candidates; already-superseded rows are out of scope.
    rows = (
        client.table("documents").select(_SELECT_COLS).is_("replaces_id", "null").execute()
    ).data or []
    logger.info("Fetched %d live document rows.", len(rows))

    plan = plan_dedupe(rows)
    _print_plan(plan)

    write_review_csv(plan, REVIEW_CSV)
    logger.info("Wrote %d cluster(s) to %s for review.", len(plan.clusters), REVIEW_CSV)

    if not args.apply:
        logger.info("\nDRY RUN — no changes written. Review the CSV, then re-run with --apply.")
        return 0

    for edge in plan.to_supersede:
        (
            client.table("documents")
            .update({"replaces_id": edge["canonical_id"]})
            .eq("id", edge["non_canonical_id"])
            .execute()
        )
    logger.info("\nSet replaces_id on %d non-canonical row(s).", len(plan.to_supersede))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
