"""The speaker-identity review queue: proposals a human should confirm or reject.

The deterministic resolver publishes only clean, unambiguous anchors
(``inferred_high``). Everything below that bar — contested members, anything the
resolver wasn't sure of — lands at ``inferred_low`` / ``inferred_medium`` and is hidden
from the public by RLS. This module shapes those below-the-gate rows into a review list
for the operator.

It is deliberately an operator tool, not a public surface: these rows name candidate
officials at a confidence we have NOT cleared for publication, so they are read with the
service key and never exposed over the anon web/API path.
"""

from __future__ import annotations

from typing import Any

# Below the public display gate (inferred_high / confirmed); these need a human look.
REVIEW_CONFIDENCE = ("inferred_low", "inferred_medium")


def shape_review_queue(
    identity_rows: list[dict[str, Any]], docs_by_id: dict[int, dict[str, Any]]
) -> list[dict[str, Any]]:
    """Join below-gate identity rows with their meeting context into a review list.

    ``identity_rows`` are ``speaker_identities`` rows (with the subject embedded);
    ``docs_by_id`` maps document_id -> the transcript's meeting metadata. Sorted by
    meeting date then cluster so the operator reviews a meeting at a time.
    """
    queue: list[dict[str, Any]] = []
    for row in identity_rows:
        doc = docs_by_id.get(row["document_id"], {})
        subject = row.get("subject") or {}
        queue.append(
            {
                "document_id": row["document_id"],
                "meeting_date": doc.get("meeting_date"),
                "meeting_title": doc.get("meeting_title"),
                "cluster_label": row["cluster_label"],
                "confidence": row["confidence"],
                "basis": row.get("basis"),
                "candidate_subject": subject.get("canonical_name"),
                "candidate_slug": subject.get("slug"),
            }
        )
    return sorted(queue, key=lambda r: (r["meeting_date"] or "", r["cluster_label"]))
