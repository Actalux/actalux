"""Detect what changed in the corpus since a timestamp.

The weekly ingest cron lands new and updated documents but the ``ingest_runs``
log records only aggregate counts, not which rows. So the digest derives "what's
new" from the documents themselves: a current row (``replaces_id IS NULL``)
created since the last run is either brand new or a fresh version of an existing
document. Each is labelled and assigned a topic; the drafter turns this into a
post.
"""

from __future__ import annotations

from collections import OrderedDict
from dataclasses import dataclass

from supabase import Client

from actalux.db import list_documents_changed_since
from actalux.digest.themes import group_by_theme, theme_for
from actalux.web.display import display_title


@dataclass(frozen=True)
class ChangedDoc:
    """One document that appeared or was updated in a digest window."""

    id: int
    title: str
    document_type: str
    theme: str
    meeting_date: str
    source_portal: str
    source_url: str
    summary: str
    status: str  # "new" or "updated"


@dataclass(frozen=True)
class ChangeDigest:
    """The set of documents that changed since ``since``, ready to draft from."""

    since: str
    entity_id: int | None
    docs: list[ChangedDoc]

    @property
    def is_empty(self) -> bool:
        return not self.docs

    def by_theme(self) -> OrderedDict[str, list[ChangedDoc]]:
        """Documents grouped into topic headings in display order (empties dropped)."""
        return group_by_theme(self.docs)


def build_change_digest(
    client: Client,
    since: str,
    *,
    entity_id: int | None = None,
    limit: int = 500,
) -> ChangeDigest:
    """Collect the documents created at or after ``since`` into a topic-grouped digest.

    ``since`` is an inclusive ISO-8601 lower bound on ``created_at`` (the ingest
    time), so a document re-ingested unchanged (which updates only
    ``last_checked_at``, not ``created_at``) does not resurface. A row with
    ``version > 1`` is labelled "updated"; otherwise "new".
    """
    rows = list_documents_changed_since(client, since, entity_id=entity_id, limit=limit)
    docs: list[ChangedDoc] = []
    for row in rows:
        document_type = row.get("document_type", "") or ""
        status = "updated" if (row.get("version") or 1) > 1 else "new"
        docs.append(
            ChangedDoc(
                id=row["id"],
                title=display_title(row),
                document_type=document_type,
                theme=theme_for(document_type),
                meeting_date=str(row.get("meeting_date") or ""),
                source_portal=row.get("source_portal", "") or "",
                source_url=row.get("source_url", "") or "",
                summary=row.get("summary", "") or "",
                status=status,
            )
        )
    return ChangeDigest(since=since, entity_id=entity_id, docs=docs)
