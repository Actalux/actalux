"""Change-digest → cited Substack draft.

The weekly ingest cron lands new and updated documents; this package turns
"what changed this run" into a citation-backed, nonpartisan draft for human
review. Nothing here publishes: the draft is written to a file and (optionally)
emailed, and a person decides what to post.

Pipeline: :func:`build_change_digest` (which documents changed since a
timestamp, grouped by topic) -> :func:`draft_post` (a themed roundup whose every
claim cites a source passage, reusing the same citation-verified
``generate_summary`` as the site) -> :func:`send_draft_email` (provider-agnostic
SMTP delivery, a no-op when SMTP is unconfigured).
"""

from __future__ import annotations

from actalux.digest.change_digest import ChangedDoc, ChangeDigest, build_change_digest
from actalux.digest.delivery import send_draft_email
from actalux.digest.drafter import DraftPost, draft_post
from actalux.digest.themes import THEME_ORDER, group_by_theme, theme_for

__all__ = [
    "THEME_ORDER",
    "ChangeDigest",
    "ChangedDoc",
    "DraftPost",
    "build_change_digest",
    "draft_post",
    "group_by_theme",
    "send_draft_email",
    "theme_for",
]
