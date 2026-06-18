#!/usr/bin/env python3
"""Draft a cited "what changed" post from recently ingested records.

Reads the documents that landed since a timestamp (or the last ``--days``),
groups them into topics, and writes a citation-backed, nonpartisan draft for
human review. Optionally emails the draft. It NEVER publishes — a person
reviews and posts.

Usage:
    # Everything ingested in the last 7 days (default), write ./draft.md
    python scripts/draft_substack.py

    # Since an explicit ingest timestamp, write a file, and email it
    python scripts/draft_substack.py --since 2026-06-11T09:00:00+00:00 \
        --out draft.md --email

Run under `doppler run --project mac --config dev -- ...` locally so the
Supabase/OpenAI/SMTP env vars are present. In CI the ingest workflow sets them.
"""

from __future__ import annotations

import argparse
import logging
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from actalux.config import load_config
from actalux.db import get_client, get_entity_by_path
from actalux.digest import build_change_digest, draft_post, send_draft_email

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("draft_substack")

DEFAULT_ENTITY_PATH = "mo/clayton/schools"
DEFAULT_DAYS = 7


def resolve_entity_id(client: Any, entity_path: str) -> int:
    """Resolve a 'state/place/body' path to its entities.id, or abort."""
    parts = entity_path.strip("/").split("/")
    if len(parts) != 3:
        raise SystemExit(f"--entity must be 'state/place/body', got {entity_path!r}")
    entity = get_entity_by_path(client, *parts)
    if not entity:
        raise SystemExit(f"Unknown entity {entity_path!r}; seed it first.")
    return entity["id"]


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--since",
        help="ISO-8601 lower bound on ingest time (created_at). Overrides --days.",
    )
    parser.add_argument(
        "--days",
        type=int,
        default=DEFAULT_DAYS,
        help=f"Look back this many days when --since is absent (default {DEFAULT_DAYS}).",
    )
    parser.add_argument("--entity", default=DEFAULT_ENTITY_PATH, help="state/place/body path.")
    parser.add_argument(
        "--out", default="draft.md", type=Path, help="Where to write the markdown draft."
    )
    parser.add_argument("--email", action="store_true", help="Also email the draft (if SMTP set).")
    parser.add_argument(
        "--limit", type=int, default=500, help="Max documents to include in one digest."
    )
    parser.add_argument("--model", help="Override the summary model.")
    args = parser.parse_args()

    since = args.since or (datetime.now(UTC) - timedelta(days=args.days)).isoformat()

    config = load_config()
    # Prefer the service key (present in CI / for ingest) so reads never hit an RLS
    # surprise; fall back to the publishable key for a plain local run.
    key = config.supabase_service_key or config.supabase_key
    client = get_client(config.supabase_url, key)
    entity_id = resolve_entity_id(client, args.entity)

    digest = build_change_digest(client, since, entity_id=entity_id, limit=args.limit)
    if digest.is_empty:
        logger.info("No new or updated documents since %s; nothing to draft.", since)
        return

    if not config.openai_api_key:
        logger.warning("No OPENAI_API_KEY set; topics will list documents without cited summaries.")

    generated_on = datetime.now(UTC).date().isoformat()
    draft = draft_post(
        client,
        digest,
        config.openai_api_key,
        generated_on=generated_on,
        model=args.model or config.summary_model,
        site_base_url=config.site_base_url,
    )

    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(draft.markdown, encoding="utf-8")
    logger.info(
        "Wrote draft to %s — %d document(s) across %d topic(s); "
        "%d citation(s) verified, %d dropped.",
        args.out,
        draft.doc_count,
        draft.theme_count,
        draft.citations_verified,
        draft.citations_dropped,
    )

    if args.email:
        sent = send_draft_email(
            draft.subject,
            draft.markdown,
            host=config.smtp_host,
            port=config.smtp_port,
            user=config.smtp_user,
            password=config.smtp_password,
            email_from=config.draft_email_from,
            email_to=config.draft_email_to,
        )
        if not sent:
            logger.info("Email not sent (SMTP unconfigured); the draft file is at %s.", args.out)


if __name__ == "__main__":
    sys.exit(main())
