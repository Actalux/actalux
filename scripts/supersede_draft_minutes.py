"""Point a meeting's draft minutes at the final minutes that replaced them.

A board publishes draft minutes, then approves a final set at the following
meeting. The archive holds both, and until they are linked a search can return
draft text as though it were the adopted record, while the meetings endpoint
shows one meeting with two minutes documents.

Setting the draft's ``replaces_id`` to the final's id makes the final canonical:
the draft drops out of search and the meetings views, and a citation deep link
to the draft redirects to the final rather than 404ing. Nothing is deleted — the
draft row and its chunks stay exactly where they are, and the link is undone by
clearing one column, which is why this is safe to run on a public record.

Deliberately narrow. A pair qualifies only when a single (entity, meeting_date)
has exactly one draft-titled minutes document and exactly one that is not. Two
genuinely different meetings on one date — a retreat and a business meeting, a
tax-rate hearing and a regular session — do not qualify, and neither does a pair
of same-meeting files whose names merely differ, because neither is a draft. Both
of those exist in the corpus and must be left alone.

Run (prefix with `doppler run --project mac --config dev --`):
  uv run python scripts/supersede_draft_minutes.py            # dry run
  uv run python scripts/supersede_draft_minutes.py --apply    # write
"""

from __future__ import annotations

import argparse
import logging
import re
import sys
from collections import defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from actalux.config import load_config  # noqa: E402
from actalux.db import fetch_all_rows, get_client  # noqa: E402

logger = logging.getLogger(__name__)

# "- DRAFT", "MM draft", "Minutes Draft". Word-bounded so a name that merely
# contains the letters (a "Drafting Committee" report) does not qualify.
_DRAFT_RE = re.compile(r"\bdrafts?\b", re.I)


def is_draft(row: dict) -> bool:
    name = f"{row.get('source_file') or ''} {row.get('meeting_title') or ''}"
    return bool(_DRAFT_RE.search(name))


def find_pairs(rows: list[dict]) -> tuple[list[tuple[dict, dict]], list[tuple]]:
    """Return (draft, final) pairs and the groups deliberately skipped."""
    groups: dict[tuple, list[dict]] = defaultdict(list)
    for r in rows:
        if r.get("replaces_id") or r.get("document_type") != "minutes":
            continue
        if not r.get("meeting_date"):
            continue
        groups[(r["entity_id"], str(r["meeting_date"]))].append(r)

    pairs: list[tuple[dict, dict]] = []
    skipped: list[tuple] = []
    for key, members in sorted(groups.items()):
        if len(members) < 2:
            continue
        drafts = [m for m in members if is_draft(m)]
        finals = [m for m in members if not is_draft(m)]
        if len(drafts) == 1 and len(finals) == 1:
            pairs.append((drafts[0], finals[0]))
        else:
            skipped.append((key, members))
    return pairs, skipped


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--apply", action="store_true", help="write to the DB (default: dry run)")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    config = load_config()
    if args.apply and not config.supabase_service_key:
        raise SystemExit("ACTALUX_SUPABASE_SERVICE_KEY is required to --apply.")
    client = get_client(config.supabase_url, config.supabase_service_key or config.supabase_key)

    rows = fetch_all_rows(
        lambda: client.table("documents").select(
            "id,entity_id,document_type,meeting_date,meeting_title,source_file,replaces_id"
        )
    )
    pairs, skipped = find_pairs(rows)

    for draft, final in pairs:
        logger.info(
            "  %s  draft %s -> final %s\n      draft: %s\n      final: %s",
            draft["meeting_date"],
            draft["id"],
            final["id"],
            (draft.get("source_file") or "")[:60],
            (final.get("source_file") or "")[:60],
        )
    for (entity_id, meeting_date), members in skipped:
        logger.info(
            "  %s entity %s SKIPPED (not one draft + one final): %s",
            meeting_date,
            entity_id,
            [m["id"] for m in members],
        )

    logger.info("pairs to link: %d   groups skipped: %d", len(pairs), len(skipped))
    if not args.apply:
        logger.info("Dry run. Re-run with --apply to write.")
        return 0

    for draft, final in pairs:
        client.table("documents").update({"replaces_id": final["id"]}).eq(
            "id", draft["id"]
        ).execute()
    logger.info("Linked %d drafts to their final minutes.", len(pairs))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
