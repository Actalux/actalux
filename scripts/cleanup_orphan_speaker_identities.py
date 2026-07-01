"""Delete speaker_identities left on superseded (non-current) transcript documents.

``speaker_identities.document_id`` is ``ON DELETE CASCADE``, but a re-transcribe does
not delete the old document — it supersedes it (sets ``replaces_id`` to the new row and
keeps the old one for the version chain). The old document's speaker-identity rows then
linger: never read (the web reads current documents only), but clutter that grows one
row per re-transcribed cluster. This removes them.

Only identities on documents with ``replaces_id IS NOT NULL`` are touched; current-doc
identities are never deleted. The confidence-protection trigger (migrate_035) guards
UPDATEs only, so deleting a confirmed orphan is allowed by design ("to un-confirm,
delete the row").

Run (prefix with `doppler run --project mac --config dev --`):
    uv run python scripts/cleanup_orphan_speaker_identities.py            # dry run
    uv run python scripts/cleanup_orphan_speaker_identities.py --apply    # delete
"""

from __future__ import annotations

import argparse
import logging

from actalux.config import load_config
from actalux.db import fetch_all_rows, get_client

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

_BATCH = 100  # id-delete batch size, well under PostgREST's URL/param limits


def _superseded_doc_ids(client, doc_ids: list[int]) -> set[int]:
    """Of the given documents, those that have been superseded (replaces_id set)."""
    superseded: set[int] = set()
    for i in range(0, len(doc_ids), _BATCH):
        batch = doc_ids[i : i + _BATCH]
        rows = (
            client.table("documents")
            .select("id, replaces_id")
            .in_("id", batch)
            .not_.is_("replaces_id", "null")
            .execute()
            .data
        )
        superseded.update(r["id"] for r in rows)
    return superseded


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--apply", action="store_true", help="delete the orphans (default: dry run)"
    )
    args = parser.parse_args()

    config = load_config()
    if args.apply and not config.supabase_service_key:
        raise SystemExit("ACTALUX_SUPABASE_SERVICE_KEY is required to --apply.")
    client = get_client(config.supabase_url, config.supabase_service_key or config.supabase_key)

    identities = fetch_all_rows(
        lambda: client.table("speaker_identities").select("id, document_id, confidence")
    )
    doc_ids = sorted({r["document_id"] for r in identities})
    superseded = _superseded_doc_ids(client, doc_ids)
    orphans = [r for r in identities if r["document_id"] in superseded]

    logger.info(
        "%d speaker_identities total; %d on superseded documents (%d current docs superseded).",
        len(identities),
        len(orphans),
        len(superseded),
    )
    if not orphans:
        logger.info("Nothing to clean up.")
        return 0

    if not args.apply:
        logger.info("Dry run. Re-run with --apply to delete the %d orphan(s).", len(orphans))
        return 0

    orphan_ids = [r["id"] for r in orphans]
    for i in range(0, len(orphan_ids), _BATCH):
        batch = orphan_ids[i : i + _BATCH]
        client.table("speaker_identities").delete().in_("id", batch).execute()
    logger.info("Deleted %d orphan speaker_identities.", len(orphan_ids))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
