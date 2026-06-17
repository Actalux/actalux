"""Supersede thin HTML twins with their full PDF document (dedup #14).

Several documents were ingested twice: once as a thin Diligent "library item"
HTML landing page (the boilerplate ``…General Library Item Name…Release Date…``
stub, ~1k chars / 1 chunk) and once as the real PDF (the full budget or
resolution, tens of thousands of chars / many chunks). The twins share an
entity + meeting_date + document_type but have different ``source_ref`` and
``content_hash``, so ingest-time dedup never collapsed them — both show as
separate entries in every ``replaces_id IS NULL`` listing (e.g. two
"2024-2025 …Budget" rows on /browse/budgets, only one of which embeds the PDF).

This marks each HTML stub as superseded by its PDF (``replaces_id`` -> pdf id),
so the PDF becomes the single canonical entry. Deep-links to a superseded HTML
chunk still resolve through ``resolve_canonical_document`` /
``resolve_canonical_chunk`` (content-matched, fails safe to a "superseded
version" notice — never a wrong citation). No ``budget_line_items`` cite any of
these HTML docs (verified), and no hardcoded ``Source(doc_id, …)`` citation in
code references them.

The pairs are an explicit allow-list, NOT derived from the loose
entity+date+type key — that key produced two FALSE twins (minutes-index vs
calendar PDF; Prop-O news vs certified-results PDF) that are genuinely
different documents and must be left alone.

Idempotent and reversible-by-inspection: only sets ``replaces_id`` on rows that
are still current, never deletes anything. Dry-run by default.

Run (prefix with `doppler run --project mac --config dev --`):
  uv run python scripts/dedup_html_pdf_twins.py            # dry run
  uv run python scripts/dedup_html_pdf_twins.py --apply    # write replaces_id
"""

from __future__ import annotations

import argparse
import logging

from actalux.config import load_config
from actalux.db import get_client

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# Verified true PDF/HTML twins: (html_stub_id -> canonical_pdf_id). Confirmed by
# content inspection — each HTML is the ~1k-char library-item stub, each PDF the
# full document (see confirm_twins output, 2026-06-17).
TWINS: list[tuple[int, int]] = [
    (249, 243),  # BOE Gun Safety Legislation Resolution (2022-10-26)
    (251, 245),  # Anti-Bias / Anti-Racism Board Resolution (2020-08-04)
    (264, 257),  # 2019-2020 District Budget
    (266, 260),  # 2021-2022 District Budget
    (268, 262),  # 2024-2025 District Budget
    (269, 263),  # 2023-2024 District Budget
]


def _fetch(client, doc_id: int) -> dict | None:
    rows = (
        client.table("documents")
        .select("id, source_file, content, replaces_id")
        .eq("id", doc_id)
        .execute()
        .data
    )
    return rows[0] if rows else None


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--apply", action="store_true", help="write replaces_id (default: dry run)")
    args = parser.parse_args()

    config = load_config()
    read_client = get_client(config.supabase_url, config.supabase_key)
    write_client = None
    if args.apply:
        if not config.supabase_service_key:
            raise SystemExit("ACTALUX_SUPABASE_SERVICE_KEY is required to --apply.")
        write_client = get_client(config.supabase_url, config.supabase_service_key)

    superseded = 0
    skipped = 0
    for html_id, pdf_id in TWINS:
        html = _fetch(read_client, html_id)
        pdf = _fetch(read_client, pdf_id)

        # Re-verify invariants at write time; refuse anything unexpected.
        if html is None or pdf is None:
            logger.error("SKIP %d->%d: missing row (html=%s pdf=%s)", html_id, pdf_id, html, pdf)
            skipped += 1
            continue
        if html["replaces_id"] == pdf_id:
            logger.info("already superseded: html #%d -> pdf #%d", html_id, pdf_id)
            skipped += 1
            continue
        if html["replaces_id"] is not None:
            logger.error(
                "SKIP %d->%d: html already superseded by %s", html_id, pdf_id, html["replaces_id"]
            )
            skipped += 1
            continue
        if pdf["replaces_id"] is not None:
            logger.error("SKIP %d->%d: pdf is itself superseded", html_id, pdf_id)
            skipped += 1
            continue
        hf = (html["source_file"] or "").lower()
        pf = (pdf["source_file"] or "").lower()
        if not hf.endswith((".html", ".htm")) or not pf.endswith(".pdf"):
            logger.error("SKIP %d->%d: extension mismatch (%s, %s)", html_id, pdf_id, hf, pf)
            skipped += 1
            continue
        if len(pdf["content"] or "") <= len(html["content"] or ""):
            logger.error(
                "SKIP %d->%d: pdf not richer than html (pdf=%d html=%d) — refusing",
                html_id,
                pdf_id,
                len(pdf["content"] or ""),
                len(html["content"] or ""),
            )
            skipped += 1
            continue

        if not args.apply:
            logger.info(
                "would supersede: html #%d %r -> pdf #%d %r",
                html_id,
                html["source_file"],
                pdf_id,
                pdf["source_file"],
            )
            superseded += 1
            continue

        # Guard the write with replaces_id IS NULL so a concurrent change can't be
        # clobbered, mirroring backfill_document_source_ref's defensive predicate.
        assert write_client is not None  # set whenever args.apply (checked above)
        (
            write_client.table("documents")
            .update({"replaces_id": pdf_id})
            .eq("id", html_id)
            .is_("replaces_id", "null")
            .execute()
        )
        # Confirm the write landed: a guarded update can affect zero rows under a
        # concurrent change or an RLS policy without raising, so verify by re-fetch
        # rather than trust the call. Read back through the write client to bypass
        # any anon-read RLS lag.
        check = _fetch(write_client, html_id)
        if check is None or check["replaces_id"] != pdf_id:
            logger.error(
                "WRITE NOT CONFIRMED %d->%d: replaces_id is %s after update",
                html_id,
                pdf_id,
                None if check is None else check["replaces_id"],
            )
            skipped += 1
            continue
        logger.info("superseded: html #%d -> pdf #%d", html_id, pdf_id)
        superseded += 1

    verb = "superseded" if args.apply else "would supersede"
    logger.info("Done: %d %s, %d skipped.", superseded, verb, skipped)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
