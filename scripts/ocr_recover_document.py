#!/usr/bin/env python3
"""Re-extract one document with OCR and replace its content + chunks in place.

Some PDFs have broken-font pages that the ingest text layer extracts as mojibake
(wrong-codepoint glyphs). The parser now OCR-recovers garbled pages
(ingest/parser.parse_pdf), so re-parsing the source PDF yields clean text. This
rebuilds ONE existing document from its source PDF and replaces its content,
content_hash, and chunks IN PLACE — keeping the document id (so /document/{id} and
curated links survive) while refreshing the chunk set (old chunks deleted, new
chunks embedded and inserted).

Re-chunking gives the document's chunks new ids, so this is safe only for a
document nothing references by chunk id. Verified for doc 87 (no budget_line_items
or corrections point at its chunks); re-check before pointing it at another doc.

Embeddings ARE recomputed here (unlike the control-char cleanup) because the text
genuinely changed — the recovered pages carry real words the old vectors never saw.

Dry-run by default; --apply writes (needs ACTALUX_SUPABASE_SERVICE_KEY).

Usage:
  doppler run --project mac --config dev -- uv run python scripts/ocr_recover_document.py
  doppler run --project mac --config dev -- uv run python scripts/ocr_recover_document.py --apply
"""

from __future__ import annotations

import argparse
import logging
import os
from pathlib import Path

from actalux.config import load_config
from actalux.db import get_client, insert_chunks
from actalux.ingest.chunker import chunk_document, validate_chunks
from actalux.ingest.embedder import embed_chunks
from actalux.ingest.hashing import content_hash
from actalux.ingest.parser import exotic_char_ratio, parse_file

logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger(__name__)

DEFAULT_DOC_ID = 87
DEFAULT_PDF = Path(
    "data/documents/Volume1-ClaytonMasterPlan-Process-Priorities-CostEstimations-HVAC.pdf"
)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--doc-id", type=int, default=DEFAULT_DOC_ID)
    parser.add_argument("--pdf", type=Path, default=DEFAULT_PDF)
    parser.add_argument("--apply", action="store_true", help="Write changes (default: dry-run).")
    args = parser.parse_args()

    if not args.pdf.exists():
        raise SystemExit(f"PDF not found: {args.pdf}")

    cfg = load_config()
    url = os.environ["ACTALUX_SUPABASE_URL"]
    key_var = "ACTALUX_SUPABASE_SERVICE_KEY" if args.apply else "ACTALUX_SUPABASE_KEY"
    try:
        key = os.environ[key_var]
    except KeyError as exc:
        raise SystemExit(
            f"Missing {exc}; run under doppler run --project mac --config dev -- ..."
        ) from exc
    client = get_client(url, key)

    rows = (
        client.table("documents")
        .select("id, content, content_hash")
        .eq("id", args.doc_id)
        .is_("replaces_id", "null")
        .execute()
    ).data
    if not rows:
        raise SystemExit(f"Document {args.doc_id} not found (or superseded)")
    old = rows[0]
    old_content = old.get("content") or ""

    logger.info("Re-parsing %s (OCR-recovering garbled pages)...", args.pdf.name)
    text = parse_file(args.pdf)
    new_hash = content_hash(text)

    logger.info(
        "doc #%d: content %d -> %d chars; exotic ratio %.3f -> %.3f; hash %s",
        args.doc_id,
        len(old_content),
        len(text),
        exotic_char_ratio(old_content),
        exotic_char_ratio(text),
        "unchanged" if new_hash == old.get("content_hash") else "changed",
    )

    chunks = chunk_document(
        args.doc_id,
        text,
        target_words=cfg.chunk_target_words,
        overlap_sentences=cfg.chunk_overlap_sentences,
    )
    valid = validate_chunks(chunks, text)
    logger.info("re-chunked into %d chunks (%d valid)", len(chunks), len(valid))
    if not valid:
        raise SystemExit("No valid chunks produced; aborting.")

    if not args.apply:
        logger.info("\nDRY RUN — no changes written. Re-run with --apply to write.")
        return 0

    # Embed before any write, so a failure here destroys nothing.
    embedded = embed_chunks(valid, model_name=cfg.embedding_model)
    client.table("documents").update({"content": text, "content_hash": new_hash}).eq(
        "id", args.doc_id
    ).execute()
    client.table("chunks").delete().eq("document_id", args.doc_id).execute()
    insert_chunks(client, embedded)
    logger.info(
        "\nApplied: doc #%d content+hash updated, %d chunks replaced.",
        args.doc_id,
        len(embedded),
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
