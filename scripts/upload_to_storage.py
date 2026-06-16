#!/usr/bin/env python3
"""Upload local PDF source files to the Supabase Storage ``documents`` bucket.

Uploads are idempotent: a file already present in the bucket is skipped. Only
``.pdf`` files are uploaded — ``.html``/``.txt``/``.md`` are deliberately not
mirrored to storage (raw markup/text dumps are not worth embedding, and the
canonical "Open original" link comes from ``documents.source_url``, which this
script never touches). The public URL for an uploaded file is derived on demand
via ``actalux.web.storage.stored_file_url``; there is no DB write here.

Usage:
    doppler run --project mac --config dev -- uv run python scripts/upload_to_storage.py

Requires ACTALUX_SUPABASE_URL and ACTALUX_SUPABASE_SERVICE_KEY in the
environment (the service key is needed for storage writes).
"""

from __future__ import annotations

import logging
import os
from pathlib import Path

from supabase import create_client

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

BUCKET = "documents"
DATA_DIR = Path("data/documents")
# Only PDFs are mirrored to storage (operator decision): HTML/TXT/MD are raw
# dumps, not useful embeds.
UPLOAD_SUFFIX = ".pdf"
PDF_MIME = "application/pdf"
PROGRESS_EVERY = 20
# storage list() is server-paginated (default page size 100); page explicitly so
# skip-existing stays correct past the first page (the corpus exceeds 100 PDFs).
LIST_PAGE_SIZE = 100


def list_existing_keys(storage_client) -> set[str]:
    """Return every object name in the bucket, paging past the 100-row default.

    The storage list endpoint caps each response at ``LIST_PAGE_SIZE``; without
    paging, a re-run would only skip the first page and re-attempt files already
    present. Pages by offset until a short (final) page is returned.
    """
    names: set[str] = set()
    offset = 0
    while True:
        page = storage_client.storage.from_(BUCKET).list(
            options={"limit": LIST_PAGE_SIZE, "offset": offset}
        )
        names.update(f["name"] for f in page)
        if len(page) < LIST_PAGE_SIZE:
            break
        offset += LIST_PAGE_SIZE
    return names


def main() -> None:
    url = os.environ["ACTALUX_SUPABASE_URL"]
    service_key = os.environ["ACTALUX_SUPABASE_SERVICE_KEY"]

    # Service client: storage writes bypass RLS.
    storage_client = create_client(url, service_key)

    # List files already in the bucket so re-runs skip them (idempotent).
    try:
        existing_names = list_existing_keys(storage_client)
    except Exception:
        existing_names = set()
    logger.info("Found %d files already in storage", len(existing_names))

    pdf_files = sorted(
        f for f in DATA_DIR.iterdir() if f.is_file() and f.suffix.lower() == UPLOAD_SUFFIX
    )
    logger.info("Found %d local PDF files to consider", len(pdf_files))

    uploaded = 0
    skipped = 0
    errors = 0

    for file_path in pdf_files:
        storage_key = file_path.name

        if storage_key in existing_names:
            skipped += 1
            continue

        file_bytes = file_path.read_bytes()
        try:
            storage_client.storage.from_(BUCKET).upload(
                storage_key,
                file_bytes,
                {"content-type": PDF_MIME},
            )
            uploaded += 1
            if uploaded % PROGRESS_EVERY == 0:
                logger.info("  Uploaded %d files...", uploaded)
        except Exception as exc:
            logger.error("  Failed %s: %s", storage_key, exc)
            errors += 1

    logger.info(
        "Upload complete: %d uploaded, %d skipped, %d errors",
        uploaded,
        skipped,
        errors,
    )


if __name__ == "__main__":
    main()
