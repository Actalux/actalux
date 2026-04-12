#!/usr/bin/env python3
"""Upload local documents to Supabase Storage and update DB with public URLs.

Usage:
    doppler run --project mac --config dev -- uv run python scripts/upload_to_storage.py

Requires SUPABASE_SERVICE_KEY in environment (for storage uploads).
"""

from __future__ import annotations

import logging
import mimetypes
import os
from pathlib import Path

from supabase import create_client

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

BUCKET = "documents"
DATA_DIR = Path("data/documents")

MIME_OVERRIDES = {
    ".txt": "text/plain",
    ".md": "text/markdown",
    ".html": "text/html",
    ".htm": "text/html",
    ".pdf": "application/pdf",
}


def get_mime_type(path: Path) -> str:
    """Get MIME type for a file."""
    ext = path.suffix.lower()
    if ext in MIME_OVERRIDES:
        return MIME_OVERRIDES[ext]
    mime, _ = mimetypes.guess_type(str(path))
    return mime or "application/octet-stream"


def main() -> None:
    url = os.environ["SUPABASE_URL"]
    service_key = os.environ["SUPABASE_SERVICE_KEY"]
    anon_key = os.environ["SUPABASE_KEY"]

    # Service client for storage uploads
    storage_client = create_client(url, service_key)
    # Anon client for DB updates (uses same schema)
    db_client = create_client(url, anon_key)

    # Get all documents from DB
    result = db_client.table("documents").select("id, source_file").execute()
    db_docs = {d["source_file"]: d["id"] for d in result.data}
    logger.info("Found %d documents in DB", len(db_docs))

    # List files already in storage
    try:
        existing = storage_client.storage.from_(BUCKET).list()
        existing_names = {f["name"] for f in existing}
    except Exception:
        existing_names = set()
    logger.info("Found %d files already in storage", len(existing_names))

    # Upload files
    uploaded = 0
    skipped = 0
    errors = 0

    doc_files = sorted(
        f for f in DATA_DIR.iterdir() if f.is_file() and not f.name.endswith(".json")
    )

    for file_path in doc_files:
        storage_key = file_path.name

        if storage_key in existing_names:
            skipped += 1
            continue

        mime = get_mime_type(file_path)
        file_bytes = file_path.read_bytes()

        try:
            storage_client.storage.from_(BUCKET).upload(
                storage_key,
                file_bytes,
                {"content-type": mime},
            )
            uploaded += 1

            if uploaded % 20 == 0:
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

    # Update DB with storage URLs
    base_url = storage_client.storage.from_(BUCKET).get_public_url("")
    updated = 0

    for source_file, doc_id in db_docs.items():
        storage_url = f"{base_url}{source_file}"
        db_client.table("documents").update({"source_url": storage_url}).eq("id", doc_id).execute()
        updated += 1

    logger.info("Updated %d document rows with storage URLs", updated)


if __name__ == "__main__":
    main()
