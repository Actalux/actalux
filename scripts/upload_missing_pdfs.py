"""Upload source PDFs that are referenced by a document row but missing from storage.

Many documents were ingested (rows + chunks) without their source PDF ever being
uploaded to the ``documents`` storage bucket, so the reader pane's "Original
document" iframe renders the bucket's 404 JSON instead of the PDF. This script
finds every current PDF-backed document whose storage object is missing and
(re)uploads the local source file from ``data/documents/`` under the same key.

Additive and idempotent: it only uploads objects that are absent (HEAD != 200);
it never deletes or rewrites DB rows. Dry-run by default.

Run (prefix with `doppler run --project mac --config dev --`):
  uv run python scripts/upload_missing_pdfs.py            # dry run: list what's missing
  uv run python scripts/upload_missing_pdfs.py --apply    # upload the missing PDFs
"""

from __future__ import annotations

import argparse
import logging
import urllib.request
from pathlib import Path

from actalux.config import load_config
from actalux.db import get_client
from actalux.web.storage import BUCKET, stored_file_url

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

DOCS_DIR = Path(__file__).resolve().parent.parent / "data" / "documents"


def _object_exists(url: str) -> bool:
    """True when the public storage URL serves the object (HEAD 200)."""
    try:
        req = urllib.request.Request(url, method="HEAD")
        with urllib.request.urlopen(req, timeout=15) as resp:
            return resp.status == 200
    except Exception:
        return False


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--apply", action="store_true", help="upload (default: dry run)")
    args = parser.parse_args()

    config = load_config()
    # Reads use the public URL; the upload needs the service key (bucket write).
    read_client = get_client(config.supabase_url, config.supabase_key)
    write_client = None
    if args.apply:
        if not config.supabase_service_key:
            raise SystemExit("ACTALUX_SUPABASE_SERVICE_KEY is required to --apply.")
        write_client = get_client(config.supabase_url, config.supabase_service_key)

    docs = (
        read_client.table("documents")
        .select("id,source_file")
        .is_("replaces_id", "null")
        .execute()
        .data
    ) or []
    pdfs = [d for d in docs if (d["source_file"] or "").lower().endswith(".pdf")]

    missing_local: list[str] = []
    uploaded = 0
    present = 0
    for d in pdfs:
        key = d["source_file"]
        if _object_exists(stored_file_url(key, config)):
            present += 1
            continue
        local = DOCS_DIR / key
        if not local.exists():
            missing_local.append(key)
            logger.warning("MISSING object AND no local file for #%s: %s", d["id"], key)
            continue
        if not args.apply:
            logger.info("would upload #%s: %s (%d bytes)", d["id"], key, local.stat().st_size)
            uploaded += 1
            continue
        try:
            write_client.storage.from_(BUCKET).upload(
                key,
                local.read_bytes(),
                {"content-type": "application/pdf", "upsert": "true"},
            )
            uploaded += 1
            logger.info("uploaded #%s -> %s/%s", d["id"], BUCKET, key)
        except Exception:
            logger.exception("upload failed for #%s: %s", d["id"], key)

    verb = "uploaded" if args.apply else "would upload"
    logger.info(
        "Done: %d PDF docs, %d already present, %d %s, %d missing locally.",
        len(pdfs),
        present,
        uploaded,
        verb,
        len(missing_local),
    )
    if missing_local:
        logger.warning("No local source file for: %s", ", ".join(sorted(missing_local)))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
