"""Compress an over-limit source PDF and upload the reduced copy to storage.

Supabase's free tier caps a stored object at 50 MB, so a larger source PDF
(e.g. the ~55 MB facilities master plan, doc #87) can never be uploaded and both
its inline embed and "Open original" link 404 -- the document is unviewable even
though its parsed text and citations are intact. This compresses the local source
with Ghostscript (``/ebook``, 150 dpi -- downsamples images, preserves text) to a
copy that fits, and uploads it under the same key so both views resolve.

The stored copy is a reduced-resolution derivative for viewing convenience; the
verbatim citations come from the original parse and are unaffected. The 55 MB
original in ``data/documents/`` is left untouched (the source of truth for a
future full-resolution upload once storage allows it).

Run (prefix with `doppler run --project mac --config dev --`):
  uv run python scripts/compress_and_upload_pdf.py <source_file>          # dry run
  uv run python scripts/compress_and_upload_pdf.py <source_file> --apply  # upload
"""

from __future__ import annotations

import argparse
import logging
import subprocess
import tempfile
import urllib.request
from pathlib import Path

from actalux.config import load_config
from actalux.db import get_client
from actalux.web.storage import BUCKET, stored_file_url

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

DOCS_DIR = Path(__file__).resolve().parent.parent / "data" / "documents"
SIZE_LIMIT = 50 * 1024 * 1024  # Supabase free-tier per-object cap
# Try the higher-quality preset first; fall back to the smaller one only if /ebook
# still exceeds the cap. /ebook ~150 dpi keeps maps/text legible; /screen ~72 dpi.
GS_PRESETS = ("/ebook", "/screen")


def _compress(src: Path, preset: str, out: Path) -> None:
    subprocess.run(
        [
            "gs",
            "-sDEVICE=pdfwrite",
            "-dCompatibilityLevel=1.4",
            f"-dPDFSETTINGS={preset}",
            "-dNOPAUSE",
            "-dQUIET",
            "-dBATCH",
            f"-sOutputFile={out}",
            str(src),
        ],
        check=True,
    )


def _object_exists(url: str) -> bool:
    try:
        req = urllib.request.Request(url, method="HEAD")
        with urllib.request.urlopen(req, timeout=15) as resp:
            return resp.status == 200
    except Exception:
        return False


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("source_file", help="filename under data/documents/")
    parser.add_argument("--apply", action="store_true", help="upload (default: dry run)")
    args = parser.parse_args()

    config = load_config()
    src = DOCS_DIR / args.source_file
    if not src.exists():
        raise SystemExit(f"Source not found: {src}")
    logger.info("Source %s is %.1f MB", args.source_file, src.stat().st_size / 1e6)

    with tempfile.TemporaryDirectory() as tmp:
        chosen: Path | None = None
        for preset in GS_PRESETS:
            out = Path(tmp) / f"compressed{preset.strip('/')}.pdf"
            _compress(src, preset, out)
            size = out.stat().st_size
            logger.info("%s -> %.1f MB", preset, size / 1e6)
            if size < SIZE_LIMIT:
                chosen = out
                logger.info("Using %s (%.1f MB, under the 50 MB cap).", preset, size / 1e6)
                break
        if chosen is None:
            raise SystemExit("Even /screen exceeds 50 MB; split the PDF or upgrade storage.")

        if not args.apply:
            logger.info("Dry run: would upload %s as %s.", chosen.name, args.source_file)
            return 0

        if not config.supabase_service_key:
            raise SystemExit("ACTALUX_SUPABASE_SERVICE_KEY is required to --apply.")
        write_client = get_client(config.supabase_url, config.supabase_service_key)
        write_client.storage.from_(BUCKET).upload(
            args.source_file,
            chosen.read_bytes(),
            {"content-type": "application/pdf", "upsert": "true"},
        )
        logger.info("Uploaded %s -> %s/%s", chosen.name, BUCKET, args.source_file)

    ok = _object_exists(stored_file_url(args.source_file, config))
    logger.info("Verify public URL HEAD 200: %s", ok)
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
