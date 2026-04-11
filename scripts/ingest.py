#!/usr/bin/env python3
"""Ingest official documents into Actalux.

Usage:
    python scripts/ingest.py data/documents/

Processes all PDF, HTML, and Markdown files in the given directory.
Each file is parsed, chunked, embedded, validated, and stored in Supabase.

Expects a directory structure like:
    data/documents/
    ├── 2026-03-15_board-meeting/
    │   ├── agenda.pdf
    │   ├── minutes.pdf
    │   └── packet.pdf
    └── 2026-04-08_board-meeting/
        ├── agenda.pdf
        └── minutes.html

Meeting date is extracted from the directory name (YYYY-MM-DD prefix).
Document type is inferred from the filename (agenda, minutes, packet, resolution).
"""

from __future__ import annotations

import logging
import re
import sys
from datetime import date
from pathlib import Path
from typing import Any

from actalux.config import load_config
from actalux.db import get_client, insert_chunks, insert_document, insert_ingest_run
from actalux.errors import ActaluxError, ParseError
from actalux.ingest.chunker import chunk_document, validate_chunks
from actalux.ingest.embedder import embed_chunks
from actalux.ingest.parser import parse_file
from actalux.models import Document, IngestRun

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(name)s %(levelname)s %(message)s",
)
logger = logging.getLogger(__name__)

SUPPORTED_EXTENSIONS = {".pdf", ".html", ".htm", ".md", ".markdown", ".txt"}
DATE_RE = re.compile(r"^(\d{4}-\d{2}-\d{2})")
DOC_TYPE_PATTERNS = {
    "agenda": re.compile(r"agenda", re.IGNORECASE),
    "minutes": re.compile(r"minutes", re.IGNORECASE),
    "packet": re.compile(r"packet|board.?pack", re.IGNORECASE),
    "resolution": re.compile(r"resolution", re.IGNORECASE),
}


def infer_meeting_date(dir_name: str) -> date | None:
    """Extract YYYY-MM-DD from directory name."""
    match = DATE_RE.match(dir_name)
    if match:
        return date.fromisoformat(match.group(1))
    return None


def infer_document_type(filename: str) -> str:
    """Guess document type from filename."""
    for doc_type, pattern in DOC_TYPE_PATTERNS.items():
        if pattern.search(filename):
            return doc_type
    return "other"


def infer_meeting_title(dir_name: str) -> str:
    """Create a meeting title from the directory name."""
    # Remove date prefix and clean up
    title = DATE_RE.sub("", dir_name).strip("_- ")
    return title.replace("-", " ").replace("_", " ").title() or "Board Meeting"


def ingest_directory(data_dir: Path) -> None:
    """Ingest all meeting directories under data_dir."""
    config = load_config()
    client = get_client(config.supabase_url, config.supabase_key)

    meeting_dirs = sorted(
        [d for d in data_dir.iterdir() if d.is_dir()],
        key=lambda d: d.name,
    )

    if not meeting_dirs:
        # Flat directory with files directly
        logger.info("No subdirectories found. Treating %s as a single meeting.", data_dir)
        meeting_dirs = [data_dir]

    total_docs = 0
    total_chunks = 0
    total_failed = 0

    for meeting_dir in meeting_dirs:
        meeting_date = infer_meeting_date(meeting_dir.name)
        meeting_title = infer_meeting_title(meeting_dir.name)

        doc_files = [
            f
            for f in meeting_dir.iterdir()
            if f.is_file() and f.suffix.lower() in SUPPORTED_EXTENSIONS
        ]

        if not doc_files:
            logger.warning("No supported files in %s, skipping", meeting_dir.name)
            continue

        logger.info(
            "Processing meeting: %s (%s) — %d documents",
            meeting_title,
            meeting_date or "unknown date",
            len(doc_files),
        )

        docs_ingested = 0
        docs_failed = 0
        errors: list[str] = []

        for doc_file in sorted(doc_files):
            try:
                result = ingest_single_file(
                    client=client,
                    path=doc_file,
                    meeting_date=meeting_date or date.today(),
                    meeting_title=meeting_title,
                    config=config,
                )
                docs_ingested += 1
                total_chunks += result["chunks"]
                logger.info(
                    "  ✓ %s: %d chunks ingested",
                    doc_file.name,
                    result["chunks"],
                )
            except ActaluxError as exc:
                docs_failed += 1
                errors.append(f"{doc_file.name}: {exc}")
                logger.error("  ✗ %s: %s", doc_file.name, exc)

        total_docs += docs_ingested
        total_failed += docs_failed

        # Log the ingest run
        run = IngestRun(
            meeting_date=meeting_date or date.today(),
            meeting_title=meeting_title,
            docs_found=len(doc_files),
            docs_ingested=docs_ingested,
            docs_failed=docs_failed,
            errors=errors,
        )
        try:
            insert_ingest_run(client, run)
        except Exception as exc:
            logger.error("Failed to log ingest run: %s", exc)

    logger.info(
        "Ingestion complete: %d documents, %d chunks, %d failures",
        total_docs,
        total_chunks,
        total_failed,
    )

    if total_failed > 0:
        logger.warning("⚠ %d documents failed to ingest. Check errors above.", total_failed)
        sys.exit(1)


def ingest_single_file(
    client: Any,
    path: Path,
    meeting_date: date,
    meeting_title: str,
    config: Any,
) -> dict[str, int]:
    """Parse, chunk, embed, validate, and store a single document.

    Returns {"chunks": N} with the number of chunks stored.
    """
    # 1. Parse
    text = parse_file(path)

    # 2. Store the document
    doc = Document(
        meeting_date=meeting_date,
        meeting_title=meeting_title,
        document_type=infer_document_type(path.name),
        source_url="",  # will be set when we have URLs
        source_file=path.name,
        content=text,
    )
    doc_id = insert_document(client, doc)

    # 3. Chunk
    chunks = chunk_document(
        document_id=doc_id,
        text=text,
        target_words=config.chunk_target_words,
        overlap_sentences=config.chunk_overlap_sentences,
    )

    # 4. Validate (citation integrity: each chunk must be a substring of source)
    valid_chunks = validate_chunks(chunks, text)
    if len(valid_chunks) < len(chunks):
        logger.warning(
            "%d/%d chunks failed validation for %s",
            len(chunks) - len(valid_chunks),
            len(chunks),
            path.name,
        )

    if not valid_chunks:
        raise ParseError(f"All chunks failed validation for {path.name}")

    # 5. Embed
    embedded_chunks = embed_chunks(valid_chunks, model_name=config.embedding_model)

    # 6. Store chunks
    insert_chunks(client, embedded_chunks)

    return {"chunks": len(embedded_chunks)}


def main() -> None:
    if len(sys.argv) < 2:
        print("Usage: python scripts/ingest.py <data_directory>")
        print("Example: python scripts/ingest.py data/documents/")
        sys.exit(1)

    data_dir = Path(sys.argv[1])
    if not data_dir.is_dir():
        print(f"Error: {data_dir} is not a directory")
        sys.exit(1)

    ingest_directory(data_dir)


if __name__ == "__main__":
    main()
