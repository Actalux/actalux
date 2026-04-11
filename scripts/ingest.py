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
ISO_DATE_RE = re.compile(r"^(\d{4}-\d{2}-\d{2})")
# "April 10, 2024" or "Aug 14 2024" (full or abbreviated, with or without comma)
NATURAL_DATE_RE = re.compile(
    r"(Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|Jun(?:e)?|Jul(?:y)?"
    r"|Aug(?:ust)?|Sep(?:tember)?|Oct(?:ober)?|Nov(?:ember)?|Dec(?:ember)?)"
    r"\s+(\d{1,2}),?\s+(\d{4})",
    re.IGNORECASE,
)
# "10-29-25" or "2-04-26" — M-D-YY with 2-digit year
SHORT_DATE_RE = re.compile(r"^(\d{1,2})-(\d{1,2})-(\d{2})\b")
# "jan21" — abbreviated month + day (no separator), e.g., "jan21_board_meeting.txt"
COMPACT_DATE_RE = re.compile(
    r"(jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)(\d{1,2})",
    re.IGNORECASE,
)
# "2024-2025" fiscal year anywhere in the filename
FISCAL_YEAR_RE = re.compile(r"(\d{4})-(\d{4})\s+")

MONTH_NAMES = {
    "jan": 1,
    "january": 1,
    "feb": 2,
    "february": 2,
    "mar": 3,
    "march": 3,
    "apr": 4,
    "april": 4,
    "may": 5,
    "jun": 6,
    "june": 6,
    "jul": 7,
    "july": 7,
    "aug": 8,
    "august": 8,
    "sep": 9,
    "september": 9,
    "oct": 10,
    "october": 10,
    "nov": 11,
    "november": 11,
    "dec": 12,
    "december": 12,
}

DOC_TYPE_PATTERNS = {
    "agenda": re.compile(r"agenda", re.IGNORECASE),
    "minutes": re.compile(r"minutes", re.IGNORECASE),
    "packet": re.compile(r"packet|board.?pack", re.IGNORECASE),
    "resolution": re.compile(r"resolution", re.IGNORECASE),
    "budget": re.compile(r"budget", re.IGNORECASE),
    "presentation": re.compile(r"presentation|preliminary.?plan", re.IGNORECASE),
    "ballot": re.compile(r"ballot", re.IGNORECASE),
}

# Fallback: if filename doesn't match any pattern but is a .txt file
# from the transcripts directory, classify as "transcript"
TRANSCRIPT_EXTENSIONS = {".txt"}


def infer_meeting_date(name: str) -> date | None:
    """Extract a date from a directory or filename.

    Handles:
      - ISO prefix: "2024-03-15_board-meeting"
      - Short date: "10-29-25 Board of Education Meeting.txt" (M-D-YY)
      - Natural date: "April 10, 2024 Meeting Minutes.pdf"
      - Compact date: "jan21_board_meeting.txt" (monDD)
      - Fiscal year: "2024-2025 School District of Clayton Budget.html"
        (uses July 1 of the start year as the fiscal year start)
    """
    # Try ISO date prefix first
    match = ISO_DATE_RE.match(name)
    if match:
        return date.fromisoformat(match.group(1))

    # Try short date ("M-D-YY" with 2-digit year)
    match = SHORT_DATE_RE.match(name)
    if match:
        month = int(match.group(1))
        day = int(match.group(2))
        short_year = int(match.group(3))
        year = 2000 + short_year if short_year < 50 else 1900 + short_year
        return date(year, month, day)

    # Try natural date ("Month DD, YYYY")
    match = NATURAL_DATE_RE.search(name)
    if match:
        month = MONTH_NAMES[match.group(1).lower()]
        day = int(match.group(2))
        year = int(match.group(3))
        return date(year, month, day)

    # Try compact month+year ("Feb2025", "April2026" — no day)
    month_year_match = re.search(
        r"(Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|Jun(?:e)?|Jul(?:y)?"
        r"|Aug(?:ust)?|Sep(?:tember)?|Oct(?:ober)?|Nov(?:ember)?|Dec(?:ember)?)"
        r"(\d{4})",
        name,
        re.IGNORECASE,
    )
    if month_year_match:
        month = MONTH_NAMES[month_year_match.group(1).lower()]
        year = int(month_year_match.group(2))
        return date(year, month, 1)

    # Try compact date ("jan21" — abbreviated month + day, no year)
    match = COMPACT_DATE_RE.search(name)
    if match:
        month = MONTH_NAMES[match.group(1).lower()]
        day = int(match.group(2))
        # Infer year: assume most recent past occurrence
        from datetime import date as date_type

        today = date_type.today()
        candidate = date(today.year, month, day)
        if candidate > today:
            candidate = date(today.year - 1, month, day)
        return candidate

    # Try fiscal year ("2024-2025 ..." anywhere in name)
    match = FISCAL_YEAR_RE.search(name)
    if match:
        start_year = int(match.group(1))
        return date(start_year, 7, 1)

    return None


def infer_document_type(filename: str) -> str:
    """Guess document type from filename."""
    for doc_type, pattern in DOC_TYPE_PATTERNS.items():
        if pattern.search(filename):
            return doc_type
    # .txt files that match "Board of Education" are transcripts
    ext = Path(filename).suffix.lower()
    if ext in TRANSCRIPT_EXTENSIONS and re.search(r"board", filename, re.IGNORECASE):
        return "transcript"
    return "other"


def infer_meeting_title(name: str) -> str:
    """Create a meeting title from a directory or filename.

    Strips the file extension and date prefix to produce a clean title.
    """
    # Remove extension if present
    title = Path(name).stem

    # Remove ISO date prefix
    title = ISO_DATE_RE.sub("", title).strip("_- ")

    # Clean up separators
    title = title.replace("-", " ").replace("_", " ").strip()

    return title or "Board Meeting"


def ingest_directory(data_dir: Path) -> None:
    """Ingest documents from data_dir.

    Supports two layouts:
      1. Subdirectories per meeting: data/documents/2024-03-15_board-meeting/
      2. Flat directory: data/documents/April 10, 2024 Meeting Minutes.pdf
         (date and title are inferred from each filename)
    """
    config = load_config()
    client = get_client(config.supabase_url, config.supabase_key)

    # Deduplication: fetch already-ingested source_file names
    existing_result = client.table("documents").select("source_file").execute()
    already_ingested: set[str] = {r["source_file"] for r in existing_result.data}
    if already_ingested:
        logger.info(
            "Found %d already-ingested documents, will skip duplicates", len(already_ingested)
        )

    meeting_dirs = sorted(
        [d for d in data_dir.iterdir() if d.is_dir()],
        key=lambda d: d.name,
    )

    # Check for files directly in data_dir (flat layout)
    flat_files = sorted(
        f for f in data_dir.iterdir() if f.is_file() and f.suffix.lower() in SUPPORTED_EXTENSIONS
    )

    total_docs = 0
    total_chunks = 0
    total_failed = 0

    # Process flat files (each file is its own meeting/document)
    if flat_files:
        logger.info("Found %d files in flat directory %s", len(flat_files), data_dir)
        for doc_file in flat_files:
            if doc_file.name in already_ingested:
                logger.info("  SKIP (already ingested): %s", doc_file.name)
                continue

            meeting_date = infer_meeting_date(doc_file.name) or date.today()
            meeting_title = infer_meeting_title(doc_file.name)

            logger.info(
                "Processing: %s (%s)",
                meeting_title,
                meeting_date,
            )

            try:
                result = ingest_single_file(
                    client=client,
                    path=doc_file,
                    meeting_date=meeting_date,
                    meeting_title=meeting_title,
                    config=config,
                )
                total_docs += 1
                total_chunks += result["chunks"]
                logger.info(
                    "  OK %s: %d chunks ingested",
                    doc_file.name,
                    result["chunks"],
                )
            except ActaluxError as exc:
                total_failed += 1
                logger.error("  FAIL %s: %s", doc_file.name, exc)

            # Log each file as its own ingest run
            run = IngestRun(
                meeting_date=meeting_date,
                meeting_title=meeting_title,
                docs_found=1,
                docs_ingested=1 if total_failed == 0 else 0,
                docs_failed=1 if total_failed > 0 else 0,
                errors=[],
            )
            try:
                insert_ingest_run(client, run)
            except Exception as exc:
                logger.error("Failed to log ingest run: %s", exc)

    # Process subdirectories (grouped by meeting)
    for meeting_dir in meeting_dirs:
        meeting_date = infer_meeting_date(meeting_dir.name)
        meeting_title = infer_meeting_title(meeting_dir.name)

        doc_files = sorted(
            f
            for f in meeting_dir.iterdir()
            if f.is_file() and f.suffix.lower() in SUPPORTED_EXTENSIONS
        )

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

        for doc_file in doc_files:
            if doc_file.name in already_ingested:
                logger.info("  SKIP (already ingested): %s", doc_file.name)
                continue

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
                    "  OK %s: %d chunks ingested",
                    doc_file.name,
                    result["chunks"],
                )
            except ActaluxError as exc:
                docs_failed += 1
                errors.append(f"{doc_file.name}: {exc}")
                logger.error("  FAIL %s: %s", doc_file.name, exc)

        total_docs += docs_ingested
        total_failed += docs_failed

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
        logger.warning("%d documents failed to ingest. Check errors above.", total_failed)
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
