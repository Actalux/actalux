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

import argparse
import logging
import re
import sys
from dataclasses import replace
from datetime import date
from pathlib import Path
from typing import Any
from urllib.parse import SplitResult, parse_qs, urlsplit, urlunsplit

from actalux.config import load_config
from actalux.db import (
    backfill_document_source_ref,
    delete_chunks_for_document,
    delete_document,
    document_has_chunks,
    find_document_by_content_hash,
    find_document_by_source,
    find_document_by_source_ref,
    get_client,
    get_entity_by_path,
    insert_chunks,
    insert_document,
    insert_ingest_run,
    supersede_document,
    update_document_checked,
)
from actalux.errors import ActaluxError, ParseError
from actalux.ingest import pii_guard
from actalux.ingest.bodies import get_body
from actalux.ingest.chunker import chunk_document, validate_chunks
from actalux.ingest.classify import classify_document_type, parse_meeting_date
from actalux.ingest.embedder import embed_chunks
from actalux.ingest.hashing import assign_citation_ids, content_hash, doc_stable_key
from actalux.ingest.parser import parse_file
from actalux.models import Document, IngestRun

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(name)s %(levelname)s %(message)s",
)
logger = logging.getLogger(__name__)

SUPPORTED_EXTENSIONS = {".pdf", ".html", ".htm", ".md", ".markdown", ".txt"}
# Date-prefix stripper for building a clean title (the date itself is parsed by
# actalux.ingest.classify.parse_meeting_date).
ISO_DATE_RE = re.compile(r"^(\d{4}-\d{2}-\d{2})")
TRANSCRIPT_EXTENSIONS = {".txt"}

# Canva curriculum maps are scraped as bare skill tables; the subject and grade
# band survive only in the filename, so the chunk body is unfindable by subject
# (the 1-5 Spanish map's only chunk contains no "Spanish"). See subject_header.
CANVA_MAP_RE = re.compile(r"^canva_.*curriculum_map", re.IGNORECASE)

# Owning body for ingested docs, as state/place/body. Every doc must carry an
# entity_id or it is invisible to the entity-scoped browse/search (migrate_012).
# Matches the web app's apex-redirect target (app.DEFAULT_ENTITY_PATH); override
# with --entity once a second body is crawled.
DEFAULT_ENTITY_PATH = "mo/clayton/schools"


def resolve_entity_id(client: Any, entity_path: str) -> int:
    """Resolve a 'state/place/body' path to its entities.id, or abort.

    Aborting (rather than ingesting with a NULL entity_id) is deliberate: a
    NULL-entity doc is silently absent from every entity-scoped view, so a typo'd
    or unseeded entity must fail loudly at the start, not orphan the whole batch.
    """
    parts = entity_path.strip("/").split("/")
    if len(parts) != 3:
        raise SystemExit(f"--entity must be 'state/place/body', got {entity_path!r}")
    entity = get_entity_by_path(client, *parts)
    if not entity:
        raise SystemExit(f"Unknown entity {entity_path!r}; seed it (see migrate_012) first.")
    return entity["id"]


def infer_meeting_date(name: str) -> date | None:
    """Extract a meeting date from a directory or filename.

    Delegates to the shared parser so ingest and the recategorize corrector
    derive dates identically (ISO, "Apr 12, 2023", "11.16.22", "10-29-25",
    "10 26 22", "Feb2025", fiscal "2024-2025", compact "jan21").
    """
    return parse_meeting_date(name, today=date.today())


def infer_document_type(filename: str) -> str:
    """Guess document type from filename via the shared classifier."""
    ext = Path(filename).suffix.lower()
    return classify_document_type(filename, is_text_file=ext in TRANSCRIPT_EXTENSIONS)


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


def subject_header(source_file: str, meeting_title: str) -> str:
    """Subject line to restore into a Canva curriculum-map's content, or "".

    These maps are scraped as bare skill tables whose subject lives only in the
    filename, so the chunk body can't be found by subject. Prepending the subject
    makes the map retrievable by both FTS and embedding. Scoped to this source so
    other documents' content is unchanged (a global title prefix regressed most
    queries -- see eval/README.md "Recall fixes"). Validated: the 1-5 Spanish map
    moved from absent to rank 8 in the candidate pool with no finance/governance
    regression.
    """
    if not CANVA_MAP_RE.match(source_file):
        return ""
    subject = meeting_title.strip()
    # Drop the leading "canva " filename artifact; keep "<grades> <subject> ...".
    if subject.lower().startswith("canva "):
        subject = subject[len("canva ") :].strip()
    return subject


def ingest_directory(data_dir: Path, entity_path: str = DEFAULT_ENTITY_PATH) -> None:
    """Ingest documents from data_dir.

    Supports two layouts:
      1. Subdirectories per meeting: data/documents/2024-03-15_board-meeting/
      2. Flat directory: data/documents/April 10, 2024 Meeting Minutes.pdf
         (date and title are inferred from each filename)
    """
    config = load_config()
    # Ingest writes documents/chunks/etc., so it uses the service key, which
    # bypasses RLS (the publishable key is read + corrections only).
    client = get_client(config.supabase_url, config.supabase_service_key)
    entity_id = resolve_entity_id(client, entity_path)

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
    total_blocked = 0

    # Process flat files (each file is its own meeting/document)
    if flat_files:
        logger.info("Found %d files in flat directory %s", len(flat_files), data_dir)
        for doc_file in flat_files:
            _parsed_date = infer_meeting_date(doc_file.name)
            # An undated file gets no date. Substituting today's date used to look
            # harmless, but meeting_date is what the meetings endpoint keys on, so a
            # placeholder does not merely mis-file the document — it manufactures a
            # meeting on the ingest date and serves the document as that meeting's
            # record. A NULL is excluded from those views; a plausible wrong date is
            # not. Content-derived dating happens downstream, where the text exists.
            meeting_date = _parsed_date
            flat_date_source = "filename" if _parsed_date else "undetermined"
            meeting_title = infer_meeting_title(doc_file.name)

            try:
                result = _ingest_with_dedup(
                    client=client,
                    path=doc_file,
                    meeting_date=meeting_date,
                    meeting_title=meeting_title,
                    config=config,
                    entity_id=entity_id,
                    date_source=flat_date_source,
                )
                if result["status"] == "skipped":
                    continue
                if result["status"] == "blocked":
                    total_blocked += 1
                    continue
                total_docs += 1
                total_chunks += result["chunks"]
            except ActaluxError as exc:
                total_failed += 1
                logger.error("  FAIL %s: %s", doc_file.name, exc)

    # Process subdirectories (grouped by meeting)
    for meeting_dir in meeting_dirs:
        _parsed_date = infer_meeting_date(meeting_dir.name)
        meeting_date = _parsed_date  # may be None; resolved per-doc below
        # 'filename' when the directory name parses to a date; 'default' when
        # no date pattern matched (the subdirectory name lacks a recognisable date).
        dir_date_source = "filename" if _parsed_date else "undetermined"
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
            try:
                result = _ingest_with_dedup(
                    client=client,
                    path=doc_file,
                    meeting_date=meeting_date,
                    meeting_title=meeting_title,
                    config=config,
                    entity_id=entity_id,
                    date_source=dir_date_source,
                )
                if result["status"] == "skipped":
                    continue
                if result["status"] == "blocked":
                    total_blocked += 1
                    errors.append(f"{doc_file.name}: BLOCKED by PII guard (see warning above)")
                    continue
                docs_ingested += 1
                total_chunks += result["chunks"]
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
        "Ingestion complete: %d documents, %d chunks, %d failures, %d blocked (PII)",
        total_docs,
        total_chunks,
        total_failed,
        total_blocked,
    )

    if total_blocked > 0:
        logger.warning(
            "%d document(s) BLOCKED by the PII guard — review the source files; "
            "they were NOT ingested. Set ACTALUX_PII_GUARD=warn to override.",
            total_blocked,
        )

    if total_failed > 0:
        logger.warning("%d documents failed to ingest. Check errors above.", total_failed)
        sys.exit(1)


def _pii_gate(path: Path, text: str, config: Any) -> bool:
    """Scan for high-precision PII before storing. Return True to block ingest.

    Mode (`config.pii_guard_mode`): "block" skips the document, "warn" logs and
    proceeds, "off" skips the scan. Only pattern names and offsets are logged --
    never the matched values -- so the guard never copies PII into the logs.
    """
    mode = getattr(config, "pii_guard_mode", "block")
    if mode == "off":
        return False
    findings = pii_guard.scan_text(text)
    if not findings:
        return False
    block = pii_guard.should_block(findings, mode)
    note = "not ingested; review the source file" if block else "ingested anyway; mode=warn"
    logger.warning(
        "  PII GUARD: %s %s — %s at offset %s (%s)",
        "BLOCKED" if block else "WARN",
        path.name,
        pii_guard.summarize(findings),
        ", ".join(str(f.char_offset) for f in findings[:5]),
        note,
    )
    return block


# A date is evidence of identity only when we know where it came from. These two
# provenances mean ingest fell back to the run date, so two rows can differ purely
# by when they were ingested.
_UNSOURCED_DATE_PROVENANCE = frozenset({"default", "unknown"})


def _is_same_document(
    existing: dict[str, Any],
    *,
    source_ref: str,
    meeting_date: date,
    date_source: str,
) -> bool:
    """Whether a content-hash match is the same record, or a distinct one that
    happens to read identically.

    Only positive evidence splits them: two different origin URLs, or two
    different dates that were both actually sourced from the document. Anything
    weaker (a missing origin, an ingest-day fallback date) is treated as the same
    record, which is the behaviour that held before this check existed.
    """
    prior_ref = existing.get("source_ref") or ""
    if source_ref and prior_ref and source_ref != prior_ref:
        return False
    prior_date = existing.get("meeting_date") or ""
    prior_source = existing.get("date_source") or "unknown"
    both_dates_sourced = (
        prior_source not in _UNSOURCED_DATE_PROVENANCE
        and date_source not in _UNSOURCED_DATE_PROVENANCE
    )
    if both_dates_sourced and prior_date and prior_date != meeting_date.isoformat():
        return False
    return True


def _find_existing_document(
    client: Any,
    *,
    source_ref: str,
    file_hash: str,
    portal: str,
    filename: str,
    meeting_date: date,
    date_source: str = "unknown",
    entity_id: int | None = None,
) -> dict[str, Any] | None:
    """Locate the current document this file should dedup against, or None.

    Lookup order, most stable identity first:
      1. ``source_ref`` (normalized origin URL) within the same portal -- the
         identity that survives a PDF/HTML twin or a renamed file. This is what
         actually prevents the twin-document recurrence.
      2. ``content_hash`` -- identical bytes are usually the same document even
         when the filename and origin both changed, but not always: a boilerplate
         notice reissued for a later meeting reads identically and is still a
         separate record. A hash match is therefore only accepted when nothing
         contradicts it (see ``_is_same_document``); a contradicted match falls
         through to tier 3 rather than collapsing the two, because the caller reads
         "same hash as an existing document" as "unchanged, skip" and the incoming
         record would never be stored at all.
      3. ``source_file`` (filename) -- legacy fallback for rows ingested before
         source_ref existed, or hand-added docs with no origin URL.

    When ``entity_id`` is given every tier is scoped to that body, so a record is
    only deduped against a prior version of the *same* body's record. Without it a
    video (or identical bytes) shared across bodies could wrongly supersede another
    body's current document.
    """
    if source_ref:
        existing = find_document_by_source_ref(client, portal, source_ref, entity_id)
        if existing:
            return existing
    existing = find_document_by_content_hash(client, file_hash, portal, entity_id)
    if existing:
        if _is_same_document(
            existing, source_ref=source_ref, meeting_date=meeting_date, date_source=date_source
        ):
            return existing
        logger.warning(
            "  DUPLICATE-CONTENT: %s reads identically to document %d (%s, %s) but is a "
            "different record (%s, %s); ingesting separately",
            filename,
            existing["id"],
            existing.get("source_ref") or "no source_ref",
            existing.get("meeting_date") or "no date",
            source_ref or "no source_ref",
            meeting_date.isoformat(),
        )
    return find_document_by_source(client, filename, portal, entity_id)


def _ingest_link_only(
    client: Any,
    entry: dict[str, Any],
    meeting_date: date,
    date_source: str,
    entity_id: int | None,
) -> dict[str, Any]:
    """Ingest a link-only stub: a document row pointing at an official record we
    could not parse into searchable text (issue #1). No chunks are ever built, so
    the stub can appear in meeting bundles and browse without ever appearing in
    content search.

    Deliberately NOT routed through ``_ingest_with_dedup``: its chunkless-repair
    path exists to fix documents that *should* have chunks, and its content-hash
    dedup assumes the content is the identity. A stub's identity is its
    source_ref alone, and the upgrade path — a later crawl parsing the packet
    successfully — arrives through the normal ingest, whose source_ref dedup sees
    this row, treats the real text as changed content, and supersedes the stub
    with a real version. Downgrades never happen here: an existing non-stub for
    the same source_ref wins and the stub entry is skipped.
    """
    source_ref = normalize_source_ref(entry.get("source_url", ""))
    if not source_ref:
        logger.warning("  SKIP link-only entry without source_url: %s", entry.get("source_file"))
        return {"status": "skipped", "chunks": 0}
    existing = (
        client.table("documents")
        .select("id,link_only")
        .eq("source_ref", source_ref)
        .is_("replaces_id", "null")
        .limit(1)
        .execute()
        .data
    )
    if existing:
        if not existing[0].get("link_only"):
            logger.info("  SKIP stub (real document exists): %s", source_ref)
        else:
            update_document_checked(client, existing[0]["id"])
            logger.info("  SKIP stub (already present): %s", source_ref)
        return {"status": "skipped", "chunks": 0}
    doc = Document(
        meeting_date=meeting_date,
        meeting_title=entry.get("meeting_title", ""),
        document_type=entry.get("document_type", "agenda"),
        source_url=entry.get("source_url", ""),
        source_file=entry.get("source_file", ""),
        content="",
        # Synthetic, per-ref hash: an empty-content hash shared by every stub
        # would cross-match them in the content-hash dedup tier.
        content_hash=content_hash(f"linkonly:{source_ref}"),
        source_portal=entry.get("source_portal", ""),
        source_ref=source_ref,
        date_source=date_source,
        entity_id=entity_id,
        link_only=True,
    )
    doc_id = insert_document(client, doc)
    logger.info("  NEW link-only stub %d: %s", doc_id, entry.get("meeting_title", source_ref))
    return {"status": "new", "chunks": 0}


def _ingest_with_dedup(
    client: Any,
    path: Path,
    meeting_date: date,
    meeting_title: str,
    config: Any,
    source_url: str = "",
    source_portal: str = "",
    document_type: str = "",
    entity_id: int | None = None,
    date_source: str = "unknown",
    video_id: str = "",
) -> dict[str, Any]:
    """Parse a file, check for an existing document, then ingest.

    Returns {"status": "new"|"updated"|"skipped"|"blocked", "chunks": N}.
    Dedup matches on source_ref -> content_hash -> source_file (see
    ``_find_existing_document``). Documents with high-precision PII are blocked
    before storage (see `_pii_gate`), so private records never reach the database.

    ``date_source`` records how ``meeting_date`` was derived; passed through to
    ``ingest_single_file`` so the provenance lands on every inserted row.
    """
    text = parse_file(path)
    # Restore the subject into Canva curriculum-map content (no-op for everything
    # else). Done before hashing so an existing map re-ingests as a new version,
    # and so the subject lands in both the stored content and the embedding.
    header = subject_header(path.name, meeting_title)
    if header and not text.startswith(header):
        text = f"{header}\n\n{text}"
    file_hash = content_hash(text)
    portal = source_portal or _infer_portal(path.name)
    source_ref = normalize_source_ref(source_url)

    existing = _find_existing_document(
        client,
        source_ref=source_ref,
        file_hash=file_hash,
        portal=portal,
        filename=path.name,
        meeting_date=meeting_date,
        date_source=date_source,
        entity_id=entity_id,
    )

    if existing:
        if existing.get("content_hash") == file_hash:
            # Unchanged content is normally a no-op. But a current version with NO
            # chunks (a prior ingest died after the doc row but before its chunks —
            # the free-tier-timeout failure mode) would otherwise be skipped forever.
            # Repair it in place by (re)building its chunks instead.
            if existing.get("link_only"):
                # A stub is chunkless by design; "repairing" it would publish
                # unparsed garbage into search. The upgrade to a real document
                # happens via changed content, never via repair.
                update_document_checked(client, existing["id"])
                return {"status": "skipped", "chunks": 0}
            if existing.get("replaces_id"):
                # A superseded version matching unchanged bytes is a plain skip:
                # the canonical record lives at the end of its replaces_id chain,
                # and "repairing" chunks onto a superseded row would put retired
                # text back into a searchable state.
                return {"status": "skipped", "chunks": 0}
            if not document_has_chunks(client, existing["id"]):
                # Scan before rebuilding, not just on the insert paths. Chunks are
                # what search serves and what a citation links to, so building them
                # is the step that publishes a document — and the row being repaired
                # may predate this guard or its current patterns. Blocking leaves the
                # document at 0 chunks (unsearchable, still flagged for repair); the
                # row's stored content is untouched, so a hit here needs the operator
                # to decide what happens to the row itself.
                if _pii_gate(path, text, config):
                    logger.warning(
                        "  REPAIR BLOCKED (PII in existing document %d): %s",
                        existing["id"],
                        path.name,
                    )
                    return {"status": "blocked", "chunks": 0}
                logger.warning("  REPAIR (chunkless current doc): %s", path.name)
                # Key citations on the same source_ref a fresh ingest would use, and
                # backfill it onto a legacy row that lacks one (same as the skip path).
                eff_source_ref = existing.get("source_ref") or source_ref
                doc_key = doc_stable_key(
                    eff_source_ref,
                    existing["content_hash"],
                    existing.get("source_file") or path.name,
                )
                # Clear any partial stragglers first, and clear again if the rebuild
                # fails partway, so the doc always ends at 0 chunks (== still needs
                # repair) rather than a partial that future re-ingests skip forever.
                delete_chunks_for_document(client, existing["id"])
                try:
                    n_chunks = _build_and_insert_chunks(
                        client, existing["id"], text, doc_key, config, label=path.name
                    )
                except Exception:
                    delete_chunks_for_document(client, existing["id"])
                    raise
                if source_ref and not existing.get("source_ref"):
                    backfill_document_source_ref(client, existing["id"], source_ref)
                    logger.info("  BACKFILL source_ref: %s", path.name)
                update_document_checked(client, existing["id"])
                return {"status": "updated", "chunks": n_chunks}
            # Content unchanged — mark as checked, and backfill source_ref onto a
            # legacy row that lacks one so a future twin can dedup against it.
            update_document_checked(client, existing["id"])
            if source_ref and not existing.get("source_ref"):
                backfill_document_source_ref(client, existing["id"], source_ref)
                logger.info("  BACKFILL source_ref: %s", path.name)
            logger.info("  SKIP (unchanged): %s", path.name)
            return {"status": "skipped", "chunks": 0}

        # Content changed — create new version
        if _pii_gate(path, text, config):
            return {"status": "blocked", "chunks": 0}
        old_version = existing.get("version", 1)
        logger.info(
            "  UPDATE %s: content changed (v%d -> v%d)",
            path.name,
            old_version,
            old_version + 1,
        )
        result = ingest_single_file(
            client=client,
            path=path,
            text=text,
            file_hash=file_hash,
            meeting_date=meeting_date,
            meeting_title=meeting_title,
            config=config,
            source_url=source_url,
            source_portal=portal,
            source_ref=source_ref,
            version=old_version + 1,
            document_type=document_type,
            entity_id=entity_id,
            date_source=date_source,
            video_id=video_id,
        )
        # Mark old document as replaced by the new one (retries on a timeout so a
        # slow cutover doesn't leave two current versions). Unless the old row is
        # ALREADY superseded — a draft points at the final that replaced it, and
        # overwriting that link would break the draft's redirect to the adopted
        # record. A revised draft becomes a fresh live document instead, and the
        # draft/final linker re-folds it by meeting date.
        if not existing.get("replaces_id"):
            supersede_document(client, existing["id"], result["doc_id"])
        return {"status": "updated", "chunks": result["chunks"]}

    # New document
    if _pii_gate(path, text, config):
        return {"status": "blocked", "chunks": 0}
    result = ingest_single_file(
        client=client,
        path=path,
        text=text,
        file_hash=file_hash,
        meeting_date=meeting_date,
        meeting_title=meeting_title,
        config=config,
        source_url=source_url,
        source_portal=portal,
        source_ref=source_ref,
        document_type=document_type,
        entity_id=entity_id,
        date_source=date_source,
        video_id=video_id,
    )
    logger.info("  OK %s: %d chunks ingested", path.name, result["chunks"])
    return {"status": "new", "chunks": result["chunks"]}


def _build_and_insert_chunks(
    client: Any, doc_id: int, text: str, doc_key: str, config: Any, *, label: str
) -> int:
    """Chunk, embed, citation-stamp, and insert a document's chunks; return the count.

    Shared by first-time ingest and chunkless-doc repair so both produce identical
    chunk and citation rows for the same text. ``doc_key`` is the document's stable
    key (source_ref/content_hash/source_file) so citation ids reproduce across a
    re-ingest. Raises ParseError if no chunk survives validation.
    """
    chunks = chunk_document(
        document_id=doc_id,
        text=text,
        target_words=config.chunk_target_words,
        overlap_sentences=config.chunk_overlap_sentences,
    )
    valid_chunks = validate_chunks(chunks, text)
    if len(valid_chunks) < len(chunks):
        logger.warning(
            "%d/%d chunks failed validation for %s",
            len(chunks) - len(valid_chunks),
            len(chunks),
            label,
        )
    if not valid_chunks:
        raise ParseError(f"All chunks failed validation for {label}")

    # Stamp each chunk with its stable, content-addressed citation id (what
    # citations render and route on, surviving this row's eventual re-ingest).
    embedded_chunks = embed_chunks(valid_chunks, model_name=config.embedding_model)
    citation_ids = assign_citation_ids(doc_key, [c.content for c in embedded_chunks])
    embedded_chunks = [
        replace(chunk, citation_id=cid)
        for chunk, cid in zip(embedded_chunks, citation_ids, strict=True)
    ]
    insert_chunks(client, embedded_chunks)
    return len(embedded_chunks)


def ingest_single_file(
    client: Any,
    path: Path,
    meeting_date: date,
    meeting_title: str,
    config: Any,
    text: str = "",
    file_hash: str = "",
    source_url: str = "",
    source_portal: str = "",
    source_ref: str = "",
    document_type: str = "",
    version: int = 1,
    entity_id: int | None = None,
    date_source: str = "unknown",
    video_id: str = "",
) -> dict[str, int]:
    """Parse, chunk, embed, validate, and store a single document.

    ``source_ref`` is the stable external id (see ``Document.source_ref``); when
    not supplied it is derived from ``source_url`` so direct callers still
    persist it.

    ``document_type`` overrides the filename classifier when supplied; otherwise
    the type is inferred from the filename (the default path for every crawler).

    ``date_source`` records how ``meeting_date`` was derived: ``'filename'`` when
    ``parse_meeting_date`` succeeded on the title/filename, ``'default'`` when
    the caller fell back to ``date.today()``.

    Returns {"doc_id": N, "chunks": N}.
    """
    if not text:
        text = parse_file(path)
    if not file_hash:
        file_hash = content_hash(text)

    doc = Document(
        meeting_date=meeting_date,
        meeting_title=meeting_title,
        document_type=document_type or infer_document_type(path.name),
        source_url=source_url,
        source_file=path.name,
        content=text,
        content_hash=file_hash,
        source_portal=source_portal or _infer_portal(path.name),
        source_ref=source_ref or normalize_source_ref(source_url),
        entity_id=entity_id,
        version=version,
        date_source=date_source,
        video_id=video_id,
    )
    doc_id = insert_document(client, doc)

    # The doc row is now current (replaces_id IS NULL) the instant it's inserted.
    # If building/inserting its chunks fails, delete it so a partial ingest never
    # leaves a current but chunkless (unsearchable) document behind.
    doc_key = doc_stable_key(doc.source_ref, doc.content_hash, doc.source_file)
    try:
        n_chunks = _build_and_insert_chunks(client, doc_id, text, doc_key, config, label=path.name)
    except Exception:
        delete_document(client, doc_id)
        raise

    return {"doc_id": doc_id, "chunks": n_chunks}


# YouTube is the one portal that puts a video's stable id in the QUERY string
# (``watch?v=ID``) or a short path (``youtu.be/ID``, ``/live/ID``, ``/embed/ID``,
# ``/shorts/ID``) rather than a path GUID. normalize_source_ref must keep that id
# or every meeting collapses to the same ``youtube.com/watch`` ref and dedup
# treats unrelated meetings as versions of one document.
_YOUTUBE_WATCH_HOSTS = {"youtube.com", "www.youtube.com", "m.youtube.com"}
_YOUTUBE_PATH_ID_RE = re.compile(r"^/(?:live|embed|shorts|v)/([^/?#]+)")


def _youtube_video_id(parts: SplitResult) -> str | None:
    """Extract a YouTube video id from a parsed URL, across the forms YouTube uses."""
    host = parts.netloc.lower()
    if host == "youtu.be":
        return parts.path.lstrip("/").split("/", 1)[0] or None
    if host in _YOUTUBE_WATCH_HOSTS:
        if parts.path.rstrip("/") == "/watch":
            return parse_qs(parts.query).get("v", [""])[0] or None
        if m := _YOUTUBE_PATH_ID_RE.match(parts.path):
            return m.group(1)
    return None


# CivicPlus (MeetingsManager) is like YouTube: the stable per-document id lives in
# the QUERY string (ShowPrimaryDocument?agendaID=N / ?minutesID=N) while the path is
# identical across all docs, so the query id must be kept or every minutes/agenda
# doc collapses to one ref and dedup treats unrelated meetings as one document.
_CIVICPLUS_DOC_RE = re.compile(
    r"/MeetingsManager/(?:MeetingAgenda|MeetingMinutes)/ShowPrimaryDocument", re.IGNORECASE
)
_CIVICPLUS_ID_PARAMS = ("agendaid", "minutesid")


def _civicplus_doc_id(parts: SplitResult) -> tuple[str, str] | None:
    """Return ``(param, id)`` for a CivicPlus ShowPrimaryDocument URL, else None."""
    if not _CIVICPLUS_DOC_RE.search(parts.path):
        return None
    q = {k.lower(): v for k, v in parse_qs(parts.query).items()}
    for key in _CIVICPLUS_ID_PARAMS:
        if q.get(key):
            return key, q[key][0]
    return None


def _diligent_meeting_id(parts: SplitResult) -> str | None:
    """Return the meeting id for a Diligent MeetingInformation page, else None.

    Same trap as YouTube: the per-document identity lives in the QUERY STRING
    (``MeetingInformation.aspx?Id=1373``), which the generic normalization drops
    — so two different meetings' agenda pages collapsed into one ref and the
    second agenda ingested as a "new version" superseding the first. Third
    appearance of this failure shape (YouTube, CivicPlus, now Diligent): any
    portal whose URLs are ``page.aspx?Id=N`` needs its id kept.
    """
    if not parts.path.lower().endswith("/meetinginformation.aspx"):
        return None
    q = {k.lower(): v for k, v in parse_qs(parts.query).items()}
    return q["id"][0] if q.get("id") else None


def normalize_source_ref(source_url: str) -> str:
    """Derive a stable external id from a crawler's canonical origin URL.

    Every manifest entry carries the document's origin URL, and that URL ends in
    a stable per-document id (Diligent ``/document/{guid}``, claytonschools
    ``/resource-manager/view/{guid}``, Google Docs ``/document/d/{id}``, Canva
    ``/design/{designId}/...``). Verified unique per manifest (157/157 diligent,
    13/13 finance, 24/24 curriculum, 49/49 canva), unlike ``source_file`` which
    has duplicate keys. Normalizing keeps that identity stable across rotating
    tracking params: lowercase scheme+host, drop the query string and fragment
    (``utm_*``, share modes), strip a trailing slash. Returns "" for an empty or
    unparseable URL, in which case the caller falls back to filename dedup.

    YouTube is special-cased: its video id lives in the query string, so any
    YouTube URL is canonicalized to ``https://www.youtube.com/watch?v=<id>`` to
    keep each meeting a distinct document.

    Parameters
    ----------
    source_url
        Canonical origin URL from the manifest entry.

    Returns
    -------
    str
        Normalized origin URL usable as a stable dedup key, or "".
    """
    if not source_url:
        return ""
    parts = urlsplit(source_url.strip())
    if not parts.netloc:
        return ""
    if video_id := _youtube_video_id(parts):
        return f"https://www.youtube.com/watch?v={video_id}"
    if cp := _civicplus_doc_id(parts):
        key, val = cp
        return (
            f"{parts.scheme.lower()}://{parts.netloc.lower()}{parts.path.rstrip('/')}?{key}={val}"
        )
    if mid := _diligent_meeting_id(parts):
        return f"{parts.scheme.lower()}://{parts.netloc.lower()}{parts.path.rstrip('/')}?Id={mid}"
    return urlunsplit((parts.scheme.lower(), parts.netloc.lower(), parts.path.rstrip("/"), "", ""))


def _infer_portal(filename: str) -> str:
    """Guess source portal from filename patterns."""
    if re.search(r"Meeting Minutes", filename, re.IGNORECASE):
        return "diligent"
    if re.search(r"Budget\.html$", filename, re.IGNORECASE):
        return "claytonschools"
    if re.search(r"Board of Education.*\.txt$", filename, re.IGNORECASE):
        return "youtube"
    if filename.lower().endswith(".txt") and re.search(r"board", filename, re.IGNORECASE):
        return "youtube"
    return "manual"


def wholesale_failure(new: int, updated: int, skipped: int, failed: int) -> bool:
    """True only when every attempted document failed (abort-worthy).

    An unchanged skip is a healthy outcome — the doc was fetched, parsed, and
    verified current — so a steady-state run where everything skips and only the
    perennial unsupported-format docs fail is NOT wholesale (that false-failed the
    2026-07-20 scheduled run). Wholesale means an environmental break (embedder
    down, DB unreachable) where nothing processes at all.
    """
    return bool(failed) and (new + updated + skipped) == 0


def ingest_from_manifest(manifest_path: Path, entity_path: str = DEFAULT_ENTITY_PATH) -> None:
    """Ingest documents listed in a crawler manifest JSON file.

    Each entry has: source_file, source_url, source_portal, and optionally
    meeting_date, meeting_title, document_type, and video_id. ``document_type``
    overrides the filename classifier -- needed for sources whose type cannot be
    read from the filename (a sunshine-obtained invoice/check/contract has no type
    keyword in its name). ``video_id`` sets the YouTube embed at ingest time (used
    by the transcription manifest). The file is expected to exist in data/documents/.
    """
    import json

    config = load_config()
    # Ingest writes documents/chunks/etc., so it uses the service key, which
    # bypasses RLS (the publishable key is read + corrections only).
    client = get_client(config.supabase_url, config.supabase_service_key)
    entity_id = resolve_entity_id(client, entity_path)

    manifest = json.loads(manifest_path.read_text())
    data_dir = manifest_path.parent

    total_new = 0
    total_updated = 0
    total_skipped = 0
    total_failed = 0

    for entry in manifest:
        source_file = entry["source_file"]
        file_path = data_dir / source_file
        if not file_path.exists():
            logger.error("File not found: %s", file_path)
            total_failed += 1
            continue

        if entry.get("meeting_date"):
            meeting_date = date.fromisoformat(entry["meeting_date"])
            # An explicit manifest date is operator/crawler-supplied, not parsed
            # from the filename here, so its provenance is 'manual' unless the
            # crawler declares a more specific source via entry["date_source"].
            manifest_date_source = entry.get("date_source", "manual")
        else:
            _inferred = infer_meeting_date(source_file)
            # No date in the manifest and none in the filename means the date is
            # unknown, not today. See the flat-file branch above for why a
            # placeholder here manufactures a meeting rather than just mis-filing.
            meeting_date = _inferred
            manifest_date_source = "filename" if _inferred else "undetermined"
        meeting_title = entry.get("meeting_title", infer_meeting_title(source_file))

        try:
            if entry.get("link_only"):
                result = _ingest_link_only(
                    client, entry, meeting_date, manifest_date_source, entity_id
                )
                total_skipped += 1 if result["status"] == "skipped" else 0
                total_new += 1 if result["status"] == "new" else 0
                continue
            result = _ingest_with_dedup(
                client=client,
                path=file_path,
                meeting_date=meeting_date,
                meeting_title=meeting_title,
                config=config,
                source_url=entry.get("source_url", ""),
                source_portal=entry.get("source_portal", ""),
                document_type=entry.get("document_type", ""),
                entity_id=entity_id,
                date_source=manifest_date_source,
                video_id=entry.get("video_id", ""),
            )
            if result["status"] == "new":
                total_new += 1
            elif result["status"] == "updated":
                total_updated += 1
            else:
                total_skipped += 1
        except ActaluxError as exc:
            total_failed += 1
            logger.error("FAIL %s: %s", source_file, exc)

    logger.info(
        "Manifest ingestion complete: %d new, %d updated, %d skipped, %d failed",
        total_new,
        total_updated,
        total_skipped,
        total_failed,
    )
    # Surface any failure as a CI annotation (visible without reading the log) but only
    # ABORT on a wholesale failure (see wholesale_failure): a non-zero exit under the
    # workflow's `set -e` skips every downstream step for the docs that DID ingest.
    # Partial failures are expected at corpus scale and are logged + annotated, not fatal.
    if total_failed:
        print(f"::warning::{total_failed} document(s) failed to ingest; see errors above")
    if wholesale_failure(total_new, total_updated, total_skipped, total_failed):
        logger.error("ingest failed wholesale: 0 succeeded, %d failed", total_failed)
        sys.exit(1)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Ingest official documents into Actalux.",
        epilog=(
            "Examples:\n"
            "  python scripts/ingest.py data/documents/\n"
            "  python scripts/ingest.py --manifest data/documents/diligent_manifest.json"
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("data_dir", nargs="?", help="directory of documents to ingest")
    parser.add_argument(
        "--manifest", metavar="MANIFEST", help="ingest from a crawler manifest JSON"
    )
    parser.add_argument(
        "--entity",
        default=DEFAULT_ENTITY_PATH,
        help="owning body as state/place/body (default: %(default)s)",
    )
    parser.add_argument(
        "--body",
        help="owning body by short key (schools/council); overrides --entity",
    )
    args = parser.parse_args()

    entity_path = get_body(args.body).entity_path if args.body else args.entity

    if args.manifest:
        ingest_from_manifest(Path(args.manifest), entity_path=entity_path)
    elif args.data_dir:
        data_dir = Path(args.data_dir)
        if not data_dir.is_dir():
            parser.error(f"{data_dir} is not a directory")
        ingest_directory(data_dir, entity_path=entity_path)
    else:
        parser.error("provide a data directory or --manifest <manifest.json>")


if __name__ == "__main__":
    main()
