"""Shared plumbing for the DESE loaders (ASBR, Per-Pupil, ...).

A DESE loader parses + reconciles one report, renders a clean markdown document
(each figure on its own verbatim row), and loads the figures into
budget_line_items under namespaced dimensions the live budget page never queries.
This module owns the part every loader shares: ingest the rendered markdown as a
cited document, map each figure to the chunk that carries its verbatim row, and
swap in that fiscal year's rows idempotently — doing all fallible work before any
destructive delete so a failure leaves the prior year's data intact.
"""

from __future__ import annotations

import logging
import sys
from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from pathlib import Path

# Reuse the native ingest pipeline (parse-free path: the rendered text is passed in).
sys.path.insert(0, str(Path(__file__).resolve().parent))
from ingest import ingest_single_file, resolve_entity_id  # noqa: E402  (sibling script)

from actalux.db import find_document_by_source, insert_budget_line_items  # noqa: E402
from actalux.models import BudgetLineItem  # noqa: E402

logger = logging.getLogger(__name__)

DESE_DIR = Path(__file__).resolve().parent.parent / "data" / "DESE"
RENDER_DIR = DESE_DIR / "rendered"  # outside data/documents so a dir-scan never picks it up
SOURCE_PORTAL = "dese"
ENTITY_PATH = "mo/clayton/schools"
STORAGE_BUCKET = "documents"
BASIS = "actual"  # DESE reports are filed statements of actuals


@dataclass(frozen=True)
class Figure:
    """One figure to load: everything for a BudgetLineItem bar the chunk id."""

    category: str
    dimension: str
    fund: str
    subcategory: str
    amount: Decimal
    quote: str  # the verbatim markdown row this figure was read from
    note: str


def fy_end_date(fiscal_year: str) -> date:
    """The June 30 fiscal-year-end a report reports as of (2023-2024 -> 2024-06-30)."""
    return date(int(fiscal_year.split("-")[1]), 6, 30)


def _map_quotes_to_chunks(client, doc_id: int, quotes: set[str]) -> dict[str, int]:
    """Map each verbatim row to the id of the ingested chunk that contains it."""
    rows = (
        client.table("chunks")
        .select("id,content")
        .eq("document_id", doc_id)
        .order("chunk_index")
        .execute()
        .data
    )
    mapping: dict[str, int] = {}
    for quote in quotes:
        for chunk in rows:
            if quote in (chunk.get("content") or ""):
                mapping[quote] = chunk["id"]
                break
    return mapping


def _delete_rows_for_document(client, doc_id: int) -> None:
    """Delete the budget_line_items that cite one document (used to roll back)."""
    client.table("budget_line_items").delete().eq("document_id", doc_id).execute()


def _retire_old_rows(
    client, fiscal_year: str, dimensions: tuple[str, ...], keep_doc_id: int
) -> None:
    """Delete this loader's prior rows for one year, keeping the just-inserted set.

    Scoped to (fiscal_year, basis='actual', this loader's dimensions) and excluding
    the new document's rows, so it can only remove this loader's superseded rows —
    never audit-actual, proposed, or the other DESE dataset.
    """
    client.table("budget_line_items").delete().eq("fiscal_year", fiscal_year).eq(
        "basis", BASIS
    ).in_("dimension", list(dimensions)).neq("document_id", keep_doc_id).execute()


def _delete_document(client, doc_id: int) -> None:
    """Delete a document and its chunks (chunks first; they reference the doc)."""
    client.table("chunks").delete().eq("document_id", doc_id).execute()
    client.table("documents").delete().eq("id", doc_id).execute()


def _upload_pdf(client, pdf_path: Path, key: str) -> None:
    """Upload an original report PDF to storage for an 'Open original' download."""
    try:
        client.storage.from_(STORAGE_BUCKET).upload(
            key,
            pdf_path.read_bytes(),
            {"content-type": "application/pdf", "upsert": "true"},
        )
        logger.info("  uploaded PDF -> %s/%s", STORAGE_BUCKET, key)
    except Exception as exc:  # storage is best-effort; the citation does not depend on it
        logger.warning("  PDF upload failed for %s (non-fatal): %s", key, exc)


def load_year(
    client,
    config,
    *,
    fiscal_year: str,
    md_path: Path,
    markdown: str,
    figures: list[Figure],
    dimensions: tuple[str, ...],
    meeting_title: str,
    pdf_path: Path | None = None,
    pdf_key: str = "",
) -> int:
    """Ingest the rendered doc, cite each figure, and swap in the year's rows.

    All fallible work (ingest, chunk mapping, row building) happens BEFORE any
    destructive delete, so a failure leaves the prior year's data intact; the
    just-ingested document is rolled back on a prep failure to avoid orphaning it.
    Returns the number of budget line items loaded.
    """
    prior_doc = find_document_by_source(client, md_path.name, SOURCE_PORTAL)
    result = ingest_single_file(
        client=client,
        path=md_path,
        text=markdown,
        meeting_date=fy_end_date(fiscal_year),
        meeting_title=meeting_title,
        config=config,
        source_url="",  # no public DESE per-doc URL; the original PDF is in storage
        source_portal=SOURCE_PORTAL,
        entity_id=resolve_entity_id(client, ENTITY_PATH),
        date_source="content",  # derived from the report's own stated fiscal year
    )
    doc_id = result["doc_id"]

    # Build + insert the new rows BEFORE removing anything, so a failure (uncited
    # figure, DB/network error) rolls back only the new document + its rows and
    # leaves the prior year's data fully intact.
    try:
        quote_to_chunk = _map_quotes_to_chunks(client, doc_id, {f.quote for f in figures})
        items: list[BudgetLineItem] = []
        for f in figures:
            chunk_id = quote_to_chunk.get(f.quote)
            if chunk_id is None:
                raise SystemExit(
                    f"FY{fiscal_year}: no ingested chunk contains the source row {f.quote!r}; "
                    "refusing to load an uncited figure."
                )
            items.append(
                BudgetLineItem(
                    fiscal_year=fiscal_year,
                    category=f.category,
                    amount=f.amount,
                    document_id=doc_id,
                    dimension=f.dimension,
                    fund=f.fund,
                    subcategory=f.subcategory,
                    basis=BASIS,
                    chunk_id=chunk_id,
                    source_quote=f.quote,
                    note=f.note,
                )
            )
        insert_budget_line_items(client, items)
    except BaseException:
        _delete_rows_for_document(client, doc_id)  # drop any new rows, then the new doc
        _delete_document(client, doc_id)
        raise

    # New rows + document are committed. Now retire the prior year's rows and the
    # prior document version. A failure here can only leave a transient duplicate
    # (over- not under-count), which the next run heals.
    _retire_old_rows(client, fiscal_year, dimensions, keep_doc_id=doc_id)
    if prior_doc and prior_doc["id"] != doc_id:
        _delete_document(client, prior_doc["id"])
    if pdf_path and pdf_path.exists() and pdf_key:
        _upload_pdf(client, pdf_path, pdf_key)
    logger.info(
        "  loaded %d budget line items citing doc #%d (%d chunks).",
        len(items),
        doc_id,
        result["chunks"],
    )
    return len(items)
