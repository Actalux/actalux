"""Load DESE ASBR object-level finance data into Actalux.

For each fiscal year this:
  1. parses + reconciles the official ASBR Full Report XML (actalux.ingest.asbr_xml),
  2. renders a clean, deterministic markdown document (the cited source — each
     figure on its own verbatim row),
  3. ingests that markdown as a document + chunks (source_portal='dese'),
  4. loads object-level expenditures and the per-fund statement into
     budget_line_items, each figure citing the chunk + verbatim row it came from,
  5. uploads the original PDF to storage for an "Open original" download.

Why a markdown rendering is the cited source: the SSRS PDF prints each figure on
its own line (no clean row to highlight), so a faithful, no-interpretation
markdown transform of the official XML gives clean, highlightable citations and
guarantees the stored source_quote is byte-identical to the ingested chunk. The
original PDF is preserved in storage and linked.

Isolation from the live budget page (HARD CONSTRAINT): the GAAP/budgetary
figures the Budget page renders are queried by dimension in
{'fund','source','function','budget'} with NO basis filter, so an 'actual'-basis
row under one of those dimensions would leak onto the page. ASBR rows are loaded
under disjoint namespaced dimensions 'asbr_object' / 'asbr_fund' (basis='actual')
that no current query reads — so loading is safe and non-surfacing until a
dedicated ASBR view is built.

Idempotent per year: a re-run deletes exactly this loader's rows for that fiscal
year (basis='actual', dimension in the asbr_* set) and the prior rendered
document, then re-inserts. It never touches audit-actual or proposed rows.

Run (prefix each with `doppler run --project mac --config dev --`):
  uv run python scripts/load_asbr.py --year 2023-2024            # dry run, one year
  uv run python scripts/load_asbr.py --year 2023-2024 --apply    # write one year
  uv run python scripts/load_asbr.py --apply                     # all 12 years
"""

from __future__ import annotations

import argparse
import logging
import sys
from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from pathlib import Path

# Reuse the native ingest pipeline (parse-free path: we pass the rendered text).
sys.path.insert(0, str(Path(__file__).resolve().parent))
from ingest import ingest_single_file, resolve_entity_id  # noqa: E402  (sibling script)

from actalux.config import load_config  # noqa: E402
from actalux.db import find_document_by_source, get_client, insert_budget_line_items  # noqa: E402
from actalux.ingest import asbr_xml  # noqa: E402
from actalux.ingest.asbr_xml import (  # noqa: E402
    AsbrReport,
    object_md_row,
    parse_asbr,
    render_markdown,
    summary_md_row,
)
from actalux.models import BudgetLineItem  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

DESE_DIR = Path(__file__).resolve().parent.parent / "data" / "DESE"
RENDER_DIR = DESE_DIR / "rendered"  # outside data/documents so a dir-scan never picks it up
SOURCE_PORTAL = "dese"
ENTITY_PATH = "mo/clayton/schools"
STORAGE_BUCKET = "documents"

# Namespaced dimensions, disjoint from every dimension the live budget page reads.
DIM_OBJECT = "asbr_object"
DIM_FUND = "asbr_fund"
ASBR_DIMENSIONS = (DIM_OBJECT, DIM_FUND)
BASIS = "actual"  # the ASBR is a filed statement of actuals

# Verified fiscal-year -> raw XML filename (the user's DESE downloads). The parser
# re-derives the year from the XML and apply_year asserts it matches this key, so a
# renamed/mis-downloaded file fails loudly rather than loading under a wrong year.
ASBR_FILES: dict[str, str] = {
    "2013-2014": "ASBR Full Report (12).xml",
    "2014-2015": "ASBR Full Report (11).xml",
    "2015-2016": "ASBR Full Report (10).xml",
    "2016-2017": "ASBR Full Report (9).xml",
    "2017-2018": "ASBR Full Report (8).xml",
    "2018-2019": "ASBR Full Report (7).xml",
    "2019-2020": "ASBR Full Report (6).xml",
    "2020-2021": "ASBR Full Report (5).xml",
    "2021-2022": "ASBR Full Report (4).xml",
    "2022-2023": "ASBR Full Report (3).xml",
    "2023-2024": "ASBR Full Report (2).xml",
    "2024-2025": "ASBR Full Report.xml",
}


@dataclass(frozen=True)
class _Figure:
    """An intermediate figure: everything for a BudgetLineItem bar the chunk id."""

    category: str
    dimension: str
    fund: str
    subcategory: str
    amount: Decimal
    quote: str  # the verbatim markdown row this figure was read from
    note: str


def fy_end_date(fiscal_year: str) -> date:
    """The June 30 fiscal-year-end the ASBR reports as of (e.g. 2023-2024 -> 2024-06-30)."""
    return date(int(fiscal_year.split("-")[1]), 6, 30)


def md_filename(fiscal_year: str) -> str:
    return f"dese_asbr_{fiscal_year}.md"


def pdf_storage_key(fiscal_year: str) -> str:
    return f"dese_asbr_{fiscal_year}.pdf"


def build_figures(report: AsbrReport) -> list[_Figure]:
    """Expand a reconciled report into the figures to load (no chunk ids yet)."""
    figures: list[_Figure] = []

    # Object-level expenditures (all funds) — the data the audits lack.
    for obj in report.objects:
        figures.append(
            _Figure(
                category="expenditure",
                dimension=DIM_OBJECT,
                fund="",
                subcategory=f"{obj.code} {obj.label}",
                amount=obj.amount,
                quote=object_md_row(obj),
                note="ASBR Part III-B expenditures by object (all funds)",
            )
        )

    # Per-fund statement: revenue, expenditure, beginning + ending balance.
    parts = (
        ("revenue", asbr_xml.ROW_REVENUE, "Total revenue", "ASBR Part I total revenue by fund"),
        (
            "expenditure",
            asbr_xml.ROW_EXPENDITURE,
            "Total expenditures",
            "ASBR Part I expenditures by fund",
        ),
        (
            "fund_balance",
            asbr_xml.ROW_BEGINNING,
            "Beginning Fund Balance",
            "ASBR Part I beginning fund balance",
        ),
        (
            "fund_balance",
            asbr_xml.ROW_ENDING,
            "Ending Fund Balance",
            "ASBR Part I ending fund balance",
        ),
    )
    for category, code, subcat, note in parts:
        row = report.summary[code]
        quote = summary_md_row(row)
        for fund in asbr_xml.fund_labels():
            figures.append(
                _Figure(
                    category=category,
                    dimension=DIM_FUND,
                    fund=fund,
                    subcategory=subcat,
                    amount=row.by_fund[fund],
                    quote=quote,
                    note=note,
                )
            )
    return figures


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


def _delete_budget_rows(client, fiscal_year: str) -> None:
    """Delete exactly this loader's rows for one year (basis='actual', asbr_* dims)."""
    client.table("budget_line_items").delete().eq("fiscal_year", fiscal_year).eq(
        "basis", BASIS
    ).in_("dimension", list(ASBR_DIMENSIONS)).execute()


def _delete_document(client, doc_id: int) -> None:
    """Delete a document and its chunks (chunks first; they reference the doc)."""
    client.table("chunks").delete().eq("document_id", doc_id).execute()
    client.table("documents").delete().eq("id", doc_id).execute()


def _upload_pdf(client, fiscal_year: str) -> None:
    """Upload the original ASBR PDF to storage for an 'Open original' download."""
    pdf = DESE_DIR / ASBR_FILES[fiscal_year].replace(".xml", ".pdf")
    if not pdf.exists():
        logger.warning("  PDF not found, skipping upload: %s", pdf.name)
        return
    key = pdf_storage_key(fiscal_year)
    try:
        client.storage.from_(STORAGE_BUCKET).upload(
            key,
            pdf.read_bytes(),
            {"content-type": "application/pdf", "upsert": "true"},
        )
        logger.info("  uploaded PDF -> %s/%s", STORAGE_BUCKET, key)
    except Exception as exc:  # storage is best-effort; the citation does not depend on it
        logger.warning("  PDF upload failed for %s (non-fatal): %s", key, exc)


def _summarize(fiscal_year: str, report: AsbrReport, figures: list[_Figure]) -> None:
    by_dim: dict[str, int] = {}
    for f in figures:
        by_dim[f.dimension] = by_dim.get(f.dimension, 0) + 1
    logger.info("FY%s: %d figures (%s)", fiscal_year, len(figures), dict(sorted(by_dim.items())))
    logger.info(
        "  reconciled: revenue %s, expenditures %s, ending balance %s",
        f"${report.revenue_total:,.2f}",
        f"${report.expenditure_total:,.2f}",
        f"${report.ending_balance_total:,.2f}",
    )


def process_year(client, config, fiscal_year: str, *, apply: bool) -> int:
    """Render, (optionally) ingest, and load one fiscal year. Returns rows loaded."""
    xml_path = DESE_DIR / ASBR_FILES[fiscal_year]
    if not xml_path.exists():
        raise SystemExit(f"Missing ASBR XML for FY{fiscal_year}: {xml_path}")

    report = parse_asbr(xml_path)  # parses + reconciles; raises on any mismatch
    if report.fiscal_year != fiscal_year:
        raise SystemExit(
            f"FY mismatch: {xml_path.name} contains {report.fiscal_year}, expected {fiscal_year}. "
            "Refusing to load under the wrong year."
        )

    markdown = render_markdown(report)
    RENDER_DIR.mkdir(parents=True, exist_ok=True)
    md_path = RENDER_DIR / md_filename(fiscal_year)
    md_path.write_text(markdown, encoding="utf-8")

    figures = build_figures(report)
    _summarize(fiscal_year, report, figures)

    if not apply:
        logger.info("  dry run: wrote %s; no DB writes.", md_path)
        return 0

    # Do all fallible work (ingest, chunk mapping, row building) BEFORE any
    # destructive delete, so a failure leaves the prior year's data intact. The
    # prior current document is captured first so it can be swapped out at the end.
    prior_doc = find_document_by_source(client, md_filename(fiscal_year), SOURCE_PORTAL)
    result = ingest_single_file(
        client=client,
        path=md_path,
        text=markdown,
        meeting_date=fy_end_date(fiscal_year),
        meeting_title=f"Annual Secretary of the Board Report — FY {fiscal_year}",
        config=config,
        source_url="",  # no public DESE per-doc URL; original PDF is in storage
        source_portal=SOURCE_PORTAL,
        entity_id=resolve_entity_id(client, ENTITY_PATH),
        date_source="content",  # derived from the report's own stated fiscal year
    )
    doc_id = result["doc_id"]

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
    except BaseException:
        # Roll back the just-ingested document so a prep failure can't orphan it.
        _delete_document(client, doc_id)
        raise

    # Swap: replace this year's rows, then retire the prior document version.
    _delete_budget_rows(client, fiscal_year)
    insert_budget_line_items(client, items)
    if prior_doc and prior_doc["id"] != doc_id:
        _delete_document(client, prior_doc["id"])
    _upload_pdf(client, fiscal_year)
    logger.info(
        "  loaded %d budget line items citing doc #%d (%d chunks).",
        len(items),
        doc_id,
        result["chunks"],
    )
    return len(items)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--year",
        action="append",
        metavar="FY",
        help="fiscal year 'YYYY-YYYY' (repeatable); default: all 12 available years",
    )
    parser.add_argument("--apply", action="store_true", help="write to the DB (default: dry run)")
    args = parser.parse_args()

    years = args.year or list(ASBR_FILES)
    for fy in years:
        if fy not in ASBR_FILES:
            raise SystemExit(f"Unknown fiscal year {fy!r}; available: {', '.join(ASBR_FILES)}")

    config = load_config()
    client = None
    if args.apply:
        if not config.supabase_service_key:
            raise SystemExit("ACTALUX_SUPABASE_SERVICE_KEY is required to --apply.")
        client = get_client(config.supabase_url, config.supabase_service_key)

    total = 0
    for fy in years:
        total += process_year(client, config, fy, apply=args.apply)

    if args.apply:
        logger.info("Done: loaded %d ASBR budget line items across %d year(s).", total, len(years))
    else:
        logger.info("Dry run complete for %d year(s). Re-run with --apply to write.", len(years))
    return 0


if __name__ == "__main__":
    sys.exit(main())
