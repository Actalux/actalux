"""Load DESE ASBR object-level finance data into Actalux.

For each fiscal year this parses + reconciles the official ASBR Full Report XML
(actalux.ingest.asbr_xml), renders a clean markdown document (each figure on its
own verbatim row), and — via dese_common.load_year — ingests that markdown as a
cited document and loads object-level expenditures plus the per-fund statement
into budget_line_items, each figure citing the chunk + verbatim row it came from,
and uploads the original PDF to storage.

Why a markdown rendering is the cited source: the SSRS PDF prints each figure on
its own line (no clean row to highlight), so a faithful, no-interpretation
markdown transform of the official XML gives clean, highlightable citations and
guarantees the stored source_quote is byte-identical to the ingested chunk.

Isolation from the live budget page (HARD CONSTRAINT): the GAAP/budgetary figures
the Budget page renders are queried by dimension in
{'fund','source','function','budget'} with NO basis filter, so an 'actual'-basis
row under one of those dimensions would leak onto the page. ASBR rows use the
disjoint namespaced dimensions 'asbr_object' / 'asbr_fund' that no current query
reads — so loading is safe and non-surfacing until a dedicated ASBR view exists.

Idempotent per year (see dese_common.load_year). Dry-run by default.

Run (prefix each with `doppler run --project mac --config dev --`):
  uv run python scripts/load_asbr.py --year 2023-2024            # dry run, one year
  uv run python scripts/load_asbr.py --year 2023-2024 --apply    # write one year
  uv run python scripts/load_asbr.py --apply                     # all 12 years
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from dese_common import DESE_DIR, RENDER_DIR, Figure, load_year  # noqa: E402  (sibling)

from actalux.config import load_config  # noqa: E402
from actalux.db import get_client  # noqa: E402
from actalux.ingest import asbr_xml  # noqa: E402
from actalux.ingest.asbr_xml import (  # noqa: E402
    AsbrReport,
    object_md_row,
    parse_asbr,
    render_markdown,
    summary_md_row,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# Namespaced dimensions, disjoint from every dimension the live budget page reads.
DIM_OBJECT = "asbr_object"
DIM_FUND = "asbr_fund"
ASBR_DIMENSIONS = (DIM_OBJECT, DIM_FUND)

# Verified fiscal-year -> raw XML filename (the user's DESE downloads). The parser
# re-derives the year from the XML and process_year asserts it matches this key, so
# a renamed/mis-downloaded file fails loudly rather than loading under a wrong year.
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


def md_filename(fiscal_year: str) -> str:
    return f"dese_asbr_{fiscal_year}.md"


def pdf_storage_key(fiscal_year: str) -> str:
    return f"dese_asbr_{fiscal_year}.pdf"


def build_figures(report: AsbrReport) -> list[Figure]:
    """Expand a reconciled report into the figures to load (no chunk ids yet)."""
    figures: list[Figure] = []

    # Object-level expenditures (all funds) — the data the audits lack.
    for obj in report.objects:
        figures.append(
            Figure(
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
                Figure(
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


def _summarize(fiscal_year: str, report: AsbrReport, figures: list[Figure]) -> None:
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

    return load_year(
        client,
        config,
        fiscal_year=fiscal_year,
        md_path=md_path,
        markdown=markdown,
        figures=figures,
        dimensions=ASBR_DIMENSIONS,
        meeting_title=f"Annual Secretary of the Board Report — FY {fiscal_year}",
        pdf_path=DESE_DIR / ASBR_FILES[fiscal_year].replace(".xml", ".pdf"),
        pdf_key=pdf_storage_key(fiscal_year),
    )


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
