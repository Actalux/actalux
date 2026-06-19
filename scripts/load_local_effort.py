"""Load DESE Local Effort reports into Actalux.

For each fiscal year this parses + reconciles the official Local Effort XML
(actalux.ingest.local_effort_xml), renders a clean cited markdown document, and
— via dese_common.load_year — ingests it as a cited document and loads each
local-tax revenue line into budget_line_items, each figure citing its verbatim
row, and uploads the original PDF.

Only the tax-line dollar amounts are stored as structured figures (they
reconcile to the stated grand total); the ADA count and per-ADA rate are rendered
verbatim in the cited document for context but not stored (they are not dollar
amounts and not sum-able).

The fiscal year is not embedded in the XML; it comes from the verified file map
and is cross-checked against the paired PDF before loading.

Isolation: rows use the namespaced dimension 'local_effort', which the live
budget page never queries. Idempotent per year. Dry-run by default.

Run (prefix each with `doppler run --project mac --config dev --`):
  uv run python scripts/load_local_effort.py --year 2024-2025            # dry run
  uv run python scripts/load_local_effort.py --year 2024-2025 --apply    # write one year
  uv run python scripts/load_local_effort.py --apply                     # all years
"""

from __future__ import annotations

import argparse
import logging
import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from dese_common import DESE_DIR, RENDER_DIR, Figure, load_year  # noqa: E402  (sibling)

from actalux.config import load_config  # noqa: E402
from actalux.db import get_client  # noqa: E402
from actalux.ingest.local_effort_xml import (  # noqa: E402
    LocalEffortReport,
    line_md_row,
    parse_local_effort,
    render_markdown,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

DIM_LOCAL_EFFORT = "local_effort"  # disjoint from every live-page dimension
LOCAL_EFFORT_DIMENSIONS = (DIM_LOCAL_EFFORT,)

# Verified fiscal-year -> raw XML filename. The year is cross-checked against the
# paired PDF before loading (the XML carries no year of its own).
LOCAL_EFFORT_FILES: dict[str, str] = {
    "2013-2014": "Local_Effort (11).xml",
    "2014-2015": "Local_Effort (10).xml",
    "2015-2016": "Local_Effort (9).xml",
    "2016-2017": "Local_Effort (8).xml",
    "2017-2018": "Local_Effort (7).xml",
    "2018-2019": "Local_Effort (6).xml",
    "2019-2020": "Local_Effort (5).xml",
    "2020-2021": "Local_Effort (4).xml",
    "2021-2022": "Local_Effort (3).xml",
    "2022-2023": "Local_Effort (2).xml",
    "2023-2024": "Local_Effort (1).xml",
    "2024-2025": "Local_Effort.xml",
}

_PDF_YEAR_RE = re.compile(r"Year:\s*(\d{4})\s*-\s*(\d{4})")


def md_filename(fiscal_year: str) -> str:
    return f"dese_local_effort_{fiscal_year}.md"


def pdf_storage_key(fiscal_year: str) -> str:
    return f"dese_local_effort_{fiscal_year}.pdf"


def pdf_path(fiscal_year: str) -> Path:
    return DESE_DIR / LOCAL_EFFORT_FILES[fiscal_year].replace(".xml", ".pdf")


def _verify_pdf_year(fiscal_year: str) -> None:
    """Require the paired PDF to confirm the fiscal year before loading.

    The Local Effort XML has no embedded year, so the paired PDF is the only
    independent guard against a mislabeled download. Strict by design: a missing
    PDF or an unreadable / mismatched "Year:" line aborts rather than risk loading
    an XML under the wrong year.
    """
    pdf = pdf_path(fiscal_year)
    if not pdf.exists():
        raise SystemExit(
            f"FY{fiscal_year}: paired PDF {pdf.name} not found; cannot confirm the year. "
            "Refusing to load (the XML carries no year of its own)."
        )
    import fitz  # pymupdf

    doc = fitz.open(str(pdf))
    text = str(doc[0].get_text("text")) if doc.page_count else ""
    doc.close()
    match = _PDF_YEAR_RE.search(text)
    if not match:
        raise SystemExit(
            f"FY{fiscal_year}: could not read a 'Year:' line from {pdf.name}; "
            "cannot confirm the year. Refusing to load."
        )
    found = f"{match.group(1)}-{match.group(2)}"
    if found != fiscal_year:
        raise SystemExit(
            f"FY mismatch: {pdf.name} PDF says {found}, expected {fiscal_year}. "
            "Refusing to load under the wrong year."
        )


def build_figures(report: LocalEffortReport) -> list[Figure]:
    """Expand a reconciled report into one figure per local-tax revenue line."""
    return [
        Figure(
            category="revenue",
            dimension=DIM_LOCAL_EFFORT,
            fund="",
            subcategory=line.description,
            amount=line.amount,
            quote=line_md_row(line),
            note="DESE Local Effort: local tax revenue line",
        )
        for line in report.lines
    ]


def process_year(client, config, fiscal_year: str, *, apply: bool) -> int:
    """Render, (optionally) ingest, and load one fiscal year. Returns rows loaded."""
    xml_path = DESE_DIR / LOCAL_EFFORT_FILES[fiscal_year]
    if not xml_path.exists():
        raise SystemExit(f"Missing Local Effort XML for FY{fiscal_year}: {xml_path}")

    _verify_pdf_year(fiscal_year)
    report = parse_local_effort(xml_path, fiscal_year)  # parses + reconciles
    markdown = render_markdown(report)
    RENDER_DIR.mkdir(parents=True, exist_ok=True)
    md_path = RENDER_DIR / md_filename(fiscal_year)
    md_path.write_text(markdown, encoding="utf-8")

    figures = build_figures(report)
    logger.info(
        "FY%s: %d tax lines, grand total $%s",
        fiscal_year,
        len(figures),
        f"{report.grand_total:,.2f}",
    )

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
        dimensions=LOCAL_EFFORT_DIMENSIONS,
        meeting_title=f"Local Effort — FY {fiscal_year}",
        pdf_path=pdf_path(fiscal_year),
        pdf_key=pdf_storage_key(fiscal_year),
    )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--year",
        action="append",
        metavar="FY",
        help="fiscal year 'YYYY-YYYY' (repeatable); default: all available years",
    )
    parser.add_argument("--apply", action="store_true", help="write to the DB (default: dry run)")
    args = parser.parse_args()

    years = args.year or list(LOCAL_EFFORT_FILES)
    for fy in years:
        if fy not in LOCAL_EFFORT_FILES:
            raise SystemExit(
                f"Unknown fiscal year {fy!r}; available: {', '.join(LOCAL_EFFORT_FILES)}"
            )

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
        logger.info(
            "Done: loaded %d Local Effort budget line items across %d year(s).", total, len(years)
        )
    else:
        logger.info("Dry run complete for %d year(s). Re-run with --apply to write.", len(years))
    return 0


if __name__ == "__main__":
    sys.exit(main())
