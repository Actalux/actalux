"""Load the City of Clayton audited budget figures into budget_line_items.

Source of truth: ``scripts/city_budget_figures.json`` — the audited figures read
from the City of Clayton Annual Comprehensive Financial Reports (FY2020-FY2024,
fiscal year ended September 30), filed with the Missouri State Auditor and
fetched by ``fetch_city_acfr.py``. Each figure is the verbatim audited amount
from the "Statement of Revenues, Expenditures and Changes in Fund Balances -
Governmental Funds" (per-fund totals, revenue by source, expenditure by
function) or the General Fund budgetary comparison schedule (budget vs actual).

Approach (mirrors the DESE loaders): for each year render a clean markdown digest
where every figure sits on its own verbatim-citable row, ingest it as a cited
document under the City Council entity (``source_portal='auditor'``,
``source_url`` = the State Auditor file URL, the original ACFR PDF uploaded to
storage for "Open original"), map each figure row to the chunk that carries it,
and load the figures. The figures are not reconstructed from the raw audit
tables, whose label/value columns decouple under PDF extraction; the digest is
the citation surface and the PDF the verifiable original.

Integrity guard: each year's figures are re-asserted to reconcile at load time
(per-fund sums to the Total Governmental Funds column; revenue-by-source to total
revenues; expenditure-by-function to total expenditures). A bad figure fails
loudly rather than publishing a wrong number. The major-fund set changes year to
year (6 funds in FY2020-21, 5 in FY2022, 4 in FY2023-24); the loader reads each
year's actual fund columns from the data.

City fiscal year ends September 30, so FY2024 spans Oct 1, 2023 - Sep 30, 2024
and is stored with ``fiscal_year='2023-2024'`` to match the chart's span format.

Run (after fetch_city_acfr.py):
  doppler run --project mac --config dev -- uv run python scripts/load_city_budget.py --dry-run
  doppler run --project mac --config dev -- uv run python scripts/load_city_budget.py
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from pathlib import Path

# Reuse the native ingest pipeline (the rendered digest text is passed in).
sys.path.insert(0, str(Path(__file__).resolve().parent))
from ingest import ingest_single_file, resolve_entity_id  # noqa: E402  (sibling script)

from actalux.config import load_config  # noqa: E402
from actalux.db import (  # noqa: E402
    find_document_by_source,
    get_chunk_citation_ids,
    get_client,
    insert_budget_line_items,
)
from actalux.models import BudgetLineItem  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

SCRIPT_DIR = Path(__file__).resolve().parent
FIGURES_PATH = SCRIPT_DIR / "city_budget_figures.json"
ACFR_DIR = SCRIPT_DIR.parent / "data" / "city_acfr"
RENDER_DIR = ACFR_DIR / "rendered"  # outside data/documents so a dir-scan never ingests it
SOURCE_PORTAL = "auditor"
ENTITY_PATH = "mo/clayton/council"
STORAGE_BUCKET = "documents"

# Subcategory label per category, matching the schools' GAAP fund rows.
SUBCATEGORY = {
    "revenue": "Total revenues",
    "expenditure": "Total expenditures",
    "fund_balance": "Ending fund balance",
}
# All dimensions this loader writes (used for the dry-run summary only).
DIMENSIONS = ("fund", "source", "function", "budget")


@dataclass(frozen=True)
class Figure:
    """One figure to load: everything for a BudgetLineItem bar the chunk id."""

    category: str
    dimension: str
    fund: str
    subcategory: str
    amount: Decimal
    quote: str  # the verbatim digest row this figure is cited to
    note: str
    basis: str | None = None  # set only on budget-vs-actual rows


def _usd(amount: int) -> str:
    """Render a whole-dollar figure as ``$1,234`` / ``-$1,234`` for a digest row."""
    return f"-${abs(amount):,}" if amount < 0 else f"${amount:,}"


def _fund_display(fund: str) -> str:
    """Readable fund name for a citation row (the column names omit 'Fund')."""
    if fund == "Other Governmental":
        return "Other Governmental Funds"
    return f"{fund} Fund"


def _reconcile(fy_key: str, fig: dict) -> None:
    """Re-assert every figure group ties out before any of it is loaded."""
    checks = {
        "revenue by fund": (sum(fig["revenue"]["by_fund"].values()), fig["revenue"]["total"]),
        "expenditure by fund": (
            sum(fig["expenditure"]["by_fund"].values()),
            fig["expenditure"]["total"],
        ),
        "fund balance by fund": (
            sum(fig["fund_balance"]["by_fund"].values()),
            fig["fund_balance"]["total"],
        ),
        "revenue by source": (
            sum(fig["revenue_by_source"].values()),
            fig["revenue"]["total"],
        ),
        "expenditure by function": (
            sum(fig["expenditure_by_function"].values()),
            fig["expenditure"]["total"],
        ),
    }
    for label, (got, want) in checks.items():
        if got != want:
            raise SystemExit(
                f"{fy_key}: reconciliation failed for {label}: sum {got:,} != stated total {want:,}"
            )


def build_figures(fy_key: str, fig: dict, end_year: int) -> list[Figure]:
    """Expand one year's verified figures into citeable rows (after reconciling)."""
    _reconcile(fy_key, fig)
    stmt = (
        f"City of Clayton ACFR, year ended September 30, {end_year}, Statement of Revenues, "
        f"Expenditures and Changes in Fund Balances - Governmental Funds (p. "
        f"{fig['statement_page_label']})"
    )
    items: list[Figure] = []

    # By fund: revenue / expenditure / ending fund balance, one row per fund column.
    for category in ("revenue", "expenditure", "fund_balance"):
        for fund, amount in fig[category]["by_fund"].items():
            quote = (
                f"{SUBCATEGORY[category]} - {_fund_display(fund)} (FY{end_year}, "
                f"year ended September 30, {end_year}): {_usd(amount)}"
            )
            items.append(
                Figure(
                    category=category,
                    dimension="fund",
                    fund=fund,
                    subcategory=SUBCATEGORY[category],
                    amount=Decimal(amount),
                    quote=quote,
                    note=stmt,
                )
            )

    # Revenue by source (Total Governmental Funds column).
    for source, amount in fig["revenue_by_source"].items():
        quote = f"Revenue - {source}, all governmental funds (FY{end_year}): {_usd(amount)}"
        items.append(
            Figure(
                category="revenue",
                dimension="source",
                fund="",
                subcategory=source,
                amount=Decimal(amount),
                quote=quote,
                note=f"{stmt}, revenue by source",
            )
        )

    # Expenditure by function (Total Governmental Funds column).
    for function, amount in fig["expenditure_by_function"].items():
        quote = f"Expenditure - {function}, all governmental funds (FY{end_year}): {_usd(amount)}"
        items.append(
            Figure(
                category="expenditure",
                dimension="function",
                fund="",
                subcategory=function,
                amount=Decimal(amount),
                quote=quote,
                note=f"{stmt}, expenditure by function",
            )
        )

    # General Fund budget vs actual (Required Supplementary Information).
    gf = fig["budget_vs_actual_general_fund"]
    rsi_note = (
        f"City of Clayton ACFR, year ended September 30, {end_year}, Budgetary Comparison "
        f"Schedule - General Fund (p. {gf['rsi_page_label']}). {gf['basis']}"
    )
    for category, subcat in (("revenue", "Total revenues"), ("expenditure", "Total expenditures")):
        vals = gf[category]
        if not all(vals[b] > 0 for b in ("original", "final", "actual")):
            raise SystemExit(
                f"{fy_key}: General Fund budget-vs-actual {category} has a non-positive figure"
            )
        quote = (
            f"{subcat} - General Fund budgetary comparison (FY{end_year}): "
            f"original {_usd(vals['original'])} | final {_usd(vals['final'])} | "
            f"actual {_usd(vals['actual'])}"
        )
        for basis in ("original", "final", "actual"):
            items.append(
                Figure(
                    category=category,
                    dimension="budget",
                    fund="General",
                    subcategory=subcat,
                    amount=Decimal(vals[basis]),
                    quote=quote,
                    note=rsi_note,
                    basis=basis,
                )
            )
    return items


def render_digest(fiscal_year: str, end_year: int, figures: list[Figure]) -> str:
    """Build the clean markdown digest; every figure's quote appears verbatim."""
    lines = [
        "---",
        f"meeting_date: {end_year}-09-30",
        f"meeting_title: City of Clayton ACFR — year ended September 30, {end_year}",
        f"source_file: clayton_acfr_FY{end_year}.md",
        "---",
        "",
        f"# City of Clayton — audited financial figures, fiscal year {fiscal_year}",
        "",
        f"Figures below are read verbatim from the City of Clayton Annual Comprehensive "
        f"Financial Report for the year ended September 30, {end_year} (audited), as filed "
        f"with the Missouri State Auditor. Each line is one figure from the audited "
        f"governmental-funds statements; the original report is linked as the source.",
        "",
        "## Governmental funds — revenue, expenditure, and ending fund balance by fund",
        "",
    ]
    by_dim = {d: [f for f in figures if f.dimension == d] for d in DIMENSIONS}
    lines += [f"- {f.quote}" for f in by_dim["fund"]]
    lines += ["", "## Revenue by source (all governmental funds)", ""]
    lines += [f"- {f.quote}" for f in by_dim["source"]]
    lines += ["", "## Expenditure by function (all governmental funds)", ""]
    lines += [f"- {f.quote}" for f in by_dim["function"]]
    lines += ["", "## General Fund — budget vs. actual", ""]
    # Each budget figure renders three basis rows sharing one quote; de-dup the lines.
    seen: set[str] = set()
    for f in by_dim["budget"]:
        if f.quote not in seen:
            lines.append(f"- {f.quote}")
            seen.add(f.quote)
    return "\n".join(lines) + "\n"


def _map_quotes_to_chunks(client, doc_id: int, quotes: set[str]) -> dict[str, int]:
    """Map each verbatim digest row to the id of the chunk that contains it."""
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


def _delete_document(client, doc_id: int) -> None:
    """Delete a document and its chunks (budget_line_items cascade on the FK)."""
    client.table("chunks").delete().eq("document_id", doc_id).execute()
    client.table("documents").delete().eq("id", doc_id).execute()


def _upload_pdf(client, pdf_path: Path, key: str) -> None:
    """Upload the original ACFR PDF to storage so 'Open original' embeds it."""
    try:
        client.storage.from_(STORAGE_BUCKET).upload(
            key,
            pdf_path.read_bytes(),
            {"content-type": "application/pdf", "upsert": "true"},
        )
        logger.info("  uploaded PDF -> %s/%s", STORAGE_BUCKET, key)
    except Exception as exc:  # storage is best-effort; the citation does not depend on it
        logger.warning("  PDF upload failed for %s (non-fatal): %s", key, exc)


def load_year(client, config, fy_key: str, fig: dict, source_url: str) -> int:
    """Render, ingest, cite, and load one fiscal year. Returns rows loaded.

    All fallible work happens before the prior document is removed, so a failure
    leaves the previous load intact; the just-ingested document is rolled back on
    a prep failure to avoid orphaning it.
    """
    end_year = int(fy_key[2:])  # "FY2024" -> 2024
    fiscal_year = f"{end_year - 1}-{end_year}"
    figures = build_figures(fy_key, fig, end_year)
    markdown = render_digest(fiscal_year, end_year, figures)

    RENDER_DIR.mkdir(parents=True, exist_ok=True)
    md_path = RENDER_DIR / f"clayton_acfr_FY{end_year}.md"
    md_path.write_text(markdown, encoding="utf-8")

    prior = find_document_by_source(client, md_path.name, SOURCE_PORTAL)
    result = ingest_single_file(
        client=client,
        path=md_path,
        text=markdown,
        meeting_date=date(end_year, 9, 30),
        meeting_title=f"City of Clayton ACFR — year ended September 30, {end_year}",
        config=config,
        source_url=source_url,
        source_portal=SOURCE_PORTAL,
        document_type="financial_report",
        entity_id=resolve_entity_id(client, ENTITY_PATH),
        date_source="content",  # derived from the report's own stated fiscal year
    )
    doc_id = result["doc_id"]

    try:
        quote_to_chunk = _map_quotes_to_chunks(client, doc_id, {f.quote for f in figures})
        chunk_citation = get_chunk_citation_ids(client, list(quote_to_chunk.values()))
        items: list[BudgetLineItem] = []
        for f in figures:
            chunk_id = quote_to_chunk.get(f.quote)
            if chunk_id is None:
                raise SystemExit(
                    f"{fy_key}: no ingested chunk contains the digest row {f.quote!r}; "
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
                    basis=f.basis,
                    chunk_id=chunk_id,
                    citation_id=chunk_citation.get(chunk_id, ""),
                    source_quote=f.quote,
                    note=f.note,
                )
            )
        insert_budget_line_items(client, items)
    except BaseException:
        _delete_document(client, doc_id)  # cascade drops any rows already inserted
        raise

    if prior and prior["id"] != doc_id:
        _delete_document(client, prior["id"])

    pdf_path = ACFR_DIR / f"clayton_acfr_FY{end_year}.pdf"
    if pdf_path.exists():
        _upload_pdf(client, pdf_path, md_path.stem + ".pdf")
    logger.info(
        "  FY%d: loaded %d budget line items citing doc #%d (%d chunks).",
        end_year,
        len(items),
        doc_id,
        result["chunks"],
    )
    return len(items)


def _source_urls() -> dict[str, str]:
    """fiscal-year-key -> State Auditor file URL, from the fetch manifest."""
    manifest = json.loads((ACFR_DIR / "manifest.json").read_text())
    return {f"FY{e['fiscal_year']}": e["source_url"] for e in manifest}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--dry-run", action="store_true", help="reconcile + build figures, write nothing"
    )
    args = parser.parse_args()

    figures_by_year = json.loads(FIGURES_PATH.read_text())

    if args.dry_run:
        total = 0
        for fy_key, fig in figures_by_year.items():
            end_year = int(fy_key[2:])
            built = build_figures(fy_key, fig, end_year)
            by_dim = {d: sum(1 for f in built if f.dimension == d) for d in DIMENSIONS}
            logger.info(
                "%s reconciles ✓  %d figures (fund=%d source=%d function=%d budget=%d)",
                fy_key,
                len(built),
                by_dim["fund"],
                by_dim["source"],
                by_dim["function"],
                by_dim["budget"],
            )
            total += len(built)
        logger.info("Dry run OK: %d figures across %d years.", total, len(figures_by_year))
        return 0

    config = load_config()
    client = get_client(config.supabase_url, config.supabase_service_key)
    source_urls = _source_urls()
    loaded = 0
    for fy_key, fig in figures_by_year.items():
        if fy_key not in source_urls:
            raise SystemExit(f"{fy_key}: no source_url in manifest; run fetch_city_acfr.py first")
        loaded += load_year(client, config, fy_key, fig, source_urls[fy_key])
    logger.info("Loaded %d city budget line items across %d years.", loaded, len(figures_by_year))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
