"""Load the City of Clayton adopted budget (FY2026) + multi-year CIP.

Source of truth: ``scripts/city_adopted_budget_FY2026.json`` — figures read from
the City of Clayton FY2026 adopted "Operating Budget and Capital Improvements
Plan" (the "2026 Budget" column of the all-funds summary tables, and the
"Funded Capital Projects, Fiscal Years 2026-2030" table). The PDF is fetched
from claytonmo.gov via the real-Chrome path (Akamai-blocked to bots); these are
adopted appropriations, distinct from the audited actuals in load_city_budget.py.

The adopted summary loads under the namespaced ``proposed_*`` dimensions
(basis='proposed') so it renders in the budget page's planned-budget section
(entity-aware, labelled "Adopted" for the city). The multi-year CIP loads under
a ``cip`` dimension (one row per project per year), which the planned-budget
section ignores; a dedicated CIP section renders it.

Integrity guard (asserted at load): revenue-by-fund total == revenue-by-type
total; expenditure-by-department line items reconcile to the printed total
(within the $1 rounding the source itself carries — reported, not corrected).
Every figure cites the verbatim digest row it was read from.

Run (after the FY2026 budget PDF is fetched to data/city_budget/), via
``doppler run --project mac --config dev -- uv run python``:
  scripts/load_city_adopted_budget.py --dry-run
  scripts/load_city_adopted_budget.py
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

sys.path.insert(0, str(Path(__file__).resolve().parent))
from ingest import ingest_single_file, resolve_entity_id  # noqa: E402  (sibling script)
from load_city_budget import (  # noqa: E402  (reuse the city loader's plumbing)
    _delete_document,
    _map_quotes_to_chunks,
    _upload_pdf,
    _usd,
)

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
FIGURES_PATH = SCRIPT_DIR / "city_adopted_budget_FY2026.json"
BUDGET_DIR = SCRIPT_DIR.parent / "data" / "city_budget"
RENDER_DIR = BUDGET_DIR / "rendered"  # outside data/documents so a dir-scan never ingests it
SOURCE_PORTAL = "budget"
ENTITY_PATH = "mo/clayton/council"
END_YEAR = 2026
FISCAL_YEAR = f"{END_YEAR - 1}-{END_YEAR}"  # FY2026 spans Oct 2025 - Sep 2026
PDF_NAME = "clayton_budget_FY2026.pdf"
SOURCE_URL = "https://www.claytonmo.gov/home/showpublisheddocument/8054/639089060510200000"


@dataclass(frozen=True)
class Figure:
    """One figure to load (per-figure fiscal_year, since CIP rows span years)."""

    fiscal_year: str
    category: str
    dimension: str
    fund: str
    subcategory: str
    amount: Decimal
    quote: str
    note: str
    basis: str | None = "proposed"


def _fund_display(fund: str) -> str:
    return (
        fund if fund.endswith(("Fund", "Funds", "District", "Bonds", "Service")) else f"{fund} Fund"
    )


def _reconcile(data: dict) -> None:
    rf = data["revenue_by_fund"]
    rt = data["revenue_by_type"]
    ed = data["expenditure_by_department"]
    if sum(rf["items"].values()) != rf["total"]:
        raise SystemExit("revenue_by_fund items do not sum to the stated total")
    if sum(rt["items"].values()) != rt["total"]:
        raise SystemExit("revenue_by_type items do not sum to the stated total")
    if rf["total"] != rt["total"]:
        raise SystemExit(f"revenue by fund {rf['total']} != revenue by type {rt['total']}")
    # The City's printed expenditure-by-department total carries a $1 rounding the
    # source itself shows (items sum $1 low); tolerate exactly that, never correct it.
    if abs(sum(ed["items"].values()) - ed["total"]) > 1:
        raise SystemExit("expenditure_by_department items do not reconcile to the printed total")
    for proj in data["cip"]["projects"]:
        if sum(proj["by_year"].values()) != proj["total"]:
            raise SystemExit(f"CIP project {proj['name']!r} total != sum of its years")


def build_figures(data: dict) -> list[Figure]:
    """Expand the verified adopted-budget + CIP figures into citeable rows."""
    _reconcile(data)
    src = (
        f"City of Clayton FY{END_YEAR} adopted Operating Budget & Capital Improvements Plan, "
        f"all-funds summary (p. {data['revenue_by_fund']['page_label']}-"
        f"{data['revenue_by_type']['page_label']})"
    )
    items: list[Figure] = []

    for fund, amount in data["revenue_by_fund"]["items"].items():
        items.append(
            Figure(
                fiscal_year=FISCAL_YEAR,
                category="revenue",
                dimension="proposed_fund",
                fund=fund,
                subcategory="Total revenue",
                amount=Decimal(amount),
                quote=f"Adopted revenue — {_fund_display(fund)} (FY{END_YEAR}): {_usd(amount)}",
                note=f"{src}, revenue by fund",
            )
        )
    for rev_type, amount in data["revenue_by_type"]["items"].items():
        items.append(
            Figure(
                fiscal_year=FISCAL_YEAR,
                category="revenue",
                dimension="proposed_source",
                fund="",
                subcategory=rev_type,
                amount=Decimal(amount),
                quote=f"Adopted revenue — {rev_type}, all funds (FY{END_YEAR}): {_usd(amount)}",
                note=f"{src}, revenue by type",
            )
        )
    for dept, amount in data["expenditure_by_department"]["items"].items():
        items.append(
            Figure(
                fiscal_year=FISCAL_YEAR,
                category="expenditure",
                dimension="proposed_function",
                fund="",
                subcategory=dept,
                amount=Decimal(amount),
                quote=f"Adopted expenditure — {dept}, all funds (FY{END_YEAR}): {_usd(amount)}",
                note=f"{src}, expenditure by department",
            )
        )

    cip_note = (
        f"City of Clayton FY{END_YEAR} CIP, 'Funded Capital Projects, Fiscal Years 2026-2030' "
        f"(p. {data['cip']['page_range']})"
    )
    for proj in data["cip"]["projects"]:
        for year, amount in proj["by_year"].items():
            if amount <= 0:
                continue
            items.append(
                Figure(
                    fiscal_year=f"{int(year) - 1}-{year}",
                    category="expenditure",
                    dimension="cip",
                    fund="",
                    subcategory=proj["name"],
                    amount=Decimal(amount),
                    quote=f"Capital project — {proj['name']}, FY{year}: {_usd(amount)}",
                    note=cip_note,
                )
            )
    return items


def render_digest(figures: list[Figure]) -> str:
    """Clean markdown digest; every figure's quote appears verbatim for chunk mapping."""
    lines = [
        "---",
        f"meeting_date: {END_YEAR}-09-30",
        f"meeting_title: City of Clayton adopted budget — FY{END_YEAR}",
        f"source_file: clayton_budget_FY{END_YEAR}.md",
        "---",
        "",
        f"# City of Clayton — adopted budget figures, FY{END_YEAR}",
        "",
        f"Figures below are read from the City of Clayton FY{END_YEAR} adopted Operating Budget "
        "and Capital Improvements Plan. These are budgeted appropriations (the '2026 Budget' "
        "column), distinct from the audited actuals. The original budget book is the source.",
        "",
    ]
    groups = [
        ("Adopted revenue by fund", "proposed_fund"),
        ("Adopted revenue by source (type)", "proposed_source"),
        ("Adopted expenditure by department", "proposed_function"),
        ("Capital Improvements Plan — funded projects, FY2026-2030", "cip"),
    ]
    for heading, dim in groups:
        lines += ["", f"## {heading}", ""]
        lines += [f"- {f.quote}" for f in figures if f.dimension == dim]
    return "\n".join(lines) + "\n"


def load(client, config, figures: list[Figure], markdown: str) -> int:
    """Ingest the digest, cite each figure, swap in the rows. Returns rows loaded."""
    RENDER_DIR.mkdir(parents=True, exist_ok=True)
    md_path = RENDER_DIR / f"clayton_budget_FY{END_YEAR}.md"
    md_path.write_text(markdown, encoding="utf-8")

    prior = find_document_by_source(client, md_path.name, SOURCE_PORTAL)
    result = ingest_single_file(
        client=client,
        path=md_path,
        text=markdown,
        meeting_date=date(END_YEAR, 9, 30),
        meeting_title=f"City of Clayton adopted budget — FY{END_YEAR}",
        config=config,
        source_url=SOURCE_URL,
        source_portal=SOURCE_PORTAL,
        document_type="budget",
        entity_id=resolve_entity_id(client, ENTITY_PATH),
        date_source="content",
    )
    doc_id = result["doc_id"]
    try:
        quote_to_chunk = _map_quotes_to_chunks(client, doc_id, {f.quote for f in figures})
        chunk_citation = get_chunk_citation_ids(client, list(quote_to_chunk.values()))
        rows: list[BudgetLineItem] = []
        for f in figures:
            chunk_id = quote_to_chunk.get(f.quote)
            if chunk_id is None:
                raise SystemExit(
                    f"no chunk contains the digest row {f.quote!r}; refusing uncited figure"
                )
            rows.append(
                BudgetLineItem(
                    fiscal_year=f.fiscal_year,
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
        insert_budget_line_items(client, rows)
    except BaseException:
        _delete_document(client, doc_id)
        raise
    if prior and prior["id"] != doc_id:
        _delete_document(client, prior["id"])
    pdf = BUDGET_DIR / PDF_NAME
    if pdf.exists():
        _upload_pdf(client, pdf, md_path.stem + ".pdf")
    logger.info(
        "Loaded %d adopted-budget + CIP rows citing doc #%d (%d chunks).",
        len(rows),
        doc_id,
        result["chunks"],
    )
    return len(rows)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dry-run", action="store_true", help="reconcile + build, write nothing")
    args = parser.parse_args()
    data = json.loads(FIGURES_PATH.read_text())
    figures = build_figures(data)
    by_dim = {
        d: sum(1 for f in figures if f.dimension == d)
        for d in ("proposed_fund", "proposed_source", "proposed_function", "cip")
    }
    if args.dry_run:
        logger.info("Dry run OK: %d figures (%s); reconciles ✓", len(figures), by_dim)
        return 0
    config = load_config()
    client = get_client(config.supabase_url, config.supabase_service_key)
    load(client, config, figures, render_digest(figures))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
