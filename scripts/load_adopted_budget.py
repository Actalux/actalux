"""Load the FY2024-2025 proposed budget (doc #262) into budget_line_items.

Source of truth: the figures below are transcribed verbatim from document #262,
"Proposed 2024-2025 Budget" (School District of Clayton), specifically its two
top-level summary tables -- "Budget Summary View #1 - by Fund by Object"
(chunk 5109) and "Budget Summary View #2 - by Fund by Function" (chunk 5110).
Both tables share an identical revenue block and identical fund-balance block;
they differ only in how expenditures are cut (by object vs by function). Every
figure carries the document_id (262), the chunk_id of the passage it came from,
and the verbatim source row, so every proposed-budget figure on the Budget page
drills down to its source. The full reconciliation that backs these figures is
recorded in data/drafts/adopted_budget_2024-2025_DRAFT.md.

Presentation honesty: the PDF is titled "Proposed Budget" and carries no
adopting board vote, so these rows are loaded as basis='proposed' and labelled
"Proposed (June 2024)" -- never "Adopted".

Isolation from existing views (HARD CONSTRAINT): the audited GAAP actuals and
budgetary-actuals already in budget_line_items are queried by the Budget page
and the finance router with dimension in {'fund','source','function','budget'}
and no basis filter. To keep the proposed figures from leaking into (and
double-counting) those views, the proposed rows are NAMESPACED under their own
dimensions -- 'proposed_fund', 'proposed_source', 'proposed_function',
'proposed_object' -- which no existing query reads. basis='proposed' is set
additionally as a second, independent guard.

Reconcile-on-load: each breakdown is asserted to sum to the budget's stated
totals (revenue 77,321,920; expenditures 76,416,879; the fund-balance
roll-forward End = Beg + Change, with Change = Revenue - Expenditure) before any
write. A transcription error fails loudly rather than publishing a wrong figure.

Idempotent on its OWN subset only: a re-run deletes exactly the proposed-budget
rows (document_id=262 AND basis='proposed') and re-inserts them. It never
touches the audit-actual rows.

Run:
  doppler run --project mac --config dev -- uv run python scripts/load_adopted_budget.py --dry-run
  doppler run --project mac --config dev -- uv run python scripts/load_adopted_budget.py --apply
"""

from __future__ import annotations

import argparse
import logging
import sys
from decimal import Decimal

from actalux.config import load_config
from actalux.db import get_client, insert_budget_line_items
from actalux.models import BudgetLineItem

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# --- Fixed facts about the source document (doc #262) ------------------------
FISCAL_YEAR = "2024-2025"
DOCUMENT_ID = 262
BASIS = "proposed"  # PDF is titled "Proposed Budget"; no adopting vote in the doc
# View #1 (by Fund by Object) carries revenue, by-object expenditures, and fund
# balances. View #2 (by Fund by Function) carries the identical revenue + fund
# balances and the by-function expenditures.
CHUNK_VIEW1 = 5109  # Budget Summary View #1 - by Fund by Object
CHUNK_VIEW2 = 5110  # Budget Summary View #2 - by Fund by Function
# The namespaced dimensions this loader owns. The --apply delete is scoped to
# exactly these, so a re-run replaces only this loader's rows and never touches
# audit-actual rows (or any other future proposed slice of doc 262).
PROPOSED_DIMENSIONS = (
    "proposed_fund",
    "proposed_source",
    "proposed_object",
    "proposed_function",
)
SOURCE_PAGE_NOTE = (
    "doc #262, Proposed 2024-2025 Budget (School District of Clayton), Budget Summary View"
)

# Fund columns in source order; canonical labels match load_budget.py FUNDS so
# the proposed and actual figures speak the same fund vocabulary on the page.
FUNDS = ("General", "Special Revenue (Teachers)", "Debt Service", "Capital Projects")

# Stated totals (the document's "Total All Funds" column), used only as
# load-time checksums (never stored as rows).
REVENUE_TOTAL = 77321920
EXPENDITURE_TOTAL = 76416879

# --- Per-fund totals, dimension='proposed_fund' ------------------------------
# revenue (category='revenue') and fund_balance roll-forward (category=
# 'fund_balance'); the per-fund expenditure total is carried by the object and
# function breakdowns, so the fund dimension holds revenue + ending balance.
REVENUE_BY_FUND = [24866380, 40698910, 8063430, 3693200]
REVENUE_BY_FUND_QUOTE = "TOTAL REVENUE 24,866,380 40,698,910 8,063,430 3,693,200 77,321,920"
EXPENDITURE_BY_FUND = [24756299, 40497270, 7394340, 3768970]
EXPENDITURE_BY_FUND_QUOTE = (
    "TOTAL EXPENDITURES 24,756,299 40,497,270 7,394,340 3,768,970 76,416,879"
)

# Fund-balance roll-forward (dimension='proposed_fund', category='fund_balance').
END_FUND_BALANCE_BY_FUND = [25110081, 10201640, 6169090, 3924230]
END_FUND_BALANCE_TOTAL = 45405041
BEG_FUND_BALANCE_BY_FUND = [25000000, 10000000, 5500000, 4000000]
CHANGE_IN_FUND_BALANCE_BY_FUND = [110081, 201640, 669090, -75770]
END_FUND_BALANCE_QUOTE = (
    "End Fund Bal-June 30, 2025 25,110,081 10,201,640 6,169,090 3,924,230 45,405,041"
)

# --- Revenue by source, dimension='proposed_source' --------------------------
# Five LEAF sources that sum to TOTAL REVENUE. "Local Revenue" is the
# Property-Taxes + Other subtotal; loading the leaf (not both) avoids
# double-counting -- mirrors load_budget.py's SOURCE_LABELS.
# subcategory -> ([per-fund], stated total, verbatim source row)
SOURCES: dict[str, tuple[list[int], int, str]] = {
    "Local Revenue": (
        [23876630, 38298500, 7707100, 3641500],
        73523730,
        "Local Revenue 23,876,630 38,298,500 7,707,100 3,641,500 73,523,730",
    ),
    "County Revenue": (
        [133500, 305800, 180000, 26700],
        646000,
        "County Revenue 133,500 305,800 180,000 26,700 646,000",
    ),
    "State Revenue": (
        [305990, 2007980, 0, 0],
        2313970,
        "State Revenue 305,990 2,007,980 - - 2,313,970",
    ),
    "Federal Revenue": (
        [550260, 86630, 176330, 0],
        813220,
        "Federal Revenue 550,260 86,630 176,330 - 813,220",
    ),
    "Other Revenue": (
        [0, 0, 0, 25000],
        25000,
        "Other Revenue - - - 25,000 25,000",
    ),
}

# --- Expenditure by object, dimension='proposed_object' (NEW dimension) -------
# Source: chunk 5109 (View #1 - by Fund by Object).
# subcategory -> ([per-fund], stated total, verbatim source row)
OBJECT: dict[str, tuple[list[int], int, str]] = {
    "Salaries/Wages": (
        [9781290, 31403460, 0, 0],
        41184750,
        "Salaries/Wages 9,781,290 31,403,460 - - 41,184,750",
    ),
    "Employee Benefits": (
        [3546990, 9093810, 0, 0],
        12640800,
        "Employee Benefits 3,546,990 9,093,810 - - 12,640,800",
    ),
    "Purchased Services": (
        [6553855, 0, 0, 0],
        6553855,
        "Purchased Services 6,553,855 - - - 6,553,855",
    ),
    "Supplies": (
        [4874164, 0, 0, 0],
        4874164,
        "Supplies 4,874,164 - - - 4,874,164",
    ),
    "Capital Outlay": (
        [0, 0, 0, 3177360],
        3177360,
        "Capital Outlay - - - 3,177,360 3,177,360",
    ),
    "Debt Service": (
        [0, 0, 7394340, 591610],
        7985950,
        "Debt Service - - 7,394,340 591,610 7,985,950",
    ),
}

# --- Expenditure by function, dimension='proposed_function' -------------------
# Source: chunk 5110 (View #2 - by Fund by Function). The first three labels are
# CLIPPED by the PDF's column width in the source itself; they are kept VERBATIM
# (clipped) per the content policy (cite what the source prints, do not invent).
# subcategory -> ([per-fund], stated total, verbatim source row)
FUNCTION: dict[str, tuple[list[int], int, str]] = {
    "Total Instructional Expenditu": (
        [3153654, 33247660, 0, 828760],
        37230074,
        "Total Instructional Expenditu 3,153,654 33,247,660 - 828,760 37,230,074",
    ),
    "Total Support Services Expen": (
        [19283455, 7215230, 0, 2341100],
        28839785,
        "Total Support Services Expen 19,283,455 7,215,230 - 2,341,100 28,839,785",
    ),
    "Total Non-Instruction/Suppo": (
        [2319190, 34380, 0, 7500],
        2361070,
        "Total Non-Instruction/Suppo 2,319,190 34,380 - 7,500 2,361,070",
    ),
    "Debt Service-Principal": (
        [0, 0, 6110000, 525000],
        6635000,
        "Debt Service-Principal - - 6,110,000 525,000 6,635,000",
    ),
    "Debt Service-Interest": (
        [0, 0, 1284340, 66610],
        1350950,
        "Debt Service-Interest - - 1,284,340 66,610 1,350,950",
    ),
}


def _reconcile_breakdown(
    label: str,
    rows: dict[str, tuple[list[int], int, str]],
    grand_total: int,
    column_totals: list[int],
) -> None:
    """Reconcile a breakdown BOTH ways before any row is emitted.

    Three checks, mirroring load_budget.py's two-way reconciliation:
      1. each row cross-foots to its stated total (rows sum across funds),
      2. the rows sum to the breakdown's grand total,
      3. each per-fund COLUMN sums to that fund's stated total -- so a
         fund-column transcription error cannot pass merely because the row
         totals still foot.

    Fails loudly (SystemExit) on the first mismatch so a transcription error
    never reaches the database.
    """
    running = 0
    col_sums = [0] * len(column_totals)
    for subcat, (cells, stated, _quote) in rows.items():
        cell_sum = sum(cells)
        if cell_sum != stated:
            raise SystemExit(
                f"Reconciliation failed for proposed {label} row {subcat!r}: "
                f"per-fund sum {cell_sum} != stated total {stated}"
            )
        running += stated
        for i, c in enumerate(cells):
            col_sums[i] += c
    if running != grand_total:
        raise SystemExit(
            f"Reconciliation failed for proposed {label}: rows sum {running} "
            f"!= stated grand total {grand_total}"
        )
    if col_sums != column_totals:
        raise SystemExit(
            f"Reconciliation failed for proposed {label} fund columns: "
            f"{col_sums} != stated per-fund totals {column_totals}"
        )


def build_line_items() -> list[BudgetLineItem]:
    """Expand the verified proposed figures into rows, reconciling each breakdown.

    Every row is fiscal_year=2024-2025, document_id=262, basis='proposed', under
    a namespaced 'proposed_*' dimension. Reconciliation runs before any row is
    emitted.
    """
    items: list[BudgetLineItem] = []

    # --- Reconcile the namespaced breakdowns up front. ---
    if sum(REVENUE_BY_FUND) != REVENUE_TOTAL:
        raise SystemExit(
            f"Reconciliation failed for proposed revenue-by-fund: "
            f"{sum(REVENUE_BY_FUND)} != stated total {REVENUE_TOTAL}"
        )
    if sum(EXPENDITURE_BY_FUND) != EXPENDITURE_TOTAL:
        raise SystemExit(
            f"Reconciliation failed for proposed expenditure-by-fund: "
            f"{sum(EXPENDITURE_BY_FUND)} != stated total {EXPENDITURE_TOTAL}"
        )
    _reconcile_breakdown("revenue-by-source", SOURCES, REVENUE_TOTAL, REVENUE_BY_FUND)
    _reconcile_breakdown("expenditure-by-object", OBJECT, EXPENDITURE_TOTAL, EXPENDITURE_BY_FUND)
    _reconcile_breakdown(
        "expenditure-by-function", FUNCTION, EXPENDITURE_TOTAL, EXPENDITURE_BY_FUND
    )

    # Fund-balance roll-forward: per fund, End == Beg + Change and
    # Change == Revenue - Expenditure; and the ending balances cross-foot.
    for i, fund in enumerate(FUNDS):
        change = CHANGE_IN_FUND_BALANCE_BY_FUND[i]
        if change != REVENUE_BY_FUND[i] - EXPENDITURE_BY_FUND[i]:
            raise SystemExit(
                f"Reconciliation failed for proposed fund balance {fund}: change "
                f"{change} != revenue {REVENUE_BY_FUND[i]} - expenditure "
                f"{EXPENDITURE_BY_FUND[i]}"
            )
        if END_FUND_BALANCE_BY_FUND[i] != BEG_FUND_BALANCE_BY_FUND[i] + change:
            raise SystemExit(
                f"Reconciliation failed for proposed fund balance {fund}: end "
                f"{END_FUND_BALANCE_BY_FUND[i]} != beg {BEG_FUND_BALANCE_BY_FUND[i]} "
                f"+ change {change}"
            )
    if sum(END_FUND_BALANCE_BY_FUND) != END_FUND_BALANCE_TOTAL:
        raise SystemExit(
            f"Reconciliation failed for proposed ending fund balance: "
            f"{sum(END_FUND_BALANCE_BY_FUND)} != stated total {END_FUND_BALANCE_TOTAL}"
        )

    # --- Per-fund revenue + ending fund balance (dimension='proposed_fund'). ---
    for fund, amount in zip(FUNDS, REVENUE_BY_FUND, strict=True):
        items.append(
            BudgetLineItem(
                fiscal_year=FISCAL_YEAR,
                category="revenue",
                amount=Decimal(amount),
                document_id=DOCUMENT_ID,
                dimension="proposed_fund",
                fund=fund,
                subcategory="Total revenue",
                basis=BASIS,
                chunk_id=CHUNK_VIEW1,
                source_quote=REVENUE_BY_FUND_QUOTE,
                note=f"{SOURCE_PAGE_NOTE} #1 (by Fund by Object), total revenue by fund",
            )
        )
    for fund, amount in zip(FUNDS, END_FUND_BALANCE_BY_FUND, strict=True):
        items.append(
            BudgetLineItem(
                fiscal_year=FISCAL_YEAR,
                category="fund_balance",
                amount=Decimal(amount),
                document_id=DOCUMENT_ID,
                dimension="proposed_fund",
                fund=fund,
                subcategory="End Fund Bal-June 30, 2025",
                basis=BASIS,
                chunk_id=CHUNK_VIEW1,
                source_quote=END_FUND_BALANCE_QUOTE,
                note=f"{SOURCE_PAGE_NOTE} #1 (by Fund by Object), ending fund balance",
            )
        )

    # --- Revenue by source (dimension='proposed_source'). ---
    for subcat, (cells, _stated, quote) in SOURCES.items():
        for fund, amount in zip(FUNDS, cells, strict=True):
            if amount == 0:
                continue
            items.append(
                BudgetLineItem(
                    fiscal_year=FISCAL_YEAR,
                    category="revenue",
                    amount=Decimal(amount),
                    document_id=DOCUMENT_ID,
                    dimension="proposed_source",
                    fund=fund,
                    subcategory=subcat,
                    basis=BASIS,
                    chunk_id=CHUNK_VIEW1,
                    source_quote=quote,
                    note=f"{SOURCE_PAGE_NOTE} #1 (by Fund by Object), revenue by source",
                )
            )

    # --- Expenditure by object (dimension='proposed_object', NEW dimension). ---
    for subcat, (cells, _stated, quote) in OBJECT.items():
        for fund, amount in zip(FUNDS, cells, strict=True):
            if amount == 0:
                continue
            items.append(
                BudgetLineItem(
                    fiscal_year=FISCAL_YEAR,
                    category="expenditure",
                    amount=Decimal(amount),
                    document_id=DOCUMENT_ID,
                    dimension="proposed_object",
                    fund=fund,
                    subcategory=subcat,
                    basis=BASIS,
                    chunk_id=CHUNK_VIEW1,
                    source_quote=quote,
                    note=f"{SOURCE_PAGE_NOTE} #1 (by Fund by Object), expenditure by object",
                )
            )

    # --- Expenditure by function (dimension='proposed_function'). ---
    for subcat, (cells, _stated, quote) in FUNCTION.items():
        for fund, amount in zip(FUNDS, cells, strict=True):
            if amount == 0:
                continue
            items.append(
                BudgetLineItem(
                    fiscal_year=FISCAL_YEAR,
                    category="expenditure",
                    amount=Decimal(amount),
                    document_id=DOCUMENT_ID,
                    dimension="proposed_function",
                    fund=fund,
                    subcategory=subcat,
                    basis=BASIS,
                    chunk_id=CHUNK_VIEW2,
                    source_quote=quote,
                    note=f"{SOURCE_PAGE_NOTE} #2 (by Fund by Function), expenditure by function",
                )
            )

    return items


def _log_summary(items: list[BudgetLineItem]) -> None:
    """Log a per-dimension row count and the reconciled totals."""
    by_dim: dict[str, int] = {}
    for it in items:
        by_dim[it.dimension] = by_dim.get(it.dimension, 0) + 1
    logger.info(
        "Built %d proposed-budget rows (FY%s, doc #%d):", len(items), FISCAL_YEAR, DOCUMENT_ID
    )
    for dim in sorted(by_dim):
        logger.info("  dimension=%s: %d rows", dim, by_dim[dim])
    logger.info(
        "  reconciled totals: revenue %s, expenditures %s, ending fund balance %s",
        f"{REVENUE_TOTAL:,}",
        f"{EXPENDITURE_TOTAL:,}",
        f"{END_FUND_BALANCE_TOTAL:,}",
    )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--apply",
        action="store_true",
        help="write to the database (default is dry-run: build + reconcile only)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="explicit no-op flag; dry-run is already the default without --apply",
    )
    args = parser.parse_args()

    items = build_line_items()
    _log_summary(items)

    if not args.apply:
        logger.info("Dry run (no --apply): nothing written. Reconciliation PASSED.")
        return 0

    cfg = load_config()
    # Writer: service key bypasses RLS.
    if not cfg.supabase_service_key:
        raise SystemExit("ACTALUX_SUPABASE_SERVICE_KEY is required to --apply.")
    client = get_client(cfg.supabase_url, cfg.supabase_service_key)

    # Idempotent on this loader's OWN subset only: delete exactly the rows this
    # loader owns (document 262, basis='proposed', and one of this loader's
    # namespaced dimensions). Never the audit-actual rows; never some other
    # future proposed slice of doc 262 produced by a different loader.
    deleted = (
        client.table("budget_line_items")
        .delete()
        .eq("document_id", DOCUMENT_ID)
        .eq("basis", BASIS)
        .in_("dimension", list(PROPOSED_DIMENSIONS))
        .execute()
    )
    logger.info(
        "Deleted %d existing proposed rows (document_id=%d, basis=%s, dimensions=%s).",
        len(deleted.data),
        DOCUMENT_ID,
        BASIS,
        ",".join(PROPOSED_DIMENSIONS),
    )

    ids = insert_budget_line_items(client, items)
    logger.info("Inserted %d proposed budget line items.", len(ids))
    return 0


if __name__ == "__main__":
    sys.exit(main())
