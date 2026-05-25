"""Load verified budget line items into the budget_line_items table.

Source of truth: the figures below are read verbatim from the School
District of Clayton audited financial statements -- specifically the
"Statement of Revenues, Expenditures and Changes in Fund Balances -
Governmental Funds" in each fiscal year's audit. Each figure carries the
document_id and chunk_id of the passage it came from and the verbatim
source row, so every number on the Budget page drills down to its source.

Funds, in column order, follow the audit's presentation: General, Special
Revenue (the Teachers' Fund in Missouri), Debt Service, Capital Projects.

Integrity guard: each year's four per-fund figures are asserted to sum to
the audit's stated "Total Governmental Funds" column at load time, so a
transcription error in this file fails loudly rather than publishing a
wrong figure.

Idempotent: replaces all rows in budget_line_items on each run.

Run:
  doppler run --project mac --config dev -- uv run python scripts/load_budget.py --dry-run
  doppler run --project mac --config dev -- uv run python scripts/load_budget.py
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

# Audit fund columns, in the order the figure lists below use.
FUNDS = ("General", "Special Revenue (Teachers)", "Debt Service", "Capital Projects")

# Subcategory label per category.
SUBCATEGORY = {
    "revenue": "Total revenues",
    "expenditure": "Total expenditures",
    "fund_balance": "Ending fund balance",
}

# Each entry: one fiscal year's Statement of Revenues, Expenditures and
# Changes in Fund Balances - Governmental Funds. Per-fund lists align with
# FUNDS; *_total is the audit's stated "Total Governmental Funds" column,
# used only as a load-time checksum (not stored).
YEARS: list[dict] = [
    {
        "fiscal_year": "2018-2019",
        "document_id": 429,
        "chunk_id": 7690,
        "page": 25,
        "revenue": [17973694, 29188076, 8600250, 1762832],
        "revenue_total": 57524852,
        "revenue_quote": "Total revenues 17,973,694 29,188,076 8,600,250 1,762,832 57,524,852",
        "expenditure": [18687343, 32177071, 28081872, 1485003],
        "expenditure_total": 80431289,
        "expenditure_quote": (
            "Total expenditures 18,687,343 32,177,071 28,081,872 1,485,003 80,431,289"
        ),
        "fund_balance": [4910517, 7414629, 4986479, 10803558],
        "fund_balance_total": 28115183,
        "fund_balance_quote": (
            "Fund balances at June 30, 2019 $ 4,910,517 $ 7,414,629 "
            "$ 4,986,479 $ 10,803,558 $ 28,115,183"
        ),
    },
    {
        "fiscal_year": "2019-2020",
        "document_id": 428,
        "chunk_id": 7588,
        "page": 25,
        "revenue": [22575075, 39127651, 9317594, 2754682],
        "revenue_total": 73775002,
        "revenue_quote": "Total revenues 22,575,075 39,127,651 9,317,594 2,754,682 73,775,002",
        "expenditure": [17968247, 32669282, 8288792, 9120187],
        "expenditure_total": 68046508,
        "expenditure_quote": (
            "Total expenditures 17,968,247 32,669,282 8,288,792 9,120,187 68,046,508"
        ),
        "fund_balance": [9426902, 13875528, 6357143, 4416641],
        "fund_balance_total": 34076214,
        "fund_balance_quote": (
            "Fund balances at June 30, 2020 $ 9,426,902 $ 13,875,528 "
            "$ 6,357,143 $ 4,416,641 $ 34,076,214"
        ),
    },
    {
        "fiscal_year": "2020-2021",
        "document_id": 427,
        "chunk_id": 7479,
        "page": 25,
        "revenue": [20416619, 33566340, 8030565, 4088629],
        "revenue_total": 66102153,
        "revenue_quote": "Total revenues 20,416,619 33,566,340 8,030,565 4,088,629 66,102,153",
        "expenditure": [17614862, 33856130, 8973208, 5246785],
        "expenditure_total": 65690985,
        "expenditure_quote": (
            "Total expenditures 17,614,862 33,856,130 8,973,208 5,246,785 65,690,985"
        ),
        "fund_balance": [12058750, 13589732, 5414500, 3271255],
        "fund_balance_total": 34334237,
        "fund_balance_quote": (
            "Fund balances at June 30, 2021 $ 12,058,750 $ 13,589,732 "
            "$ 5,414,500 $ 3,271,255 $ 34,334,237"
        ),
    },
    {
        "fiscal_year": "2021-2022",
        "document_id": 426,
        "chunk_id": 7371,
        "page": 25,
        "revenue": [23023796, 37992214, 8961817, 4374905],
        "revenue_total": 74352732,
        "revenue_quote": "Total revenues 23,023,796 37,992,214 8,961,817 4,374,905 74,352,732",
        "expenditure": [18778296, 34551786, 8541502, 4684117],
        "expenditure_total": 66555701,
        "expenditure_quote": (
            "Total expenditures 18,778,296 34,551,786 8,541,502 4,684,117 66,555,701"
        ),
        "fund_balance": [16477856, 17003154, 5834815, 2973198],
        "fund_balance_total": 42289023,
        "fund_balance_quote": (
            "Fund balances at June 30, 2022 $ 16,477,856 $ 17,003,154 "
            "$ 5,834,815 $ 2,973,198 $ 42,289,023"
        ),
    },
    {
        "fiscal_year": "2022-2023",
        "document_id": 425,
        "chunk_id": 7260,
        "page": 23,
        "revenue": [22970478, 36609570, 8993698, 6196088],
        "revenue_total": 74769834,
        "revenue_quote": "Total revenues 22,970,478 36,609,570 8,993,698 6,196,088 74,769,834",
        "expenditure": [20384320, 35481138, 6515483, 7015568],
        "expenditure_total": 69396509,
        "expenditure_quote": (
            "Total expenditures 20,384,320 35,481,138 6,515,483 7,015,568 69,396,509"
        ),
        "fund_balance": [16819311, 18130984, 8313030, 2210433],
        "fund_balance_total": 45473758,
        "fund_balance_quote": (
            "Fund balances at June 30, 2023 $ 16,819,311 $ 18,130,984 "
            "$ 8,313,030 $ 2,210,433 $ 45,473,758"
        ),
    },
    {
        "fiscal_year": "2023-2024",
        "document_id": 424,
        "chunk_id": 7154,
        "page": 23,
        "revenue": [30176652, 32191982, 7965222, 4986065],
        "revenue_total": 75319921,
        "revenue_quote": "Total revenues 30,176,652 32,191,982 7,965,222 4,986,065 75,319,921",
        "expenditure": [21928620, 38257600, 10822473, 4046059],
        "expenditure_total": 75054752,
        "expenditure_quote": (
            "Total expenditures 21,928,620 38,257,600 10,822,473 4,046,059 75,054,752"
        ),
        "fund_balance": [25072619, 12065366, 5455779, 3145163],
        "fund_balance_total": 45738927,
        "fund_balance_quote": (
            "Fund balances at June 30, 2024 $ 25,072,619 $ 12,065,366 "
            "$ 5,455,779 $ 3,145,163 $ 45,738,927"
        ),
    },
    {
        "fiscal_year": "2024-2025",
        "document_id": 436,
        "chunk_id": 7802,
        "page": 24,
        "revenue": [31609692, 32730947, 8031542, 5322802],
        "revenue_total": 77694983,
        "revenue_quote": "Total revenues 31,609,692 32,730,947 8,031,542 5,322,802 77,694,983",
        "expenditure": [22639043, 39850590, 7389008, 5632406],
        "expenditure_total": 75511047,
        "expenditure_quote": (
            "Total expenditures 22,639,043 39,850,590 7,389,008 5,632,406 75,511,047"
        ),
        "fund_balance": [34032713, 4945723, 6098313, 2846114],
        "fund_balance_total": 47922863,
        "fund_balance_quote": (
            "Fund balances at June 30, 2025 $ 34,032,713 $ 4,945,723 "
            "$ 6,098,313 $ 2,846,114 $ 47,922,863"
        ),
    },
]


# Revenue by source (Total Governmental Funds column), dimension='source'.
# (amount, verbatim source row) per source; per-year sum is asserted to equal
# that year's revenue_total. Quotes are the verbatim "Revenues" rows.
SOURCE_LABELS = ("Local", "County", "State", "Federal", "Other")
SOURCES: dict[str, list[tuple[int, str]]] = {
    "2018-2019": [
        (53169996, "Local $ 17,294,981 $ 26,911,952 $ 7,312,664 $ 1,650,399 $ 53,169,996"),
        (525825, "County 112,759 242,944 151,287 18,835 525,825"),
        (1742804, "State 244,496 1,498,308 1,742,804"),
        (1635582, "Federal 316,041 183,242 1,136,299 1,635,582"),
        (450645, "Other 5,417 351,630 93,598 450,645"),
    ],
    "2019-2020": [
        (70058342, "Local $ 21,799,840 $ 37,182,930 $ 8,349,892 $ 2,725,680 $ 70,058,342"),
        (610998, "County 123,380 281,775 182,266 23,577 610,998"),
        (1716291, "State 308,641 1,407,650 - - 1,716,291"),
        (1237578, "Federal 329,360 122,782 785,436 - 1,237,578"),
        (151793, "Other 13,854 132,514 - 5,425 151,793"),
    ],
    "2020-2021": [
        (61835038, "Local $ 19,160,260 $ 31,310,825 $ 7,644,096 $ 3,719,857 $ 61,835,038"),
        (613816, "County 126,706 258,168 183,033 45,909 613,816"),
        (1710141, "State 251,491 1,457,571 - 1,079 1,710,141"),
        (1826199, "Federal 878,266 464,325 203,436 280,172 1,826,199"),
        (116959, "Other (104) 75,451 - 41,612 116,959"),
    ],
    "2021-2022": [
        (69305093, "Local $ 20,708,617 $ 35,774,596 $ 8,603,404 $ 4,218,476 $ 69,305,093"),
        (633758, "County 117,824 248,983 181,784 85,167 633,758"),
        (2198582, "State 360,218 1,838,364 - - 2,198,582"),
        (2144249, "Federal 1,834,041 127,434 176,629 6,145 2,144,249"),
        (71050, "Other 3,096 2,837 - 65,117 71,050"),
    ],
    "2022-2023": [
        (70171316, "Local $ 21,409,679 $ 34,073,686 $ 8,635,557 $ 6,052,394 $ 70,171,316"),
        (708616, "County 116,542 292,308 181,809 117,957 708,616"),
        (2363931, "State 426,308 1,937,623 - - 2,363,931"),
        (1468009, "Federal 985,724 305,953 176,332 - 1,468,009"),
        (57962, "Other 32,225 - - 25,737 57,962"),
    ],
    "2023-2024": [
        (71803874, "Local $ 28,982,530 $ 30,296,836 $ 7,642,552 $ 4,881,956 $ 71,803,874"),
        (597991, "County 154,028 227,855 144,959 71,149 597,991"),
        (1907709, "State 355,405 1,552,304 - - 1,907,709"),
        (958434, "Federal 665,736 114,987 177,711 - 958,434"),
        (51913, "Other 18,953 - - 32,960 51,913"),
    ],
    "2024-2025": [
        (74513876, "Local $ 30,682,463 $ 30,903,035 $ 7,705,321 $ 5,223,057 $ 74,513,876"),
        (556534, "County 152,039 201,794 147,970 54,731 556,534"),
        (1826712, "State 338,091 1,488,621 - - 1,826,712"),
        (738247, "Federal 422,499 137,497 178,251 - 738,247"),
        (59614, "Other 14,600 - - 45,014 59,614"),
    ],
}


def build_line_items() -> list[BudgetLineItem]:
    """Expand the verified figures into rows, asserting each year reconciles."""
    items: list[BudgetLineItem] = []
    years_by_fy = {y["fiscal_year"]: y for y in YEARS}

    for y in YEARS:
        for category in ("revenue", "expenditure", "fund_balance"):
            amounts = y[category]
            stated_total = y[f"{category}_total"]
            actual_total = sum(amounts)
            if actual_total != stated_total:
                raise SystemExit(
                    f"Reconciliation failed for {y['fiscal_year']} {category}: "
                    f"per-fund sum {actual_total} != stated total {stated_total}"
                )
            for fund, amount in zip(FUNDS, amounts, strict=True):
                items.append(
                    BudgetLineItem(
                        fiscal_year=y["fiscal_year"],
                        category=category,
                        amount=Decimal(amount),
                        document_id=y["document_id"],
                        fund=fund,
                        subcategory=SUBCATEGORY[category],
                        chunk_id=y["chunk_id"],
                        source_quote=y[f"{category}_quote"],
                        note=(
                            f"FY{y['fiscal_year']} audit, Statement of Revenues, "
                            f"Expenditures and Changes in Fund Balances - Governmental "
                            f"Funds, p. {y['page']}"
                        ),
                    )
                )

    # Revenue by source (dimension='source'): must sum to the year's total revenues.
    for fiscal_year, rows in SOURCES.items():
        y = years_by_fy[fiscal_year]
        actual_total = sum(amount for amount, _ in rows)
        if actual_total != y["revenue_total"]:
            raise SystemExit(
                f"Reconciliation failed for {fiscal_year} revenue-by-source: "
                f"sum {actual_total} != stated total revenues {y['revenue_total']}"
            )
        for label, (amount, quote) in zip(SOURCE_LABELS, rows, strict=True):
            items.append(
                BudgetLineItem(
                    fiscal_year=fiscal_year,
                    category="revenue",
                    amount=Decimal(amount),
                    document_id=y["document_id"],
                    dimension="source",
                    subcategory=label,
                    chunk_id=y["chunk_id"],
                    source_quote=quote,
                    note=(
                        f"FY{fiscal_year} audit, Statement of Revenues, Expenditures "
                        f"and Changes in Fund Balances - Governmental Funds, p. {y['page']}, "
                        f"revenue by source"
                    ),
                )
            )
    return items


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--dry-run", action="store_true", help="build and reconcile, but do not write"
    )
    args = parser.parse_args()

    items = build_line_items()
    logger.info(
        "Built %d line items across %d fiscal years (all reconciled).", len(items), len(YEARS)
    )
    for y in YEARS:
        logger.info(
            "  %s: revenue %s, expenditure %s, ending balance %s",
            y["fiscal_year"],
            f"{y['revenue_total']:,}",
            f"{y['expenditure_total']:,}",
            f"{y['fund_balance_total']:,}",
        )

    if args.dry_run:
        logger.info("--dry-run: nothing written.")
        return 0

    cfg = load_config()
    client = get_client(cfg.supabase_url, cfg.supabase_key)

    # Replace: this file is the single source of truth for budget_line_items.
    deleted = client.table("budget_line_items").delete().gte("id", 0).execute()
    logger.info("Deleted %d existing rows.", len(deleted.data))

    ids = insert_budget_line_items(client, items)
    logger.info("Inserted %d budget line items.", len(ids))
    return 0


if __name__ == "__main__":
    sys.exit(main())
