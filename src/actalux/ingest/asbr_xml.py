"""Parse a Missouri DESE ASBR Full Report XML into a reconciled finance summary.

The Annual Secretary of the Board Report (ASBR) is the authoritative annual
finance report each Missouri district files with DESE. Its SSRS XML export
carries several tables ("tablixes"); this module reads the three that form a
fully self-reconciling picture and ignores the detail tables whose rows embed
subtotals (Tablix21 function detail, Tablix6 source detail) and therefore do
not foot by naive summation.

The element/attribute mappings below were verified against the printed PDF for
FY2013-2014 and FY2023-2024:

- ``Tablix23`` "Part III-B Expenditures Grand Total" (row ``SUPPORT_SERVICE_CODE
  =9999``) — expenditures by OBJECT. Its seven numeric columns are object-code
  columns (NOT funds, despite SSRS attribute names like ``GENERAL_FUND8``); they
  sum to the grand total in ``Textbox302``.
- ``Tablix3`` "Part I Summary" (``Details2`` rows keyed by ``REVENUE_CODE``) —
  the fund-level statement: beginning balance (3111), total revenue (5899),
  transfers (5510/6710), expenditures (9999), ending balance (3112), by fund.
- ``Tablix9`` ``Details8`` (``REVENUE_CODE2=5899``) — total revenue, used only
  to cross-check the Part I revenue row.

Every figure traces to the official XML; ``reconcile`` asserts the cross-tablix
identities (object sum == fund-expenditure sum == grand total; per-fund
roll-forward End = Beg + Revenue - Expenditure + TransferTo - TransferFrom)
before any caller consumes the data, so a parse error fails loudly rather than
publishing a wrong figure.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path
from xml.etree import ElementTree as ET

from actalux.errors import ParseError

NS = "ASBR_x0020_Full_x0020_Report"

# Tablix23 grand-total row: (xml attribute, object code, canonical label), in the
# object-code order the report prints. Verified against the PDF "Part III-B
# Expenditures Grand Total" table for FY2013-14 and FY2023-24.
OBJECT_COLUMNS: tuple[tuple[str, str, str], ...] = (
    ("GENERAL_FUND8", "6110", "Certificated Salaries"),
    ("SPECIAL_REVENUE8", "6150", "Non-Certificated Salaries"),
    ("DEBT_SERVICE6", "6200", "Employee Benefits"),
    ("CAPITAL_PROJECTS8", "6300", "Purchased Services"),
    ("TOTAL_UNADJ_RATE7", "6400", "Supplies"),
    ("Textbox339", "6500", "Capital Outlay"),
    ("Textbox301", "6600", "Other Objects"),
)
GRAND_TOTAL_ATTR = "Textbox302"

# Tablix3 Part I fund columns: (xml attribute, canonical fund label). Textbox393
# is the Debt Service column (confirmed by the per-fund roll-forward). Labels
# match load_budget.py / load_adopted_budget.py so the page speaks one fund
# vocabulary.
FUND_COLUMNS: tuple[tuple[str, str], ...] = (
    ("GENERAL_FUND", "General"),
    ("SPECIAL_REVENUE", "Special Revenue (Teachers)"),
    ("Textbox393", "Debt Service"),
    ("CAPITAL_PROJECTS", "Capital Projects"),
)
FUND_TOTAL_ATTR = "TOTAL"

# Tablix3 Part I summary row codes (Details2 @REVENUE_CODE).
ROW_BEGINNING = "3111"
ROW_REVENUE = "5899"
ROW_TRANSFER_TO = "5510"
ROW_TRANSFER_FROM = "6710"
ROW_EXPENDITURE = "9999"
ROW_ENDING = "3112"
ROW_REVENUE_AND_BALANCES = "5999"

# Tablix9 Details8 (REVENUE_CODE2=5899) "Total Revenues" — an independent revenue
# statement used only to cross-check the Part I (Tablix3) revenue row, so a bad
# Tablix3 revenue can't pass merely because the roll-forward balances with the
# same bad inputs. (attribute, canonical fund label); total in TOTAL_UNADJ_RATE2.
T9_FUND_COLUMNS: tuple[tuple[str, str], ...] = (
    ("GENERAL_FUND2", "General"),
    ("SPECIAL_REVENUE2", "Special Revenue (Teachers)"),
    ("DEBT_SERVICE2", "Debt Service"),
    ("CAPITAL_PROJECTS2", "Capital Projects"),
)
T9_TOTAL_ATTR = "TOTAL_UNADJ_RATE2"

_FISCAL_YEAR_RE = re.compile(r"Fiscal Year\s+(\d{4}-\d{4})")


def _q(tag: str) -> str:
    return f"{{{NS}}}{tag}"


def parse_money(raw: str | None) -> Decimal:
    """Parse an ASBR money cell into an exact Decimal.

    Handles the four cell formats the report uses: ``$1,234,567.89`` (Tablix21/6),
    parenthetical negatives ``($362,049.22)``, bare decimals ``47555384.08``
    (Tablix3/23), and a dash ``-`` (or ``$-``) meaning zero/not-applicable.
    """
    if raw is None:
        return Decimal(0)
    text = raw.strip()
    if text in ("", "-", "$-"):
        return Decimal(0)
    negative = text.startswith("(") and text.endswith(")")
    text = text.strip("()").replace("$", "").replace(",", "").strip()
    if text in ("", "-"):
        return Decimal(0)
    try:
        value = Decimal(text)
    except (ArithmeticError, ValueError) as exc:
        raise ParseError(f"Unparseable ASBR money cell {raw!r}: {exc}") from exc
    return -value if negative else value


@dataclass(frozen=True)
class ObjectExpenditure:
    """One object-level expenditure total (all funds) from the grand-total row."""

    code: str  # e.g. "6110"
    label: str  # e.g. "Certificated Salaries"
    amount: Decimal


@dataclass(frozen=True)
class FundSummaryRow:
    """One Part I summary line broken out by fund, with the all-funds total."""

    code: str  # e.g. "3111"
    label: str  # verbatim Part I label, e.g. "Beginning Fund Balances"
    by_fund: dict[str, Decimal]  # canonical fund label -> amount
    total: Decimal


@dataclass(frozen=True)
class AsbrReport:
    """A reconciled ASBR finance summary for one fiscal year."""

    fiscal_year: str  # "2023-2024"
    objects: tuple[ObjectExpenditure, ...]
    expenditure_grand_total: Decimal
    summary: dict[str, FundSummaryRow]  # REVENUE_CODE -> row

    @property
    def revenue_total(self) -> Decimal:
        return self.summary[ROW_REVENUE].total

    @property
    def expenditure_total(self) -> Decimal:
        return self.summary[ROW_EXPENDITURE].total

    @property
    def beginning_balance_total(self) -> Decimal:
        return self.summary[ROW_BEGINNING].total

    @property
    def ending_balance_total(self) -> Decimal:
        return self.summary[ROW_ENDING].total


def _fiscal_year(root: ET.Element) -> str:
    el = root.find(f".//{_q('Tablix1')}")
    raw = el.get("Textbox36") if el is not None else None
    match = _FISCAL_YEAR_RE.search(raw or "")
    if not match:
        raise ParseError(f"Could not find fiscal year in Tablix1 (got {raw!r})")
    return match.group(1)


def _parse_objects(root: ET.Element) -> tuple[tuple[ObjectExpenditure, ...], Decimal]:
    row = next(
        (
            e
            for e in root.findall(f".//{_q('Tablix23')}//{_q('Details21')}")
            if e.get("SUPPORT_SERVICE_CODE") == "9999"
        ),
        None,
    )
    if row is None:
        raise ParseError("Tablix23 grand-total row (SUPPORT_SERVICE_CODE=9999) not found")
    objects = tuple(
        ObjectExpenditure(code=code, label=label, amount=parse_money(row.get(attr)))
        for attr, code, label in OBJECT_COLUMNS
    )
    return objects, parse_money(row.get(GRAND_TOTAL_ATTR))


def _parse_summary(root: ET.Element) -> dict[str, FundSummaryRow]:
    summary: dict[str, FundSummaryRow] = {}
    for el in root.findall(f".//{_q('Tablix3')}//{_q('Details2')}"):
        code = el.get("REVENUE_CODE")
        if code is None:
            continue
        by_fund = {label: parse_money(el.get(attr)) for attr, label in FUND_COLUMNS}
        summary[code] = FundSummaryRow(
            code=code,
            label=(el.get("Textbox392") or "").strip(),
            by_fund=by_fund,
            total=parse_money(el.get(FUND_TOTAL_ATTR)),
        )
    return summary


def reconcile(report: AsbrReport) -> None:
    """Assert every cross-tablix identity; raise ParseError on the first mismatch.

    These are exact (to the cent) — the ASBR is a balanced statutory filing, so a
    mismatch means a parse/mapping error, not source rounding.
    """
    fund_labels = [label for _, label in FUND_COLUMNS]

    # 1. Object columns sum to the grand total.
    obj_sum = sum((o.amount for o in report.objects), Decimal(0))
    if obj_sum != report.expenditure_grand_total:
        raise ParseError(
            f"FY{report.fiscal_year}: object columns sum {obj_sum} != grand total "
            f"{report.expenditure_grand_total}"
        )

    # 2. Object grand total (Tablix23) == fund expenditure total (Tablix3 9999).
    if report.expenditure_grand_total != report.expenditure_total:
        raise ParseError(
            f"FY{report.fiscal_year}: Tablix23 grand total {report.expenditure_grand_total} "
            f"!= Tablix3 expenditure total {report.expenditure_total}"
        )

    # 3. Each Part I row cross-foots (per-fund cells sum to its stated total).
    for code in (ROW_BEGINNING, ROW_REVENUE, ROW_EXPENDITURE, ROW_ENDING):
        rowsum = sum((report.summary[code].by_fund[f] for f in fund_labels), Decimal(0))
        if rowsum != report.summary[code].total:
            raise ParseError(
                f"FY{report.fiscal_year}: Part I row {code} fund cells sum {rowsum} "
                f"!= stated total {report.summary[code].total}"
            )

    # 4. Per-fund (and total) roll-forward:
    #    End = Beginning + Revenue - Expenditure + TransferTo - TransferFrom.
    # The transfer sign convention is the report's, confirmed per fund: "5510
    # Transfer To" is an inflow INTO the fund (+), "6710 Transfer From" is an
    # outflow OUT of the fund (-). The all-funds total nets these to zero, so the
    # total reconciles either way; only the per-fund check pins the sign.
    def _cell(code: str, fund: str) -> Decimal:
        row = report.summary.get(code)
        return row.by_fund[fund] if row else Decimal(0)

    for fund in [*fund_labels, "__total__"]:
        if fund == "__total__":
            beg, rev, exp = (
                report.beginning_balance_total,
                report.revenue_total,
                report.expenditure_total,
            )
            tin = report.summary.get(ROW_TRANSFER_TO)
            tout = report.summary.get(ROW_TRANSFER_FROM)
            t_in = tin.total if tin else Decimal(0)
            t_out = tout.total if tout else Decimal(0)
            end = report.ending_balance_total
        else:
            beg, rev, exp = (
                _cell(ROW_BEGINNING, fund),
                _cell(ROW_REVENUE, fund),
                _cell(ROW_EXPENDITURE, fund),
            )
            t_in, t_out = _cell(ROW_TRANSFER_TO, fund), _cell(ROW_TRANSFER_FROM, fund)
            end = _cell(ROW_ENDING, fund)
        expected = beg + rev - exp + t_in - t_out
        if expected != end:
            raise ParseError(
                f"FY{report.fiscal_year}: roll-forward fails for {fund}: Beg {beg} + Rev {rev} "
                f"- Exp {exp} + TransferTo {t_in} - TransferFrom {t_out} = {expected} != End {end}"
            )

    # 5. If present, Total Revenue And Balances (5999) == Beginning + Revenue.
    rev_bal = report.summary.get(ROW_REVENUE_AND_BALANCES)
    if rev_bal is not None:
        for fund in [*fund_labels, "__total__"]:
            if fund == "__total__":
                got = rev_bal.total
                expected = report.beginning_balance_total + report.revenue_total
            else:
                got = rev_bal.by_fund[fund]
                expected = _cell(ROW_BEGINNING, fund) + _cell(ROW_REVENUE, fund)
            if got != expected:
                raise ParseError(
                    f"FY{report.fiscal_year}: Total Revenue And Balances {fund} {got} "
                    f"!= Beginning + Revenue {expected}"
                )


def _check_revenue_crosscheck(root: ET.Element, report: AsbrReport) -> None:
    """Cross-check the Part I revenue row against the independent Tablix9 statement.

    If the Tablix9 "Total Revenues" (5899) row is present, its total and per-fund
    cells must equal the Part I (Tablix3) revenue row. Absent in an export, the
    Tablix3 row stands alone (no failure) -- it is a corroborating check, not the
    primary source.
    """
    row = next(
        (
            e
            for e in root.findall(f".//{_q('Tablix9')}//{_q('Details8')}")
            if e.get("REVENUE_CODE2") == ROW_REVENUE
        ),
        None,
    )
    if row is None:
        return
    rev = report.summary[ROW_REVENUE]
    if parse_money(row.get(T9_TOTAL_ATTR)) != rev.total:
        raise ParseError(
            f"FY{report.fiscal_year}: Tablix9 total revenue {parse_money(row.get(T9_TOTAL_ATTR))} "
            f"!= Part I revenue total {rev.total}"
        )
    for attr, fund in T9_FUND_COLUMNS:
        if parse_money(row.get(attr)) != rev.by_fund[fund]:
            raise ParseError(
                f"FY{report.fiscal_year}: Tablix9 revenue {fund} {parse_money(row.get(attr))} "
                f"!= Part I revenue {rev.by_fund[fund]}"
            )


def parse_asbr(path: Path) -> AsbrReport:
    """Parse + reconcile one ASBR Full Report XML. Raises ParseError on any issue."""
    try:
        root = ET.parse(path).getroot()
    except ET.ParseError as exc:
        raise ParseError(f"Invalid ASBR XML {path.name}: {exc}") from exc

    objects, grand_total = _parse_objects(root)
    report = AsbrReport(
        fiscal_year=_fiscal_year(root),
        objects=objects,
        expenditure_grand_total=grand_total,
        summary=_parse_summary(root),
    )
    for code in (ROW_BEGINNING, ROW_REVENUE, ROW_EXPENDITURE, ROW_ENDING):
        if code not in report.summary:
            raise ParseError(f"FY{report.fiscal_year}: Part I summary missing row {code}")
    reconcile(report)
    _check_revenue_crosscheck(root, report)
    return report


def _fmt(amount: Decimal) -> str:
    """Render a Decimal as the report does: ``$29,794,237.97`` (negatives parenthesized)."""
    negative = amount < 0
    body = f"${abs(amount):,.2f}"
    return f"({body})" if negative else body


def fund_labels() -> list[str]:
    """Canonical fund labels in report column order."""
    return [label for _, label in FUND_COLUMNS]


# Row builders are shared by render_markdown (below) and the loader, so a budget
# line item's source_quote is byte-identical to the row that lands in the chunk.
def object_md_row(obj: ObjectExpenditure) -> str:
    """The Part III-B object-expenditure markdown row, e.g.
    ``| 6110 Certificated Salaries | $29,794,237.97 |``."""
    return f"| {obj.code} {obj.label} | {_fmt(obj.amount)} |"


def summary_md_row(row: FundSummaryRow) -> str:
    """The Part I fund-summary markdown row (per-fund cells + total)."""
    cells = " | ".join(_fmt(row.by_fund[f]) for f in fund_labels())
    return f"| {row.label} | {cells} | {_fmt(row.total)} |"


def render_markdown(report: AsbrReport, *, source_url: str = "") -> str:
    """Render a reconciled report into a clean, citable markdown document.

    The output is a deterministic, no-interpretation transform of the official
    XML: each figure appears on its own table row so a budget line item can cite
    the verbatim row (the row string is what load_asbr stores as source_quote and
    is guaranteed to appear in the ingested chunk).
    """
    funds = fund_labels()
    lines: list[str] = [
        f"# Annual Secretary of the Board Report (ASBR) — Fiscal Year {report.fiscal_year}",
        "",
        "School District of Clayton (096102) · Missouri Department of Elementary "
        "and Secondary Education.",
        "",
        "## Part I — Fund Summary",
        "",
        "| Line | " + " | ".join(funds) + " | Total All Funds |",
        "|---|" + "---|" * (len(funds) + 1),
    ]
    lines += [
        summary_md_row(report.summary[c])
        for c in (ROW_BEGINNING, ROW_REVENUE, ROW_EXPENDITURE, ROW_ENDING)
    ]

    lines += [
        "",
        "## Part III-B — Expenditures by Object (All Funds)",
        "",
        "| Object | Amount |",
        "|---|---|",
    ]
    lines += [object_md_row(obj) for obj in report.objects]
    lines.append(f"| Total Expenditures | {_fmt(report.expenditure_grand_total)} |")

    if source_url:
        lines += ["", f"Source: official ASBR filed with DESE — {source_url}"]
    lines.append("")
    return "\n".join(lines)
