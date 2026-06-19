"""Parse a Missouri DESE Local Effort report XML.

Local Effort is the district's local tax revenue used in the state foundation
formula. The report is a single ``Tablix1`` with one ``Details`` row per tax
line (``Textbox16`` description, ``TOTAL`` amount), a grand total of those lines
at ``Tablix1/@Textbox15``, and two report-level context metrics: Average Daily
Attendance (``Textbox3``) and total taxes per ADA (``Textbox12``).

The tax-line amounts are the structured figures Actalux stores (they reconcile:
the lines sum to the grand total). The ADA count and the per-ADA rate are
rendered verbatim in the cited document for context but not stored (they are not
dollar amounts and not sum-able), mirroring the Per-Pupil treatment of rates.

The fiscal year is not embedded in the XML; the caller supplies it and verifies
it against the paired PDF.
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path
from xml.etree import ElementTree as ET

from actalux.errors import ParseError

NS = "Local_Effort"

# The line amounts are filed to the cent and sum to the stated grand total
# exactly; a $0.01 tolerance absorbs any source rounding while still catching a
# real parse/mapping error (which is dollars, not cents).
_TOL = Decimal("0.01")


def _q(tag: str) -> str:
    return f"{{{NS}}}{tag}"


def parse_money(raw: str | None) -> Decimal:
    """Parse a Local Effort money/number cell ($-formatted or bare) into a Decimal."""
    if raw is None or raw.strip() in ("", "-"):
        return Decimal(0)
    try:
        return Decimal(raw.replace("$", "").replace(",", "").strip())
    except (ArithmeticError, ValueError) as exc:
        raise ParseError(f"Unparseable Local Effort cell {raw!r}: {exc}") from exc


@dataclass(frozen=True)
class LocalEffortLine:
    """One local-tax revenue line (e.g. 'Part II, Line 5111 Current Taxes')."""

    description: str
    amount: Decimal


@dataclass(frozen=True)
class LocalEffortReport:
    """A reconciled Local Effort report for one fiscal year (year supplied by caller)."""

    fiscal_year: str
    lines: tuple[LocalEffortLine, ...]
    grand_total: Decimal
    ada: Decimal
    per_ada: Decimal
    ada_label: str
    per_ada_label: str

    @property
    def lines_total(self) -> Decimal:
        return sum((line.amount for line in self.lines), Decimal(0))


def _clean(raw: str | None) -> str:
    """Collapse a label cell's CR/LF + surrounding whitespace into one clean line."""
    return " ".join((raw or "").split())


def _parse_lines(root: ET.Element) -> list[LocalEffortLine]:
    lines: list[LocalEffortLine] = []
    for d in root.findall(f".//{_q('Details')}"):
        description = _clean(d.get("Textbox16"))
        if not description:  # a Details row with no description is not a tax line
            continue
        lines.append(LocalEffortLine(description=description, amount=parse_money(d.get("TOTAL"))))
    return lines


def reconcile(report: LocalEffortReport) -> None:
    """Assert the tax lines sum to the stated grand total; raise ParseError otherwise."""
    fy = report.fiscal_year
    if not report.lines:
        raise ParseError(f"FY{fy}: Local Effort report has no tax lines")
    if abs(report.lines_total - report.grand_total) > _TOL:
        raise ParseError(
            f"FY{fy}: Local Effort lines sum {report.lines_total} != "
            f"stated grand total {report.grand_total}"
        )


def parse_local_effort(path: Path, fiscal_year: str) -> LocalEffortReport:
    """Parse + reconcile one Local Effort XML. ``fiscal_year`` is supplied by the caller."""
    try:
        root = ET.parse(path).getroot()
    except ET.ParseError as exc:
        raise ParseError(f"Invalid Local Effort XML {path.name}: {exc}") from exc

    tablix = next(iter(root.findall(f".//{_q('Tablix1')}")), None)
    if tablix is None:
        raise ParseError(f"FY{fiscal_year}: Local Effort Tablix1 not found")

    report = LocalEffortReport(
        fiscal_year=fiscal_year,
        lines=tuple(_parse_lines(root)),
        grand_total=parse_money(tablix.get("Textbox15")),
        ada=parse_money(root.get("Textbox3")),
        per_ada=parse_money(root.get("Textbox12")),
        ada_label=_clean(root.get("Textbox2")) or "Resident I ADA plus Resident II ADA",
        per_ada_label=_clean(root.get("Textbox8")) or "Total taxes per ADA",
    )
    reconcile(report)
    return report


def _fmt(amount: Decimal) -> str:
    return f"${amount:,.2f}"


def line_md_row(line: LocalEffortLine) -> str:
    """The tax-line markdown row a structured figure cites."""
    return f"| {line.description} | {_fmt(line.amount)} |"


def render_markdown(report: LocalEffortReport) -> str:
    """Render a clean, citable markdown document for one Local Effort report."""
    fy = report.fiscal_year
    lines: list[str] = [
        f"# Local Effort — Fiscal Year {fy}",
        "",
        "School District of Clayton (096-102) · Missouri Department of Elementary "
        "and Secondary Education.",
        "",
        "Local effort is the district's local tax revenue counted in the state foundation formula.",
        "",
        "## Local Tax Revenue (All Funds)",
        "",
        "| Source | Amount |",
        "|---|---|",
    ]
    lines += [line_md_row(line) for line in report.lines]
    lines.append(f"| Total Local Effort (taxes) | {_fmt(report.grand_total)} |")

    lines += [
        "",
        "## Local Effort per Average Daily Attendance",
        "",
        "| Metric | Value |",
        "|---|---|",
        f"| {report.ada_label} | {report.ada:,.4f} |",
        f"| {report.per_ada_label} | {_fmt(report.per_ada)} |",
        "",
    ]
    return "\n".join(lines)
