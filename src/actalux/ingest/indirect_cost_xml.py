"""Parse a Missouri DESE Indirect Cost Calculation report XML.

The indirect cost rate is the share of allowable indirect costs a district may
recover on grants, computed as (allowable indirect costs / eligible base) x 90%.
The report comes in three schema variants, all parsed into one shape here:

- **Prior2018** (namespace ``Indirect_Cost_CalculationPrior2018``): every value is
  a ``Textbox{N}`` attribute on the root; rates are decimal fractions; the report
  says "Non-Restricted".
- **2019+** (``Indirect_Cost_Calculation2019``) and the **2017-18 outlier**
  (``Indirect_Cost_Calculation``): identical Tablix structure; rates are percent
  strings. They differ only in the rate term ("Unrestricted" vs "Non-Restricted").

Only five dollar figures are kept (Grand Total all funds; allowable indirect
costs and eligible base, each for the restricted and unrestricted rate) plus the
two headline rates. The rate is reconciled against (allowable / base) x 90% as an
integrity guard. The figure-to-Textbox / figure-to-Tablix mapping was verified
field-by-field against the official PDFs for one file of each variant.

The fiscal year is not in the XML; the caller supplies it and verifies it against
the paired PDF.
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path
from xml.etree import ElementTree as ET

from actalux.errors import ParseError

NS_PRIOR2018 = "Indirect_Cost_CalculationPrior2018"
NS_2019 = "Indirect_Cost_Calculation2019"
NS_OUTLIER = "Indirect_Cost_Calculation"

# The rate is filed rounded to 0.01% (the Tablix variants) or to full precision
# (Prior2018). 0.0005 (= 0.05 percentage points) absorbs that rounding while still
# catching a real mis-mapping (which would be off by whole points or more).
_RATE_TOL = Decimal("0.0005")
_NINETY_PCT = Decimal("0.9")

# Prior2018 stores each figure on a root Textbox attribute (verified vs the PDF).
_PRIOR2018_TEXTBOX = {
    "grand_total": "Textbox7",  # line 1
    "allowable_unrestricted": "Textbox42",  # line 15
    "allowable_restricted": "Textbox46",  # line 16
    "base_unrestricted": "Textbox61",  # line 17
    "base_restricted": "Textbox64",  # line 18
    "rate_unrestricted": "Textbox58",
    "rate_restricted": "Textbox71",
    "rate_unrestricted_label": "Textbox56",
    "rate_restricted_label": "Textbox69",
}

# The Tablix variants put each figure at Tablix{T}/Details{D}/@{attr} (verified).
_TABLIX_PATH = {
    "grand_total": ("Tablix2", "Details1", "Total1"),  # line 1
    "allowable_unrestricted": ("Tablix16", "Details15", "Total15"),  # line 15
    "allowable_restricted": ("Tablix17", "Details12", "Total12"),  # line 16
    "base_unrestricted": ("Tablix18", "Details16", "Total16"),  # line 17
    "base_restricted": ("Tablix19", "Details17", "Total17"),  # line 18
    "rate_unrestricted": ("Tablix20", "Details18", "NonRestrictedRate"),
    "rate_restricted": ("Tablix21", "Details19", "NonRestrictedRate2"),
    "rate_unrestricted_label": ("Tablix20", "Details18", "Calculation"),
    "rate_restricted_label": ("Tablix21", "Details19", "Calculation2"),
}

# The five dollar figures Actalux stores, with the canonical DESE line label
# (term-substituted). The rates are rendered for context but not stored as
# structured figures (they are percentages, not dollar amounts).
_LINE_LABELS = {
    1: "Part I, Line 9999 Grand Total - All Funds",
    15: "Allowable Indirect Costs, {term} (Lines 10-14)",
    16: "Allowable Indirect Costs, Restricted (Lines 10, 11, and 14)",
    17: "Other Allowable Indirect Costs, {term} (Line 9 less Line 15)",
    18: "Other Allowable Indirect Costs, Restricted (Line 9 less Line 16)",
}


def parse_money(raw: str | None) -> Decimal:
    """Parse a money cell ('$75,837,799.36' or bare '59125445.29') into a Decimal."""
    if raw is None or raw.strip() in ("", "-"):
        return Decimal(0)
    try:
        return Decimal(raw.replace("$", "").replace(",", "").strip())
    except (ArithmeticError, ValueError) as exc:
        raise ParseError(f"Unparseable Indirect Cost money cell {raw!r}: {exc}") from exc


def parse_rate(raw: str | None) -> Decimal:
    """Parse a rate as a fraction: '25.98%' -> 0.2598; '0.2598' -> 0.2598."""
    if raw is None or not raw.strip():
        raise ParseError("Indirect Cost rate cell is empty")
    s = raw.strip()
    try:
        if s.endswith("%"):
            return Decimal(s[:-1].replace(",", "").strip()) / 100
        return Decimal(s)
    except (ArithmeticError, ValueError) as exc:
        raise ParseError(f"Unparseable Indirect Cost rate {raw!r}: {exc}") from exc


def _clean(raw: str | None) -> str:
    """Collapse a label's CR/LF + surrounding whitespace into one clean line."""
    return " ".join((raw or "").split())


@dataclass(frozen=True)
class IndirectCostLine:
    """One stored dollar figure from the calculation (line number + label + amount)."""

    line_number: int
    description: str
    amount: Decimal


@dataclass(frozen=True)
class IndirectCostReport:
    """A reconciled Indirect Cost report for one fiscal year (year supplied by caller)."""

    fiscal_year: str
    term: str  # the report's word for the non-restricted concept ("Unrestricted"/"Non-Restricted")
    lines: tuple[IndirectCostLine, ...]  # the five stored dollar figures, in line order
    rate_unrestricted: Decimal  # fraction
    rate_restricted: Decimal  # fraction
    rate_unrestricted_label: str
    rate_restricted_label: str


def _detect_namespace(root: ET.Element) -> str:
    """Return the report's XML namespace, or raise on an unknown variant."""
    if not root.tag.startswith("{"):
        raise ParseError(f"Indirect Cost root has no namespace: {root.tag!r}")
    ns = root.tag[1:].split("}", 1)[0]
    if ns not in (NS_PRIOR2018, NS_2019, NS_OUTLIER):
        raise ParseError(f"Unknown Indirect Cost variant namespace {ns!r}")
    return ns


def _read_prior2018(root: ET.Element) -> dict[str, str | None]:
    return {field: root.get(attr) for field, attr in _PRIOR2018_TEXTBOX.items()}


def _read_tablix(root: ET.Element, ns: str) -> dict[str, str | None]:
    def q(tag: str) -> str:
        return f"{{{ns}}}{tag}"

    values: dict[str, str | None] = {}
    for field, (tablix, details, attr) in _TABLIX_PATH.items():
        el = root.find(f".//{q(tablix)}//{q(details)}")
        if el is None:
            raise ParseError(f"Indirect Cost {field}: {tablix}/{details} not found")
        values[field] = el.get(attr)
    return values


def _term_from_label(label: str) -> str:
    """The report's leading rate term: 'Unrestricted ...' / 'Non-Restricted ...' -> that word.

    Raises on an unrecognized term rather than guessing, so a future schema whose
    rate label changes fails loud instead of silently mislabeling the figures.
    """
    first = (label.split() or [""])[0]
    if first not in ("Unrestricted", "Non-Restricted"):
        raise ParseError(f"Unrecognized Indirect Cost rate term in label {label!r}")
    return first


def reconcile(report: IndirectCostReport) -> None:
    """Assert the dollar figures are present and each rate = allowable/base x 90%."""
    fy = report.fiscal_year
    amounts = {line.line_number: line.amount for line in report.lines}
    for ln in (1, 15, 16, 17, 18):
        if amounts.get(ln, Decimal(0)) <= 0:
            raise ParseError(f"FY{fy}: Indirect Cost line {ln} missing or non-positive")

    for who, allowable_ln, base_ln, rate in (
        ("unrestricted", 15, 17, report.rate_unrestricted),
        ("restricted", 16, 18, report.rate_restricted),
    ):
        computed = (amounts[allowable_ln] / amounts[base_ln]) * _NINETY_PCT
        if abs(computed - rate) > _RATE_TOL:
            raise ParseError(
                f"FY{fy}: {who} rate {rate} != (line {allowable_ln} / line {base_ln}) x 90% "
                f"= {computed:.6f}"
            )


def parse_indirect_cost(path: Path, fiscal_year: str) -> IndirectCostReport:
    """Parse + reconcile one Indirect Cost XML (any variant). Year supplied by caller."""
    try:
        root = ET.parse(path).getroot()
    except ET.ParseError as exc:
        raise ParseError(f"Invalid Indirect Cost XML {path.name}: {exc}") from exc

    ns = _detect_namespace(root)
    raw = _read_prior2018(root) if ns == NS_PRIOR2018 else _read_tablix(root, ns)

    term = _term_from_label(_clean(raw["rate_unrestricted_label"]))
    labels = {ln: text.format(term=term) for ln, text in _LINE_LABELS.items()}
    line_for = {
        1: "grand_total",
        15: "allowable_unrestricted",
        16: "allowable_restricted",
        17: "base_unrestricted",
        18: "base_restricted",
    }
    lines = tuple(
        IndirectCostLine(line_number=ln, description=labels[ln], amount=parse_money(raw[field]))
        for ln, field in line_for.items()
    )

    report = IndirectCostReport(
        fiscal_year=fiscal_year,
        term=term,
        lines=lines,
        rate_unrestricted=parse_rate(raw["rate_unrestricted"]),
        rate_restricted=parse_rate(raw["rate_restricted"]),
        rate_unrestricted_label=_clean(raw["rate_unrestricted_label"]),
        rate_restricted_label=_clean(raw["rate_restricted_label"]),
    )
    reconcile(report)
    return report


def _fmt(amount: Decimal) -> str:
    return f"${amount:,.2f}"


def _fmt_rate(rate: Decimal) -> str:
    return f"{rate * 100:.2f}%"


def line_md_row(line: IndirectCostLine) -> str:
    """The dollar-figure markdown row a structured figure cites."""
    return f"| {line.line_number} | {line.description} | {_fmt(line.amount)} |"


def render_markdown(report: IndirectCostReport) -> str:
    """Render a clean, citable markdown document for one Indirect Cost report."""
    fy = report.fiscal_year
    lines: list[str] = [
        f"# Indirect Cost Calculation — Fiscal Year {fy}",
        "",
        "School District of Clayton (096-102) · Missouri Department of Elementary "
        "and Secondary Education.",
        "",
        "The indirect cost rate is the share of allowable indirect costs the district "
        "may recover on grants, computed as (allowable indirect costs / eligible base) "
        "x 90%.",
        "",
        "## Allowable Costs and Rate Base (All Funds)",
        "",
        "| Line | Item | Amount |",
        "|---|---|---|",
    ]
    lines += [line_md_row(line) for line in report.lines]

    lines += [
        "",
        "## Indirect Cost Rates",
        "",
        "| Rate | Percentage |",
        "|---|---|",
        f"| {report.rate_unrestricted_label} | {_fmt_rate(report.rate_unrestricted)} |",
        f"| {report.rate_restricted_label} | {_fmt_rate(report.rate_restricted)} |",
        "",
    ]
    return "\n".join(lines)
