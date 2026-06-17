"""Parse a Missouri DESE Per-Pupil Building-Level Expenditures Summary XML.

The report carries two tables for the same set of school buildings, both as
``Detail`` rows under Tablix1 (distinguished by attribute suffix), verified
against the printed PDF for FY2018-19 through FY2024-25:

- **Absolute dollars** (``Building2`` attributes) — each building's actual
  expenditures, split building-level vs district-level, each federal +
  state/local. These reconcile exactly: per row, total = federal + state/local;
  across buildings, the building+district totals sum to the district totals in
  the ``Details2`` (``9999``) row. This is the structured data Actalux stores.
- **Per-pupil rates** (``Building`` attributes, the report's headline "Line
  32/33" metric, on September-membership basis) — rendered verbatim in the cited
  document for context, but NOT stored as structured figures (rates are not
  sum-able and use a different membership base than the absolute table).

The fiscal year is not embedded in the XML; the caller supplies it and verifies
it against the paired PDF.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path
from xml.etree import ElementTree as ET

from actalux.errors import ParseError

NS = "PerPupilBuildingLevelExpendituresSummary"

# The absolute-dollar cells are independently rounded to the cent (each is a
# per-pupil rate times membership, rounded), so a row total can differ from its
# federal+state/local parts, and a column sum from its rounded total, by a few
# cents. These tolerances absorb that source rounding while still catching any
# real parse/mapping error (which is dollars, not cents). Sized for the worst
# case: a row total vs two rounded parts; a district total vs ~10 rounded parts.
_ROW_TOL = Decimal("0.02")
_SUM_TOL = Decimal("0.10")

_BUILDING_RE = re.compile(r"^\s*(\d+)\s*-\s*(.+?)\s*$")


def _q(tag: str) -> str:
    return f"{{{NS}}}{tag}"


def parse_money(raw: str | None) -> Decimal:
    """Parse a Per-Pupil money/number cell ($-formatted or bare) into a Decimal."""
    if raw is None or raw.strip() in ("", "-"):
        return Decimal(0)
    try:
        return Decimal(raw.replace("$", "").replace(",", "").strip())
    except (ArithmeticError, ValueError) as exc:
        raise ParseError(f"Unparseable Per-Pupil cell {raw!r}: {exc}") from exc


def _split_building(raw: str) -> tuple[str, str]:
    """'1050-CLAYTON HIGH' -> ('1050', 'Clayton High')."""
    m = _BUILDING_RE.match(raw or "")
    if not m:
        return ("", (raw or "").strip())
    return (m.group(1), m.group(2).strip().title())


@dataclass(frozen=True)
class BuildingExpenditure:
    """One building's absolute expenditures (set 2), building- and district-level."""

    code: str
    name: str
    membership: Decimal
    building_federal: Decimal
    building_state_local: Decimal
    building_total: Decimal
    district_federal: Decimal
    district_state_local: Decimal
    district_total: Decimal


@dataclass(frozen=True)
class BuildingPerPupil:
    """One building's per-pupil rates (set 1) — rendered for context, not stored."""

    code: str
    name: str
    membership: Decimal
    building_per_pupil: Decimal
    district_per_pupil: Decimal
    total_per_pupil: Decimal


@dataclass(frozen=True)
class PerPupilReport:
    """A reconciled Per-Pupil report for one fiscal year (year supplied by caller)."""

    fiscal_year: str
    absolute: tuple[BuildingExpenditure, ...]
    per_pupil: tuple[BuildingPerPupil, ...]
    district_federal: Decimal  # Details2 9999 — building+district combined
    district_state_local: Decimal

    @property
    def district_total(self) -> Decimal:
        return self.district_federal + self.district_state_local


def _parse_absolute(root: ET.Element) -> list[BuildingExpenditure]:
    rows: list[BuildingExpenditure] = []
    for d in root.findall(f".//{_q('Detail')}"):
        raw = d.get("Building2")
        if raw is None:  # this Detail is a set-1 (per-pupil) row, not absolute
            continue
        code, name = _split_building(raw)
        rows.append(
            BuildingExpenditure(
                code=code,
                name=name,
                membership=parse_money(d.get("TotalSeptemberMembership4")),
                building_federal=parse_money(d.get("FederalExpendituresBuilding2")),
                building_state_local=parse_money(d.get("StateLocalExpendituresBuilding2")),
                building_total=parse_money(d.get("TotalBuilding2")),
                district_federal=parse_money(d.get("FederalExpendituresDistrict2")),
                district_state_local=parse_money(d.get("StateLocalExpendituresDistrict2")),
                district_total=parse_money(d.get("TotalDistrict2")),
            )
        )
    return rows


def _parse_per_pupil(root: ET.Element) -> list[BuildingPerPupil]:
    rows: list[BuildingPerPupil] = []
    for d in root.findall(f".//{_q('Detail')}"):
        raw = d.get("Building")
        if raw is None:  # this Detail is a set-2 (absolute) row
            continue
        code, name = _split_building(raw)
        rows.append(
            BuildingPerPupil(
                code=code,
                name=name,
                membership=parse_money(d.get("TotalSeptemberMembership1")),
                building_per_pupil=parse_money(d.get("TotalBuilding")),
                district_per_pupil=parse_money(d.get("TotalDistrict")),
                total_per_pupil=parse_money(d.get("ExpendituresPerSeptemberMembership")),
            )
        )
    return rows


def reconcile(report: PerPupilReport) -> None:
    """Assert the absolute-dollar identities exactly; raise ParseError on mismatch."""
    fy = report.fiscal_year
    if not report.absolute:
        raise ParseError(f"FY{fy}: Per-Pupil report has no building rows")

    sum_fed = Decimal(0)
    sum_sl = Decimal(0)
    for b in report.absolute:
        if abs(b.building_federal + b.building_state_local - b.building_total) > _ROW_TOL:
            raise ParseError(
                f"FY{fy}: {b.name} building total {b.building_total} != "
                f"federal {b.building_federal} + state/local {b.building_state_local}"
            )
        if abs(b.district_federal + b.district_state_local - b.district_total) > _ROW_TOL:
            raise ParseError(
                f"FY{fy}: {b.name} district total {b.district_total} != "
                f"federal {b.district_federal} + state/local {b.district_state_local}"
            )
        sum_fed += b.building_federal + b.district_federal
        sum_sl += b.building_state_local + b.district_state_local

    if abs(sum_fed - report.district_federal) > _SUM_TOL:
        raise ParseError(
            f"FY{fy}: building+district federal {sum_fed} != district total "
            f"{report.district_federal}"
        )
    if abs(sum_sl - report.district_state_local) > _SUM_TOL:
        raise ParseError(
            f"FY{fy}: building+district state/local {sum_sl} != district total "
            f"{report.district_state_local}"
        )


def parse_per_pupil(path: Path, fiscal_year: str) -> PerPupilReport:
    """Parse + reconcile one Per-Pupil XML. ``fiscal_year`` is supplied by the caller."""
    try:
        root = ET.parse(path).getroot()
    except ET.ParseError as exc:
        raise ParseError(f"Invalid Per-Pupil XML {path.name}: {exc}") from exc

    district = next(iter(root.findall(f".//{_q('Details2')}")), None)
    if district is None:
        raise ParseError(f"FY{fiscal_year}: Per-Pupil district-total row (Details2) not found")

    report = PerPupilReport(
        fiscal_year=fiscal_year,
        absolute=tuple(_parse_absolute(root)),
        per_pupil=tuple(_parse_per_pupil(root)),
        district_federal=parse_money(district.get("FederalExpendituresBuilding3")),
        district_state_local=parse_money(district.get("StateLocalExpendituresBuilding3")),
    )
    reconcile(report)
    return report


def _fmt(amount: Decimal) -> str:
    return f"${amount:,.2f}"


def building_md_row(b: BuildingExpenditure) -> str:
    """The absolute-expenditure markdown row a structured figure cites."""
    return (
        f"| {b.code} {b.name} | {_fmt(b.building_total)} | {_fmt(b.district_total)} | "
        f"{_fmt(b.building_total + b.district_total)} |"
    )


def render_markdown(report: PerPupilReport) -> str:
    """Render a clean, citable markdown document with both tables."""
    fy = report.fiscal_year
    lines: list[str] = [
        f"# Per-Pupil Building Expenditures Summary — Fiscal Year {fy}",
        "",
        "School District of Clayton (096-102) · Missouri Department of Elementary "
        "and Secondary Education.",
        "",
        "## Expenditures by Building (All Funds)",
        "",
        "| Building | Building-Level | District-Level Allocated | Total |",
        "|---|---|---|---|",
    ]
    lines += [building_md_row(b) for b in report.absolute]
    lines.append(f"| District Total | | | {_fmt(report.district_total)} |")

    lines += [
        "",
        "## Per-Pupil Expenditures by Building (September-Membership Basis, Line 32/33)",
        "",
        "| Building | September Membership | Building-Level Per Pupil | "
        "District-Level Per Pupil | Total Per Pupil |",
        "|---|---|---|---|---|",
    ]
    for p in report.per_pupil:
        lines.append(
            f"| {p.code} {p.name} | {p.membership:,.2f} | {_fmt(p.building_per_pupil)} | "
            f"{_fmt(p.district_per_pupil)} | {_fmt(p.total_per_pupil)} |"
        )
    lines.append("")
    return "\n".join(lines)
