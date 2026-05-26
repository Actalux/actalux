"""Server-rendered inline-SVG charts for the Budget page.

No JS, no charting library — DESIGN.md mandates CSS-only with inline SVG.
Bars are filled with ink (solid for revenue, a diagonal hatch for
expenditure) so the two series are distinguished by texture rather than a
second hue; vermillion (`--accent`) is reserved for the deficit marker, not
decorative fill. Colors come from CSS custom properties via classes, so the
SVG re-themes with the stylesheet.

These functions only compute geometry and emit markup from data they are
given; they never invent figures.
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Any

from markupsafe import Markup, escape

# Time-series chart canvas (viewBox units; CSS scales width to 100%).
_CHART_W = 720
_CHART_H = 320
_PAD_LEFT = 64
_PAD_RIGHT = 16
_PAD_TOP = 24
_PAD_BOTTOM = 48
_GRIDLINES = 4


@dataclass(frozen=True)
class YearTotals:
    """Revenue and expenditure totals for one fiscal year."""

    fiscal_year: str
    revenue: Decimal
    expenditure: Decimal

    @property
    def is_deficit(self) -> bool:
        return self.expenditure > self.revenue


def aggregate_by_year(items: list[dict[str, Any]]) -> list[YearTotals]:
    """Sum revenue and expenditure per fiscal year, oldest first."""
    years: dict[str, dict[str, Decimal]] = {}
    for item in items:
        category = item.get("category", "")
        if category not in ("revenue", "expenditure"):
            continue
        fy = item["fiscal_year"]
        bucket = years.setdefault(fy, {"revenue": Decimal(0), "expenditure": Decimal(0)})
        bucket[category] += Decimal(str(item["amount"]))
    return [
        YearTotals(fy, totals["revenue"], totals["expenditure"])
        for fy, totals in sorted(years.items())
    ]


@dataclass(frozen=True)
class Share:
    """One labelled slice's share of a single year's total (a fund or a source)."""

    label: str
    amount: Decimal
    pct: float  # 0-100, share of the year's total


def _breakdown(
    items: list[dict[str, Any]], fiscal_year: str, category: str, key: str
) -> list[Share]:
    """Shares of one year's `category` total, grouped by the `key` field, largest first."""
    totals: dict[str, Decimal] = {}
    for item in items:
        if item.get("category") != category or item.get("fiscal_year") != fiscal_year:
            continue
        label = item.get(key) or "Unspecified"
        totals[label] = totals.get(label, Decimal(0)) + Decimal(str(item["amount"]))
    grand = sum(totals.values())
    if grand <= 0:
        return []
    shares = [Share(label, amount, float(amount / grand * 100)) for label, amount in totals.items()]
    return sorted(shares, key=lambda s: s.amount, reverse=True)


def fund_breakdown(items: list[dict[str, Any]], fiscal_year: str) -> list[Share]:
    """Expenditure by fund for one fiscal year, largest first."""
    return _breakdown(items, fiscal_year, category="expenditure", key="fund")


def source_breakdown(items: list[dict[str, Any]], fiscal_year: str) -> list[Share]:
    """Revenue by source for one fiscal year, largest first."""
    return _breakdown(items, fiscal_year, category="revenue", key="subcategory")


def function_breakdown(items: list[dict[str, Any]], fiscal_year: str) -> list[Share]:
    """Expenditure by function for one fiscal year, largest first.

    Sums the function x fund matrix across funds, so each share is a function's
    total Governmental Funds expenditure.
    """
    return _breakdown(items, fiscal_year, category="expenditure", key="subcategory")


def usd(amount: Decimal | float | int) -> str:
    """Format a dollar amount with thousands separators, no cents."""
    return f"${Decimal(str(amount)):,.0f}"


def _short_year(fiscal_year: str) -> str:
    """'2023-2024' -> '23-24'; leaves other formats untouched."""
    parts = fiscal_year.split("-")
    if len(parts) == 2 and all(p.isdigit() for p in parts):
        return f"{parts[0][-2:]}-{parts[1][-2:]}"
    return fiscal_year


def _nice_ceiling(value: Decimal) -> Decimal:
    """Round a max value up to a clean axis ceiling (1/2/5 x 10^n)."""
    if value <= 0:
        return Decimal(1)
    magnitude = Decimal(10) ** (len(str(int(value))) - 1)
    for step in (Decimal(1), Decimal(2), Decimal(5), Decimal(10)):
        ceiling = step * magnitude
        if value <= ceiling:
            return ceiling
    return Decimal(10) * magnitude


def _axis_label(value: Decimal) -> str:
    """Compact dollar label for a chart axis tick (e.g. '$75M', '$12.5M')."""
    if value == 0:
        return "$0"
    return f"${value / Decimal(1_000_000):g}M"


def revenue_expenditure_svg(year_totals: list[YearTotals]) -> Markup:
    """Grouped bar chart of revenue vs expenditure by fiscal year."""
    if not year_totals:
        return Markup("")

    plot_w = _CHART_W - _PAD_LEFT - _PAD_RIGHT
    plot_h = _CHART_H - _PAD_TOP - _PAD_BOTTOM
    baseline = _PAD_TOP + plot_h

    max_val = max(
        (max(yt.revenue, yt.expenditure) for yt in year_totals),
        default=Decimal(1),
    )
    ceiling = _nice_ceiling(max_val)

    def y_for(value: Decimal) -> float:
        return baseline - float(value / ceiling) * plot_h

    parts: list[str] = [
        f'<svg class="chart" viewBox="0 0 {_CHART_W} {_CHART_H}" '
        f'role="img" aria-label="Revenue versus expenditure by fiscal year" '
        f'preserveAspectRatio="xMidYMid meet">',
        '<defs><pattern id="hatch" width="6" height="6" patternUnits="userSpaceOnUse" '
        'patternTransform="rotate(45)">'
        '<rect width="6" height="6" class="hatch-bg"/>'
        '<line x1="0" y1="0" x2="0" y2="6" class="hatch-line"/></pattern></defs>',
    ]

    # Horizontal gridlines + y-axis dollar labels.
    for i in range(_GRIDLINES + 1):
        gv = ceiling * Decimal(i) / Decimal(_GRIDLINES)
        gy = y_for(gv)
        parts.append(
            f'<line class="grid" x1="{_PAD_LEFT}" y1="{gy:.1f}" '
            f'x2="{_CHART_W - _PAD_RIGHT}" y2="{gy:.1f}"/>'
        )
        parts.append(
            f'<text class="axis-y" x="{_PAD_LEFT - 8}" y="{gy + 3:.1f}" '
            f'text-anchor="end">{escape(_axis_label(gv))}</text>'
        )

    # Grouped bars per year.
    slot_w = plot_w / len(year_totals)
    bar_w = min(slot_w * 0.30, 48)
    gap = bar_w * 0.18
    for idx, yt in enumerate(year_totals):
        slot_center = _PAD_LEFT + slot_w * (idx + 0.5)
        rev_x = slot_center - bar_w - gap / 2
        exp_x = slot_center + gap / 2
        rev_y = y_for(yt.revenue)
        exp_y = y_for(yt.expenditure)
        anchor = f"#fy-{escape(yt.fiscal_year)}"
        parts.append(
            f'<a href="{anchor}"><title>{escape(yt.fiscal_year)} revenue '
            f"{escape(usd(yt.revenue))}</title>"
            f'<rect class="bar bar-revenue" x="{rev_x:.1f}" y="{rev_y:.1f}" '
            f'width="{bar_w:.1f}" height="{baseline - rev_y:.1f}"/></a>'
        )
        deficit_cls = " bar-deficit" if yt.is_deficit else ""
        parts.append(
            f'<a href="{anchor}"><title>{escape(yt.fiscal_year)} expenditure '
            f"{escape(usd(yt.expenditure))}</title>"
            f'<rect class="bar bar-expenditure{deficit_cls}" x="{exp_x:.1f}" '
            f'y="{exp_y:.1f}" width="{bar_w:.1f}" height="{baseline - exp_y:.1f}"/></a>'
        )
        parts.append(
            f'<text class="axis-x" x="{slot_center:.1f}" y="{baseline + 18:.1f}" '
            f'text-anchor="middle">{escape(_short_year(yt.fiscal_year))}</text>'
        )

    parts.append(
        f'<line class="axis-base" x1="{_PAD_LEFT}" y1="{baseline}" '
        f'x2="{_CHART_W - _PAD_RIGHT}" y2="{baseline}"/>'
    )
    parts.append("</svg>")
    return Markup("".join(parts))
