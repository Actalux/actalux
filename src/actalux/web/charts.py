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


def _shares(items: list[dict[str, Any]], *, where: dict[str, str], group_key: str) -> list[Share]:
    """Shares grouped by `group_key`, over rows matching every field in `where`, largest first."""
    totals: dict[str, Decimal] = {}
    for item in items:
        if any(item.get(k) != v for k, v in where.items()):
            continue
        label = item.get(group_key) or "Unspecified"
        totals[label] = totals.get(label, Decimal(0)) + Decimal(str(item["amount"]))
    grand = sum(totals.values())
    if grand <= 0:
        return []
    shares = [Share(label, amount, float(amount / grand * 100)) for label, amount in totals.items()]
    return sorted(shares, key=lambda s: s.amount, reverse=True)


def fund_breakdown(items: list[dict[str, Any]], fiscal_year: str) -> list[Share]:
    """Expenditure by fund for one fiscal year, largest first."""
    return _shares(
        items, where={"category": "expenditure", "fiscal_year": fiscal_year}, group_key="fund"
    )


def source_breakdown(items: list[dict[str, Any]], fiscal_year: str) -> list[Share]:
    """Revenue by source for one fiscal year, largest first."""
    return _shares(
        items, where={"category": "revenue", "fiscal_year": fiscal_year}, group_key="subcategory"
    )


def function_breakdown(items: list[dict[str, Any]], fiscal_year: str) -> list[Share]:
    """Expenditure by function for one fiscal year, largest first.

    Sums the function x fund matrix across funds, so each share is a function's
    total Governmental Funds expenditure.
    """
    return _shares(
        items,
        where={"category": "expenditure", "fiscal_year": fiscal_year},
        group_key="subcategory",
    )


def cross_split(
    items: list[dict[str, Any]], fiscal_year: str, *, match: dict[str, str], group_key: str
) -> list[Share]:
    """One year's rows matching `match`, split across `group_key`, largest first.

    Drives the fund<->function matrix drill: a function's funds (``match`` on the
    function, ``group_key='fund'``) or a fund's functions (the reverse).
    """
    return _shares(items, where={"fiscal_year": fiscal_year, **match}, group_key=group_key)


@dataclass(frozen=True)
class YearPoint:
    """One fiscal year's amount for a single tracked component, with its citation."""

    fiscal_year: str
    amount: Decimal
    chunk_id: int | None = None


def component_trend(
    items: list[dict[str, Any]], *, category: str, key: str, value: str
) -> list[YearPoint]:
    """A component's amount per fiscal year, oldest first.

    Groups rows where ``item[key] == value`` and category matches by fiscal year,
    summing amounts (e.g. a function summed across its funds) and keeping the
    statement chunk each year traces to.
    """
    agg: dict[str, tuple[Decimal, int | None]] = {}
    for item in items:
        if item.get("category") != category or item.get(key) != value:
            continue
        fy = item["fiscal_year"]
        amount, chunk = agg.get(fy, (Decimal(0), None))
        agg[fy] = (
            amount + Decimal(str(item["amount"])),
            chunk if chunk is not None else item.get("chunk_id"),
        )
    return [YearPoint(fy, amount, chunk) for fy, (amount, chunk) in sorted(agg.items())]


@dataclass(frozen=True)
class BudgetActual:
    """One fund's budget-vs-actual line (revenues or expenditures) for a year."""

    fund: str
    category: str  # "revenue" | "expenditure"
    original: Decimal
    final: Decimal
    actual: Decimal
    chunk_id: int | None = None

    @property
    def variance(self) -> Decimal:
        """Actual minus final budget (positive = above budget)."""
        return self.actual - self.final


# Fund display order, matching the GAAP charts.
_BUDGET_FUND_ORDER = (
    "General",
    "Special Revenue (Teachers)",
    "Debt Service",
    "Capital Projects",
)


def budget_vs_actual(items: list[dict[str, Any]], fiscal_year: str) -> list[BudgetActual]:
    """One year's budget-vs-actual lines, fund order then revenues before expenditures.

    Expects dimension='budget' rows (basis in original/final/actual). Each
    (fund, category) collapses its three basis rows into one line.
    """
    grouped: dict[tuple[str, str], dict[str, Decimal]] = {}
    chunks: dict[tuple[str, str], int | None] = {}
    for item in items:
        if item.get("fiscal_year") != fiscal_year:
            continue
        basis = item.get("basis")
        if basis not in ("original", "final", "actual"):
            continue
        key = (item.get("fund") or "", item.get("category") or "")
        grouped.setdefault(key, {})[basis] = Decimal(str(item["amount"]))
        chunks.setdefault(key, item.get("chunk_id"))

    lines: list[BudgetActual] = []
    for fund in _BUDGET_FUND_ORDER:
        for category in ("revenue", "expenditure"):
            vals = grouped.get((fund, category))
            if not vals or not {"original", "final", "actual"} <= vals.keys():
                continue
            lines.append(
                BudgetActual(
                    fund=fund,
                    category=category,
                    original=vals["original"],
                    final=vals["final"],
                    actual=vals["actual"],
                    chunk_id=chunks.get((fund, category)),
                )
            )
    return lines


def trend_svg(points: list[YearPoint]) -> Markup:
    """Single-series bar chart of one component's amount across fiscal years."""
    if not points:
        return Markup("")

    plot_w = _CHART_W - _PAD_LEFT - _PAD_RIGHT
    plot_h = _CHART_H - _PAD_TOP - _PAD_BOTTOM
    baseline = _PAD_TOP + plot_h
    ceiling = _nice_ceiling(max((p.amount for p in points), default=Decimal(1)))

    def y_for(value: Decimal) -> float:
        return baseline - float(value / ceiling) * plot_h

    parts: list[str] = [
        f'<svg class="chart" viewBox="0 0 {_CHART_W} {_CHART_H}" '
        f'role="img" aria-label="Amount by fiscal year" preserveAspectRatio="xMidYMid meet">'
    ]
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

    slot_w = plot_w / len(points)
    bar_w = min(slot_w * 0.5, 56)
    for idx, p in enumerate(points):
        cx = _PAD_LEFT + slot_w * (idx + 0.5)
        x = cx - bar_w / 2
        y = y_for(p.amount)
        parts.append(
            f"<title>{escape(p.fiscal_year)} {escape(usd(p.amount))}</title>"
            f'<rect class="bar bar-trend" x="{x:.1f}" y="{y:.1f}" '
            f'width="{bar_w:.1f}" height="{baseline - y:.1f}"/>'
        )
        parts.append(
            f'<text class="axis-x" x="{cx:.1f}" y="{baseline + 18:.1f}" '
            f'text-anchor="middle">{escape(_short_year(p.fiscal_year))}</text>'
        )

    parts.append(
        f'<line class="axis-base" x1="{_PAD_LEFT}" y1="{baseline}" '
        f'x2="{_CHART_W - _PAD_RIGHT}" y2="{baseline}"/>'
    )
    parts.append("</svg>")
    return Markup("".join(parts))


# Horizontal tier bar (facilities priority tiers). Its own small canvas so the
# three rows read as a compact comparison, not the tall time-series canvas.
_TIER_BAR_W = 720
_TIER_ROW_H = 44
_TIER_LABEL_W = 150
_TIER_BAR_RIGHT = 132  # room for the dollar label at the end of each bar
_TIER_BAR_H = 18


@dataclass(frozen=True)
class TierBar:
    """One priority tier's bar: its label, dollar amount, and accent flag.

    ``immediate`` marks the single tier rendered in vermillion (the plan's Red /
    immediate-needs tier). Per DESIGN.md the accent punctuates one bar only; all
    other tiers use the neutral ink fill.
    """

    label: str
    amount: int
    immediate: bool = False


def tier_bar_svg(bars: list[TierBar]) -> Markup:
    """Horizontal bar chart of priority-tier totals, accent on the immediate tier.

    Bars are scaled to the largest tier. Only the ``immediate`` bar takes the
    vermillion fill (``bar-immediate``); the rest use the neutral chart ink, so
    the accent flags the most-urgent tier exactly once.
    """
    if not bars:
        return Markup("")

    height = _TIER_ROW_H * len(bars)
    track_x = _TIER_LABEL_W
    track_w = _TIER_BAR_W - _TIER_LABEL_W - _TIER_BAR_RIGHT
    max_amount = max((b.amount for b in bars), default=0)
    ceiling = max_amount if max_amount > 0 else 1

    parts: list[str] = [
        f'<svg class="chart tier-bars" viewBox="0 0 {_TIER_BAR_W} {height}" '
        f'role="img" aria-label="Identified need by priority tier" '
        f'preserveAspectRatio="xMidYMid meet">'
    ]
    for idx, bar in enumerate(bars):
        row_y = idx * _TIER_ROW_H
        text_y = row_y + _TIER_ROW_H / 2 + 4
        bar_y = row_y + (_TIER_ROW_H - _TIER_BAR_H) / 2
        bar_w = track_w * bar.amount / ceiling
        fill_cls = "bar-immediate" if bar.immediate else "bar-tier"
        parts.append(f'<text class="tier-label" x="0" y="{text_y:.1f}">{escape(bar.label)}</text>')
        parts.append(
            f"<title>{escape(bar.label)} {escape(usd(bar.amount))}</title>"
            f'<rect class="bar {fill_cls}" x="{track_x}" y="{bar_y:.1f}" '
            f'width="{bar_w:.1f}" height="{_TIER_BAR_H}"/>'
        )
        parts.append(
            f'<text class="tier-amount" x="{track_x + bar_w + 8:.1f}" y="{text_y:.1f}">'
            f"{escape(usd(bar.amount))}</text>"
        )
    parts.append("</svg>")
    return Markup("".join(parts))


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
