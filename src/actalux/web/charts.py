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


@dataclass(frozen=True)
class CitedShare:
    """A breakdown slice that carries the chunk + verbatim quote it was read from.

    Used by the proposed-budget section, where each slice (a revenue source, a
    fund, an expenditure object, or an expenditure function) must show the
    verbatim source quote and deep-link to the passage it was transcribed from --
    the product's citation-first promise. ``amount`` is the slice's total across
    the grouped rows; ``pct`` is its share of the breakdown total.
    """

    label: str
    amount: Decimal
    pct: float  # 0-100, share of the breakdown total
    chunk_id: int | None = None
    # Durable citation reference for the link/hash: the stable citation_id when
    # the source row has one, else the numeric chunk_id. Renders via chunk_hash_id
    # and routes via /chunk/{ref}; survives the source document's re-ingest.
    cite_ref: int | str | None = None
    source_quote: str = ""


def proposed_breakdown(
    items: list[dict[str, Any]],
    *,
    group_key: str = "subcategory",
    where: dict[str, str] | None = None,
) -> list[CitedShare]:
    """Aggregate proposed rows by ``group_key``, summed within group, largest first.

    ``items`` are the rows for a single namespaced proposed dimension (already
    one fiscal year, one ``proposed_*`` dimension, ``basis='proposed'`` -- see
    ``get_proposed_budget_line_items``). Pass ``where`` to restrict to a subset
    (e.g. ``{"category": "revenue"}`` to split the fund dimension's revenue rows
    by fund, excluding its fund_balance rows). Each group keeps the chunk id and
    verbatim source quote of the first row that carries it, so every figure can
    show its quote and deep-link to its source.
    """
    where = where or {}
    totals: dict[str, Decimal] = {}
    chunks: dict[str, int | None] = {}
    refs: dict[str, int | str | None] = {}
    quotes: dict[str, str] = {}
    for item in items:
        if any(item.get(k) != v for k, v in where.items()):
            continue
        label = item.get(group_key) or "Unspecified"
        totals[label] = totals.get(label, Decimal(0)) + Decimal(str(item["amount"]))
        chunks.setdefault(label, item.get("chunk_id"))
        refs.setdefault(label, item.get("citation_id") or item.get("chunk_id"))
        quotes.setdefault(label, item.get("source_quote") or "")
    grand = sum(totals.values())
    if grand <= 0:
        return []
    shares = [
        CitedShare(
            label=label,
            amount=amount,
            pct=float(amount / grand * 100),
            chunk_id=chunks.get(label),
            cite_ref=refs.get(label),
            source_quote=quotes.get(label, ""),
        )
        for label, amount in totals.items()
    ]
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
    # Durable citation reference (stable citation_id, else numeric chunk_id).
    cite_ref: int | str | None = None


def component_trend(
    items: list[dict[str, Any]], *, category: str, key: str, value: str
) -> list[YearPoint]:
    """A component's amount per fiscal year, oldest first.

    Groups rows where ``item[key] == value`` and category matches by fiscal year,
    summing amounts (e.g. a function summed across its funds) and keeping the
    statement chunk each year traces to.
    """
    agg: dict[str, tuple[Decimal, int | None, int | str | None]] = {}
    for item in items:
        if item.get("category") != category or item.get(key) != value:
            continue
        fy = item["fiscal_year"]
        amount, chunk, ref = agg.get(fy, (Decimal(0), None, None))
        agg[fy] = (
            amount + Decimal(str(item["amount"])),
            chunk if chunk is not None else item.get("chunk_id"),
            ref if ref is not None else (item.get("citation_id") or item.get("chunk_id")),
        )
    return [YearPoint(fy, amount, chunk, ref) for fy, (amount, chunk, ref) in sorted(agg.items())]


# --- Stacked multi-year actuals (DESE state filings) ------------------------
# The DESE section stacks several components (expenditure objects, fund-balance
# funds, schools) per fiscal year. Each (component, year) cell keeps the chunk it
# was read from so every charted and tabulated figure deep-links to its source.

# Distinct monochrome ramp steps available as `.bar-stack-{i}` CSS classes; the
# renderer cycles through them, so a dataset with more series than steps reuses
# colors rather than indexing past the defined classes.
STACK_RAMP_STEPS = 7


@dataclass(frozen=True)
class StackCell:
    """One component's amount in one fiscal year, with the chunk it cites."""

    amount: Decimal
    chunk_id: int | None
    # Durable citation reference (stable citation_id, else numeric chunk_id).
    cite_ref: int | str | None = None


@dataclass(frozen=True)
class StackSeries:
    """One stacked component (an object, a fund, a school) across fiscal years.

    ``total`` is the component's sum over all years, used to order the stack and
    legend (largest at the bottom / first) so the ordering is stable year to year.
    """

    label: str
    cells: dict[str, StackCell]  # fiscal_year -> cell
    total: Decimal


@dataclass(frozen=True)
class StackChart:
    """A complete stacked dataset: ordered fiscal years and components.

    ``series`` is ordered largest-total first (the bottom of each stacked bar and
    the first legend entry). ``fiscal_years`` is oldest first.
    """

    fiscal_years: tuple[str, ...]
    series: tuple[StackSeries, ...]

    @property
    def year_totals(self) -> dict[str, Decimal]:
        """Each fiscal year's stacked total (sum of every component that year)."""
        totals: dict[str, Decimal] = {fy: Decimal(0) for fy in self.fiscal_years}
        for s in self.series:
            for fy, cell in s.cells.items():
                totals[fy] = totals.get(fy, Decimal(0)) + cell.amount
        return totals


def build_stack(
    items: list[dict[str, Any]], *, group_key: str, where: dict[str, str] | None = None
) -> StackChart:
    """Group rows into stacked series by ``group_key``, summed per fiscal year.

    ``where`` restricts which rows participate (e.g.
    ``{"category": "fund_balance", "subcategory": "Ending Fund Balance"}`` to chart
    only the reserve line). Each (series, year) cell keeps the chunk id of the first
    contributing row, so every figure traces to the passage it was read from.
    Series are ordered largest grand total first; years oldest first.
    """
    where = where or {}
    years: set[str] = set()
    acc: dict[str, dict[str, list[Any]]] = {}  # label -> fy -> [amount, chunk_id, cite_ref]
    for item in items:
        if any(item.get(k) != v for k, v in where.items()):
            continue
        label = item.get(group_key) or "Unspecified"
        fy = item["fiscal_year"]
        years.add(fy)
        cell = acc.setdefault(label, {}).setdefault(fy, [Decimal(0), None, None])
        cell[0] += Decimal(str(item["amount"]))
        if cell[1] is None:
            cell[1] = item.get("chunk_id")
        if cell[2] is None:
            cell[2] = item.get("citation_id") or item.get("chunk_id")

    series: list[StackSeries] = []
    for label, by_year in acc.items():
        cells = {fy: StackCell(amount, chunk, ref) for fy, (amount, chunk, ref) in by_year.items()}
        total = sum((c.amount for c in cells.values()), Decimal(0))
        series.append(StackSeries(label=label, cells=cells, total=total))
    series.sort(key=lambda s: s.total, reverse=True)
    return StackChart(fiscal_years=tuple(sorted(years)), series=tuple(series))


@dataclass(frozen=True)
class BudgetActual:
    """One fund's budget-vs-actual line (revenues or expenditures) for a year."""

    fund: str
    category: str  # "revenue" | "expenditure"
    original: Decimal
    final: Decimal
    actual: Decimal
    chunk_id: int | None = None
    # Durable citation reference (stable citation_id, else numeric chunk_id).
    cite_ref: int | str | None = None

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
    refs: dict[tuple[str, str], int | str | None] = {}
    for item in items:
        if item.get("fiscal_year") != fiscal_year:
            continue
        basis = item.get("basis")
        if basis not in ("original", "final", "actual"):
            continue
        key = (item.get("fund") or "", item.get("category") or "")
        grouped.setdefault(key, {})[basis] = Decimal(str(item["amount"]))
        chunks.setdefault(key, item.get("chunk_id"))
        refs.setdefault(key, item.get("citation_id") or item.get("chunk_id"))

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
                    cite_ref=refs.get((fund, category)),
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
            f'text-anchor="middle">{escape(short_year(p.fiscal_year))}</text>'
        )

    parts.append(
        f'<line class="axis-base" x1="{_PAD_LEFT}" y1="{baseline}" '
        f'x2="{_CHART_W - _PAD_RIGHT}" y2="{baseline}"/>'
    )
    parts.append("</svg>")
    return Markup("".join(parts))


@dataclass(frozen=True)
class CapitalBar:
    """One fiscal year's capital outlay — audited actual, or planned (CIP)."""

    fiscal_year: str
    amount: Decimal
    planned: bool  # True -> hatch fill + CIP-section link; False -> solid audited bar
    href: str | None = None  # deep-link target for the bar (source chunk, or #anchor)


def capital_outlay_svg(bars: list[CapitalBar]) -> Markup:
    """Single timeline of capital outlay by fiscal year: solid actuals, hatched plan.

    Audited (ACFR) years are solid ink; planned (CIP) years use the hatch texture and
    link to the CIP section, so a forward plan is never presented as an actual. Each
    bar deep-links to the source its figure was read from. Years are ordered oldest
    first; a gap year with no figure simply has no bar (its absence reads on the
    x-axis labels rather than being silently closed up).
    """
    if not bars:
        return Markup("")
    bars = sorted(bars, key=lambda b: b.fiscal_year)

    plot_w = _CHART_W - _PAD_LEFT - _PAD_RIGHT
    plot_h = _CHART_H - _PAD_TOP - _PAD_BOTTOM
    baseline = _PAD_TOP + plot_h
    ceiling = _nice_ceiling(max((b.amount for b in bars), default=Decimal(1)))

    def y_for(value: Decimal) -> float:
        return baseline - float(value / ceiling) * plot_h

    parts: list[str] = [
        f'<svg class="chart" viewBox="0 0 {_CHART_W} {_CHART_H}" role="img" '
        f'aria-label="Capital outlay by fiscal year" preserveAspectRatio="xMidYMid meet">',
        '<defs><pattern id="hatch" width="6" height="6" patternUnits="userSpaceOnUse" '
        'patternTransform="rotate(45)">'
        '<rect width="6" height="6" class="hatch-bg"/>'
        '<line x1="0" y1="0" x2="0" y2="6" class="hatch-line"/></pattern></defs>',
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

    slot_w = plot_w / len(bars)
    bar_w = min(slot_w * 0.5, 56)
    for idx, b in enumerate(bars):
        cx = _PAD_LEFT + slot_w * (idx + 0.5)
        x = cx - bar_w / 2
        y = y_for(b.amount)
        cls = "bar-capital-planned" if b.planned else "bar-capital-actual"
        kind = "planned" if b.planned else "actual"
        rect = (
            f"<title>{escape(b.fiscal_year)} {escape(usd(b.amount))} ({kind})</title>"
            f'<rect class="bar {cls}" x="{x:.1f}" y="{y:.1f}" '
            f'width="{bar_w:.1f}" height="{baseline - y:.1f}"/>'
        )
        if b.href:
            rect = f'<a href="{escape(b.href)}">{rect}</a>'
        parts.append(rect)
        parts.append(
            f'<text class="axis-x" x="{cx:.1f}" y="{baseline + 18:.1f}" '
            f'text-anchor="middle">{escape(short_year(b.fiscal_year))}</text>'
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


def usd_m(amount: Decimal | float | int) -> str:
    """Compact millions form for dense matrices, e.g. '$29.79M'.

    Two decimals keeps four significant figures for district-scale dollars; the
    exact, to-the-cent value is always one click away on the cited source.
    """
    return f"${Decimal(str(amount)) / Decimal(1_000_000):,.2f}M"


def short_year(fiscal_year: str) -> str:
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


def stacked_bar_svg(chart: StackChart, *, aria_label: str) -> Markup:
    """Stacked bar chart of a StackChart: one bar per fiscal year, components stacked.

    Components stack largest-first from the baseline up, filled with a monochrome
    warm-grey ramp (``.bar-stack-{i}``) so the series read by value, not hue — the
    DESIGN.md "texture/value, not a rainbow" rule. A hairline paper stroke (the
    ``.seg`` class) separates adjacent segments. Each segment carries a ``<title>``
    for hover and, when its figure cites a chunk, deep-links to
    ``/chunk/{id}/source`` (the citation-first promise made clickable on the chart
    too, mirroring the cited matrix below it).

    Only positive cells are drawn (a stacked bar cannot represent a negative
    component), and the axis ceiling is scaled to the plotted positive stack, so a
    negative figure can never push a bar past the top of the chart; such a figure
    still appears, cited, in the matrix below.
    """
    if not chart.fiscal_years or not chart.series:
        return Markup("")

    plot_w = _CHART_W - _PAD_LEFT - _PAD_RIGHT
    plot_h = _CHART_H - _PAD_TOP - _PAD_BOTTOM
    baseline = _PAD_TOP + plot_h
    # Scale to the plotted (positive) stack height, not the net total, so the bars
    # never exceed the axis when a component is negative (it is skipped below).
    plotted_totals: dict[str, Decimal] = dict.fromkeys(chart.fiscal_years, Decimal(0))
    for s in chart.series:
        for fy, cell in s.cells.items():
            if cell.amount > 0:
                plotted_totals[fy] = plotted_totals.get(fy, Decimal(0)) + cell.amount
    ceiling = _nice_ceiling(max(plotted_totals.values(), default=Decimal(1)))

    def y_for(value: Decimal) -> float:
        return baseline - float(value / ceiling) * plot_h

    parts: list[str] = [
        f'<svg class="chart" viewBox="0 0 {_CHART_W} {_CHART_H}" '
        f'role="img" aria-label="{escape(aria_label)}" preserveAspectRatio="xMidYMid meet">'
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

    slot_w = plot_w / len(chart.fiscal_years)
    bar_w = min(slot_w * 0.5, 56)
    for idx, fy in enumerate(chart.fiscal_years):
        cx = _PAD_LEFT + slot_w * (idx + 0.5)
        x = cx - bar_w / 2
        y_cursor = baseline
        for si, s in enumerate(chart.series):
            cell = s.cells.get(fy)
            if cell is None or cell.amount <= 0:
                continue
            seg_h = float(cell.amount / ceiling) * plot_h
            seg_y = y_cursor - seg_h
            cls = f"bar-stack-{si % STACK_RAMP_STEPS}"
            seg = (
                f"<title>{escape(fy)} · {escape(s.label)} {escape(usd(cell.amount))}</title>"
                f'<rect class="bar seg {cls}" x="{x:.1f}" y="{seg_y:.1f}" '
                f'width="{bar_w:.1f}" height="{seg_h:.1f}"/>'
            )
            # Deep-link the segment to the passage its figure was read from, when
            # cited. Route on the durable cite_ref (stable id, else numeric).
            ref = cell.cite_ref if cell.cite_ref is not None else cell.chunk_id
            if ref is not None:
                seg = f'<a href="/chunk/{ref}/source">{seg}</a>'
            parts.append(seg)
            y_cursor = seg_y
        parts.append(
            f'<text class="axis-x" x="{cx:.1f}" y="{baseline + 18:.1f}" '
            f'text-anchor="middle">{escape(short_year(fy))}</text>'
        )

    parts.append(
        f'<line class="axis-base" x1="{_PAD_LEFT}" y1="{baseline}" '
        f'x2="{_CHART_W - _PAD_RIGHT}" y2="{baseline}"/>'
    )
    parts.append("</svg>")
    return Markup("".join(parts))


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
        # Jump to the audited figures ledger below (per-year anchors were replaced
        # by the year x fund matrices, which live under this one section anchor).
        anchor = "#audited-figures"
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
            f'text-anchor="middle">{escape(short_year(yt.fiscal_year))}</text>'
        )

    parts.append(
        f'<line class="axis-base" x1="{_PAD_LEFT}" y1="{baseline}" '
        f'x2="{_CHART_W - _PAD_RIGHT}" y2="{baseline}"/>'
    )
    parts.append("</svg>")
    return Markup("".join(parts))
