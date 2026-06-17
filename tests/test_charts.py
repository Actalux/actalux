"""Tests for the Budget page chart helpers."""

from __future__ import annotations

from decimal import Decimal

from actalux.web.charts import (
    STACK_RAMP_STEPS,
    TierBar,
    _axis_label,
    aggregate_by_year,
    budget_vs_actual,
    build_stack,
    component_trend,
    cross_split,
    function_breakdown,
    fund_breakdown,
    proposed_breakdown,
    revenue_expenditure_svg,
    short_year,
    source_breakdown,
    stacked_bar_svg,
    tier_bar_svg,
    trend_svg,
    usd,
    usd_m,
)


def _item(fy: str, category: str, amount: str, fund: str = "General") -> dict:
    return {"fiscal_year": fy, "category": category, "amount": amount, "fund": fund}


ITEMS = [
    _item("2022-2023", "revenue", "68000000"),
    _item("2022-2023", "expenditure", "65000000"),
    _item("2023-2024", "revenue", "70000000"),
    _item("2023-2024", "expenditure", "72000000"),
    _item("2023-2024", "expenditure", "8000000", "Capital"),
]


class TestAggregateByYear:
    def test_sums_and_orders_oldest_first(self):
        totals = aggregate_by_year(ITEMS)
        assert [t.fiscal_year for t in totals] == ["2022-2023", "2023-2024"]
        assert totals[1].revenue == Decimal("70000000")
        # 72M General + 8M Capital
        assert totals[1].expenditure == Decimal("80000000")

    def test_deficit_flag(self):
        totals = aggregate_by_year(ITEMS)
        assert totals[0].is_deficit is False  # 68M rev > 65M exp
        assert totals[1].is_deficit is True  # 80M exp > 70M rev

    def test_ignores_non_rev_exp_categories(self):
        items = ITEMS + [
            {"fiscal_year": "2023-2024", "category": "fund_balance", "amount": "5000000"}
        ]
        totals = aggregate_by_year(items)
        # fund_balance must not inflate either series
        assert totals[1].revenue == Decimal("70000000")
        assert totals[1].expenditure == Decimal("80000000")

    def test_empty(self):
        assert aggregate_by_year([]) == []


class TestFundBreakdown:
    def test_shares_sum_and_order(self):
        funds = fund_breakdown(ITEMS, "2023-2024")
        assert [f.label for f in funds] == ["General", "Capital"]  # largest first
        assert funds[0].amount == Decimal("72000000")
        assert round(funds[0].pct, 1) == 90.0
        assert round(funds[1].pct, 1) == 10.0

    def test_only_expenditures_of_the_named_year(self):
        # Revenue rows and other years are excluded.
        funds = fund_breakdown(ITEMS, "2022-2023")
        assert [f.label for f in funds] == ["General"]
        assert funds[0].amount == Decimal("65000000")

    def test_missing_year_is_empty(self):
        assert fund_breakdown(ITEMS, "1999-2000") == []


class TestSourceBreakdown:
    SOURCE_ITEMS = [
        {"fiscal_year": "2023-2024", "category": "revenue", "subcategory": s, "amount": a}
        for s, a in [("Local", "71803874"), ("State", "1907709"), ("Federal", "958434")]
    ]

    def test_groups_by_subcategory_largest_first(self):
        sources = source_breakdown(self.SOURCE_ITEMS, "2023-2024")
        assert [s.label for s in sources] == ["Local", "State", "Federal"]
        assert sources[0].amount == Decimal("71803874")
        assert round(sources[0].pct, 1) == 96.2  # Local dominates

    def test_missing_year_is_empty(self):
        assert source_breakdown(self.SOURCE_ITEMS, "1999-2000") == []


class TestProposedBreakdown:
    """Aggregates namespaced proposed rows by subcategory across funds, keeping
    a citing chunk per slice."""

    # One source split across two funds (must sum), one in a single fund.
    ROWS = [
        {"subcategory": "Local Revenue", "amount": "23876630", "chunk_id": 5109},
        {"subcategory": "Local Revenue", "amount": "38298500", "chunk_id": 5109},
        {"subcategory": "County Revenue", "amount": "133500", "chunk_id": 5109},
    ]

    def test_sums_across_funds_and_orders_largest_first(self):
        shares = proposed_breakdown(self.ROWS)
        assert [s.label for s in shares] == ["Local Revenue", "County Revenue"]
        # Local = 23,876,630 + 38,298,500
        assert shares[0].amount == Decimal("62175130")
        assert shares[1].amount == Decimal("133500")

    def test_carries_chunk_and_quote_for_citation(self):
        rows = [{**r, "source_quote": "County Revenue 133,500"} for r in self.ROWS]
        shares = proposed_breakdown(rows)
        assert all(s.chunk_id == 5109 for s in shares)
        county = next(s for s in shares if s.label == "County Revenue")
        assert county.source_quote == "County Revenue 133,500"

    def test_pct_is_share_of_breakdown_total(self):
        shares = proposed_breakdown(self.ROWS)
        assert round(sum(s.pct for s in shares), 1) == 100.0

    def test_group_by_fund_with_where_filter(self):
        # The fund dimension mixes revenue + fund_balance rows; group by fund and
        # restrict to revenue so fund_balance rows don't inflate the revenue mix.
        rows = [
            {
                "fund": "General",
                "category": "revenue",
                "subcategory": "Total revenue",
                "amount": "24866380",
                "chunk_id": 5109,
            },
            {
                "fund": "Special Revenue (Teachers)",
                "category": "revenue",
                "subcategory": "Total revenue",
                "amount": "40698910",
                "chunk_id": 5109,
            },
            {
                "fund": "General",
                "category": "fund_balance",
                "subcategory": "End Fund Bal-June 30, 2025",
                "amount": "25110081",
                "chunk_id": 5109,
            },
        ]
        shares = proposed_breakdown(rows, group_key="fund", where={"category": "revenue"})
        assert [s.label for s in shares] == ["Special Revenue (Teachers)", "General"]
        assert shares[1].amount == Decimal("24866380")  # fund_balance row excluded

    def test_empty(self):
        assert proposed_breakdown([]) == []


class TestFunctionBreakdown:
    # function x fund matrix: each function appears once per fund it spends in.
    FN_ITEMS = [
        {"fiscal_year": "2024-2025", "category": "expenditure", "subcategory": sub, "amount": amt}
        for sub, amt in [
            ("Instruction", "2900882"),  # General
            ("Instruction", "32727714"),  # Special Revenue
            ("Instruction", "1043599"),  # Capital
            ("Operation of plant", "8786759"),
            ("Operation of plant", "3037670"),
        ]
    ]

    def test_sums_a_function_across_its_funds(self):
        fns = function_breakdown(self.FN_ITEMS, "2024-2025")
        assert [f.label for f in fns] == ["Instruction", "Operation of plant"]
        # Instruction = 2,900,882 + 32,727,714 + 1,043,599
        assert fns[0].amount == Decimal("36672195")
        assert fns[1].amount == Decimal("11824429")

    def test_excludes_revenue_rows(self):
        items = self.FN_ITEMS + [
            {
                "fiscal_year": "2024-2025",
                "category": "revenue",
                "subcategory": "Local",
                "amount": "74513876",
            }
        ]
        fns = function_breakdown(items, "2024-2025")
        assert "Local" not in [f.label for f in fns]

    def test_missing_year_is_empty(self):
        assert function_breakdown(self.FN_ITEMS, "1999-2000") == []


# Function x fund matrix rows for the drill helpers: two functions, two years.
MATRIX_ITEMS = [
    {
        "fiscal_year": fy,
        "category": "expenditure",
        "subcategory": sub,
        "fund": fund,
        "amount": amt,
        "chunk_id": chunk,
    }
    for fy, sub, fund, amt, chunk in [
        ("2023-2024", "Instruction", "General", "2972138", 7154),
        ("2023-2024", "Instruction", "Special Revenue (Teachers)", "31133438", 7154),
        ("2023-2024", "Operation of plant", "General", "8434210", 7154),
        ("2024-2025", "Instruction", "General", "2900882", 7802),
        ("2024-2025", "Instruction", "Special Revenue (Teachers)", "32727714", 7802),
        ("2024-2025", "Instruction", "Capital Projects", "1043599", 7802),
        ("2024-2025", "Operation of plant", "General", "8786759", 7802),
        ("2024-2025", "Operation of plant", "Capital Projects", "3037670", 7802),
    ]
]


class TestCrossSplit:
    def test_function_splits_across_its_funds_largest_first(self):
        split = cross_split(
            MATRIX_ITEMS,
            "2024-2025",
            match={"category": "expenditure", "subcategory": "Instruction"},
            group_key="fund",
        )
        assert [s.label for s in split] == [
            "Special Revenue (Teachers)",
            "General",
            "Capital Projects",
        ]
        assert split[0].amount == Decimal("32727714")
        assert sum(s.amount for s in split) == Decimal("36672195")

    def test_fund_splits_across_its_functions(self):
        split = cross_split(
            MATRIX_ITEMS,
            "2024-2025",
            match={"category": "expenditure", "fund": "General"},
            group_key="subcategory",
        )
        assert [s.label for s in split] == ["Operation of plant", "Instruction"]
        assert split[0].amount == Decimal("8786759")

    def test_scoped_to_the_named_year(self):
        split = cross_split(
            MATRIX_ITEMS,
            "2023-2024",
            match={"category": "expenditure", "subcategory": "Instruction"},
            group_key="fund",
        )
        # 2023-2024 Instruction has no Capital Projects cell.
        assert [s.label for s in split] == ["Special Revenue (Teachers)", "General"]


class TestComponentTrend:
    def test_sums_across_funds_per_year_oldest_first(self):
        trend = component_trend(
            MATRIX_ITEMS, category="expenditure", key="subcategory", value="Instruction"
        )
        assert [p.fiscal_year for p in trend] == ["2023-2024", "2024-2025"]
        # 2023-24: 2,972,138 + 31,133,438 ; 2024-25: 2,900,882 + 32,727,714 + 1,043,599
        assert trend[0].amount == Decimal("34105576")
        assert trend[1].amount == Decimal("36672195")

    def test_carries_the_statement_chunk_per_year(self):
        trend = component_trend(
            MATRIX_ITEMS, category="expenditure", key="subcategory", value="Instruction"
        )
        assert trend[0].chunk_id == 7154
        assert trend[1].chunk_id == 7802

    def test_unknown_component_is_empty(self):
        assert (
            component_trend(MATRIX_ITEMS, category="expenditure", key="subcategory", value="Nope")
            == []
        )


class TestTrendSvg:
    def test_empty_returns_empty_string(self):
        assert str(trend_svg([])) == ""

    def test_renders_a_bar_per_year(self):
        trend = component_trend(
            MATRIX_ITEMS, category="expenditure", key="subcategory", value="Instruction"
        )
        svg = str(trend_svg(trend))
        assert "<svg" in svg and "</svg>" in svg
        assert svg.count('class="bar bar-trend"') == 2  # one bar per year
        assert "23-24" in svg and "24-25" in svg  # short-year x labels


class TestBudgetVsActual:
    BUDGET_ITEMS = [
        {
            "fiscal_year": "2024-2025",
            "category": cat,
            "fund": fund,
            "basis": basis,
            "amount": amt,
            "chunk_id": 7862,
        }
        for fund, cat, basis, amt in [
            ("General", "revenue", "original", "24006880"),
            ("General", "revenue", "final", "24058267"),
            ("General", "revenue", "actual", "31708277"),
            ("General", "expenditure", "original", "23761689"),
            ("General", "expenditure", "final", "24069207"),
            ("General", "expenditure", "actual", "22507352"),
            ("Debt Service", "revenue", "original", "8063430"),
            ("Debt Service", "revenue", "final", "8063430"),
            ("Debt Service", "revenue", "actual", "8045686"),
        ]
    ]

    def test_collapses_three_bases_into_one_line(self):
        lines = budget_vs_actual(self.BUDGET_ITEMS, "2024-2025")
        gen_rev = next(b for b in lines if b.fund == "General" and b.category == "revenue")
        assert gen_rev.original == Decimal("24006880")
        assert gen_rev.final == Decimal("24058267")
        assert gen_rev.actual == Decimal("31708277")
        # variance = actual - final (positive: collected above budget)
        assert gen_rev.variance == Decimal("7650010")

    def test_fund_order_and_revenue_before_expenditure(self):
        labels = [(b.fund, b.category) for b in budget_vs_actual(self.BUDGET_ITEMS, "2024-2025")]
        assert labels == [
            ("General", "revenue"),
            ("General", "expenditure"),
            ("Debt Service", "revenue"),
        ]

    def test_incomplete_line_is_dropped(self):
        # A (fund, category) missing one of the three bases is not emitted.
        items = [
            {
                "fiscal_year": "2024-2025",
                "category": "revenue",
                "fund": "Capital Projects",
                "basis": "original",
                "amount": "100",
            }
        ]
        assert budget_vs_actual(items, "2024-2025") == []

    def test_missing_year_is_empty(self):
        assert budget_vs_actual(self.BUDGET_ITEMS, "1999-2000") == []


class TestUsd:
    def test_thousands_separator_no_cents(self):
        assert usd(Decimal("1234567.89")) == "$1,234,568"
        assert usd(0) == "$0"


class TestUsdM:
    def test_compact_millions_two_decimals(self):
        assert usd_m(Decimal("29794237.97")) == "$29.79M"
        assert usd_m(Decimal("498434")) == "$0.50M"
        assert usd_m(0) == "$0.00M"


class TestShortYear:
    def test_compacts_fiscal_year(self):
        assert short_year("2023-2024") == "23-24"

    def test_leaves_other_formats_untouched(self):
        assert short_year("FY2024") == "FY2024"


# DESE-style rows: one document per fiscal year (its chunk), several components
# per year. Mirrors the asbr_object shape (subcategory = object, all expenditure).
STACK_ITEMS = [
    {
        "fiscal_year": fy,
        "category": "expenditure",
        "dimension": "asbr_object",
        "subcategory": sub,
        "amount": amt,
        "chunk_id": chunk,
    }
    for fy, sub, amt, chunk in [
        ("2022-2023", "6110 Certificated Salaries", "30000000", 9001),
        ("2022-2023", "6200 Employee Benefits", "9000000", 9001),
        ("2023-2024", "6110 Certificated Salaries", "31000000", 9002),
        ("2023-2024", "6200 Employee Benefits", "10000000", 9002),
        # A component absent in the first year (appears only in 2023-2024).
        ("2023-2024", "6500 Capital Outlay", "2000000", 9002),
    ]
]


class TestBuildStack:
    def test_years_oldest_first_and_series_largest_total_first(self):
        chart = build_stack(STACK_ITEMS, group_key="subcategory")
        assert chart.fiscal_years == ("2022-2023", "2023-2024")
        # Certificated Salaries (61M total) > Benefits (19M) > Capital Outlay (2M).
        assert [s.label for s in chart.series] == [
            "6110 Certificated Salaries",
            "6200 Employee Benefits",
            "6500 Capital Outlay",
        ]

    def test_cells_sum_per_year_and_keep_their_chunk(self):
        chart = build_stack(STACK_ITEMS, group_key="subcategory")
        salaries = next(s for s in chart.series if s.label.startswith("6110"))
        assert salaries.cells["2023-2024"].amount == Decimal("31000000")
        assert salaries.cells["2023-2024"].chunk_id == 9002
        assert salaries.total == Decimal("61000000")

    def test_missing_cell_is_absent_not_zero(self):
        chart = build_stack(STACK_ITEMS, group_key="subcategory")
        capital = next(s for s in chart.series if s.label.startswith("6500"))
        assert "2022-2023" not in capital.cells  # no Capital Outlay that year

    def test_year_totals_sum_every_component(self):
        chart = build_stack(STACK_ITEMS, group_key="subcategory")
        assert chart.year_totals["2022-2023"] == Decimal("39000000")
        assert chart.year_totals["2023-2024"] == Decimal("43000000")

    def test_where_filter_restricts_rows(self):
        rows = [
            {
                "fiscal_year": "2023-2024",
                "category": "fund_balance",
                "subcategory": "Ending Fund Balance",
                "fund": "General",
                "amount": "25000000",
            },
            {
                "fiscal_year": "2023-2024",
                "category": "fund_balance",
                "subcategory": "Beginning Fund Balance",
                "fund": "General",
                "amount": "20000000",
            },
        ]
        chart = build_stack(
            rows,
            group_key="fund",
            where={"category": "fund_balance", "subcategory": "Ending Fund Balance"},
        )
        # Only the ending-balance row participates.
        assert chart.series[0].cells["2023-2024"].amount == Decimal("25000000")

    def test_empty(self):
        chart = build_stack([], group_key="subcategory")
        assert chart.fiscal_years == ()
        assert chart.series == ()


class TestStackedBarSvg:
    def test_empty_returns_empty_string(self):
        assert str(stacked_bar_svg(build_stack([], group_key="subcategory"), aria_label="x")) == ""

    def test_renders_a_segment_per_cell_with_ramp_classes(self):
        chart = build_stack(STACK_ITEMS, group_key="subcategory")
        svg = str(stacked_bar_svg(chart, aria_label="Expenditure by object"))
        assert "<svg" in svg and "</svg>" in svg
        # Five cells across the two years -> five stacked segments.
        assert svg.count('class="bar seg ') == 5
        # Ramp classes stay within the defined step count.
        assert "bar-stack-0" in svg and "bar-stack-1" in svg
        assert f"bar-stack-{STACK_RAMP_STEPS}" not in svg
        assert "22-23" in svg and "23-24" in svg  # short-year x labels
        # Every (cited) segment deep-links to its source passage.
        assert svg.count('href="/chunk/') == 5

    def test_ceiling_scales_to_positive_stack_not_net_total(self):
        # A negative component is skipped in the bars (a stack can't show it) and
        # must not shrink the axis: the positive stack still fits under the ceiling.
        rows = [
            {"fiscal_year": "2023-2024", "subcategory": "A", "amount": "9000000", "chunk_id": 1},
            {"fiscal_year": "2023-2024", "subcategory": "B", "amount": "-4000000", "chunk_id": 2},
        ]
        chart = build_stack(rows, group_key="subcategory")
        svg = str(stacked_bar_svg(chart, aria_label="x"))
        # Only the positive component is drawn; the negative one is omitted.
        assert svg.count('class="bar seg ') == 1


class TestAxisLabel:
    def test_compact_millions(self):
        assert _axis_label(Decimal(0)) == "$0"
        assert _axis_label(Decimal(75_000_000)) == "$75M"
        assert _axis_label(Decimal(100_000_000)) == "$100M"
        assert _axis_label(Decimal(12_500_000)) == "$12.5M"


class TestRevenueExpenditureSvg:
    def test_empty_returns_empty_string(self):
        assert str(revenue_expenditure_svg([])) == ""

    def test_renders_bars_and_deficit_marker(self):
        svg = str(revenue_expenditure_svg(aggregate_by_year(ITEMS)))
        assert "<svg" in svg and "</svg>" in svg
        assert 'class="bar bar-revenue"' in svg
        assert "bar-expenditure" in svg
        assert "bar-deficit" in svg  # 2023-2024 is a deficit year
        assert 'id="hatch"' in svg  # expenditure hatch pattern defined
        # Bars jump to the audited figures ledger, not the removed per-year anchors.
        assert 'href="#audited-figures"' in svg
        assert "#fy-" not in svg


class TestTierBarSvg:
    """Facilities priority-tier horizontal bar: accent on the immediate bar only."""

    def test_empty_returns_empty_string(self):
        assert str(tier_bar_svg([])) == ""

    def test_accent_only_on_immediate_bar(self):
        bars = [
            TierBar("Red", 23_458_924, immediate=True),
            TierBar("Yellow", 28_305_058),
            TierBar("Green", 42_372_894),
        ]
        svg = str(tier_bar_svg(bars))
        assert "<svg" in svg and "</svg>" in svg
        # The immediate (Red) bar takes the vermillion class; no other bar does.
        assert svg.count("bar-immediate") == 1
        # The two non-immediate tiers use the neutral tier fill.
        assert svg.count("bar-tier") == 2
        # Each bar labels its tier and its dollar amount.
        assert "Red" in svg and "Yellow" in svg and "Green" in svg
        assert "$23,458,924" in svg

    def test_bar_widths_scale_to_largest_tier(self):
        # The largest tier fills the track; a half-size tier is ~half as wide.
        bars = [TierBar("Big", 100, immediate=True), TierBar("Half", 50)]
        svg = str(tier_bar_svg(bars))
        widths = [float(seg.split('width="')[1].split('"')[0]) for seg in svg.split("<rect")[1:]]
        assert len(widths) == 2
        assert widths[0] > widths[1]
        assert abs(widths[1] / widths[0] - 0.5) < 0.01
