"""Tests for the Budget page chart helpers."""

from __future__ import annotations

from decimal import Decimal

from actalux.web.charts import (
    _axis_label,
    aggregate_by_year,
    component_trend,
    cross_split,
    function_breakdown,
    fund_breakdown,
    revenue_expenditure_svg,
    source_breakdown,
    trend_svg,
    usd,
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


class TestUsd:
    def test_thousands_separator_no_cents(self):
        assert usd(Decimal("1234567.89")) == "$1,234,568"
        assert usd(0) == "$0"


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
