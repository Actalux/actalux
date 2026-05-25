"""Tests for the Budget page chart helpers."""

from __future__ import annotations

from decimal import Decimal

from actalux.web.charts import (
    _axis_label,
    aggregate_by_year,
    fund_breakdown,
    revenue_expenditure_svg,
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
        assert [f.fund for f in funds] == ["General", "Capital"]  # largest first
        assert funds[0].amount == Decimal("72000000")
        assert round(funds[0].pct, 1) == 90.0
        assert round(funds[1].pct, 1) == 10.0

    def test_only_expenditures_of_the_named_year(self):
        # Revenue rows and other years are excluded.
        funds = fund_breakdown(ITEMS, "2022-2023")
        assert [f.fund for f in funds] == ["General"]
        assert funds[0].amount == Decimal("65000000")

    def test_missing_year_is_empty(self):
        assert fund_breakdown(ITEMS, "1999-2000") == []


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
