"""Tests for the City of Clayton audited-budget loader (scripts/load_city_budget.py).

Covers reconciliation-on-build (each year's figures cross-foot to the audited
Total Governmental Funds totals), the digest-citability invariant the load relies
on (every figure's quote appears verbatim in the rendered digest, so it can map
to a chunk), and that a corrupted figure fails loudly rather than loading.
"""

from __future__ import annotations

import copy
import json
from decimal import Decimal

import pytest

from scripts import load_city_budget as loader

FIGURES = json.loads(loader.FIGURES_PATH.read_text())


class TestReconciliation:
    """Every breakdown must cross-foot to the audit's stated Total Governmental column."""

    @pytest.mark.parametrize("fy_key", list(FIGURES))
    def test_each_year_reconciles(self, fy_key: str) -> None:
        fig = FIGURES[fy_key]
        figs = loader.build_figures(fy_key, fig, int(fy_key[2:]))
        rev_fund = sum(f.amount for f in figs if f.dimension == "fund" and f.category == "revenue")
        exp_fund = sum(
            f.amount for f in figs if f.dimension == "fund" and f.category == "expenditure"
        )
        fb_fund = sum(
            f.amount for f in figs if f.dimension == "fund" and f.category == "fund_balance"
        )
        by_source = sum(f.amount for f in figs if f.dimension == "source")
        by_function = sum(f.amount for f in figs if f.dimension == "function")
        assert rev_fund == Decimal(fig["revenue"]["total"])
        assert exp_fund == Decimal(fig["expenditure"]["total"])
        assert fb_fund == Decimal(fig["fund_balance"]["total"])
        assert by_source == Decimal(fig["revenue"]["total"])
        assert by_function == Decimal(fig["expenditure"]["total"])

    def test_corrupted_total_fails_loudly(self) -> None:
        fig = copy.deepcopy(FIGURES["FY2024"])
        fig["revenue"]["total"] += 1  # break the checksum
        with pytest.raises(SystemExit):
            loader.build_figures("FY2024", fig, 2024)


class TestDigestCitability:
    """Every cited figure row must appear verbatim in the digest, or the chunk map fails."""

    @pytest.mark.parametrize("fy_key", list(FIGURES))
    def test_every_quote_is_in_the_digest(self, fy_key: str) -> None:
        end_year = int(fy_key[2:])
        figs = loader.build_figures(fy_key, FIGURES[fy_key], end_year)
        md = loader.render_digest(f"{end_year - 1}-{end_year}", end_year, figs)
        for f in figs:
            assert f.quote in md


class TestFundColumnsVaryByYear:
    """The major-fund set is read per year (it shrinks 6 -> 5 -> 4 across FY2020-24)."""

    def test_column_counts(self) -> None:
        assert len(FIGURES["FY2020"]["fund_columns"]) == 6
        assert len(FIGURES["FY2022"]["fund_columns"]) == 5
        assert len(FIGURES["FY2024"]["fund_columns"]) == 4


class TestBudgetVsActualRows:
    """General Fund budget-vs-actual yields original/final/actual rows for rev + exp."""

    def test_six_budget_rows_per_year(self) -> None:
        figs = loader.build_figures("FY2024", FIGURES["FY2024"], 2024)
        budget = [f for f in figs if f.dimension == "budget"]
        assert len(budget) == 6  # (revenue, expenditure) x (original, final, actual)
        assert {f.basis for f in budget} == {"original", "final", "actual"}
        assert all(f.fund == "General" for f in budget)
