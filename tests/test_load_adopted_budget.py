"""Tests for the FY2024-2025 proposed-budget loader (scripts/load_adopted_budget.py).

Covers reconciliation-on-load (each breakdown sums to the budget's stated totals)
and the row-isolation invariants that keep the proposed figures from leaking into
the existing audited-actuals views.
"""

from __future__ import annotations

from decimal import Decimal

import pytest

from scripts import load_adopted_budget as loader


@pytest.fixture(scope="module")
def items():
    return loader.build_line_items()


class TestReconciliation:
    """Every breakdown must cross-foot to the budget's stated totals."""

    def test_revenue_by_source_sums_to_total_revenue(self, items) -> None:
        total = sum(it.amount for it in items if it.dimension == "proposed_source")
        assert total == Decimal(loader.REVENUE_TOTAL) == Decimal(77321920)

    def test_revenue_by_fund_sums_to_total_revenue(self, items) -> None:
        total = sum(
            it.amount
            for it in items
            if it.dimension == "proposed_fund" and it.category == "revenue"
        )
        assert total == Decimal(loader.REVENUE_TOTAL)

    def test_expenditure_by_object_sums_to_total_expenditures(self, items) -> None:
        total = sum(it.amount for it in items if it.dimension == "proposed_object")
        assert total == Decimal(loader.EXPENDITURE_TOTAL) == Decimal(76416879)

    def test_expenditure_by_function_sums_to_total_expenditures(self, items) -> None:
        total = sum(it.amount for it in items if it.dimension == "proposed_function")
        assert total == Decimal(loader.EXPENDITURE_TOTAL)

    def test_object_and_function_reconcile_to_the_same_total(self, items) -> None:
        obj = sum(it.amount for it in items if it.dimension == "proposed_object")
        fn = sum(it.amount for it in items if it.dimension == "proposed_function")
        assert obj == fn

    def test_ending_fund_balance_sums_to_stated_total(self, items) -> None:
        total = sum(
            it.amount
            for it in items
            if it.dimension == "proposed_fund" and it.category == "fund_balance"
        )
        assert total == Decimal(loader.END_FUND_BALANCE_TOTAL) == Decimal(45405041)

    def test_fund_balance_rollforward_holds(self) -> None:
        # End == Beg + Change and Change == Revenue - Expenditure, per fund.
        for i in range(len(loader.FUNDS)):
            change = loader.CHANGE_IN_FUND_BALANCE_BY_FUND[i]
            assert change == loader.REVENUE_BY_FUND[i] - loader.EXPENDITURE_BY_FUND[i]
            assert loader.END_FUND_BALANCE_BY_FUND[i] == loader.BEG_FUND_BALANCE_BY_FUND[i] + change

    def test_reconciliation_fails_loudly_on_a_bad_figure(self, monkeypatch) -> None:
        # Corrupt one source figure; build_line_items must raise, not publish.
        bad = dict(loader.SOURCES)
        cells, _stated, quote = bad["County Revenue"]
        bad["County Revenue"] = ([cells[0] + 1, *cells[1:]], _stated, quote)
        monkeypatch.setattr(loader, "SOURCES", bad)
        with pytest.raises(SystemExit):
            loader.build_line_items()

    def test_reconciliation_catches_a_fund_column_error(self, monkeypatch) -> None:
        # A fund-column error that keeps every ROW total correct (move $1 between
        # two funds within one row) must still be caught by the column check.
        bad = dict(loader.SOURCES)
        cells, stated, quote = bad["County Revenue"]
        moved = [cells[0] + 1, cells[1] - 1, *cells[2:]]
        bad["County Revenue"] = (moved, stated, quote)  # row total unchanged
        monkeypatch.setattr(loader, "SOURCES", bad)
        with pytest.raises(SystemExit):
            loader.build_line_items()


class TestRowIsolation:
    """The proposed rows must be namespaced + basis-flagged so existing views
    (dimension fund/source/function/budget, no basis filter) never see them."""

    _ACTUAL_DIMENSIONS = {"fund", "source", "function", "budget"}

    def test_every_row_is_basis_proposed(self, items) -> None:
        assert items
        assert all(it.basis == "proposed" for it in items)

    def test_every_row_is_for_doc_262(self, items) -> None:
        assert all(it.document_id == 262 for it in items)

    def test_every_row_is_fy_2024_2025(self, items) -> None:
        assert all(it.fiscal_year == "2024-2025" for it in items)

    def test_dimensions_are_all_namespaced(self, items) -> None:
        dims = {it.dimension for it in items}
        assert dims == {
            "proposed_fund",
            "proposed_source",
            "proposed_object",
            "proposed_function",
        }

    def test_no_row_collides_with_an_actuals_dimension(self, items) -> None:
        # The hard constraint: not one proposed row carries an actuals dimension.
        assert not any(it.dimension in self._ACTUAL_DIMENSIONS for it in items)

    def test_local_subtotal_not_double_counted(self, items) -> None:
        # Only the five leaf sources load — the "Local Revenue" leaf, never the
        # Property-Taxes/Other split alongside it (which would double-count).
        labels = {it.subcategory for it in items if it.dimension == "proposed_source"}
        assert labels == {
            "Local Revenue",
            "County Revenue",
            "State Revenue",
            "Federal Revenue",
            "Other Revenue",
        }


class TestCitations:
    """Each figure carries a verbatim quote and a chunk to deep-link to."""

    def test_every_row_has_a_chunk_and_quote(self, items) -> None:
        for it in items:
            assert it.chunk_id in (loader.CHUNK_VIEW1, loader.CHUNK_VIEW2)
            assert it.source_quote

    def test_function_views_cite_view2_chunk(self, items) -> None:
        for it in items:
            if it.dimension == "proposed_function":
                assert it.chunk_id == loader.CHUNK_VIEW2

    def test_clipped_function_labels_kept_verbatim(self, items) -> None:
        labels = {it.subcategory for it in items if it.dimension == "proposed_function"}
        # The three clipped labels are preserved exactly as printed (no "fixing").
        assert "Total Instructional Expenditu" in labels
        assert "Total Support Services Expen" in labels
        assert "Total Non-Instruction/Suppo" in labels
