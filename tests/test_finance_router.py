"""Tests for structured-finance routing.

Two layers: the keyword router (finance_intent) is pure and tested directly
against the eval finance queries plus negative controls; the evidence builder
(build_finance_evidence) is tested with the two DB helpers monkeypatched, so the
across-funds aggregation and the citeable-dict shape are pinned without a live
Supabase. The router is the new judgment surface -- the cases below encode which
queries it must and must not claim.
"""

from __future__ import annotations

import pytest

from actalux.search import finance
from actalux.search.finance import FinanceIntent, build_finance_evidence, finance_intent


class TestRouterRoutes:
    """Queries that must resolve to a structured intent, with the right slice."""

    def test_function_query_aggregates_across_funds(self) -> None:
        it = finance_intent("how much did the district spend on instruction")
        assert it is not None
        assert it.dimension == "function"
        assert it.category == "expenditure"
        assert it.subcategories == ("Instruction",)
        assert it.aggregate_funds is True

    def test_named_fund_total_with_year(self) -> None:
        it = finance_intent("general fund total expenditures fiscal year 2024")
        assert it == FinanceIntent(
            measure=it.measure,  # opaque label; identity-checked via the rest
            dimension="fund",
            category="expenditure",
            funds=("General",),
            subcategories=("Total expenditures",),
            fiscal_year="2023-2024",  # fiscal years are named by their end year
            aggregate_funds=False,
        )

    def test_fund_balance_over_time_is_district_total(self) -> None:
        it = finance_intent("ending fund balance and district reserves over time")
        assert it is not None
        assert it.category == "fund_balance"
        assert it.funds == ()  # no specific fund -> district total
        assert it.aggregate_funds is True
        assert it.fiscal_year is None  # "over time" suppresses any single-year filter

    def test_debt_principal_and_interest_pulls_both_subcategories(self) -> None:
        it = finance_intent("debt service principal and interest payments")
        assert it is not None
        assert it.dimension == "function"
        assert set(it.subcategories) == {
            "Debt service - Principal retirements",
            "Debt service - Interest and other charges",
        }

    def test_capital_projects_fund(self) -> None:
        it = finance_intent("capital projects fund spending")
        assert it is not None
        assert it.dimension == "fund"
        assert it.funds == ("Capital Projects",)
        assert it.category == "expenditure"

    def test_operation_of_plant_function(self) -> None:
        it = finance_intent("operation of plant and facilities maintenance costs")
        assert it is not None
        assert it.dimension == "function"
        assert it.subcategories == ("Operation of plant",)

    def test_named_revenue_source(self) -> None:
        it = finance_intent("how much state revenue did the district receive")
        assert it is not None
        assert it.dimension == "source"
        assert it.category == "revenue"
        assert it.subcategories == ("State",)

    def test_year_token_maps_to_fiscal_year(self) -> None:
        it = finance_intent("general fund expenditures in 2022")
        assert it is not None
        assert it.fiscal_year == "2021-2022"

    def test_trend_word_overrides_year_token(self) -> None:
        it = finance_intent("general fund expenditures by year since 2020")
        assert it is not None
        assert it.fiscal_year is None  # "by year" is a trend ask


class TestRouterDeclines:
    """Queries that must stay on the text path (return None)."""

    @pytest.mark.parametrize(
        "query",
        [
            "per-pupil expenditure by building",  # no structured per-building figure
            "property tax rate hearing and tax levy",  # a millage, not a fund figure
            "superintendent contract and evaluation",  # governance, no figure
            "closed session topics",  # governance
            "tell me about the budget",  # narrative, no figure category
            "high school math instruction curriculum map grades 9-12",  # 'instruction', no measure
            "school principal evaluation",  # 'principal' is a person here, not debt
            "how much does the math program cost",  # figure verb but no structured label
        ],
    )
    def test_declines(self, query: str) -> None:
        assert finance_intent(query) is None


# --- Evidence builder (DB helpers monkeypatched) -----------------------------

_DOC = {1: {"meeting_date": "2024-09-01", "meeting_title": "ACFR FY24", "document_type": "report"}}


def _item(fy: str, fund: str, sub: str, amount: float, cid: int, cat: str = "expenditure") -> dict:
    return {
        "fiscal_year": fy,
        "fund": fund,
        "subcategory": sub,
        "category": cat,
        "amount": amount,
        "chunk_id": cid,
        "document_id": 1,
        "source_quote": f"{sub} ... {amount:,.0f}",
    }


class TestEvidenceBuilder:
    def _patch(self, monkeypatch, rows: list[dict]) -> None:
        monkeypatch.setattr(finance, "get_budget_line_items", lambda *a, **k: rows)
        monkeypatch.setattr(finance, "get_documents", lambda *a, **k: _DOC)

    def test_aggregates_funds_into_one_citeable_row_per_year(self, monkeypatch) -> None:
        # Instruction split across two funds in one year -> one summed row.
        rows = [
            _item("2023-2024", "General", "Instruction", 3_000_000, 10),
            _item("2023-2024", "Special Revenue (Teachers)", "Instruction", 27_000_000, 10),
        ]
        self._patch(monkeypatch, rows)
        intent = FinanceIntent(
            measure="x",
            dimension="function",
            category="expenditure",
            subcategories=("Instruction",),
            aggregate_funds=True,
        )
        evidence = build_finance_evidence(None, intent)
        assert len(evidence) == 1
        e = evidence[0]
        assert "$30,000,000" in e["content"]  # 3M + 27M summed
        assert "all governmental funds" in e["content"]
        assert e["chunk_id"] == 10
        assert e["hash_id"] == "#q000a"  # chunk_hash_id(10)
        assert e["document_type"] == "report"

    def test_per_fund_rows_when_not_aggregating(self, monkeypatch) -> None:
        rows = [
            _item("2023-2024", "Capital Projects", "Total expenditures", 5_600_000, 21),
            _item("2024-2025", "Capital Projects", "Total expenditures", 6_100_000, 22),
        ]
        self._patch(monkeypatch, rows)
        intent = FinanceIntent(
            measure="x",
            dimension="fund",
            category="expenditure",
            funds=("Capital Projects",),
            subcategories=("Total expenditures",),
            aggregate_funds=False,
        )
        evidence = build_finance_evidence(None, intent)
        assert len(evidence) == 2
        # most-recent fiscal year first
        assert "2024-2025" in evidence[0]["content"]
        assert "Capital Projects Fund" in evidence[0]["content"]

    def test_no_rows_returns_empty(self, monkeypatch) -> None:
        self._patch(monkeypatch, [])
        intent = FinanceIntent(measure="x", dimension="fund", category="expenditure")
        assert build_finance_evidence(None, intent) == []

    def test_entity_id_reaches_the_query(self, monkeypatch) -> None:
        # The body scope must reach get_budget_line_items, so a finance ask on a
        # body with no budget data cannot surface another body's figures.
        captured: dict = {}

        def fake_get(_client, **kwargs):
            captured.update(kwargs)
            return []

        monkeypatch.setattr(finance, "get_budget_line_items", fake_get)
        monkeypatch.setattr(finance, "get_documents", lambda *a, **k: _DOC)
        intent = FinanceIntent(measure="x", dimension="fund", category="expenditure")
        build_finance_evidence(None, intent, entity_id=2)
        assert captured.get("entity_id") == 2


class TestAssembleEvidenceScoping:
    """The finance route must inherit the answer's entity filter (cross-talk fix)."""

    def test_finance_route_scopes_to_filter_entity(self, monkeypatch) -> None:
        from actalux.search.answer import assemble_evidence
        from actalux.search.hybrid import SearchFilters

        captured: dict = {}

        def fake_get(_client, **kwargs):
            captured.update(kwargs)
            return [_item("2023-2024", "General", "Total expenditures", 1_000_000, 5)]

        monkeypatch.setattr(finance, "get_budget_line_items", fake_get)
        monkeypatch.setattr(finance, "get_documents", lambda *a, **k: _DOC)
        _evidence, route = assemble_evidence(
            None,
            "general fund total expenditures",
            [0.0],
            filters=SearchFilters(entity_id=7),
        )
        assert route == "structured-finance"
        assert captured.get("entity_id") == 7
