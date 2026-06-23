"""Tests for the City of Clayton adopted-budget + CIP loader.

Covers reconciliation-on-build (revenue-by-fund == revenue-by-type; expenditure
reconciles to the printed total within the source's own $1 rounding), the
digest-citability invariant, and the dimension split (planned summary under
``proposed_*`` + the multi-year CIP under ``cip``).
"""

from __future__ import annotations

import copy
import json

import pytest

from scripts import load_city_adopted_budget as loader

DATA = json.loads(loader.FIGURES_PATH.read_text())


class TestReconciliation:
    def test_revenue_fund_equals_type(self) -> None:
        figs = loader.build_figures(DATA)  # raises if any gate fails
        rev_fund = sum(f.amount for f in figs if f.dimension == "proposed_fund")
        rev_type = sum(f.amount for f in figs if f.dimension == "proposed_source")
        assert rev_fund == rev_type == DATA["revenue_by_fund"]["total"]

    def test_corrupted_revenue_total_fails(self) -> None:
        bad = copy.deepcopy(DATA)
        bad["revenue_by_type"]["total"] += 100  # break the gate
        with pytest.raises(SystemExit):
            loader.build_figures(bad)

    def test_two_dollar_department_gap_would_fail(self) -> None:
        # The source carries a $1 rounding (tolerated); a $2 gap must fail.
        bad = copy.deepcopy(DATA)
        first = next(iter(bad["expenditure_by_department"]["items"]))
        bad["expenditure_by_department"]["items"][first] -= 2
        with pytest.raises(SystemExit):
            loader.build_figures(bad)


class TestDigestAndDimensions:
    def test_every_quote_in_digest(self) -> None:
        figs = loader.build_figures(DATA)
        md = loader.render_digest(figs)
        for f in figs:
            assert f.quote in md

    def test_dimension_counts(self) -> None:
        from collections import Counter

        figs = loader.build_figures(DATA)
        counts = Counter(f.dimension for f in figs)
        assert counts["proposed_fund"] == len(DATA["revenue_by_fund"]["items"])
        assert counts["proposed_source"] == len(DATA["revenue_by_type"]["items"])
        assert counts["proposed_function"] == len(DATA["expenditure_by_department"]["items"])
        # CIP: one row per nonzero project-year cell.
        cip_cells = sum(1 for p in DATA["cip"]["projects"] for v in p["by_year"].values() if v > 0)
        assert counts["cip"] == cip_cells

    def test_cip_rows_span_multiple_fiscal_years(self) -> None:
        figs = loader.build_figures(DATA)
        cip_years = {f.fiscal_year for f in figs if f.dimension == "cip"}
        assert len(cip_years) >= 2  # the CIP is genuinely multi-year
