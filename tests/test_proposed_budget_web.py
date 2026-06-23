"""Web-layer tests for the proposed-budget (A6) section of the Budget page.

Two concerns:

1. EXISTING-VIEWS-UNAFFECTED regression — proves that adding proposed rows
   (dimension 'proposed_*', basis 'proposed') to budget_line_items does NOT
   change what the existing fund/source/function/budget views read, because
   those queries filter on the actuals dimensions and never see the namespaced
   proposed rows.

2. New-section render — the Budget page renders a clearly-separated "Proposed
   Budget (June 2024)" section with cited figures, and never labels it "Adopted".
"""

from __future__ import annotations

from unittest.mock import patch

from fastapi.testclient import TestClient

from actalux.db import get_budget_line_items, get_proposed_budget_line_items
from actalux.web.app import _capital_outlay_context, app

client = TestClient(app, raise_server_exceptions=False)

_FAKE_ENTITY = {
    "id": 1,
    "body_slug": "schools",
    "type": "school_district",
    "display_name": "Clayton School District",
    "place": {"state": "mo", "slug": "clayton", "display_name": "Clayton"},
}


# --- A filtering fake Supabase client over an in-memory budget_line_items table.
# It applies .eq()/.is_() predicates and .order() to the fixture rows, so it
# exercises the REAL query construction in db.py rather than stubbing it out.


class _FakeQuery:
    def __init__(self, rows: list[dict]) -> None:
        self._rows = rows
        self._filters: list[tuple] = []
        self._order: list[tuple[str, bool]] = []

    def select(self, *_a, **_k) -> _FakeQuery:
        return self

    def eq(self, col: str, val: object) -> _FakeQuery:
        # Embedded-resource predicates (e.g. "documents.entity_id") filter a joined
        # table; this flat single-entity fixture can't model the join, and entity
        # scoping is covered by the finance + real-data checks, so skip them here.
        if "." not in col:
            self._filters.append(("eq", col, val))
        return self

    def is_(self, col: str, val: object) -> _FakeQuery:
        self._filters.append(("is", col, val))
        return self

    def order(self, col: str, desc: bool = False) -> _FakeQuery:
        self._order.append((col, desc))
        return self

    def execute(self) -> _FakeResult:
        out = []
        for r in self._rows:
            keep = True
            for kind, col, val in self._filters:
                if kind == "eq" and r.get(col) != val:
                    keep = False
                    break
                if kind == "is" and val == "null" and r.get(col) is not None:
                    keep = False
                    break
            if keep:
                out.append(r)
        for col, desc in reversed(self._order):
            out.sort(key=lambda x: (x.get(col) is None, x.get(col)), reverse=desc)
        return _FakeResult(out)


class _FakeResult:
    def __init__(self, data: list[dict]) -> None:
        self.data = data
        self.count = len(data)


class _FakeClient:
    def __init__(self, rows: list[dict]) -> None:
        self._rows = rows

    def table(self, _name: str) -> _FakeQuery:
        return _FakeQuery(self._rows)


def _row(dimension, category, fund, sub, amount, basis=None, chunk_id=900):
    return {
        "id": _row.counter,
        "fiscal_year": "2024-2025",
        "dimension": dimension,
        "category": category,
        "fund": fund,
        "subcategory": sub,
        "amount": str(amount),
        "basis": basis,
        "document_id": 262 if basis == "proposed" else 436,
        "chunk_id": chunk_id,
        "source_quote": f"{sub} {amount}",
        "note": "",
    }


_row.counter = 0


def _make_rows():
    """A mixed table: actuals (no basis) + proposed (basis='proposed')."""
    _row.counter = 0
    rows = []
    # Actuals (the existing views read these).
    rows.append(_row("fund", "revenue", "General", "Total revenues", 31609692))
    rows.append(_row("fund", "expenditure", "General", "Total expenditures", 22639043))
    rows.append(_row("source", "revenue", "General", "Local", 30682463))
    rows.append(_row("function", "expenditure", "General", "Instruction", 2900882))
    rows.append(_row("budget", "revenue", "General", "Total revenues", 31708277, basis="actual"))
    # Proposed (namespaced; must be invisible to the actuals views).
    rows.append(
        _row(
            "proposed_fund",
            "revenue",
            "General",
            "Total revenue",
            24866380,
            basis="proposed",
            chunk_id=5109,
        )
    )
    rows.append(
        _row(
            "proposed_fund",
            "revenue",
            "Special Revenue (Teachers)",
            "Total revenue",
            40698910,
            basis="proposed",
            chunk_id=5109,
        )
    )
    # A fund_balance row under proposed_fund: the by-fund REVENUE render must
    # exclude it (the where={"category": "revenue"} filter).
    rows.append(
        _row(
            "proposed_fund",
            "fund_balance",
            "General",
            "End Fund Bal-June 30, 2025",
            25110081,
            basis="proposed",
            chunk_id=5109,
        )
    )
    rows.append(
        _row(
            "proposed_source",
            "revenue",
            "General",
            "Local Revenue",
            23876630,
            basis="proposed",
            chunk_id=5109,
        )
    )
    rows.append(
        _row(
            "proposed_object",
            "expenditure",
            "General",
            "Salaries/Wages",
            9781290,
            basis="proposed",
            chunk_id=5109,
        )
    )
    rows.append(
        _row(
            "proposed_function",
            "expenditure",
            "General",
            "Total Instructional Expenditu",
            3153654,
            basis="proposed",
            chunk_id=5110,
        )
    )
    return rows


class TestExistingViewsUnaffected:
    """The hard constraint: proposed rows must not leak into the actuals views."""

    def _ids(self, rows):
        return sorted(r["id"] for r in rows)

    def test_fund_view_identical_with_and_without_proposed_rows(self) -> None:
        actuals_only = [r for r in _make_rows() if r["basis"] != "proposed"]
        mixed = _make_rows()
        before = get_budget_line_items(_FakeClient(actuals_only), dimension="fund")
        after = get_budget_line_items(_FakeClient(mixed), dimension="fund")
        assert self._ids(before) == self._ids(after)

    def test_source_view_identical(self) -> None:
        actuals_only = [r for r in _make_rows() if r["basis"] != "proposed"]
        mixed = _make_rows()
        before = get_budget_line_items(_FakeClient(actuals_only), dimension="source")
        after = get_budget_line_items(_FakeClient(mixed), dimension="source")
        assert self._ids(before) == self._ids(after)

    def test_function_view_identical(self) -> None:
        actuals_only = [r for r in _make_rows() if r["basis"] != "proposed"]
        mixed = _make_rows()
        before = get_budget_line_items(_FakeClient(actuals_only), dimension="function")
        after = get_budget_line_items(_FakeClient(mixed), dimension="function")
        assert self._ids(before) == self._ids(after)

    def test_budget_vs_actual_view_identical(self) -> None:
        actuals_only = [r for r in _make_rows() if r["basis"] != "proposed"]
        mixed = _make_rows()
        before = get_budget_line_items(_FakeClient(actuals_only), dimension="budget")
        after = get_budget_line_items(_FakeClient(mixed), dimension="budget")
        assert self._ids(before) == self._ids(after)

    def test_no_actuals_view_returns_any_proposed_row(self) -> None:
        mixed = _make_rows()
        for dim in ("fund", "source", "function", "budget"):
            rows = get_budget_line_items(_FakeClient(mixed), dimension=dim)
            assert all(r["basis"] != "proposed" for r in rows)
            assert all(not str(r["dimension"]).startswith("proposed_") for r in rows)

    def test_proposed_helper_returns_only_proposed_rows(self) -> None:
        mixed = _make_rows()
        rows = get_proposed_budget_line_items(_FakeClient(mixed), "2024-2025", "proposed_source")
        assert rows
        assert all(r["basis"] == "proposed" for r in rows)
        assert all(r["dimension"] == "proposed_source" for r in rows)


class TestProposedSectionRender:
    """The Budget page renders the proposed section, citation-first, never 'Adopted'."""

    @patch("actalux.web.app._budget_quote_sections", return_value=[])
    @patch("actalux.web.app._get_db")
    @patch("actalux.web.app.get_entity_by_path", return_value=_FAKE_ENTITY)
    def test_proposed_section_renders_with_citations(self, mock_ent, mock_db, mock_quotes) -> None:
        mock_db.return_value = _FakeClient(_make_rows())
        r = client.get("/mo/clayton/schools/budget")
        assert r.status_code == 200
        # The clearly-separated heading.
        assert "Proposed Budget (June 2024)" in r.text
        # A proposed figure links to its source chunk (citation-first). The
        # verbatim quote itself lives behind that link (/chunk/{id}/source), not
        # inline on the budget page: the raw-quote rows were removed as noise, so
        # traceability is the citation link, not an on-page quote dump.
        assert "/chunk/5109/source" in r.text
        # The source document is cited.
        assert "doc #262" in r.text

    @patch("actalux.web.app._budget_quote_sections", return_value=[])
    @patch("actalux.web.app._get_db")
    @patch("actalux.web.app.get_entity_by_path", return_value=_FAKE_ENTITY)
    def test_proposed_revenue_by_fund_is_per_fund_not_collapsed(
        self, mock_ent, mock_db, mock_quotes
    ) -> None:
        # Regression for the codex MAJOR: by-fund revenue must split per fund,
        # not collapse to a single "Total revenue" row.
        mock_db.return_value = _FakeClient(_make_rows())
        r = client.get("/mo/clayton/schools/budget")
        assert r.status_code == 200
        assert "Proposed revenue by fund" in r.text
        # Both funds appear; the fund_balance row is excluded from the revenue mix.
        assert "Special Revenue (Teachers)" in r.text

    @patch("actalux.web.app._budget_quote_sections", return_value=[])
    @patch("actalux.web.app._get_db")
    @patch("actalux.web.app.get_entity_by_path", return_value=_FAKE_ENTITY)
    def test_proposed_section_never_labelled_adopted(self, mock_ent, mock_db, mock_quotes) -> None:
        mock_db.return_value = _FakeClient(_make_rows())
        r = client.get("/mo/clayton/schools/budget")
        assert r.status_code == 200
        assert "Adopted" not in r.text

    @patch("actalux.web.app._budget_quote_sections", return_value=[])
    @patch("actalux.web.app._get_db")
    @patch("actalux.web.app.get_entity_by_path", return_value=_FAKE_ENTITY)
    def test_no_proposed_section_when_no_proposed_rows(
        self, mock_ent, mock_db, mock_quotes
    ) -> None:
        # Actuals only: the proposed section must not render at all.
        actuals_only = [r for r in _make_rows() if r["basis"] != "proposed"]
        mock_db.return_value = _FakeClient(actuals_only)
        r = client.get("/mo/clayton/schools/budget")
        assert r.status_code == 200
        assert "Proposed Budget (June 2024)" not in r.text

    @patch("actalux.web.app._get_db")
    @patch("actalux.web.app.get_entity_by_path", return_value=_FAKE_ENTITY)
    @patch("actalux.web.app._budget_quote_sections")
    def test_what_district_said_renders_quote_led_cited_records(
        self, mock_quotes, mock_ent, mock_db
    ) -> None:
        """The 'what the district has said' list is quote-led (cited-record cards):
        document identity + one clean verbatim sentence + a link to the original,
        not a raw windowed-snippet dump. See DESIGN.md "Citations resolve to the
        original"."""
        mock_quotes.return_value = [
            {
                "label": "Budget approval & spending",
                "query": "budget officer proposed",
                "results": [
                    {
                        "cite_ref": "7a60af78",
                        "hash_id": "#q7a60af78",
                        "content": (
                            "[] Prior to July, the budget officer submits a proposed "
                            "budget. Other boilerplate follows here."
                        ),
                        "document_type": "budget",
                        "source_portal": "diligent",
                        "meeting_date": "2020-06-24",
                        "meeting_title": "June 24, 2020 Budget",
                        "section": "",
                    }
                ],
            }
        ]
        mock_db.return_value = _FakeClient(_make_rows())
        r = client.get("/mo/clayton/schools/budget")
        assert r.status_code == 200
        # Quote-led cited-record card (not the retired raw-snippet result-item).
        assert "cited-record" in r.text
        # One clean verbatim sentence, with the leading "[]" extraction noise gone.
        assert "Prior to July, the budget officer submits a proposed budget." in r.text
        assert "[] Prior to July" not in r.text
        # Opens the original; the portal is shown in the record meta.
        assert "/chunk/7a60af78/source" in r.text
        assert "Open the original" in r.text


def _capital_row(dimension, sub, fy, amount, citation_id=None):
    """A budget_line_items row for capital-outlay context tests."""
    return {
        "fiscal_year": fy,
        "dimension": dimension,
        "category": "expenditure",
        "fund": "",
        "subcategory": sub,
        "amount": str(amount),
        "basis": "proposed" if dimension == "cip" else None,
        "document_id": 1451 if dimension == "cip" else 1441,
        "chunk_id": 700,
        "citation_id": citation_id,
        "source_quote": f"{sub} {amount}",
        "note": "",
    }


class TestCapitalOutlayContext:
    def test_actuals_and_planned_aggregate_per_year(self) -> None:
        rows = [
            _capital_row("function", "Capital outlay", "2022-2023", 3863391, citation_id="aa"),
            _capital_row("function", "Capital outlay", "2023-2024", 4246444, citation_id="bb"),
            _capital_row("function", "Public safety", "2023-2024", 9000000),  # not capital
            # Two FY2026 CIP projects must aggregate into one planned year.
            _capital_row("cip", "Project A", "2025-2026", 700000),
            _capital_row("cip", "Project B", "2025-2026", 4028078),
        ]
        ctx = _capital_outlay_context(_FakeClient(rows))["capital_outlay"]
        assert ctx is not None
        # Audited actuals: oldest first, only the Capital outlay line.
        assert [(p.fiscal_year, str(p.amount)) for p in ctx["actual"]] == [
            ("2022-2023", "3863391"),
            ("2023-2024", "4246444"),
        ]
        assert ctx["actual"][1].cite_ref == "bb"  # carries the stable citation id
        assert ctx["has_planned"] is True
        svg = str(ctx["svg"])
        assert 'class="bar bar-capital-actual"' in svg
        assert 'class="bar bar-capital-planned"' in svg
        # The aggregated planned bar ($4,728,078) links to the cited CIP section.
        assert 'href="#cip-plan"' in svg
        assert "$4,728,078 (planned)" in svg

    def test_no_capital_rows_omits_the_section(self) -> None:
        rows = [_capital_row("function", "Public safety", "2023-2024", 9000000)]
        assert _capital_outlay_context(_FakeClient(rows)) == {"capital_outlay": None}
