"""Tests for the DESE ASBR Full Report XML parser + reconciler.

The synthetic-XML tests exercise the parsing, money formats, and every
reconciliation guard and always run. The real-file tests assert the verified
figures against the actual Clayton ASBR XMLs in data/DESE/ and skip when that
(untracked, local) data is absent so CI stays green.
"""

from __future__ import annotations

from decimal import Decimal
from pathlib import Path

import pytest

from actalux.errors import ParseError
from actalux.ingest import asbr_xml
from actalux.ingest.asbr_xml import parse_asbr, parse_money, render_markdown

DESE = Path(__file__).resolve().parent.parent / "data" / "DESE"
REAL_2324 = DESE / "ASBR Full Report (2).xml"
REAL_1314 = DESE / "ASBR Full Report (12).xml"


# --- A synthetic, reconciling ASBR XML -------------------------------------
# Funds: General 100, Teachers 200, Debt 300, Capital 400 (beginning).
# Objects sum to 220 = the 9999 expenditure total. Ending = Beg + Rev - Exp.
def _synthetic(
    *,
    object_overrides: dict[str, str] | None = None,
    summary_overrides: dict[tuple[str, str], str] | None = None,
    tablix9_total: str = "260",
) -> str:
    obj = {
        "GENERAL_FUND8": "100",
        "SPECIAL_REVENUE8": "20",
        "DEBT_SERVICE6": "30",
        "CAPITAL_PROJECTS8": "25",
        "TOTAL_UNADJ_RATE7": "15",
        "Textbox339": "10",
        "Textbox301": "20",
        "Textbox302": "220",
    }
    obj.update(object_overrides or {})
    obj_attrs = " ".join(f'{k}="{v}"' for k, v in obj.items())

    # (code, label, General, Teachers, Debt, Capital, Total)
    rows = {
        "3111": ["Beginning Fund Balances", "100", "200", "300", "400", "1000"],
        "5899": ["Total Revenue (See Part II)", "50", "60", "70", "80", "260"],
        "5510": ["Transfer To", "0", "0", "0", "0", "0"],
        "6710": ["Transfer From", "0", "0", "0", "0", "0"],
        "9999": ["Expenditures (See Part III)", "40", "50", "60", "70", "220"],
        "3112": ["Ending Fund Balances", "110", "210", "310", "410", "1040"],
    }
    for (code, field), value in (summary_overrides or {}).items():
        idx = {"label": 0, "General": 1, "Teachers": 2, "Debt": 3, "Capital": 4, "Total": 5}[field]
        rows[code][idx] = value

    details2 = "\n".join(
        f'<Details2 REVENUE_CODE="{code}" Textbox392="{r[0]}" GENERAL_FUND="{r[1]}" '
        f'SPECIAL_REVENUE="{r[2]}" Textbox393="{r[3]}" CAPITAL_PROJECTS="{r[4]}" TOTAL="{r[5]}"/>'
        for code, r in rows.items()
    )
    # Tablix9 "Total Revenues" (5899) cross-check row; per-fund matches the 5899
    # Part I row (50/60/70/80), total defaults to the matching 260.
    tablix9 = (
        f'<Tablix9><Details8_Collection><Details8 REVENUE_CODE2="5899" Name="Total Revenues" '
        f'GENERAL_FUND2="50" SPECIAL_REVENUE2="60" DEBT_SERVICE2="70" CAPITAL_PROJECTS2="80" '
        f'TOTAL_UNADJ_RATE2="{tablix9_total}"/></Details8_Collection></Tablix9>'
    )
    return (
        '<Report xmlns="ASBR_x0020_Full_x0020_Report">'
        '<Tablix1 Textbox36="ASBR Fiscal Year 2099-2100"/>'
        f'<Tablix23><Details21_Collection><Details21 SUPPORT_SERVICE_CODE="9999" {obj_attrs}/>'
        "</Details21_Collection></Tablix23>"
        f"<Tablix3><Details2_Collection>{details2}</Details2_Collection></Tablix3>"
        f"{tablix9}"
        "</Report>"
    )


def _write(tmp_path: Path, xml: str) -> Path:
    path = tmp_path / "asbr.xml"
    path.write_text(xml, encoding="utf-8")
    return path


class TestParseMoney:
    def test_dollar_formatted(self) -> None:
        assert parse_money("$29,794,237.97") == Decimal("29794237.97")

    def test_parenthetical_negative(self) -> None:
        assert parse_money("($362,049.22)") == Decimal("-362049.22")

    def test_bare_decimal(self) -> None:
        assert parse_money("47555384.08") == Decimal("47555384.08")

    def test_dash_and_blank_and_zero_are_zero(self) -> None:
        assert parse_money("-") == Decimal(0)
        assert parse_money("$-") == Decimal(0)
        assert parse_money("$0.00") == Decimal(0)
        assert parse_money(None) == Decimal(0)


class TestParseSynthetic:
    def test_parses_and_reconciles(self, tmp_path: Path) -> None:
        report = parse_asbr(_write(tmp_path, _synthetic()))
        assert report.fiscal_year == "2099-2100"
        assert report.expenditure_grand_total == Decimal("220")
        assert report.expenditure_total == Decimal("220")
        assert report.revenue_total == Decimal("260")
        assert report.ending_balance_total == Decimal("1040")

    def test_object_codes_and_labels_in_order(self, tmp_path: Path) -> None:
        report = parse_asbr(_write(tmp_path, _synthetic()))
        assert [o.code for o in report.objects] == [
            "6110",
            "6150",
            "6200",
            "6300",
            "6400",
            "6500",
            "6600",
        ]
        salaries = report.objects[0]
        assert salaries.label == "Certificated Salaries"
        assert salaries.amount == Decimal("100")


class TestReconciliationGuards:
    def test_object_sum_mismatch_fails(self, tmp_path: Path) -> None:
        # Bump one object so the seven no longer sum to the grand total.
        xml = _synthetic(object_overrides={"Textbox301": "21"})
        with pytest.raises(ParseError, match="object columns sum"):
            parse_asbr(_write(tmp_path, xml))

    def test_grand_total_vs_fund_total_mismatch_fails(self, tmp_path: Path) -> None:
        # Make Tablix23 grand total disagree with Tablix3 expenditure total.
        # Keep objects footing to the new grand total so check #2 is what trips.
        xml = _synthetic(object_overrides={"Textbox301": "30", "Textbox302": "230"})
        with pytest.raises(ParseError, match="grand total"):
            parse_asbr(_write(tmp_path, xml))

    def test_rollforward_mismatch_fails(self, tmp_path: Path) -> None:
        xml = _synthetic(summary_overrides={("3112", "Total"): "9999", ("3112", "General"): "9999"})
        with pytest.raises(ParseError, match="roll-forward|fund cells sum"):
            parse_asbr(_write(tmp_path, xml))

    def test_missing_required_row_fails(self, tmp_path: Path) -> None:
        xml = _synthetic().replace('REVENUE_CODE="3112"', 'REVENUE_CODE="3199"')
        with pytest.raises(ParseError, match="missing row 3112"):
            parse_asbr(_write(tmp_path, xml))

    def test_tablix9_revenue_mismatch_fails(self, tmp_path: Path) -> None:
        # Tablix9 total revenue disagrees with the Part I (Tablix3) revenue total.
        xml = _synthetic(tablix9_total="261")
        with pytest.raises(ParseError, match="Tablix9 total revenue"):
            parse_asbr(_write(tmp_path, xml))


class TestRenderMarkdown:
    def test_contains_verbatim_object_and_total_rows(self, tmp_path: Path) -> None:
        report = parse_asbr(_write(tmp_path, _synthetic()))
        md = render_markdown(report)
        assert "| 6110 Certificated Salaries | $100.00 |" in md
        assert "| Total Expenditures | $220.00 |" in md
        assert "Fiscal Year 2099-2100" in md


@pytest.mark.skipif(not REAL_2324.exists(), reason="local DESE data not present")
class TestRealFY2324:
    def test_verified_totals_and_object_mapping(self) -> None:
        report = parse_asbr(REAL_2324)
        assert report.fiscal_year == "2023-2024"
        assert report.expenditure_grand_total == Decimal("75603483.99")
        assert report.revenue_total == Decimal("76133808.31")
        assert report.beginning_balance_total == Decimal("47555384.08")
        assert report.ending_balance_total == Decimal("48085708.40")
        # Pin ALL seven object amounts (verified against the PDF "Part III-B
        # Expenditures Grand Total" table) so a future permutation of OBJECT_COLUMNS
        # — which reconciliation alone can't catch, since the seven still sum to the
        # grand total — fails loudly here.
        objects = {o.code: o for o in report.objects}
        assert objects["6110"] == report.objects[0]  # order preserved
        assert objects["6110"].label == "Certificated Salaries"
        expected = {
            "6110": "29794237.97",
            "6150": "9064920.42",
            "6200": "11645161.54",
            "6300": "5642713.28",
            "6400": "4774164.78",
            "6500": "3321948.81",
            "6600": "11360337.19",
        }
        for code, amount in expected.items():
            assert objects[code].amount == Decimal(amount)

    def test_render_carries_verified_rows(self) -> None:
        md = render_markdown(parse_asbr(REAL_2324))
        assert "| 6110 Certificated Salaries | $29,794,237.97 |" in md
        assert "$75,603,483.99" in md


@pytest.mark.skipif(not REAL_1314.exists(), reason="local DESE data not present")
def test_real_fy1314_reconciles() -> None:
    report = parse_asbr(REAL_1314)
    assert report.fiscal_year == "2013-2014"
    assert report.expenditure_grand_total == Decimal("63416581.11")


@pytest.mark.skipif(not REAL_2324.exists(), reason="local DESE data not present")
def test_object_columns_are_seven() -> None:
    assert len(asbr_xml.OBJECT_COLUMNS) == 7
    assert parse_asbr(REAL_2324).objects[0].code == "6110"
