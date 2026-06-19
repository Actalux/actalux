"""Tests for the DESE Local Effort XML parser.

Synthetic tests exercise parsing + the grand-total reconciliation guard and
always run. Real-file tests assert verified figures against the actual Clayton
Local Effort XMLs in data/DESE/ and skip when that local data is absent.
"""

from __future__ import annotations

from decimal import Decimal
from pathlib import Path

import pytest

from actalux.errors import ParseError
from actalux.ingest.local_effort_xml import line_md_row, parse_local_effort, render_markdown

DESE = Path(__file__).resolve().parent.parent / "data" / "DESE"
# Verified fiscal-year -> file (the user's DESE downloads).
REAL_FILES = {
    "2013-2014": "Local_Effort (11).xml",
    "2014-2015": "Local_Effort (10).xml",
    "2015-2016": "Local_Effort (9).xml",
    "2016-2017": "Local_Effort (8).xml",
    "2017-2018": "Local_Effort (7).xml",
    "2018-2019": "Local_Effort (6).xml",
    "2019-2020": "Local_Effort (5).xml",
    "2020-2021": "Local_Effort (4).xml",
    "2021-2022": "Local_Effort (3).xml",
    "2022-2023": "Local_Effort (2).xml",
    "2023-2024": "Local_Effort (1).xml",
    "2024-2025": "Local_Effort.xml",
}
REAL_2425 = DESE / REAL_FILES["2024-2025"]


def _synthetic(*, grand_total_override: str | None = None, omit_lines: bool = False) -> str:
    # Three lines (incl. a negative) summing to the stated grand total of 5000.
    grand = grand_total_override or "5000.00"
    rows = (
        ""
        if omit_lines
        else (
            '<Details Textbox16="Part II, Line 5111 Current Taxes" TOTAL="4800.00" />'
            '<Details Textbox16="Part II, Line 5112 Delinquent Taxes" TOTAL="-100.00" />'
            '<Details Textbox16="Part II, Line 5114 Financial Institution Tax" TOTAL="300.00" />'
        )
    )
    return (
        '<Report xmlns="Local_Effort" Textbox2="Resident I ADA plus Resident II ADA" '
        'Textbox3="100.0000" Textbox8="Total taxes per ADA" Textbox12="50.0">'
        f'<Tablix1 Textbox15="{grand}"><Details_Collection>{rows}'
        "</Details_Collection></Tablix1></Report>"
    )


def _write(tmp_path: Path, xml: str) -> Path:
    p = tmp_path / "local_effort.xml"
    p.write_text(xml, encoding="utf-8")
    return p


class TestParseSynthetic:
    def test_parses_and_reconciles(self, tmp_path: Path) -> None:
        report = parse_local_effort(_write(tmp_path, _synthetic()), "2099-2100")
        assert report.fiscal_year == "2099-2100"
        assert len(report.lines) == 3
        assert report.grand_total == Decimal("5000.00")
        assert report.lines_total == Decimal("5000.00")
        assert report.ada == Decimal("100.0000")
        assert report.per_ada == Decimal("50.0")
        assert report.lines[0].description == "Part II, Line 5111 Current Taxes"
        assert report.lines[0].amount == Decimal("4800.00")

    def test_line_md_row_format(self, tmp_path: Path) -> None:
        report = parse_local_effort(_write(tmp_path, _synthetic()), "2099-2100")
        assert line_md_row(report.lines[1]) == "| Part II, Line 5112 Delinquent Taxes | $-100.00 |"


class TestReconciliationGuards:
    def test_grand_total_mismatch_fails(self, tmp_path: Path) -> None:
        xml = _synthetic(grand_total_override="5001.00")
        with pytest.raises(ParseError, match="grand total"):
            parse_local_effort(_write(tmp_path, xml), "2099-2100")

    def test_no_lines_fails(self, tmp_path: Path) -> None:
        xml = _synthetic(omit_lines=True)
        with pytest.raises(ParseError, match="no tax lines"):
            parse_local_effort(_write(tmp_path, xml), "2099-2100")


@pytest.mark.skipif(not REAL_2425.exists(), reason="local DESE data not present")
class TestRealFY2425:
    def test_verified_figures(self) -> None:
        report = parse_local_effort(REAL_2425, "2024-2025")
        assert len(report.lines) == 5
        assert report.grand_total == Decimal("63958343.62")
        assert report.ada == Decimal("2210.9933")
        by_desc = {line.description: line.amount for line in report.lines}
        assert by_desc["Part II, Line 5111 Current Taxes"] == Decimal("62962140.82")
        assert by_desc["Part II, Line 5112 Delinquent Taxes"] == Decimal("-978405.61")

    def test_render_carries_verified_rows(self) -> None:
        md = render_markdown(parse_local_effort(REAL_2425, "2024-2025"))
        assert "| Part II, Line 5111 Current Taxes | $62,962,140.82 |" in md
        assert "| Total Local Effort (taxes) | $63,958,343.62 |" in md


@pytest.mark.parametrize("fy", list(REAL_FILES))
def test_all_real_years_reconcile(fy: str) -> None:
    path = DESE / REAL_FILES[fy]
    if not path.exists():
        pytest.skip("local DESE data not present")
    report = parse_local_effort(path, fy)  # raises on any reconciliation failure
    assert report.lines
    assert report.grand_total != 0
