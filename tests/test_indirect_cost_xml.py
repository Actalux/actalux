"""Tests for the DESE Indirect Cost Calculation XML parser (3 schema variants).

Synthetic tests exercise both the Prior2018 (Textbox) and Tablix layouts plus the
rate-reconciliation guard, and always run. Real-file tests assert the verified
figures (cross-checked against the official PDFs) for one file of each variant and
skip when the local DESE data is absent.
"""

from __future__ import annotations

from decimal import Decimal
from pathlib import Path

import pytest

from actalux.errors import ParseError
from actalux.ingest.indirect_cost_xml import (
    line_md_row,
    parse_indirect_cost,
    render_markdown,
)

DESE = Path(__file__).resolve().parent.parent / "data" / "DESE"
REAL_FILES = {
    "2013-2014": "Indirect_Cost_CalculationPrior2018 (3).xml",
    "2014-2015": "Indirect_Cost_CalculationPrior2018 (2).xml",
    "2015-2016": "Indirect_Cost_CalculationPrior2018 (1).xml",
    "2016-2017": "Indirect_Cost_CalculationPrior2018.xml",
    "2017-2018": "Indirect_Cost_Calculation.xml",
    "2018-2019": "Indirect_Cost_Calculation2019 (6).xml",
    "2019-2020": "Indirect_Cost_Calculation2019 (5).xml",
    "2020-2021": "Indirect_Cost_Calculation2019 (4).xml",
    "2021-2022": "Indirect_Cost_Calculation2019 (3).xml",
    "2022-2023": "Indirect_Cost_Calculation2019 (2).xml",
    "2023-2024": "Indirect_Cost_Calculation2019 (1).xml",
    "2024-2025": "Indirect_Cost_Calculation2019.xml",
}
REAL_PRIOR = DESE / REAL_FILES["2016-2017"]  # Prior2018 variant
REAL_OUTLIER = DESE / REAL_FILES["2017-2018"]  # 2017-18 outlier
REAL_2019 = DESE / REAL_FILES["2024-2025"]  # 2019+ variant

_UNR_LABEL = "{term} Indirect Cost Rate Percentage ((Line 15/Line 17) x 90%)"
_RES_LABEL = "Restricted Indirect Cost Rate Percentage ((Line 16/Line 18) x 90%)"


def _prior2018(*, grand_total: str = "1000", rate_unrestricted: str = "0.81") -> str:
    # line15/line17 = 90/100 -> 0.81; line16/line18 = 9/100 -> 0.081 (each x 90%).
    return (
        '<Report xmlns="Indirect_Cost_CalculationPrior2018" '
        f'Textbox7="{grand_total}" Textbox42="90" Textbox46="9" '
        'Textbox61="100" Textbox64="100" '
        f'Textbox58="{rate_unrestricted}" Textbox71="0.081" '
        f'Textbox56="{_UNR_LABEL.format(term="Non-Restricted")}" '
        f'Textbox69="{_RES_LABEL}" />'
    )


def _tablix(*, term: str = "Unrestricted") -> str:
    def cell(tablix: str, details: str, attr: str, value: str, extra: str = "") -> str:
        coll = f"{details}_Collection"
        return f'<{tablix}><{coll}><{details} {attr}="{value}"{extra} /></{coll}></{tablix}>'

    return (
        '<Report xmlns="Indirect_Cost_Calculation2019">'
        + cell("Tablix2", "Details1", "Total1", "$1,000.00")
        + cell("Tablix16", "Details15", "Total15", "$90.00")
        + cell("Tablix17", "Details12", "Total12", "$9.00")
        + cell("Tablix18", "Details16", "Total16", "$100.00")
        + cell("Tablix19", "Details17", "Total17", "$100.00")
        + cell(
            "Tablix20",
            "Details18",
            "NonRestrictedRate",
            "81.00%",
            extra=f' Calculation="{_UNR_LABEL.format(term=term)}"',
        )
        + cell(
            "Tablix21",
            "Details19",
            "NonRestrictedRate2",
            "8.10%",
            extra=f' Calculation2="{_RES_LABEL}"',
        )
        + "</Report>"
    )


def _write(tmp_path: Path, xml: str) -> Path:
    p = tmp_path / "indirect_cost.xml"
    p.write_text(xml, encoding="utf-8")
    return p


class TestParsePrior2018Synthetic:
    def test_parses_and_reconciles(self, tmp_path: Path) -> None:
        report = parse_indirect_cost(_write(tmp_path, _prior2018()), "2099-2100")
        assert report.term == "Non-Restricted"
        amounts = {line.line_number: line.amount for line in report.lines}
        assert amounts[1] == Decimal("1000")
        assert amounts[15] == Decimal("90")
        assert amounts[18] == Decimal("100")
        assert report.rate_unrestricted == Decimal("0.81")
        assert report.rate_restricted == Decimal("0.081")


class TestParseTablixSynthetic:
    def test_parses_and_reconciles(self, tmp_path: Path) -> None:
        report = parse_indirect_cost(_write(tmp_path, _tablix()), "2099-2100")
        assert report.term == "Unrestricted"
        amounts = {line.line_number: line.amount for line in report.lines}
        assert amounts[1] == Decimal("1000.00")
        assert amounts[15] == Decimal("90.00")
        assert report.rate_unrestricted == Decimal("0.81")  # parsed from "81.00%"
        assert report.rate_restricted == Decimal("0.081")

    def test_term_follows_label(self, tmp_path: Path) -> None:
        report = parse_indirect_cost(_write(tmp_path, _tablix(term="Non-Restricted")), "2099-2100")
        assert report.term == "Non-Restricted"
        # line 15 label carries the report's term, substituted in.
        assert "Allowable Indirect Costs, Non-Restricted" in report.lines[1].description


class TestReconciliationGuards:
    def test_rate_mismatch_fails(self, tmp_path: Path) -> None:
        # Claimed unrestricted rate (0.50) != (90/100) x 90% = 0.81.
        xml = _prior2018(rate_unrestricted="0.50")
        with pytest.raises(ParseError, match="unrestricted rate"):
            parse_indirect_cost(_write(tmp_path, xml), "2099-2100")

    def test_missing_line_fails(self, tmp_path: Path) -> None:
        xml = _prior2018(grand_total="0")
        with pytest.raises(ParseError, match="line 1"):
            parse_indirect_cost(_write(tmp_path, xml), "2099-2100")


@pytest.mark.skipif(not REAL_PRIOR.exists(), reason="local DESE data not present")
class TestRealPrior2018:
    def test_verified_figures(self) -> None:
        report = parse_indirect_cost(REAL_PRIOR, "2016-2017")
        assert report.term == "Non-Restricted"
        amounts = {line.line_number: line.amount for line in report.lines}
        assert amounts[1] == Decimal("59125445.29")
        assert amounts[15] == Decimal("10653745.41")
        assert amounts[16] == Decimal("2313650.72")
        assert amounts[17] == Decimal("38827629.97")
        assert amounts[18] == Decimal("47167724.66")
        assert report.rate_unrestricted == Decimal("0.246947106388116")
        assert report.rate_restricted == Decimal("0.044146408651462")


@pytest.mark.skipif(not REAL_2019.exists(), reason="local DESE data not present")
class TestRealFY2425:
    def test_verified_figures(self) -> None:
        report = parse_indirect_cost(REAL_2019, "2024-2025")
        assert report.term == "Unrestricted"
        amounts = {line.line_number: line.amount for line in report.lines}
        assert amounts[1] == Decimal("75837799.36")
        assert amounts[15] == Decimal("14119975.31")
        assert amounts[16] == Decimal("1555931.93")
        assert report.rate_unrestricted == Decimal("0.2598")
        assert report.rate_restricted == Decimal("0.0228")

    def test_render_carries_verified_rows(self) -> None:
        md = render_markdown(parse_indirect_cost(REAL_2019, "2024-2025"))
        line15 = "| 15 | Allowable Indirect Costs, Unrestricted (Lines 10-14) | $14,119,975.31 |"
        assert line15 in md
        assert "| 1 | Part I, Line 9999 Grand Total - All Funds | $75,837,799.36 |" in md
        assert "25.98%" in md


@pytest.mark.skipif(not REAL_OUTLIER.exists(), reason="local DESE data not present")
class TestRealOutlier:
    def test_tablix_with_nonrestricted_term(self) -> None:
        report = parse_indirect_cost(REAL_OUTLIER, "2017-2018")
        assert report.term == "Non-Restricted"  # outlier: Tablix structure, old term
        assert report.rate_unrestricted == Decimal("0.2443")
        assert line_md_row(report.lines[0]).startswith("| 1 |")


@pytest.mark.parametrize("fy", list(REAL_FILES))
def test_all_real_years_reconcile(fy: str) -> None:
    path = DESE / REAL_FILES[fy]
    if not path.exists():
        pytest.skip("local DESE data not present")
    report = parse_indirect_cost(path, fy)  # raises on any reconciliation failure
    assert len(report.lines) == 5
    assert report.rate_unrestricted > 0
