"""Tests for the DESE Per-Pupil Building-Level Expenditures XML parser.

Synthetic tests exercise parsing + the absolute-dollar reconciliation guards and
always run. Real-file tests assert verified figures against the actual Clayton
Per-Pupil XMLs in data/DESE/ and skip when that local data is absent.
"""

from __future__ import annotations

from decimal import Decimal
from pathlib import Path

import pytest

from actalux.errors import ParseError
from actalux.ingest.perpupil_xml import building_md_row, parse_per_pupil, render_markdown

DESE = Path(__file__).resolve().parent.parent / "data" / "DESE"
# Verified fiscal-year -> file (the user's DESE downloads).
REAL_FILES = {
    "2018-2019": "PerPupilBuildingLevelExpendituresSummary (6).xml",
    "2019-2020": "PerPupilBuildingLevelExpendituresSummary (5).xml",
    "2020-2021": "PerPupilBuildingLevelExpendituresSummary (4).xml",
    "2021-2022": "PerPupilBuildingLevelExpendituresSummary (3).xml",
    "2022-2023": "PerPupilBuildingLevelExpendituresSummary (2).xml",
    "2023-2024": "PerPupilBuildingLevelExpendituresSummary (1).xml",
    "2024-2025": "PerPupilBuildingLevelExpendituresSummary.xml",
}
REAL_2324 = DESE / REAL_FILES["2023-2024"]


def _synthetic(
    *, building_total_override: str | None = None, district_fed_override: str | None = None
) -> str:
    # Two buildings. Per row total = federal + state/local; across buildings the
    # building+district totals sum to the Details2 (9999) district totals.
    a_bldg_total = building_total_override or "100"
    details2_fed = district_fed_override or "45"
    return (
        '<Report xmlns="PerPupilBuildingLevelExpendituresSummary"><Tablix1><table1>'
        # set 1 (per-pupil rates) rows
        '<Detail Building="1000-SCHOOL A" TotalSeptemberMembership1="10" '
        'FederalExpendituresBuilding="1" StateLocalExpendituresBuilding="9" TotalBuilding="10" '
        'FederalExpendituresDistrict="0.5" StateLocalExpendituresDistrict="4.5" TotalDistrict="5" '
        'ExpendituresPerSeptemberMembership="15"/>'
        '<Detail Building="2000-SCHOOL B" TotalSeptemberMembership1="20" '
        'FederalExpendituresBuilding="1" StateLocalExpendituresBuilding="4" TotalBuilding="5" '
        'FederalExpendituresDistrict="0.5" StateLocalExpendituresDistrict="2" TotalDistrict="2.5" '
        'ExpendituresPerSeptemberMembership="7.5"/>'
        # set 2 (absolute $) rows
        f'<Detail Building2="1000-SCHOOL A" TotalSeptemberMembership4="10" '
        f'FederalExpendituresBuilding2="10" StateLocalExpendituresBuilding2="90" '
        f'TotalBuilding2="{a_bldg_total}" FederalExpendituresDistrict2="5" '
        'StateLocalExpendituresDistrict2="45" TotalDistrict2="50" '
        'ExpendituresPerSeptemberMembership2="15"/>'
        '<Detail Building2="2000-SCHOOL B" TotalSeptemberMembership4="20" '
        'FederalExpendituresBuilding2="20" StateLocalExpendituresBuilding2="80" '
        'TotalBuilding2="100" FederalExpendituresDistrict2="10" '
        'StateLocalExpendituresDistrict2="40" TotalDistrict2="50" '
        'ExpendituresPerSeptemberMembership2="7.5"/>'
        "</table1></Tablix1>"
        f'<Tablix4><Details2_Collection><Details2 BuildingEXTotal="9999-DISTRICT TOTALS" '
        f'TotalSeptemberMembership5="30" FederalExpendituresBuilding3="{details2_fed}" '
        'StateLocalExpendituresBuilding3="255" TotalBuilding3="0"/></Details2_Collection></Tablix4>'
        "</Report>"
    )


def _write(tmp_path: Path, xml: str) -> Path:
    p = tmp_path / "perpupil.xml"
    p.write_text(xml, encoding="utf-8")
    return p


class TestParseSynthetic:
    def test_parses_and_reconciles(self, tmp_path: Path) -> None:
        report = parse_per_pupil(_write(tmp_path, _synthetic()), "2099-2100")
        assert report.fiscal_year == "2099-2100"
        assert len(report.absolute) == 2
        assert len(report.per_pupil) == 2
        assert report.district_total == Decimal("300")
        a = report.absolute[0]
        assert (a.code, a.name) == ("1000", "School A")
        assert a.building_total == Decimal("100")

    def test_building_md_row_format(self, tmp_path: Path) -> None:
        report = parse_per_pupil(_write(tmp_path, _synthetic()), "2099-2100")
        assert building_md_row(report.absolute[0]) == (
            "| 1000 School A | $100.00 | $50.00 | $150.00 |"
        )


class TestReconciliationGuards:
    def test_row_total_mismatch_fails(self, tmp_path: Path) -> None:
        # building_total no longer equals federal + state/local for School A.
        xml = _synthetic(building_total_override="101")
        with pytest.raises(ParseError, match="building total"):
            parse_per_pupil(_write(tmp_path, xml), "2099-2100")

    def test_district_total_mismatch_fails(self, tmp_path: Path) -> None:
        # Details2 federal no longer equals the sum across buildings.
        xml = _synthetic(district_fed_override="44")
        with pytest.raises(ParseError, match="federal"):
            parse_per_pupil(_write(tmp_path, xml), "2099-2100")


@pytest.mark.skipif(not REAL_2324.exists(), reason="local DESE data not present")
class TestRealFY2324:
    def test_verified_absolute_figures(self) -> None:
        report = parse_per_pupil(REAL_2324, "2023-2024")
        assert len(report.absolute) == 5  # five Clayton school buildings
        assert report.district_federal == Decimal("599678.14")
        assert report.district_state_local == Decimal("54739584.33")
        by_name = {b.name: b for b in report.absolute}
        chs = by_name["Clayton High"]
        assert chs.building_total == Decimal("15187465.29")
        assert chs.district_total == Decimal("4976756.66")

    def test_render_carries_verified_row(self) -> None:
        md = render_markdown(parse_per_pupil(REAL_2324, "2023-2024"))
        assert "| 1050 Clayton High | $15,187,465.29 |" in md
        assert "September-Membership Basis" in md  # per-pupil table present for context


@pytest.mark.parametrize("fy", list(REAL_FILES))
def test_all_real_years_reconcile(fy: str) -> None:
    path = DESE / REAL_FILES[fy]
    if not path.exists():
        pytest.skip("local DESE data not present")
    report = parse_per_pupil(path, fy)  # raises on any reconciliation failure
    assert report.absolute
    assert report.district_total > 0
