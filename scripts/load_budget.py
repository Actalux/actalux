"""Load verified budget line items into the budget_line_items table.

Source of truth: the figures below are read verbatim from the School
District of Clayton audited financial statements -- specifically the
"Statement of Revenues, Expenditures and Changes in Fund Balances -
Governmental Funds" in each fiscal year's audit. Each figure carries the
document_id and chunk_id of the passage it came from and the verbatim
source row, so every number on the Budget page drills down to its source.

Funds, in column order, follow the audit's presentation: General, Special
Revenue (the Teachers' Fund in Missouri), Debt Service, Capital Projects.

Breakdowns loaded: by fund (dimension='fund'), revenue by source
(dimension='source'), expenditure by function (dimension='function', the
function x fund matrix from the same statement), and budget vs actual
(dimension='budget', cash/budgetary basis from the per-fund budget-and-actual
schedules - kept separate from the GAAP figures above).

Integrity guard: each year's four per-fund figures are asserted to sum to
the audit's stated "Total Governmental Funds" column at load time; the
function matrix is additionally asserted to reconcile both ways (fund columns
to the verified per-fund expenditures, all functions to the grand total). A
transcription error in this file fails loudly rather than publishing a wrong
figure.

Idempotent: replaces all rows in budget_line_items on each run.

Run:
  doppler run --project mac --config dev -- uv run python scripts/load_budget.py --dry-run
  doppler run --project mac --config dev -- uv run python scripts/load_budget.py
"""

from __future__ import annotations

import argparse
import logging
import sys
from decimal import Decimal

from actalux.config import load_config
from actalux.db import get_client, insert_budget_line_items
from actalux.models import BudgetLineItem

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# Audit fund columns, in the order the figure lists below use.
FUNDS = ("General", "Special Revenue (Teachers)", "Debt Service", "Capital Projects")

# Subcategory label per category.
SUBCATEGORY = {
    "revenue": "Total revenues",
    "expenditure": "Total expenditures",
    "fund_balance": "Ending fund balance",
}

# Each entry: one fiscal year's Statement of Revenues, Expenditures and
# Changes in Fund Balances - Governmental Funds. Per-fund lists align with
# FUNDS; *_total is the audit's stated "Total Governmental Funds" column,
# used only as a load-time checksum (not stored).
YEARS: list[dict] = [
    {
        "fiscal_year": "2018-2019",
        "document_id": 429,
        "chunk_id": 7690,
        "page": 25,
        "revenue": [17973694, 29188076, 8600250, 1762832],
        "revenue_total": 57524852,
        "revenue_quote": "Total revenues 17,973,694 29,188,076 8,600,250 1,762,832 57,524,852",
        "expenditure": [18687343, 32177071, 28081872, 1485003],
        "expenditure_total": 80431289,
        "expenditure_quote": (
            "Total expenditures 18,687,343 32,177,071 28,081,872 1,485,003 80,431,289"
        ),
        "fund_balance": [4910517, 7414629, 4986479, 10803558],
        "fund_balance_total": 28115183,
        "fund_balance_quote": (
            "Fund balances at June 30, 2019 $ 4,910,517 $ 7,414,629 "
            "$ 4,986,479 $ 10,803,558 $ 28,115,183"
        ),
    },
    {
        "fiscal_year": "2019-2020",
        "document_id": 428,
        "chunk_id": 7588,
        "page": 25,
        "revenue": [22575075, 39127651, 9317594, 2754682],
        "revenue_total": 73775002,
        "revenue_quote": "Total revenues 22,575,075 39,127,651 9,317,594 2,754,682 73,775,002",
        "expenditure": [17968247, 32669282, 8288792, 9120187],
        "expenditure_total": 68046508,
        "expenditure_quote": (
            "Total expenditures 17,968,247 32,669,282 8,288,792 9,120,187 68,046,508"
        ),
        "fund_balance": [9426902, 13875528, 6357143, 4416641],
        "fund_balance_total": 34076214,
        "fund_balance_quote": (
            "Fund balances at June 30, 2020 $ 9,426,902 $ 13,875,528 "
            "$ 6,357,143 $ 4,416,641 $ 34,076,214"
        ),
    },
    {
        "fiscal_year": "2020-2021",
        "document_id": 427,
        "chunk_id": 7479,
        "page": 25,
        "revenue": [20416619, 33566340, 8030565, 4088629],
        "revenue_total": 66102153,
        "revenue_quote": "Total revenues 20,416,619 33,566,340 8,030,565 4,088,629 66,102,153",
        "expenditure": [17614862, 33856130, 8973208, 5246785],
        "expenditure_total": 65690985,
        "expenditure_quote": (
            "Total expenditures 17,614,862 33,856,130 8,973,208 5,246,785 65,690,985"
        ),
        "fund_balance": [12058750, 13589732, 5414500, 3271255],
        "fund_balance_total": 34334237,
        "fund_balance_quote": (
            "Fund balances at June 30, 2021 $ 12,058,750 $ 13,589,732 "
            "$ 5,414,500 $ 3,271,255 $ 34,334,237"
        ),
    },
    {
        "fiscal_year": "2021-2022",
        "document_id": 426,
        "chunk_id": 7371,
        "page": 25,
        "revenue": [23023796, 37992214, 8961817, 4374905],
        "revenue_total": 74352732,
        "revenue_quote": "Total revenues 23,023,796 37,992,214 8,961,817 4,374,905 74,352,732",
        "expenditure": [18778296, 34551786, 8541502, 4684117],
        "expenditure_total": 66555701,
        "expenditure_quote": (
            "Total expenditures 18,778,296 34,551,786 8,541,502 4,684,117 66,555,701"
        ),
        "fund_balance": [16477856, 17003154, 5834815, 2973198],
        "fund_balance_total": 42289023,
        "fund_balance_quote": (
            "Fund balances at June 30, 2022 $ 16,477,856 $ 17,003,154 "
            "$ 5,834,815 $ 2,973,198 $ 42,289,023"
        ),
    },
    {
        "fiscal_year": "2022-2023",
        "document_id": 425,
        "chunk_id": 7260,
        "page": 23,
        "revenue": [22970478, 36609570, 8993698, 6196088],
        "revenue_total": 74769834,
        "revenue_quote": "Total revenues 22,970,478 36,609,570 8,993,698 6,196,088 74,769,834",
        "expenditure": [20384320, 35481138, 6515483, 7015568],
        "expenditure_total": 69396509,
        "expenditure_quote": (
            "Total expenditures 20,384,320 35,481,138 6,515,483 7,015,568 69,396,509"
        ),
        "fund_balance": [16819311, 18130984, 8313030, 2210433],
        "fund_balance_total": 45473758,
        "fund_balance_quote": (
            "Fund balances at June 30, 2023 $ 16,819,311 $ 18,130,984 "
            "$ 8,313,030 $ 2,210,433 $ 45,473,758"
        ),
    },
    {
        "fiscal_year": "2023-2024",
        "document_id": 424,
        "chunk_id": 7154,
        "page": 23,
        "revenue": [30176652, 32191982, 7965222, 4986065],
        "revenue_total": 75319921,
        "revenue_quote": "Total revenues 30,176,652 32,191,982 7,965,222 4,986,065 75,319,921",
        "expenditure": [21928620, 38257600, 10822473, 4046059],
        "expenditure_total": 75054752,
        "expenditure_quote": (
            "Total expenditures 21,928,620 38,257,600 10,822,473 4,046,059 75,054,752"
        ),
        "fund_balance": [25072619, 12065366, 5455779, 3145163],
        "fund_balance_total": 45738927,
        "fund_balance_quote": (
            "Fund balances at June 30, 2024 $ 25,072,619 $ 12,065,366 "
            "$ 5,455,779 $ 3,145,163 $ 45,738,927"
        ),
    },
    {
        "fiscal_year": "2024-2025",
        "document_id": 436,
        "chunk_id": 7802,
        "page": 24,
        "revenue": [31609692, 32730947, 8031542, 5322802],
        "revenue_total": 77694983,
        "revenue_quote": "Total revenues 31,609,692 32,730,947 8,031,542 5,322,802 77,694,983",
        "expenditure": [22639043, 39850590, 7389008, 5632406],
        "expenditure_total": 75511047,
        "expenditure_quote": (
            "Total expenditures 22,639,043 39,850,590 7,389,008 5,632,406 75,511,047"
        ),
        "fund_balance": [34032713, 4945723, 6098313, 2846114],
        "fund_balance_total": 47922863,
        "fund_balance_quote": (
            "Fund balances at June 30, 2025 $ 34,032,713 $ 4,945,723 "
            "$ 6,098,313 $ 2,846,114 $ 47,922,863"
        ),
    },
]


# Revenue by source (Total Governmental Funds column), dimension='source'.
# (amount, verbatim source row) per source; per-year sum is asserted to equal
# that year's revenue_total. Quotes are the verbatim "Revenues" rows.
SOURCE_LABELS = ("Local", "County", "State", "Federal", "Other")
SOURCES: dict[str, list[tuple[int, str]]] = {
    "2018-2019": [
        (53169996, "Local $ 17,294,981 $ 26,911,952 $ 7,312,664 $ 1,650,399 $ 53,169,996"),
        (525825, "County 112,759 242,944 151,287 18,835 525,825"),
        (1742804, "State 244,496 1,498,308 1,742,804"),
        (1635582, "Federal 316,041 183,242 1,136,299 1,635,582"),
        (450645, "Other 5,417 351,630 93,598 450,645"),
    ],
    "2019-2020": [
        (70058342, "Local $ 21,799,840 $ 37,182,930 $ 8,349,892 $ 2,725,680 $ 70,058,342"),
        (610998, "County 123,380 281,775 182,266 23,577 610,998"),
        (1716291, "State 308,641 1,407,650 - - 1,716,291"),
        (1237578, "Federal 329,360 122,782 785,436 - 1,237,578"),
        (151793, "Other 13,854 132,514 - 5,425 151,793"),
    ],
    "2020-2021": [
        (61835038, "Local $ 19,160,260 $ 31,310,825 $ 7,644,096 $ 3,719,857 $ 61,835,038"),
        (613816, "County 126,706 258,168 183,033 45,909 613,816"),
        (1710141, "State 251,491 1,457,571 - 1,079 1,710,141"),
        (1826199, "Federal 878,266 464,325 203,436 280,172 1,826,199"),
        (116959, "Other (104) 75,451 - 41,612 116,959"),
    ],
    "2021-2022": [
        (69305093, "Local $ 20,708,617 $ 35,774,596 $ 8,603,404 $ 4,218,476 $ 69,305,093"),
        (633758, "County 117,824 248,983 181,784 85,167 633,758"),
        (2198582, "State 360,218 1,838,364 - - 2,198,582"),
        (2144249, "Federal 1,834,041 127,434 176,629 6,145 2,144,249"),
        (71050, "Other 3,096 2,837 - 65,117 71,050"),
    ],
    "2022-2023": [
        (70171316, "Local $ 21,409,679 $ 34,073,686 $ 8,635,557 $ 6,052,394 $ 70,171,316"),
        (708616, "County 116,542 292,308 181,809 117,957 708,616"),
        (2363931, "State 426,308 1,937,623 - - 2,363,931"),
        (1468009, "Federal 985,724 305,953 176,332 - 1,468,009"),
        (57962, "Other 32,225 - - 25,737 57,962"),
    ],
    "2023-2024": [
        (71803874, "Local $ 28,982,530 $ 30,296,836 $ 7,642,552 $ 4,881,956 $ 71,803,874"),
        (597991, "County 154,028 227,855 144,959 71,149 597,991"),
        (1907709, "State 355,405 1,552,304 - - 1,907,709"),
        (958434, "Federal 665,736 114,987 177,711 - 958,434"),
        (51913, "Other 18,953 - - 32,960 51,913"),
    ],
    "2024-2025": [
        (74513876, "Local $ 30,682,463 $ 30,903,035 $ 7,705,321 $ 5,223,057 $ 74,513,876"),
        (556534, "County 152,039 201,794 147,970 54,731 556,534"),
        (1826712, "State 338,091 1,488,621 - - 1,826,712"),
        (738247, "Federal 422,499 137,497 178,251 - 738,247"),
        (59614, "Other 14,600 - - 45,014 59,614"),
    ],
}


# Expenditure by function (Total Governmental Funds), dimension='function'. Each
# function carries its four per-fund cells in FUNDS order [General, Special
# Revenue, Debt Service, Capital Projects], read from the expenditure block of
# the same Governmental Funds statement. Two checks at load time: each year's
# fund columns sum to that year's verified per-fund "Total expenditures", and
# every function row's cells sum to the grand total. FY2018-19's source collapsed
# its empty cells (no dashes); its split below is the unique assignment that
# satisfies all four audited column totals.
FUNCTION_MATRIX: dict[str, dict[str, list[int]]] = {
    "2018-2019": {
        "Instruction": [2759538, 26868880, 0, 665281],
        "Attendance and guidance": [712672, 1310617, 0, 0],
        "Health services": [437221, 126345, 0, 0],
        "Improvement of instruction and professional development": [420831, 600954, 0, 0],
        "Media services": [344194, 606639, 0, 2618],
        "Board of Education services": [203563, 0, 0, 0],
        "Executive administration": [1408521, 1123656, 0, 283320],
        "Building level administration": [1086820, 1536469, 0, 0],
        "Operation of plant": [7152373, 0, 0, 401749],
        "Security services": [203979, 0, 0, 48676],
        "Nonallowable transportation": [185253, 0, 0, 0],
        "Food services": [1184498, 0, 0, 11990],
        "Business services": [939330, 0, 0, 0],
        "Central office support services": [448930, 0, 0, 1098],
        "Adult/community programs": [1199620, 3511, 0, 0],
        "Debt service - Principal retirements": [0, 0, 23750000, 7122],
        "Debt service - Interest and other charges": [0, 0, 4331872, 63149],
    },
    "2019-2020": {
        "Instruction": [2194594, 26956239, 0, 616870],
        "Attendance and guidance": [721359, 1346623, 0, 0],
        "Health services": [479307, 158637, 0, 0],
        "Improvement of instruction and professional development": [303007, 970133, 0, 0],
        "Media services": [357872, 588093, 0, 1309],
        "Board of Education services": [215141, 0, 0, 4528],
        "Executive administration": [1537873, 1050968, 0, 25257],
        "Building level administration": [1082408, 1594989, 0, 10470],
        "Operation of plant": [7296625, 0, 0, 1018075],
        "Security services": [171267, 0, 0, 80511],
        "Nonallowable transportation": [190403, 0, 0, 0],
        "Food services": [966005, 0, 0, 0],
        "Business services": [913553, 0, 0, 0],
        "Central office support services": [410932, 0, 0, 1574],
        "Adult/community programs": [1127901, 3600, 0, 11299],
        "Facilities acquisition and construction": [0, 0, 0, 6947251],
        "Debt service - Principal retirements": [0, 0, 4935000, 320000],
        "Debt service - Interest and other charges": [0, 0, 3353792, 83043],
    },
    "2020-2021": {
        "Instruction": [2024025, 27748406, 0, 1048194],
        "Attendance and guidance": [750317, 1385574, 0, 0],
        "Health services": [638257, 211849, 0, 13606],
        "Improvement of instruction and professional development": [168646, 1117884, 0, 0],
        "Media services": [361959, 604720, 0, 6177],
        "Board of Education services": [196702, 0, 0, 0],
        "Executive administration": [1624208, 1190156, 0, 97784],
        "Building level administration": [1058731, 1594375, 0, 7570],
        "Operation of plant": [7364685, 0, 0, 1755025],
        "Security services": [190429, 0, 0, 205414],
        "Nonallowable transportation": [112574, 0, 0, 0],
        "Food services": [582583, 0, 0, 1574],
        "Business services": [982985, 0, 0, 9812],
        "Central office support services": [472269, 0, 0, 1371],
        "Adult/community programs": [1086492, 3166, 0, 13107],
        "Facilities acquisition and construction": [0, 0, 0, 1492652],
        "Debt service - Principal retirements": [0, 0, 6915000, 480000],
        "Debt service - Interest and other charges": [0, 0, 2058208, 114499],
    },
    "2021-2022": {
        "Instruction": [2120006, 28273865, 0, 703693],
        "Attendance and guidance": [755832, 1425228, 0, 0],
        "Health services": [681646, 218904, 0, 7980],
        "Improvement of instruction and professional development": [311663, 1139285, 0, 0],
        "Media services": [358347, 537841, 0, 1422],
        "Board of Education services": [220250, 0, 0, 0],
        "Executive administration": [1631790, 1278517, 0, 40677],
        "Building level administration": [1068014, 1674124, 0, 2556],
        "Operation of plant": [7595132, 0, 0, 1154610],
        "Security services": [207176, 0, 0, 2155933],
        "Nonallowable transportation": [160660, 0, 0, 0],
        "Food services": [1064557, 0, 0, 0],
        "Business services": [920753, 0, 0, 0],
        "Central office support services": [481028, 504, 0, 0],
        "Adult/community programs": [1201442, 3518, 0, 24329],
        "Debt service - Principal retirements": [0, 0, 6720000, 490000],
        "Debt service - Interest and other charges": [0, 0, 1821502, 102917],
    },
    "2022-2023": {
        "Instruction": [2497564, 29132042, 0, 761515],
        "Attendance and guidance": [840799, 1372781, 0, 0],
        "Health services": [720003, 203497, 0, 0],
        "Improvement of instruction and professional development": [436018, 1197202, 0, 0],
        "Media services": [480380, 560187, 0, 0],
        "Board of Education services": [251438, 0, 0, 1280],
        "Executive administration": [1867741, 1294462, 0, 89762],
        "Building level administration": [1153652, 1711528, 0, 60000],
        "Operation of plant": [7731392, 0, 0, 2686959],
        "Security services": [292449, 0, 0, 2782421],
        "Nonreimbursable transportation": [317486, 0, 0, 0],
        "Food services": [1075550, 0, 0, 20365],
        "Business services": [1075426, 0, 0, 0],
        "Central office support services": [482435, 3936, 0, 2110],
        "Adult/community programs": [1161987, 5503, 0, 20063],
        "Debt service - Principal retirements": [0, 0, 4920000, 500000],
        "Debt service - Interest and other charges": [0, 0, 1595483, 91093],
    },
    "2023-2024": {
        "Instruction": [2972138, 31133438, 0, 794410],
        "Attendance and guidance": [922628, 1497828, 0, 0],
        "Health services": [743527, 134052, 0, 5145],
        "Improvement of instruction and professional development": [511079, 1431857, 0, 0],
        "Media services": [429283, 618066, 0, 0],
        "Board of Education services": [295005, 0, 0, 2024],
        "Executive administration": [1747699, 1524257, 0, 229799],
        "Building level administration": [1240887, 1749575, 0, 1172],
        "Operation of plant": [8434210, 0, 0, 2097058],
        "Security services": [496305, 0, 0, 125824],
        "Nonreimbursable transportation": [306529, 0, 0, 0],
        "Food services": [1151905, 0, 0, 19305],
        "Business services": [1044836, 0, 0, 49279],
        "Central office support services": [314688, 166979, 0, 0],
        "Adult/community programs": [1317901, 1548, 0, 6213],
        "Facilities acquisition and construction": [0, 0, 0, 121586],
        "Debt service - Principal retirements": [0, 0, 9410000, 515000],
        "Debt service - Interest and other charges": [0, 0, 1412473, 79244],
    },
    "2024-2025": {
        "Instruction": [2900882, 32727714, 0, 1043599],
        "Attendance and guidance": [1076534, 1592498, 0, 0],
        "Health services": [771221, 211125, 0, 4995],
        "Improvement of instruction and professional development": [410979, 1420098, 0, 0],
        "Media services": [460164, 659606, 0, 1799],
        "Board of Education services": [263051, 0, 0, 0],
        "Executive administration": [1998689, 1356989, 0, 242074],
        "Building level administration": [1160239, 1789335, 0, 18055],
        "Operation of plant": [8786759, 0, 0, 3037670],
        "Security services": [493077, 0, 0, 639902],
        "Nonreimbursable transportation": [324278, 0, 0, 0],
        "Food services": [1186086, 0, 0, 10555],
        "Business services": [1082951, 0, 0, 34842],
        "Central office support services": [332304, 89442, 0, 1311],
        "Adult/community programs": [1391829, 3783, 0, 6004],
        "Debt service - Principal retirements": [0, 0, 6110000, 525000],
        "Debt service - Interest and other charges": [0, 0, 1279008, 66600],
    },
}


def _function_row_quote(label: str, cells: list[int], total: int) -> str:
    """Render the audit's expenditure-by-function row as a citation quote.

    Empty fund cells show as an em dash, mirroring the dashes the audit prints
    in FY2019-20 onward (FY2018-19 left them blank in the source).
    """
    parts = [f"{c:,}" if c else "—" for c in cells]
    return f"{label} {' '.join(parts)} {total:,}"


# Budget vs actual (dimension='budget'), cash/budgetary basis, per fund. Each
# fund-year carries Total revenues and Total expenditures at three bases:
# original budget, final budget, actual. Read from each fund's "Schedule of
# Revenues, Expenditures and Changes in Fund Balance - Budget and Actual - Cash
# Basis". These are budgetary-basis figures and differ from the GAAP figures
# above, so they live under their own dimension and never mix into the GAAP
# charts. Each (original, final, actual, chunk_id) triple was reconciled against
# the schedule's stated variance columns at extraction: |final - original| and
# |actual - final| matched the printed "Original to final" / "Final to actual".
BUDGET_VS_ACTUAL: dict[str, dict] = {
    "2018-2019": {
        "doc": 429,
        "General": {
            "rev": (16364320, 16844860, 17849518, 7745),
            "exp": (19438270, 19583800, 18615448, 7745),
        },
        "Special Revenue": {
            "rev": (30932280, 27188980, 29172041, 7746),
            "exp": (33020600, 33091540, 32191556, 7746),
        },
        "Debt Service": {
            "rev": (8241540, 8090030, 8598170, 7753),
            "exp": (28084540, 28084540, 28081872, 7753),
        },
        "Capital Projects": {
            "rev": (1289030, 1437290, 1793984, 7754),
            "exp": (1515080, 2132770, 2098469, 7754),
        },
    },
    "2019-2020": {
        "doc": 428,
        "General": {
            "rev": (19861950, 22083570, 22581777, 7651),
            "exp": (19997090, 20224640, 18053213, 7651),
        },
        "Special Revenue": {
            "rev": (36170940, 37979710, 39102161, 7652),
            "exp": (33615510, 33618390, 32754695, 7653),
        },
        "Debt Service": {
            "rev": (9355540, 9238630, 9313162, 7659),
            "exp": (7829650, 8291510, 8288389, 7659),
        },
        "Capital Projects": {
            "rev": (2612120, 2638610, 3280681, 7660),
            "exp": (2153320, 6256370, 4078860, 7660),
        },
    },
    "2020-2021": {
        "doc": 427,
        "General": {
            "rev": (22179480, 20952270, 20794490, 7541),
            "exp": (20550630, 21234700, 17521743, 7541),
        },
        "Special Revenue": {
            "rev": (36424990, 35046590, 34314514, 7542),
            "exp": (34411760, 34511600, 33775525, 7542),
        },
        "Debt Service": {
            "rev": (8371730, 8407370, 8338121, 7548),
            "exp": (8977790, 8977790, 8973611, 7548),
        },
        "Capital Projects": {
            "rev": (1430850, 4126430, 4179476, 7549),
            "exp": (2313230, 6924720, 5042334, 7549),
        },
    },
    "2021-2022": {
        "doc": 426,
        "General": {
            "rev": (22851660, 22277750, 22345089, 7433),
            "exp": (21375980, 21591370, 18781278, 7433),
        },
        "Special Revenue": {
            "rev": (34872800, 36911690, 37187005, 7434),
            "exp": (35432760, 35365630, 34520563, 7434),
        },
        "Debt Service": {
            "rev": (8921400, 8632800, 8654588, 7440),
            "exp": (8546030, 8546030, 8541502, 7440),
        },
        "Capital Projects": {
            "rev": (3607960, 4167170, 4281274, 7441),
            "exp": (2410130, 9204560, 4858737, 7441),
        },
    },
    "2022-2023": {
        "doc": 425,
        "General": {
            "rev": (22651810, 22674260, 22865227, 7322),
            "exp": (21684020, 21988010, 20164719, 7322),
        },
        "Special Revenue": {
            "rev": (34633900, 34775150, 36485372, 7323),
            "exp": (36146680, 36224600, 35527822, 7323),
        },
        "Debt Service": {
            "rev": (8567790, 8567790, 8998620, 7329),
            "exp": (6520030, 6520030, 6515483, 7329),
        },
        "Capital Projects": {
            "rev": (5246150, 5246150, 6194613, 7330),
            "exp": (2820460, 8766280, 7001989, 7330),
        },
    },
    "2023-2024": {
        "doc": 424,
        "General": {
            "rev": (29672260, 29725310, 29999528, 7214),
            "exp": (23270262, 23483542, 22063876, 7214),
        },
        "Special Revenue": {
            "rev": (32945150, 32967970, 32162461, 7215),
            "exp": (37937690, 38213740, 38208562, 7215),
        },
        "Debt Service": {
            "rev": (9745330, 9745330, 7948702, 7221),
            "exp": (10826930, 10826930, 10822104, 7221),
        },
        "Capital Projects": {
            "rev": (5083500, 5083500, 4978628, 7222),
            "exp": (2572370, 5187250, 3815527, 7222),
        },
    },
    "2024-2025": {
        "doc": 436,
        "General": {
            "rev": (24006880, 24058267, 31708277, 7862),
            "exp": (23761689, 24069207, 22507352, 7862),
        },
        "Special Revenue": {
            "rev": (40642910, 40714733, 32925089, 7863),
            "exp": (40466370, 40425483, 39868534, 7863),
        },
        "Debt Service": {
            "rev": (8063430, 8063430, 8045686, 7869),
            "exp": (7394340, 7394340, 7389078, 7869),
        },
        "Capital Projects": {
            "rev": (3678200, 3678200, 5340972, 7870),
            "exp": (3761470, 6207370, 5244280, 7870),
        },
    },
}

# Budget-schedule fund labels match the GAAP fund labels used elsewhere.
_BUDGET_FUND_LABEL = {"Special Revenue": "Special Revenue (Teachers)"}
_BASES = ("original", "final", "actual")


def build_line_items() -> list[BudgetLineItem]:
    """Expand the verified figures into rows, asserting each year reconciles."""
    items: list[BudgetLineItem] = []
    years_by_fy = {y["fiscal_year"]: y for y in YEARS}

    for y in YEARS:
        for category in ("revenue", "expenditure", "fund_balance"):
            amounts = y[category]
            stated_total = y[f"{category}_total"]
            actual_total = sum(amounts)
            if actual_total != stated_total:
                raise SystemExit(
                    f"Reconciliation failed for {y['fiscal_year']} {category}: "
                    f"per-fund sum {actual_total} != stated total {stated_total}"
                )
            for fund, amount in zip(FUNDS, amounts, strict=True):
                items.append(
                    BudgetLineItem(
                        fiscal_year=y["fiscal_year"],
                        category=category,
                        amount=Decimal(amount),
                        document_id=y["document_id"],
                        fund=fund,
                        subcategory=SUBCATEGORY[category],
                        chunk_id=y["chunk_id"],
                        source_quote=y[f"{category}_quote"],
                        note=(
                            f"FY{y['fiscal_year']} audit, Statement of Revenues, "
                            f"Expenditures and Changes in Fund Balances - Governmental "
                            f"Funds, p. {y['page']}"
                        ),
                    )
                )

    # Revenue by source (dimension='source'): must sum to the year's total revenues.
    for fiscal_year, rows in SOURCES.items():
        y = years_by_fy[fiscal_year]
        actual_total = sum(amount for amount, _ in rows)
        if actual_total != y["revenue_total"]:
            raise SystemExit(
                f"Reconciliation failed for {fiscal_year} revenue-by-source: "
                f"sum {actual_total} != stated total revenues {y['revenue_total']}"
            )
        for label, (amount, quote) in zip(SOURCE_LABELS, rows, strict=True):
            items.append(
                BudgetLineItem(
                    fiscal_year=fiscal_year,
                    category="revenue",
                    amount=Decimal(amount),
                    document_id=y["document_id"],
                    dimension="source",
                    subcategory=label,
                    chunk_id=y["chunk_id"],
                    source_quote=quote,
                    note=(
                        f"FY{fiscal_year} audit, Statement of Revenues, Expenditures "
                        f"and Changes in Fund Balances - Governmental Funds, p. {y['page']}, "
                        f"revenue by source"
                    ),
                )
            )

    # Expenditure by function (dimension='function'): the function x fund matrix
    # from the same statement. One row per nonzero cell. Two integrity checks per
    # year: fund columns sum to the verified per-fund "Total expenditures", and
    # all functions sum to the grand total.
    for fiscal_year, functions in FUNCTION_MATRIX.items():
        y = years_by_fy[fiscal_year]
        col_sums = [0, 0, 0, 0]
        grand = 0
        for label, cells in functions.items():
            row_total = sum(cells)
            grand += row_total
            quote = _function_row_quote(label, cells, row_total)
            for i, (fund, amount) in enumerate(zip(FUNDS, cells, strict=True)):
                col_sums[i] += amount
                if amount == 0:
                    continue
                items.append(
                    BudgetLineItem(
                        fiscal_year=fiscal_year,
                        category="expenditure",
                        amount=Decimal(amount),
                        document_id=y["document_id"],
                        dimension="function",
                        fund=fund,
                        subcategory=label,
                        chunk_id=y["chunk_id"],
                        source_quote=quote,
                        note=(
                            f"FY{fiscal_year} audit, Statement of Revenues, "
                            f"Expenditures and Changes in Fund Balances - Governmental "
                            f"Funds, p. {y['page']}, expenditure by function"
                        ),
                    )
                )
        if col_sums != y["expenditure"]:
            raise SystemExit(
                f"Reconciliation failed for {fiscal_year} function-by-fund columns: "
                f"{col_sums} != verified per-fund expenditures {y['expenditure']}"
            )
        if grand != y["expenditure_total"]:
            raise SystemExit(
                f"Reconciliation failed for {fiscal_year} function grand total: "
                f"{grand} != stated total expenditures {y['expenditure_total']}"
            )

    # Budget vs actual (dimension='budget'): per fund, Total revenues and Total
    # expenditures at original/final/actual (cash/budgetary basis). Three rows
    # per (fund, category). Guard: every figure positive and the full grid present.
    n_budget = 0
    for fiscal_year, funds in BUDGET_VS_ACTUAL.items():
        document_id = funds["doc"]
        for fund_key in ("General", "Special Revenue", "Debt Service", "Capital Projects"):
            fund_label = _BUDGET_FUND_LABEL.get(fund_key, fund_key)
            for kind, category, subcat in (
                ("rev", "revenue", "Total revenues"),
                ("exp", "expenditure", "Total expenditures"),
            ):
                original, final, actual, chunk_id = funds[fund_key][kind]
                if not (original > 0 and final > 0 and actual > 0):
                    raise SystemExit(
                        f"Budget-vs-actual {fiscal_year} {fund_key} {kind}: non-positive figure"
                    )
                quote = (
                    f"{subcat} — original ${original:,} · final ${final:,} · "
                    f"actual ${actual:,} (budgetary/cash basis)"
                )
                note = (
                    f"FY{fiscal_year} audit, Schedule of Revenues, Expenditures and Changes in "
                    f"Fund Balance - Budget and Actual - Cash Basis - {fund_label} Fund (unaudited)"
                )
                for basis, amount in zip(_BASES, (original, final, actual), strict=True):
                    items.append(
                        BudgetLineItem(
                            fiscal_year=fiscal_year,
                            category=category,
                            amount=Decimal(amount),
                            document_id=document_id,
                            dimension="budget",
                            fund=fund_label,
                            subcategory=subcat,
                            basis=basis,
                            chunk_id=chunk_id,
                            source_quote=quote,
                            note=note,
                        )
                    )
                    n_budget += 1
    expected = len(BUDGET_VS_ACTUAL) * 4 * 2 * 3
    if n_budget != expected:
        raise SystemExit(f"Budget-vs-actual: built {n_budget} rows, expected {expected}")
    return items


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--dry-run", action="store_true", help="build and reconcile, but do not write"
    )
    args = parser.parse_args()

    items = build_line_items()
    logger.info(
        "Built %d line items across %d fiscal years (all reconciled).", len(items), len(YEARS)
    )
    for y in YEARS:
        logger.info(
            "  %s: revenue %s, expenditure %s, ending balance %s",
            y["fiscal_year"],
            f"{y['revenue_total']:,}",
            f"{y['expenditure_total']:,}",
            f"{y['fund_balance_total']:,}",
        )

    if args.dry_run:
        logger.info("--dry-run: nothing written.")
        return 0

    cfg = load_config()
    # Writer: use the service key (bypasses RLS).
    client = get_client(cfg.supabase_url, cfg.supabase_service_key)

    # Replace: this file is the single source of truth for budget_line_items.
    deleted = client.table("budget_line_items").delete().gte("id", 0).execute()
    logger.info("Deleted %d existing rows.", len(deleted.data))

    ids = insert_budget_line_items(client, items)
    logger.info("Inserted %d budget line items.", len(ids))
    return 0


if __name__ == "__main__":
    sys.exit(main())
