"""Curated, source-cited dataset for the Long-Range Facilities Master Plan page.

Every figure here is transcribed verbatim from the plan's own documents and
carries a ``Source`` (document id + a verbatim anchor string that identifies the
passage it came from). Nothing is computed, inferred, or rounded beyond what the
source prints; the page resolves each anchor to its chunk at render time so the
figure links to the exact passage (citation-first, per the content policy).

Two cost frames are kept deliberately separate — conflating them would misstate
the plan:
  * IDENTIFIED NEEDS ($94.1M): the facility-condition assessment, broken down by
    priority tier, by assessment scope, and by location.
  * FUTURE DEVELOPMENT OPTIONS ($129M-$178M): conceptual renovation / new-build
    options presented on top of the assessment, requiring further study.

Soft costs (design fees, contingency, CM, testing, permitting) are excluded from
the assessment figures; the plan recommends adding 10-25%.
"""

from __future__ import annotations

from dataclasses import dataclass

# Plan-level facts shown in the lede / overview, each from a verbatim passage.
PLAN_TITLE = "Long-Range Facilities Master Plan"
PLAN_YEARS = "2024-2025"
PLAN_CONSULTANT = "Paragon Architecture"
PLAN_HORIZON = "10+ years"
# SITE_COUNT is defined below as len(LOCATION_COSTS) — it is a count of the
# verbatim per-location rows, not a separately transcribed figure, so the page
# evidences it with the location table itself rather than a passage citation.


@dataclass(frozen=True)
class Source:
    """A citation: the document the figure came from + a verbatim anchor in it.

    The anchor is resolved to a specific chunk at render time, so the figure links
    to the exact source passage even if chunk ids change on re-ingest.
    """

    doc_id: int
    anchor: str


@dataclass(frozen=True)
class CostRow:
    """One row of the tiered cost tables (dollars, verbatim from the source)."""

    label: str
    red: int
    yellow: int
    green: int

    @property
    def total(self) -> int:
        return self.red + self.yellow + self.green


# --- Priority tiers (the plan's own Red/Yellow/Green, with its time-horizon gloss).
# Glosses are verbatim from the deliverables overview (#q20d7 / doc 87): red =
# "immediate needs (2-5 Years)", yellow = "next 5-10 years". Green is the longest
# tier; the plan does not print a year range for it, so it is left as "long-term".
TIERS = (
    ("Red", "Immediate (2-5 years)"),
    ("Yellow", "Near-term (5-10 years)"),
    ("Green", "Long-term"),
)
TIER_SOURCE = Source(87, "approximately $23.5 million dollars in immediate needs")

# Printed priority totals (verbatim from the source's "PRIORITY TOTAL" row). Stored
# rather than summed so the page shows exactly what the document published; a test
# checks the transcribed cells reconcile to these within source rounding.
TIER_TOTALS = {"red": 23_458_924, "yellow": 28_305_058, "green": 42_372_894}
GRAND_TOTAL = 94_136_875  # as published ("just over $94 million")

# --- Identified needs by assessment scope (#q0699 / doc 83).
SCOPE_COSTS = (
    CostRow("HVAC", 18_786_495, 4_258_800, 12_051_000),
    CostRow("Building Envelope", 1_601_498, 12_358_797, 760_449),
    CostRow("Roofing", 1_270_070, 5_409_444, 6_094_320),
    CostRow("Playgrounds & Playfields", 403_166, 1_084_965, 1_580_511),
    CostRow("Flooring", 208_922, 675_661, 8_765_007),
    CostRow("Ceilings", 305_685, 1_346_213, 3_674_371),
    CostRow("Walls", 332_522, 1_995_749, 8_009_258),
    CostRow("Parking Lots & Drives", 132_944, 891_197, 1_437_977),
    CostRow("Exterior Lighting", 417_622, 284_232, 0),
)

# --- Identified needs by location (#q0699 / doc 83).
LOCATION_COSTS = (
    CostRow("Clayton High School", 7_614_544, 13_238_170, 15_623_709),
    CostRow("Wydown Middle School", 1_129_401, 3_283_716, 13_210_122),
    CostRow("Captain Elementary School", 3_216_077, 4_222_330, 1_967_932),
    CostRow("Glenridge Elementary School", 3_133_871, 2_289_230, 3_221_942),
    CostRow("Meramec Elementary School", 4_522_796, 2_483_300, 4_734_302),
    CostRow("Athletics & Activities", 986_821, 647_649, 865_139),
    CostRow("The Family Center", 2_104_975, 533_347, 1_484_305),
    CostRow("Administrative Center", 558_735, 1_167_919, 1_160_071),
    CostRow("Facility Services", 191_704, 439_397, 105_372),
)
COST_SOURCE = Source(83, "Long Range Improvement Costs by Scope")
COST_SOFT_COST_NOTE = (
    "Soft costs (design fees, contingency, construction management, testing, "
    "permitting) are not included; the plan recommends adding 10-25%."
)

# Locations assessed = the number of verbatim per-location rows above (not a
# separately transcribed figure). Derived from the table so it can never drift
# from it; the page points readers to the location table as its evidence.
SITE_COUNT = len(LOCATION_COSTS)


# --- Future development options (#q06af / doc 83). Values are the plan's rounded
# order-of-magnitude millions (2025 dollars, pre-bid, with 20-35% contingency).
@dataclass(frozen=True)
class SiteOption:
    site: str
    level2_m: int | None  # $ millions, or None if not offered at this level
    level3_m: int | None


RENOVATION_OPTIONS = (
    SiteOption("The Family Center", 8, None),
    SiteOption("Captain Elementary", 19, None),
    SiteOption("Glenridge Elementary", 27, 44),
    SiteOption("Meramec Elementary", 27, 44),
    SiteOption("Wydown Middle School", 4, None),
    SiteOption("Clayton High School", 29, None),
    SiteOption("Athletics & Activities", 23, 30),
)
RENOVATION_TOTAL_LABEL = "$137M to $178M"  # "Total Construction +/- $137m to $178m"

NEW_SCHOOLS = (
    ("New Captain Elementary", 43),
    ("New Glenridge Elementary", 43),
    ("New Meramec Elementary", 43),
)
NEW_SCHOOLS_TOTAL_M = 129
OPTIONS_SOURCE = Source(83, "Total potential future long-range projects at each site")


# --- Funding context (#q06ae and #q06af / doc 83).
@dataclass(frozen=True)
class FundingFact:
    label: str
    value: str


# The "$90M without a tax increase" line is the plan's Feb 2025 PROJECTION, not the
# funding reality: the actual measure put to voters was a $135M bond at the higher
# $0.6320 levy (see BOND below). It is labelled as a projection here so the page
# never presents it as the adopted figure.
FUNDING_FACTS = (
    FundingFact("Current debt", "$34,752,000, fully paid off by March 1, 2029"),
    FundingFact("Current debt levy", "$0.5110 per $100 assessed valuation"),
    FundingFact(
        "Bonding capacity (Feb 2025 projection)",
        "Up to $90M of bonds without a tax increase",
    ),
    FundingFact("Annual capital budget", "≈$4M per year"),
)
FUNDING_SOURCE = Source(83, "Will support up to $90M of bonds")


# --- The GO bond (official public records: the board resolution + county ballot).
# These are cited by stable chunk id, the same way the structured budget table
# cites each figure — the hash ids the citations were verified against
# (#q1fcc / #q06db) are exactly chunk_hash_id(8140) / chunk_hash_id(1755).
@dataclass(frozen=True)
class CitedChunk:
    """A direct chunk-id citation (links to /chunk/{id}/source).

    Used where the verified citation is a specific chunk rather than an anchor
    string — e.g. the bond resolution and ballot passages.
    """

    chunk_id: int


@dataclass(frozen=True)
class BondMeasure:
    """The general-obligation bond placed on the ballot, with its sources.

    Every field is verbatim from the official board resolution and the county
    ballot; ``result`` is an operator-stated fact whose certified-result citation
    is not yet located, so ``result_citation`` is None and the page marks the
    source slot pending rather than omitting or fabricating it.
    """

    amount: str
    purpose: str
    ballot_date: str
    majority_required: str
    estimated_levy: str
    resolution_source: CitedChunk
    ballot_source: CitedChunk
    result: str
    result_citation: CitedChunk | None


BOND = BondMeasure(
    amount="$135,000,000",
    purpose="constructing facility improvements",
    ballot_date="April 7, 2026",
    majority_required="four-sevenths (57.142857%)",
    estimated_levy="$0.6320 per $100 assessed valuation",
    resolution_source=CitedChunk(8140),  # board minutes Jan 21, 2026 (doc 501, #q1fcc)
    ballot_source=CitedChunk(1755),  # county ballot proposition (doc 84, #q06db)
    result="Passed",
    result_citation=None,  # certified results not yet located; slot marked pending
)

# --- District-wide priority themes (#q2015 / doc 87), in the plan's order.
DISTRICT_THEMES = (
    "Safety & Security",
    "Maintenance & HVAC",
    "Accessibility & Inclusiveness",
    "Curriculum & Programming",
)
PRIORITIES_SOURCE = Source(87, "DISTRICT-WIDE COMMON PROJECT THEMES")


@dataclass(frozen=True)
class Milestone:
    when: str
    title: str


# --- Process timeline (#q1ff7 schedule + #q2013/#q2014 retreats / doc 87).
TIMELINE = (
    Milestone("April 2024", "Board selects Paragon Architecture to lead the plan"),
    Milestone("May-July 2024", "Kickoff, building assessments, and tours with principals"),
    Milestone("Aug-Oct 2024", "Steering Committee and building-level Sub-Committee meetings"),
    Milestone("Nov 13, 2024", "Volume II (Demographic Study) delivered to the district"),
    Milestone("December 2024", "Board of Education retreat: review of options and cost estimates"),
    Milestone("January 2025", "Board of Education retreat: design options refined"),
    Milestone("Feb 19, 2025", "Final presentation by Paragon; Volume I delivered"),
)
TIMELINE_SOURCE = Source(87, "BOARD OF EDUCATION RETREAT")
