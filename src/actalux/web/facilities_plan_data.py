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


# Lede plan-level citations (doc 87, the master-plan volume). The delivery date and
# the consultant name each carry their own verbatim anchor so the lede stat tiles
# are grounded rather than asserted. The delivery-date line is duplicated across the
# cover and the following page (chunk overlap), so the anchor extends past the date
# into the unique "THE IMPORTANCE OF" run to identify a single passage. The bare
# "Paragon Architecture" string occurs dozens of times in the volume, so the
# consultant is anchored on the unique "About Paragon Architecture" narrative line.
DELIVERY_SOURCE = Source(87, "Delivered to District on:\n02.19.2025\n\n3\nTHE IMPORTANCE OF")
CONSULTANT_SOURCE = Source(87, "Paragon Architecture got its humble start in 2010")
PLAN_DELIVERED = "Feb 2025"  # from DELIVERY_SOURCE: "Delivered to District on: 02.19.2025"


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
# The options table is duplicated across two overlapping chunks (the funding page
# and the summary page). Anchoring on the table header alone is ambiguous, so the
# anchor extends to the options-notes/Summary boundary that is unique to the chunk
# carrying the full options table.
OPTIONS_SOURCE = Source(83, "Does not include acquisition and renovation of swing space\n\nSummary")


# --- Funding context (doc 83). Each fact carries its own verbatim anchor so it is
# grounded individually rather than under one page-level citation.
@dataclass(frozen=True)
class FundingFact:
    label: str
    value: str
    source: Source


# The bonding-capacity line is the plan's Feb 2025 PROJECTION, not the funding
# reality: the actual measure put to voters was a $135M bond at the higher $0.6320
# levy (see BOND below). It is labelled as a projection here so the page never
# presents it as the adopted figure. The plan prints a tax-rate-framing clause
# alongside this figure; that clause is campaign-style framing and is deliberately
# not carried over (the value is given on its own).
FUNDING_FACTS = (
    FundingFact(
        "Current debt",
        "$34,752,000, fully paid off by March 1, 2029",
        Source(83, "Current Debt = $34,752,000 \nto be fully paid off by March 1, 2029"),
    ),
    FundingFact(
        "Current debt levy",
        "$0.5110 per $100 assessed valuation",
        Source(83, "Current Debt Levy = $0.5110 \nper $100 assessed valuation"),
    ),
    FundingFact(
        "Bonding capacity (Feb 2025 projection)",
        "Up to $90M of bonds",
        Source(83, "district can borrow up to $90m"),
    ),
    FundingFact(
        "Annual capital budget",
        "≈$4M per year",
        Source(83, "The district spends an average of $4m annually for capital maintenance"),
    ),
)


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

    Every field is verbatim from the official board resolution, the county ballot,
    and the St. Louis County certified results. ``result`` / ``result_detail`` are
    the certified outcome, cited to ``result_citation``.
    """

    amount: str
    purpose: str
    ballot_date: str
    majority_required: str
    estimated_levy: str
    resolution_source: CitedChunk
    ballot_source: CitedChunk
    result: str
    result_detail: str
    result_citation: CitedChunk


BOND = BondMeasure(
    amount="$135,000,000",
    purpose="constructing facility improvements",
    ballot_date="April 7, 2026",
    majority_required="four-sevenths (57.142857%)",
    estimated_levy="$0.6320 per $100 assessed valuation",
    resolution_source=CitedChunk(8140),  # board minutes Jan 21, 2026 (doc 501, #q1fcc)
    ballot_source=CitedChunk(1755),  # county ballot proposition (doc 84, #q06db)
    result="Approved",
    # Verbatim from the St. Louis County certified results (doc 504, chunk 8710):
    # "YES 2,516 89.25% / NO 303 10.75% ... Four-Sevenths Majority Required".
    result_detail="2,516 yes (89.25%) to 303 no; four-sevenths majority required",
    result_citation=CitedChunk(8710),  # St. Louis County certified results (doc 504)
)

# --- District-wide priority themes (#q2015 / doc 87), in the plan's order.
DISTRICT_THEMES = (
    "Safety & Security",
    "Maintenance & HVAC",
    "Accessibility & Inclusiveness",
    "Curriculum & Programming",
)
# The "DISTRICT-WIDE COMMON PROJECT THEMES" heading appears in more than one chunk;
# the unique anchor is the sentence that introduces the four themes.
PRIORITIES_SOURCE = Source(87, "referred to as “District-Wide Common Master Plan Themes,”")


@dataclass(frozen=True)
class Milestone:
    """One dated step in the facilities initiative, with its own citation.

    ``source`` is either a ``Source`` (verbatim anchor resolved at render time) or a
    ``CitedChunk`` (a stable chunk id), so every milestone deep-links to the exact
    passage it was read from — the same per-figure grounding the funding facts use.
    """

    when: str
    title: str
    source: Source | CitedChunk


# --- Initiative timeline. This spans the FULL arc — plan development (2024-2025),
# bond authorization (Jan 2026), and voter approval (Apr 2026) — not just the plan
# document's Feb 2025 delivery. (No "implementation underway" milestone: the only
# corpus source for it was the district's news release, which carries campaign
# framing + a campaign URL; it was removed rather than surface non-neutral content.)
# Month-only schedule items map to the plan's stated 2024-2025 window; days are
# given only where the source prints one. Each milestone carries its own citation.
#
# Where dates: doc 87's LRFMP Schedule lists "May: Kickoff... June: Conduct building
# assessments...", the deliverables list the six Steering Committee meetings, and the
# retreat block prints "Dec. 4, 2024 / Jan. 8, 2025 / Jan. 22, 2025: BOE Retreat".
# The schedule block is duplicated across overlapping chunks, so each schedule anchor
# extends to a chunk-unique boundary to identify a single passage.
TIMELINE = (
    Milestone(
        "May 2024",
        "Kickoff meeting and Board of Education introduction",
        Source(
            87,
            "solutions that benefit the entire Clayton Community.\n\n10\n"
            "LRFMP Schedule:\nMay:   Kickoff meeting and BOE introduction",
        ),
    ),
    Milestone(
        "Summer 2024",
        "District-wide building assessments",
        Source(
            87,
            "solutions that benefit the entire Clayton Community.\n\n10\n"
            "LRFMP Schedule:\nMay:   Kickoff meeting and BOE introduction\n"
            "June:   Conduct building assessments",
        ),
    ),
    Milestone(
        "Aug 2024 - Jan 2025",
        "Steering Committee and building-level Sub-Committee meetings",
        Source(87, "community connections for years to come. INTRODUCTION\n\n4"),
    ),
    Milestone(
        "Nov 13, 2024",
        "Volume II (Demographic Study) delivered to the district",
        Source(88, "to the district on Nov. 13, 2024"),
    ),
    Milestone(
        "Dec 4, 2024 - Jan 22, 2025",
        "Board of Education retreats on options, costs, and funding",
        Source(
            87,
            "Dec. 4, 2024: BOE Retreat\n•\t\nJan. 8, 2025: BOE Retreat (virtual)\n•\t\n"
            "Jan. 22, 2025: BOE Retreat and Master Plan Update\n•\t\n"
            "Feb. 19, 2025: BOE Final Presentation of Long-Range Facility Master Plan"
            "\n\nThank you",
        ),
    ),
    Milestone(
        "Feb 19, 2025",
        "Volume I (the master plan) delivered to the district",
        DELIVERY_SOURCE,  # reuses the verified delivery-date anchor (chunk 8177)
    ),
    Milestone(
        "Jan 21, 2026",
        "Board adopts the resolution calling the $135M bond election",
        CitedChunk(8140),  # board minutes Jan 21, 2026 (doc 501, #q1fcc) — as the bond cite
    ),
    Milestone(
        "April 7, 2026",
        "Voters approve Proposition O (2,516 to 303)",
        CitedChunk(8710),  # St. Louis County certified results (doc 504)
    ),
)
