"""Data-integrity guards for the curated Facilities Master Plan dataset.

Actalux is a citation-first archive: published figures must be verbatim or a
faithful arithmetic of verbatim cells. These tests guard that the transcribed
tier cells reconcile to the published priority totals (within source rounding),
that the derived counts cannot drift from their tables, and that the bond's
public-record citations match the verified hash ids.
"""

from __future__ import annotations

from actalux.models import chunk_hash_id
from actalux.web import facilities_plan_data as fpd

# The plan published its tier totals to the dollar but its per-cell figures carry
# at most $1 of independent rounding, so cell sums reconcile to within $1.
_SOURCE_ROUNDING = 1


def _tier_sum(rows: tuple[fpd.CostRow, ...]) -> dict[str, int]:
    return {
        "red": sum(r.red for r in rows),
        "yellow": sum(r.yellow for r in rows),
        "green": sum(r.green for r in rows),
    }


class TestTierReconciliation:
    """Transcribed cells reconcile to the published PRIORITY TOTAL figures."""

    def test_grand_total_matches_published(self) -> None:
        # The plan published "$94,136,875"; the page must show exactly that.
        assert fpd.GRAND_TOTAL == 94_136_875

    def test_tier_totals_sum_to_grand_total_within_rounding(self) -> None:
        assert abs(sum(fpd.TIER_TOTALS.values()) - fpd.GRAND_TOTAL) <= _SOURCE_ROUNDING

    def test_scope_cells_reconcile_to_tier_totals(self) -> None:
        sums = _tier_sum(fpd.SCOPE_COSTS)
        for tier, total in fpd.TIER_TOTALS.items():
            assert abs(sums[tier] - total) <= _SOURCE_ROUNDING

    def test_location_cells_reconcile_to_tier_totals(self) -> None:
        sums = _tier_sum(fpd.LOCATION_COSTS)
        for tier, total in fpd.TIER_TOTALS.items():
            assert abs(sums[tier] - total) <= _SOURCE_ROUNDING

    def test_cost_row_total_is_faithful_sum(self) -> None:
        row = fpd.SCOPE_COSTS[0]
        assert row.total == row.red + row.yellow + row.green


class TestDerivedCounts:
    """Counts presented as figures must be derived from a verbatim table, not free-typed."""

    def test_site_count_equals_location_rows(self) -> None:
        # SITE_COUNT is the number of verbatim per-location rows, so it can never
        # drift from the location table it is evidenced by.
        assert fpd.SITE_COUNT == len(fpd.LOCATION_COSTS)


class TestBondCitations:
    """The bond's public-record citations match the verified hash ids."""

    def test_resolution_and_ballot_chunk_ids_match_hashes(self) -> None:
        # #q1fcc / #q06db are the verified resolution / ballot citations.
        assert chunk_hash_id(fpd.BOND.resolution_source.chunk_id) == "#q1fcc"
        assert chunk_hash_id(fpd.BOND.ballot_source.chunk_id) == "#q06db"

    def test_result_is_passed_with_citation_pending(self) -> None:
        # Operator-stated fact; certified-result citation not yet located, so the
        # slot is explicitly pending (None) rather than omitted or fabricated.
        assert fpd.BOND.result == "Passed"
        assert fpd.BOND.result_citation is None

    def test_ninety_million_framed_as_projection(self) -> None:
        # The plan's $90M figure must be labelled as the Feb 2025 projection so it
        # is never presented as the funding reality (the actual measure is $135M).
        labels = [f.label for f in fpd.FUNDING_FACTS]
        assert any("projection" in label.lower() for label in labels)
