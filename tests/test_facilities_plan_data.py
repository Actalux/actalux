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

    def test_result_is_approved_with_certified_citation(self) -> None:
        # The certified St. Louis County result (doc 504, chunk 8710) grounds the
        # outcome: the measure was approved, with the verbatim vote totals carried
        # in result_detail and cited to that chunk. No "pending" slot remains.
        assert fpd.BOND.result == "Approved"
        assert fpd.BOND.result_citation is not None
        assert fpd.BOND.result_citation.chunk_id == 8710
        assert "2,516 yes (89.25%)" in fpd.BOND.result_detail
        assert "303 no" in fpd.BOND.result_detail

    def test_ninety_million_framed_as_projection(self) -> None:
        # The plan's $90M figure must be labelled as the Feb 2025 projection so it
        # is never presented as the funding reality (the actual measure is $135M).
        labels = [f.label for f in fpd.FUNDING_FACTS]
        assert any("projection" in label.lower() for label in labels)


class TestFundingFactGrounding:
    """Every funding fact carries its own verbatim Source, and the page is neutral."""

    def test_every_funding_fact_has_a_source_anchor(self) -> None:
        for f in fpd.FUNDING_FACTS:
            assert isinstance(f.source, fpd.Source)
            assert f.source.doc_id == 83  # all four funding figures live in doc 83
            assert f.source.anchor.strip()

    def test_no_funding_fact_value_carries_tax_framing(self) -> None:
        # "without a tax increase" / "without increasing the property tax" is
        # campaign-style framing and must never appear in a rendered value.
        for f in fpd.FUNDING_FACTS:
            value = f.value.lower()
            assert "without a tax increase" not in value
            assert "without increasing the property tax" not in value

    def test_bonding_capacity_value_is_bare(self) -> None:
        # The $90M projection is presented as a bare figure, not a tax-framed claim.
        cap = next(f for f in fpd.FUNDING_FACTS if "projection" in f.label.lower())
        assert cap.value == "Up to $90M of bonds"


class TestLedeGrounding:
    """The lede's delivery date and consultant each carry a verbatim Source."""

    def test_delivery_and_consultant_sources_exist(self) -> None:
        assert isinstance(fpd.DELIVERY_SOURCE, fpd.Source)
        assert isinstance(fpd.CONSULTANT_SOURCE, fpd.Source)
        assert fpd.DELIVERY_SOURCE.doc_id == 87
        assert fpd.CONSULTANT_SOURCE.doc_id == 87
        # The delivery anchor carries the verbatim Feb 2025 delivery date.
        assert "02.19.2025" in fpd.DELIVERY_SOURCE.anchor
        # The consultant anchor is the unique "humble start" narrative line, not the
        # ambiguous bare "Paragon Architecture" string.
        assert "Paragon Architecture" in fpd.CONSULTANT_SOURCE.anchor
        assert fpd.PLAN_DELIVERED == "Feb 2025"


class TestTimelineGrounding:
    """The timeline spans the full initiative and grounds every milestone."""

    def test_every_milestone_has_a_citation(self) -> None:
        # Each milestone carries its own Source (anchor) or CitedChunk (chunk id);
        # none is left ungrounded.
        assert fpd.TIMELINE  # non-empty
        for m in fpd.TIMELINE:
            assert isinstance(m.source, (fpd.Source, fpd.CitedChunk))
            if isinstance(m.source, fpd.Source):
                assert m.source.anchor.strip()
            else:
                assert m.source.chunk_id > 0

    def test_no_april_2024_selection_milestone(self) -> None:
        # The ungrounded "April 2024 Board selects Paragon Architecture" step was
        # dropped — there is no April-2024 selection basis in the cited records.
        for m in fpd.TIMELINE:
            assert "selects" not in m.title.lower()
            assert m.when != "April 2024"

    def test_timeline_spans_beyond_plan_delivery(self) -> None:
        # The arc must reach bond authorization and voter approval — not end at the
        # Feb 2025 plan-document delivery.
        titles = " ".join(m.title.lower() for m in fpd.TIMELINE)
        whens = " ".join(m.when.lower() for m in fpd.TIMELINE)
        assert "proposition o" in titles  # voter approval step
        assert "bond election" in titles  # bond authorization step
        assert "2026" in whens  # the arc extends into 2026

    def test_bond_and_approval_milestones_reuse_verified_chunks(self) -> None:
        # The bond-resolution and voter-approval milestones cite the same verified
        # chunks the bond block uses (8140 / 8710), by stable chunk id.
        cited = {m.source.chunk_id for m in fpd.TIMELINE if isinstance(m.source, fpd.CitedChunk)}
        assert 8140 in cited  # board resolution (doc 501)
        assert 8710 in cited  # certified results (doc 504)

    def test_no_milestone_anchor_carries_tax_framing(self) -> None:
        # No milestone anchor string introduces the campaign tax-rate framing or the
        # campaign URL into Actalux's own copy.
        for m in fpd.TIMELINE:
            if isinstance(m.source, fpd.Source):
                anchor = m.source.anchor.lower()
                assert "without a tax increase" not in anchor
                assert "without increasing" not in anchor
                assert "claytonpropo" not in anchor
