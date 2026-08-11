"""Tests for the land-use case linker (actalux.landuse.link)."""

from __future__ import annotations

from datetime import date

from actalux.landuse.link import (
    Appearance,
    link_appearances,
    normalize_address,
)


def _app(day: date, action: str | None, addr: str = "42 North Central Avenue", **kw) -> Appearance:
    defaults = dict(
        document_id=1,
        entity_id=3,
        event_date=day,
        address_raw=addr,
        application_type="conditional_use",
        subtype_raw=None,
        action=action,
        staff_recommendation=None,
        conditions_text=None,
        video_timestamp=None,
        source_quote="q",
    )
    defaults.update(kw)
    return Appearance(**defaults)


class TestNormalizeAddress:
    def test_directionals_and_suffixes_collapse(self) -> None:
        assert normalize_address("42 N. Central Ave") == normalize_address(
            "42 North Central Avenue"
        )

    def test_distinct_addresses_stay_distinct(self) -> None:
        assert normalize_address("42 N Central Ave") != normalize_address("44 N Central Ave")


class TestLinking:
    def test_continuance_then_decision_is_one_case(self) -> None:
        cases, review = link_appearances(
            [
                _app(date(2026, 1, 5), "continued"),
                _app(date(2026, 2, 2), "approved", addr="42 N. Central Ave"),
            ]
        )
        assert len(cases) == 1 and not review
        assert cases[0].status() == "approved"
        assert cases[0].resolved_date() == date(2026, 2, 2)

    def test_new_application_after_decision_is_a_new_case(self) -> None:
        cases, _ = link_appearances(
            [
                _app(date(2024, 1, 8), "approved"),
                _app(date(2024, 6, 3), "heard"),  # same address, months later
            ]
        )
        assert len(cases) == 2

    def test_soon_after_decision_goes_to_review_not_guessed(self) -> None:
        cases, review = link_appearances(
            [
                _app(date(2026, 1, 5), "approved"),
                _app(date(2026, 1, 26), "heard"),  # 21 days after resolution
            ]
        )
        assert len(cases) == 1
        assert len(review) == 1
        assert review[0].candidate_case_index == 0

    def test_window_expiry_starts_a_fresh_case(self) -> None:
        cases, _ = link_appearances(
            [
                _app(date(2024, 1, 8), "continued"),
                _app(date(2025, 6, 2), "heard"),  # >365 days later
            ]
        )
        assert len(cases) == 2

    def test_different_application_types_never_link(self) -> None:
        cases, _ = link_appearances(
            [
                _app(date(2026, 1, 5), "heard", application_type="site_plan"),
                _app(date(2026, 1, 19), "heard", application_type="conditional_use"),
            ]
        )
        assert len(cases) == 2

    def test_truncated_address_goes_to_review(self) -> None:
        _, review = link_appearances([_app(date(2026, 1, 5), "heard", addr="75")])
        assert review and "unusable address" in review[0].reason


class TestCaseSemantics:
    def test_advisory_vs_final_role(self) -> None:
        cup, _ = link_appearances([_app(date(2026, 1, 5), "recommended")])
        site, _ = link_appearances(
            [_app(date(2026, 1, 5), "approved", application_type="site_plan")]
        )
        assert cup[0].decision_role() == "advisory"
        assert site[0].decision_role() == "final"

    def test_outcome_matches_staff_conditions_are_not_an_override(self) -> None:
        cases, _ = link_appearances(
            [_app(date(2026, 1, 5), "approved_with_conditions", staff_recommendation="approve")]
        )
        assert cases[0].outcome_matches_staff() is True

    def test_denial_against_approve_is_a_mismatch(self) -> None:
        cases, _ = link_appearances(
            [_app(date(2026, 1, 5), "denied", staff_recommendation="approve")]
        )
        assert cases[0].outcome_matches_staff() is False

    def test_unresolved_or_unstated_is_null(self) -> None:
        pending, _ = link_appearances(
            [_app(date(2026, 1, 5), "continued", staff_recommendation="approve")]
        )
        unstated, _ = link_appearances([_app(date(2026, 1, 5), "approved")])
        assert pending[0].outcome_matches_staff() is None
        assert unstated[0].outcome_matches_staff() is None

    def test_recommendation_to_council_counts_as_pc_stage_approval(self) -> None:
        cases, _ = link_appearances(
            [_app(date(2026, 1, 5), "recommended", staff_recommendation="approve")]
        )
        assert cases[0].status() == "recommended_to_council"
        assert cases[0].outcome_matches_staff() is True

    def test_latest_staff_position_wins(self) -> None:
        cases, _ = link_appearances(
            [
                _app(date(2026, 1, 5), "continued", staff_recommendation="deny"),
                _app(date(2026, 2, 2), "approved", staff_recommendation="approve"),
            ]
        )
        assert cases[0].staff_recommendation() == "approve"
        assert cases[0].outcome_matches_staff() is True
