"""Tests for the shared ingest/recategorize classifier (actalux.ingest.classify)."""

from datetime import date

from actalux.ingest.classify import (
    classify_document_type,
    is_annual_schedule,
    parse_meeting_date,
)


class TestParseMeetingDate:
    # Patterns the original ingest already handled — must not regress.
    def test_iso_prefix(self) -> None:
        assert parse_meeting_date("2024-03-15_board-meeting") == date(2024, 3, 15)

    def test_natural_date(self) -> None:
        assert parse_meeting_date("April 10, 2024 Meeting Minutes.pdf") == date(2024, 4, 10)

    def test_short_dash_date(self) -> None:
        assert parse_meeting_date("10-29-25 Board of Education Meeting.txt") == date(2025, 10, 29)

    def test_month_year_defaults_to_first(self) -> None:
        assert parse_meeting_date("Feb2025 board") == date(2025, 2, 1)

    def test_fiscal_year_to_july_start(self) -> None:
        assert parse_meeting_date("2024-2025 Clayton Budget.html") == date(2024, 7, 1)

    def test_space_separated_fiscal_year(self) -> None:
        # "Clayton 2019 2020 Budget.pdf" -> FY2019-2020 -> Jul 1, 2019.
        assert parse_meeting_date("Clayton 2019 2020 Budget.pdf") == date(2019, 7, 1)

    def test_space_separated_years_only_when_consecutive(self) -> None:
        # Two unrelated 4-digit years must not be read as a fiscal span.
        assert parse_meeting_date("survey 2018 2024 results.pdf") is None

    def test_compact_mmddyyyy(self) -> None:
        # "BOE_Adopt 20-21 Budget_06242020.pdf" -> June 24, 2020.
        assert parse_meeting_date("BOE_Adopt 20-21 Budget_06242020.pdf") == date(2020, 6, 24)

    def test_compact_mmddyyyy_rejects_invalid(self) -> None:
        # YYYYMMDD order (month 20) is not a valid MMDDYYYY date -> no false match.
        assert parse_meeting_date("report_20200624_final.pdf") is None

    def test_compact_needs_today(self) -> None:
        assert parse_meeting_date("jan21_board_meeting.txt") is None  # no today -> skip
        got = parse_meeting_date("jan21_board_meeting.txt", today=date(2026, 6, 14))
        assert got == date(2026, 1, 21)

    # Patterns that previously fell through (the bug this fixes).
    def test_mm_dot_dd_dot_yy(self) -> None:
        assert parse_meeting_date("11.16.22 Business Meeting Minutes.pdf") == date(2022, 11, 16)

    def test_mm_space_dd_space_yy(self) -> None:
        assert parse_meeting_date("10 26 22 BOE MM signed.pdf") == date(2022, 10, 26)

    def test_iso_wins_over_fiscal(self) -> None:
        # An explicit full date beats a fiscal-year span elsewhere in the name.
        assert parse_meeting_date("FY2024-2025 budget dated 2024-09-01.pdf") == date(2024, 9, 1)

    def test_no_date(self) -> None:
        assert parse_meeting_date("Board Candidate Resource Guide") is None


class TestClassifyDocumentType:
    def test_existing_minutes_keyword(self) -> None:
        assert classify_document_type("April 10, 2024 Meeting Minutes.pdf") == "minutes"

    def test_boe_mm_naming_now_minutes(self) -> None:
        assert classify_document_type("Apr 12 2023 BOE MM signed.pdf") == "minutes"

    def test_annual_schedule_first(self) -> None:
        assert classify_document_type("2024 2025 Board of Education Meeting Minutes") == "schedule"

    def test_budget_unchanged(self) -> None:
        assert classify_document_type("Y23 Budget approved 6 1 22.pdf") == "budget"

    def test_curriculum_map(self) -> None:
        assert classify_document_type("canva K-5 Art Curriculum Map.txt") == "curriculum_map"

    def test_curriculum_resource(self) -> None:
        assert classify_document_type("curriculum RIT Reference Chart K-1.pdf") == "curriculum"

    def test_transcript_needs_text_flag(self) -> None:
        assert classify_document_type("10-29-25 Board of Education Meeting.txt") == "other"
        assert (
            classify_document_type("10-29-25 Board of Education Meeting.txt", is_text_file=True)
            == "transcript"
        )

    def test_communication_prefix(self) -> None:
        assert classify_document_type("comms_summer-kindergarten.html") == "communication"

    def test_communication_prefix_wins_over_topic_word(self) -> None:
        # A news post about the budget is still a communication, not a 'budget'.
        assert (
            classify_document_type("comms_board-approves-balanced-budget.html") == "communication"
        )

    def test_unmatched_is_other(self) -> None:
        assert classify_document_type("Some Random Attachment.pdf") == "other"


class TestIsAnnualSchedule:
    def test_year_span(self) -> None:
        assert is_annual_schedule("Clayton Board of Education Meetings 2023 2024")

    def test_single_meeting_is_not(self) -> None:
        assert not is_annual_schedule("April 10, 2024 Meeting Minutes")
