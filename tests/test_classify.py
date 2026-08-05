"""Tests for the shared ingest/recategorize classifier (actalux.ingest.classify)."""

from datetime import date

from actalux.ingest.classify import (
    classify_document_type,
    is_annual_schedule,
    meeting_date_from_text,
    parse_meeting_date,
)


class TestFourDigitYearFilenames:
    """Formats the district posts that used to fall through to the ingest date.

    Each of these produced a document filed under the day it was crawled, which
    is what put December minutes under an August meeting.
    """

    def test_dotted_four_digit_year(self) -> None:
        assert parse_meeting_date("12.10.2025 BOE Meeting Minutes - DRAFT.pdf") == date(
            2025, 12, 10
        )

    def test_dotted_two_digit_year_still_works(self) -> None:
        assert parse_meeting_date("12.10.25 BOE Meeting Minutes - DRAFT.pdf") == date(2025, 12, 10)

    def test_compact_yyyymmdd(self) -> None:
        assert parse_meeting_date("20260121BondElectionResolution.pdf") == date(2026, 1, 21)

    def test_underscore_separated(self) -> None:
        assert parse_meeting_date("Exec Summary_approval_12_10_2025.pdf") == date(2025, 12, 10)

    def test_compact_mmddyyyy_not_broken_by_yyyymmdd(self) -> None:
        # "06242020" is MMDDYYYY. The YYYYMMDD pattern is anchored on a 20xx year
        # so it cannot claim this one first.
        assert parse_meeting_date("BOE_Adopt 20-21 Budget_06242020") == date(2020, 6, 24)


class TestMeetingDateFromText:
    """The packet-header reader used to repair placeholder dates."""

    HEADER = (
        "School District of Clayton\nFor Information Only | Action Required\n"
        "Board of Education\nPage 1 of 1\n{item}\n{meeting}\nSummary\n"
    )

    def test_reads_the_header_meeting_date(self) -> None:
        text = self.HEADER.format(
            item="Foster Care Transportation MOU", meeting="December 10, 2025"
        )
        assert meeting_date_from_text(text) == date(2025, 12, 10)

    def test_header_date_beats_a_period_named_in_the_body(self) -> None:
        # The filename says 20250430, but April 2025 is the period the report
        # covers; June 4 is the meeting it was presented at.
        text = self.HEADER.format(item="April 2025 Financial Reporting", meeting="June 4, 2025")
        text += "Financial Reports are attached for April 30, 2025, and are presented in two ways."
        assert meeting_date_from_text(text) == date(2025, 6, 4)

    def test_refuses_a_policy_revision_date(self) -> None:
        # Regression guard. Policies are revised AT board meetings, so this date is
        # a real meeting date and passes any calendar check — it is simply a
        # different meeting's. The label is the only thing distinguishing it, and
        # matching non-word instead of non-digit after "Revised" silently disables
        # the guard, because the gap reads "Revised Date: ".
        text = (
            "POLICY KB PUBLIC INFORMATION PROGRAM Status: DRAFT "
            "Original Adopted Date: 06/10/2015 | Last Revised Date: 05/13/2020 "
            "25B UPDATE EXPLANATION"
        )
        assert meeting_date_from_text(text) is None

    def test_refuses_an_effective_date(self) -> None:
        text = (
            "THIS CLINICAL SERVICE AGREEMENT is entered into as of "
            'September 1, 2025 (the "Effective Date")'
        )
        assert meeting_date_from_text(text) is None

    def test_calendar_predicate_rejects_a_non_meeting_date(self) -> None:
        text = self.HEADER.format(item="Some Item", meeting="December 11, 2025")
        assert meeting_date_from_text(text, {date(2025, 12, 10)}.__contains__) is None

    def test_returns_none_rather_than_guessing(self) -> None:
        assert meeting_date_from_text("A document with no date at all.") is None
        assert meeting_date_from_text("") is None


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

    def test_compact_yyyymmdd_is_read(self) -> None:
        # Previously asserted to be None: read as MMDDYYYY this is month 20, and no
        # other pattern covered it, so the file fell through to the ingest-day date.
        # The district posts this form constantly (20260121BondElectionResolution),
        # and that fallthrough is what filed 63 documents under a Sunday.
        assert parse_meeting_date("report_20200624_final.pdf") == date(2020, 6, 24)

    def test_compact_eight_digits_rejected_when_no_valid_reading(self) -> None:
        # Neither YYYYMMDD (month 99) nor MMDDYYYY (month 99) is a real date, so
        # the "no false match" intent still holds where it should.
        assert parse_meeting_date("report_20209924_final.pdf") is None

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
