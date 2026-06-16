"""Tests for the pure derivation logic in scripts/recategorize_documents.py."""

from datetime import date

from actalux.ingest.classify import is_annual_schedule
from scripts.recategorize_documents import (
    derive_document_type,
    derive_meeting_date,
    plan_changes,
)


class TestDeriveDocumentType:
    def test_only_reclassifies_other(self) -> None:
        # A doc already typed specifically is never re-typed, even if it looks
        # like something else.
        assert derive_document_type("Apr 12 2023 BOE MM signed.pdf", "", "budget") is None

    def test_boe_mm_signed_is_minutes(self) -> None:
        assert derive_document_type("Apr 12 2023 BOE MM signed.pdf", "", "other") == "minutes"

    def test_business_meeting_minutes_is_minutes(self) -> None:
        assert (
            derive_document_type("11.16.22 Business Meeting Minutes.pdf", "", "other") == "minutes"
        )

    def test_annual_schedule_is_schedule_not_minutes(self) -> None:
        # Contains "Meeting Minutes" but is an annual schedule -> schedule wins.
        assert (
            derive_document_type("2024 2025 Board of Education Meeting Minutes", "", "other")
            == "schedule"
        )

    def test_curriculum_map_beats_generic_curriculum(self) -> None:
        assert (
            derive_document_type("canva K-5 Art Curriculum Map.txt", "", "other")
            == "curriculum_map"
        )

    def test_curriculum_resource(self) -> None:
        assert (
            derive_document_type("curriculum RIT Reference Chart K-1.pdf", "", "other")
            == "curriculum"
        )

    def test_facilities_plan(self) -> None:
        assert (
            derive_document_type("Volume1 ClaytonMasterPlan Priorities.pdf", "", "other")
            == "facilities_plan"
        )

    def test_governance(self) -> None:
        assert derive_document_type("Missouri Sunshine Law.pdf", "", "other") == "governance"

    def test_unclassifiable_stays_other(self) -> None:
        assert derive_document_type("Some Random Attachment.pdf", "", "other") is None


class TestDeriveMeetingDate:
    def test_month_name(self) -> None:
        assert derive_meeting_date("Apr 12 2023 BOE MM signed.pdf", "") == date(2023, 4, 12)

    def test_month_name_with_comma(self) -> None:
        assert derive_meeting_date("December 10, 2025 BOE Meeting Minutes", "") == date(
            2025, 12, 10
        )

    def test_mm_dot_dd_dot_yy(self) -> None:
        assert derive_meeting_date("11.16.22 Business Meeting Minutes.pdf", "") == date(
            2022, 11, 16
        )

    def test_mm_space_dd_space_yy(self) -> None:
        assert derive_meeting_date("10 26 22 BOE MM signed.pdf", "") == date(2022, 10, 26)

    def test_impossible_date_rejected(self) -> None:
        # 13.40.22 is not a real date -> no confident parse.
        assert derive_meeting_date("file 13.40.22 thing.pdf", "") is None

    def test_no_date(self) -> None:
        assert derive_meeting_date("Board Candidate Resource Guide", "") is None


class TestIsAnnualSchedule:
    def test_true(self) -> None:
        assert is_annual_schedule("2023 2024 Board of Education Meeting Minutes")

    def test_false_for_single_meeting(self) -> None:
        assert not is_annual_schedule("Apr 12 2023 BOE MM signed.pdf")


class TestPlanChanges:
    def test_retype_and_redate_together(self) -> None:
        docs = [
            {
                "id": 1,
                "document_type": "other",
                "meeting_date": "2026-04-11",
                "meeting_title": "Apr 12 2023 BOE MM signed.pdf",
                "source_file": "Apr 12 2023 BOE MM signed.pdf",
            }
        ]
        (change,) = plan_changes(docs)
        # Re-dating also sets date_source='filename' so the provenance is
        # recorded in the same write (A3 requirement).
        assert change["update"] == {
            "document_type": "minutes",
            "meeting_date": "2023-04-12",
            "date_source": "filename",
        }

    def test_annual_schedule_not_redated(self) -> None:
        docs = [
            {
                "id": 2,
                "document_type": "other",
                "meeting_date": "2024-07-01",
                "meeting_title": "2024 2025 Board of Education Meeting Minutes",
                "source_file": "x.pdf",
            }
        ]
        (change,) = plan_changes(docs)
        assert change["update"] == {"document_type": "schedule"}  # no meeting_date change

    def test_no_change_is_noop(self) -> None:
        docs = [
            {
                "id": 3,
                "document_type": "minutes",
                "meeting_date": "2024-04-10",
                # date_source='filename' means provenance is already correct; a
                # second run should propose nothing.
                "date_source": "filename",
                "meeting_title": "April 10, 2024 Meeting Minutes",
                "source_file": "x.pdf",
            }
        ]
        assert plan_changes(docs) == []

    def test_provenance_only_update_when_date_correct_but_stale(self) -> None:
        # A doc re-dated by an earlier run may have the right date but stale
        # date_source ('default' or 'unknown'). plan_changes must propose a
        # provenance-only write so a subsequent --apply converges the column.
        docs = [
            {
                "id": 4,
                "document_type": "minutes",
                "meeting_date": "2024-04-10",
                "date_source": "default",  # stale — was a fallback at ingest
                "meeting_title": "April 10, 2024 Meeting Minutes",
                "source_file": "x.pdf",
            }
        ]
        (change,) = plan_changes(docs)
        assert change["update"] == {"date_source": "filename"}  # provenance only

    def test_trusted_provenance_not_overwritten(self) -> None:
        # A doc whose date came from a more reliable source ('content' or 'manual')
        # must not have its provenance downgraded to 'filename' even when the
        # filename happens to parse to the same date.
        for trusted_source in ("content", "manual"):
            docs = [
                {
                    "id": 5,
                    "document_type": "minutes",
                    "meeting_date": "2024-04-10",
                    "date_source": trusted_source,
                    "meeting_title": "April 10, 2024 Meeting Minutes",
                    "source_file": "x.pdf",
                }
            ]
            assert plan_changes(docs) == [], f"should be noop for date_source={trusted_source!r}"
