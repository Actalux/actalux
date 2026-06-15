"""Tests for homogenized display titles (web.display)."""

from datetime import date

from actalux.web.display import display_title, source_label


class TestSourceLabel:
    """The internal source_portal tag is humanized for display."""

    def test_known_portal_is_humanized(self) -> None:
        assert source_label("diligent") == "Board portal"
        assert source_label("youtube") == "Board meeting video"

    def test_case_insensitive(self) -> None:
        assert source_label("Diligent") == "Board portal"

    def test_unknown_portal_is_title_cased(self) -> None:
        assert source_label("some_new_source") == "Some New Source"

    def test_empty_is_blank(self) -> None:
        assert source_label("") == ""
        assert source_label(None) == ""


def _doc(**kw):
    base = {"document_type": "other", "meeting_date": "2026-04-11", "meeting_title": ""}
    base.update(kw)
    return base


class TestDisplayTitle:
    def test_minutes_is_date_led(self) -> None:
        d = _doc(
            document_type="minutes",
            meeting_date="2023-04-12",
            meeting_title="Apr 12 2023 BOE MM signed.pdf",
        )
        assert display_title(d) == "April 12, 2023 — Meeting Minutes"

    def test_minutes_descriptor_and_draft(self) -> None:
        d = _doc(
            document_type="minutes",
            meeting_date="2024-05-08",
            meeting_title="May 8 2024 BOE Onboarding Retreat MM draft",
        )
        assert display_title(d) == "May 8, 2024 — Meeting Minutes (Retreat, draft)"

    def test_signed_is_not_marked_draft(self) -> None:
        d = _doc(
            document_type="minutes",
            meeting_date="2022-11-16",
            meeting_title="11.16.22 Business Meeting Minutes signed.pdf",
        )
        assert display_title(d) == "November 16, 2022 — Meeting Minutes"

    def test_budget_is_date_led(self) -> None:
        d = _doc(
            document_type="budget",
            meeting_date="2022-06-01",
            meeting_title="Y23 Budget approved 6 1 22.pdf",
        )
        assert display_title(d) == "June 1, 2022 — Budget"

    def test_curriculum_map_uses_cleaned_filename(self) -> None:
        d = _doc(
            document_type="curriculum_map",
            meeting_date="2026-04-11",
            meeting_title="canva K-5 Art Curriculum Map.txt",
        )
        assert display_title(d) == "K-5 Art Curriculum Map"

    def test_governance_uses_cleaned_filename(self) -> None:
        d = _doc(
            document_type="governance",
            meeting_date="2026-04-11",
            meeting_title="Missouri Sunshine Law.pdf",
        )
        assert display_title(d) == "Missouri Sunshine Law"

    def test_accepts_date_object(self) -> None:
        d = _doc(document_type="minutes", meeting_date=date(2026, 2, 4), meeting_title="x.pdf")
        assert display_title(d) == "February 4, 2026 — Meeting Minutes"

    def test_dated_type_without_date_falls_back_to_filename(self) -> None:
        d = _doc(document_type="minutes", meeting_date=None, meeting_title="Some Minutes Doc.pdf")
        assert display_title(d) == "Some Minutes Doc"

    def test_empty_title_falls_back_to_label(self) -> None:
        assert display_title(_doc(document_type="curriculum", meeting_title="")) == "Curriculum"
