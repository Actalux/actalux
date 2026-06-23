"""Tests for the agenda-docket extractor (boundary detection + confidence grading)."""

from __future__ import annotations

import fitz  # PyMuPDF

from actalux.ingest.docket import extract_docket


def _pdf(pages: list[str]) -> bytes:
    """Build a tiny PDF from per-page text (newlines honored by insert_text)."""
    doc = fitz.open()
    for text in pages:
        doc.new_page().insert_text((72, 72), text)
    return doc.tobytes()


# Realistic page lengths: the extractor rejects dockets under ~200 chars as empty.
_DOCKET = [
    "CITY COUNCIL MEETING AGENDA\nCall to Order and Roll Call of the members present.\n"
    "Approval of the minutes of the previous regular meeting and of any special\n"
    "meetings held since, together with the consent of the members.",
    "1. Bill No. 1234 - an Ordinance amending the municipal code regarding zoning setbacks\n"
    "and lot coverage in the residential districts of the city.\n"
    "2. PUBLIC HEARING on a rezoning application for the property at 123 Main Street.\n"
    "3. Resolution authorizing a contract for street maintenance services this fiscal year.",
    "4. Consent Agenda - routine appropriations, board appointments, and minutes approval.\n"
    "5. Reports from the City Manager, the Mayor, and the standing committees of the council.\n"
    "6. Adjournment of the regular meeting of the City Council.",
]
_ATTACHMENTS = [
    "Staff Report prepared by the Department of Planning and Development Services.\n"
    "Exhibit A contains the financial detail, site maps, and supporting analysis for the\n"
    "matter under consideration, including tables, figures, and correspondence on file."
] * 5


class TestExtractDocket:
    def test_packet_boundary_at_adjournment(self) -> None:
        r = extract_docket(_pdf(_DOCKET + _ATTACHMENTS))
        assert r.confidence in ("high", "medium")
        assert r.boundary_page == 2  # the Adjournment page
        assert r.metadata["docket_page_count"] == 3
        assert r.metadata["boundary_method"] == "adjournment"
        assert "PUBLIC HEARING" in r.text  # docket kept
        assert "Exhibit A" not in r.text  # attachments excluded

    def test_short_agenda_no_attachments(self) -> None:
        r = extract_docket(_pdf(["\n".join(_DOCKET)]))  # whole docket on one page
        assert r.confidence in ("high", "medium")
        assert r.metadata["docket_page_count"] == 1
        assert r.metadata["has_adjournment"] is True

    def test_not_an_agenda_fails(self) -> None:
        prose = "Lorem ipsum dolor sit amet, consectetur adipiscing elit. " * 6
        r = extract_docket(_pdf([prose, prose]))
        assert r.confidence == "failed"
        assert r.text == ""
        assert r.boundary_page is None

    def test_no_adjournment_strong_run_is_medium(self) -> None:
        # No terminal "Adjournment", but a strong marker run that drops off cleanly
        # after the docket -> recovered as medium (ingested), not quarantined.
        r = extract_docket(_pdf([_DOCKET[0] + "\n" + _DOCKET[1], _ATTACHMENTS[0]]))
        assert r.confidence == "medium"
        assert r.metadata["has_adjournment"] is False
        assert r.metadata["boundary_method"] == "marker-run"
        assert "clean marker run" in " ".join(r.metadata["warnings"])

    def test_no_adjournment_weak_run_is_low(self) -> None:
        # A thin agenda (few markers, no Adjournment) stays low -> quarantined, since
        # the boundary guess is not well-supported.
        thin = (
            "CITY COUNCIL MEETING\n"
            "Call to Order and Roll Call of the members present.\n"
            "1. Discussion of the annual community newsletter and the\n"
            "upcoming neighborhood events calendar for residents and\n"
            "visitors to the city during the coming season this year.\n"
            "General announcements and community updates from the staff."
        )
        r = extract_docket(_pdf([thin, _ATTACHMENTS[0]]))
        assert r.confidence == "low"
        assert r.metadata["has_adjournment"] is False
        assert "no Adjournment marker" in " ".join(r.metadata["warnings"])

    def test_malformed_pdf_is_failed_not_raised(self) -> None:
        # Akamai sometimes serves a short corrupt stream that still starts with
        # "%PDF-" (clearing the crawler's magic-byte check). It must grade "failed"
        # (quarantine, link only), not raise and abort the whole crawl.
        r = extract_docket(b"%PDF-1.4 truncated, no xref, no objects")
        assert r.confidence == "failed"
        assert r.text == ""
        assert r.boundary_page is None
        assert "could not read PDF" in " ".join(r.metadata["warnings"])
