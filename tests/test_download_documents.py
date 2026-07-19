"""Unit tests for the Diligent crawler's embedded-link following (no network)."""

from __future__ import annotations

import fitz

import scripts.download_documents as dd

GUID = "06fa6382-d0a2-44f4-89d7-28961f73cea2"


def test_guid_link_matches_both_portal_hosts():
    current = f"https://claytonschools.community.diligentoneplatform.com/document/{GUID}"
    legacy = f"http://claytonschools.diligent.community/document/{GUID}"
    assert dd.GUID_LINK.match(current).group(1) == GUID
    assert dd.GUID_LINK.match(legacy).group(1) == GUID


def test_guid_link_rejects_foreign_hosts_and_non_guid_paths():
    assert dd.GUID_LINK.match(f"https://example.com/document/{GUID}") is None
    assert (
        dd.GUID_LINK.match(
            "https://claytonschools.community.diligentoneplatform.com/document/31498/"
        )
        is None
    )


def test_extract_linked_guids_reads_pdf_annotations(tmp_path):
    pdf = tmp_path / "packet.pdf"
    doc = fitz.open()
    page = doc.new_page()
    upper = GUID.upper()
    page.insert_link(
        {
            "kind": fitz.LINK_URI,
            "from": fitz.Rect(10, 10, 100, 30),
            "uri": f"https://claytonschools.diligent.community/document/{upper}",
        }
    )
    page.insert_link(
        {
            "kind": fitz.LINK_URI,
            "from": fitz.Rect(10, 40, 100, 60),
            "uri": "https://www.claytonschools.net/Page/194",
        }
    )
    doc.save(pdf)
    doc.close()
    assert dd.extract_linked_guids(pdf) == {GUID}


def test_extract_linked_guids_tolerates_non_pdf(tmp_path):
    junk = tmp_path / "notes.txt"
    junk.write_text("not a pdf")
    assert dd.extract_linked_guids(junk) == set()


def test_personnel_hold_back_catches_appendix_titles():
    held = [
        "11.19.2025 Classified Employment.pdf",
        "Certificated Staff Resignations and Retirements v2.pdf",
        "PTTEs.pdf",
        "PTTE_s 1.21.26.pdf",
        "Classified Job Change.pdf",
        "Attendance Awards (1).pdf",
        "Substitute Employment.pdf",
        "Classified New Hires.pdf",
    ]
    for name in held:
        assert dd.PERSONNEL_HOLD_BACK.search(name), name


def test_personnel_hold_back_passes_civic_records():
    kept = [
        "12.10.2025 BOE Meeting Minutes - DRAFT.pdf",
        "Policy IGBB PROGRAMS FOR GIFTED STUDENTS.pdf",
        "2026-2027 Substitute Pay Increase (1).pdf",
        "20260121BondElectionResolution.pdf",
        "BLDDcontract20251119-agenda.pdf",
    ]
    for name in kept:
        assert not dd.PERSONNEL_HOLD_BACK.search(name), name
