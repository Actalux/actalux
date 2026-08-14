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


def _packet_linking(path, guid):
    """A one-page PDF whose only link points at `guid` on the portal."""
    doc = fitz.open()
    page = doc.new_page()
    page.insert_link(
        {
            "kind": fitz.LINK_URI,
            "from": fitz.Rect(10, 10, 100, 30),
            "uri": f"https://claytonschools.diligent.community/document/{guid}",
        }
    )
    doc.save(path)
    doc.close()


def test_linked_manifest_entry_carries_the_packet_and_its_date(tmp_path, monkeypatch):
    # A packet attachment (an exec summary, a policy PDF, a signed MOU) usually
    # states no date of its own. The packet that links it does, and that is the
    # only thing that knows which meeting the attachment belongs to — dropping it
    # is what left 20 attachments undatable after the fact.
    packet = tmp_path / "12.10.2025 BOE Meeting Minutes - DRAFT.pdf"
    _packet_linking(packet, GUID)

    monkeypatch.setattr(dd, "OUTPUT_DIR", tmp_path)
    monkeypatch.setattr(dd, "LINKED_INDEX_PATH", tmp_path / "index.json")
    monkeypatch.setattr(dd, "load_linked_index", lambda: {})
    monkeypatch.setattr(
        dd, "download_document", lambda client, guid: ("Policy JEC.pdf", b"%PDF-1.4 ")
    )

    manifest: list[dict[str, str]] = []
    dd.follow_embedded_links(client=None, scan_paths=[packet], seen_guids=set(), manifest=manifest)

    assert len(manifest) == 1
    entry = manifest[0]
    assert entry["linked_from"] == packet.name
    assert entry["meeting_date"] == "2025-12-10"
    assert entry["date_source"] == "packet"


def test_linked_manifest_omits_a_date_when_the_packet_has_none(tmp_path, monkeypatch):
    # No date on the packet means no date for the attachment. Ingest must receive
    # nothing rather than something plausible.
    packet = tmp_path / "Board Candidate Resource Guide.pdf"
    _packet_linking(packet, GUID)

    monkeypatch.setattr(dd, "OUTPUT_DIR", tmp_path)
    monkeypatch.setattr(dd, "LINKED_INDEX_PATH", tmp_path / "index.json")
    monkeypatch.setattr(dd, "load_linked_index", lambda: {})
    monkeypatch.setattr(
        dd, "download_document", lambda client, guid: ("Policy JEC.pdf", b"%PDF-1.4 ")
    )

    manifest: list[dict[str, str]] = []
    dd.follow_embedded_links(client=None, scan_paths=[packet], seen_guids=set(), manifest=manifest)

    assert manifest[0]["linked_from"] == packet.name
    assert "meeting_date" not in manifest[0]
    assert "date_source" not in manifest[0]


def test_linked_manifest_never_packet_dates_minutes(tmp_path, monkeypatch):
    # Draft minutes are linked from the packet of the meeting that APPROVES
    # them, so a packet date for minutes is one meeting late, every time — the
    # first packet-dating pass filed four drafts under the wrong meetings.
    packet = tmp_path / "January 21, 2026 BOE Meeting Minutes.pdf"
    _packet_linking(packet, GUID)
    monkeypatch.setattr(dd, "OUTPUT_DIR", tmp_path)
    monkeypatch.setattr(dd, "LINKED_INDEX_PATH", tmp_path / "index.json")
    monkeypatch.setattr(dd, "load_linked_index", lambda: {})
    monkeypatch.setattr(
        dd, "download_document", lambda client, guid: ("Odd Minutes - DRAFT.pdf", b"%PDF-1.4 ")
    )
    manifest: list[dict[str, str]] = []
    dd.follow_embedded_links(client=None, scan_paths=[packet], seen_guids=set(), manifest=manifest)
    assert "meeting_date" not in manifest[0]  # undated beats one-meeting-late


def test_linked_manifest_child_filename_beats_packet_date(tmp_path, monkeypatch):
    # A child whose own filename parses keeps that identity; the packet date is
    # only a fallback for genuinely undated attachments.
    packet = tmp_path / "January 21, 2026 BOE Meeting Minutes.pdf"
    _packet_linking(packet, GUID)
    monkeypatch.setattr(dd, "OUTPUT_DIR", tmp_path)
    monkeypatch.setattr(dd, "LINKED_INDEX_PATH", tmp_path / "index.json")
    monkeypatch.setattr(dd, "load_linked_index", lambda: {})
    monkeypatch.setattr(
        dd,
        "download_document",
        lambda client, guid: ("12.10.2025 Exec Summary.pdf", b"%PDF-1.4 "),
    )
    manifest: list[dict[str, str]] = []
    dd.follow_embedded_links(client=None, scan_paths=[packet], seen_guids=set(), manifest=manifest)
    assert "meeting_date" not in manifest[0]  # ingest derives 2025-12-10 from the filename
