"""Tests for the land-use minutes segmenter (actalux.landuse.segment).

Excerpts are verbatim from real corpus documents — doc 2663 (modern grammar,
2026), doc 1413 (legacy grammar, 2017), doc 1319 (uppercase numbered hybrid) —
so a template drift that breaks parsing breaks these tests, not production
silently.
"""

from __future__ import annotations

from actalux.landuse.segment import classify, entitlement_items, segment_items

MODERN = """\
IV.
Business Matters

New Business
1. 42 North Central Avenue – Conditional Use Permit – Restaurant Use (00:01:10)
Consideration of a request by Lauren Talley, Applicant, on behalf of Tre Cuori Gelateria,
Tenant, for a conditional use permit to operate a gelateria.

Ryan Helle provided a summary of the staff report with a recommendation for the Planning
Commission to recommend approval of the conditional use permit to the City Council as
submitted. Helen DiFate made a motion to recommend approval of the CUP to the city council
as submitted. Jim Arsenault seconded the motion. The motion carried unanimously.

2. 122 North Bemiston Avenue – Architectural Review – Exterior Alterations (00:11:00)
Consideration of a request by Lisa Selligman, Applicant, on behalf of Kevin Gratkowski,
Owner, for review of proposed exterior alterations.

Helen DiFate made a motion to continue the application to a meeting date to be determined.
The motion carried with five votes in favor and one vote opposed.
"""

LEGACY = """\
ARCHITECTURAL REVIEW –ADDITION TO SINGLE-FAMILY RESIDENCE – 7100
WYDOWN BOULEVARD

Bruce Michelson, owner, and Pavel Ivenchuk, project architect, were in attendance at the
meeting. Susan Istenes explained that this is a request for the construction of a
1,450-square-foot rear 2-story addition.
"""


class TestModernGrammar:
    def test_finds_both_items(self) -> None:
        items = segment_items(MODERN)
        assert len(items) == 2

    def test_item_fields(self) -> None:
        it = segment_items(MODERN)[0]
        assert it.address_raw == "42 North Central Avenue"
        assert it.type_raw == "Conditional Use Permit"
        assert it.subtype_raw == "Restaurant Use"
        assert it.video_timestamp == "00:01:10"
        assert it.application_type == "conditional_use"

    def test_body_spans_to_next_header_and_is_verbatim(self) -> None:
        # Downstream extraction verifies source quotes against the span, so the
        # body must be a literal substring of the document.
        it = segment_items(MODERN)[0]
        assert "recommend approval of the CUP" in it.body
        assert "122 North Bemiston" not in it.body
        assert MODERN[it.start : it.end].strip() == it.body

    def test_condition_lists_do_not_false_positive(self) -> None:
        # "1. The applicant shall provide…" inside an item is numbered but is
        # not an item header — the address group must start with a digit.
        text = MODERN + "\n1. The applicant shall provide a landscaping plan.\n"
        assert len(segment_items(text)) == 2

    def test_arb_items_excluded_from_entitlements(self) -> None:
        ents = entitlement_items(MODERN)
        assert [i.application_type for i in ents] == ["conditional_use"]


class TestLegacyGrammar:
    def test_inverted_header_parses(self) -> None:
        items = segment_items(LEGACY)
        assert len(items) == 1
        it = items[0]
        # Inverted order: type first, address last, wrapped across two lines.
        assert it.type_raw == "ARCHITECTURAL REVIEW"
        assert it.address_raw.startswith("7100")
        assert it.video_timestamp is None

    def test_legacy_arb_still_excluded(self) -> None:
        assert entitlement_items(LEGACY) == []


class TestClassify:
    def test_both_architecture_spellings_are_arb(self) -> None:
        # "ARCHITECTURE REVIEW" (2022-era uppercase, doc 1319) vs
        # "Architectural Review" (everywhere else).
        assert classify("ARCHITECTURE REVIEW", "EXTERIOR ALTERATION") == "arb"
        assert classify("Architectural Review", None) == "arb"

    def test_entitlement_vocabulary(self) -> None:
        assert classify("Conditional Use Permit", None) == "conditional_use"
        assert classify("Site Plan Review", None) == "site_plan"
        assert classify("PUBLIC HEARING", "REZONING OF 25 N. CENTRAL") == "rezoning"
        assert classify("Record Plat", None) == "subdivision"
        assert classify("Clayton 2040 Comprehensive Plan", "Residential UDC") == "text_amendment"

    def test_unknown_type_surfaces_as_other_not_dropped(self) -> None:
        assert classify("SOMETHING NOVEL", None) == "other"
