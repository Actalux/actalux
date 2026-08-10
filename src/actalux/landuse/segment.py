"""Segment PC-ARB / BoA minutes into business items, across both header grammars.

The corpus uses two templates (verified against real documents,
docs/architecture/land-use-cases.md):

- **Modern, 2022-present** (reference doc 2663): numbered items,
  ``N. <address> – <type> – <subtype> (HH:MM:SS)``. Address first; the trailing
  timestamp indexes into the meeting video.
- **Legacy, 2015-2021** (reference doc 1413): unnumbered uppercase headings in
  the **inverted** order ``TYPE – SUBTYPE – ADDRESS``, no timestamps, and OCR
  artifacts like a missing space after the dash ("–ADDITION").

Everything here is deterministic and pure: text in, items out, each item carrying
its character span so downstream extraction can cite verbatim. Classification of
an item into the entitlement vocabulary is a keyword map over the type/subtype
strings — ARB-only items classify as ``arb`` so the v1 ingest filter can drop
them before a row ever exists, per the scope decision.
"""

from __future__ import annotations

import re
from dataclasses import dataclass

# En dash, em dash, or hyphen, with optional (OCR-dropped) surrounding spaces.
_DASH = r"\s*[–—-]\s*"

# Modern: "1. 42 North Central Avenue – Conditional Use Permit – Restaurant Use (00:01:10)"
# The subtype and timestamp are both optional; the address starts with a number.
_MODERN_HDR = re.compile(
    rf"^\s*(\d+)\.\s+(\d+[^\n–—-]{{3,70}}?){_DASH}([^\n–—(-]{{3,60}}?)"
    rf"(?:{_DASH}([^\n(]{{3,70}}?))?\s*(?:\((\d{{1,2}}:\d{{2}}:\d{{2}})\))?\s*$",
    re.M,
)

# Legacy: "ARCHITECTURAL REVIEW –ADDITION TO SINGLE-FAMILY RESIDENCE – 7100
# WYDOWN BOULEVARD" — all caps, type first, address last (starts with digits).
# The PDF text layer wraps long headers, so the address may continue onto the
# next line ("– 7100\nWYDOWN BOULEVARD"); one continuation line is allowed and
# the captured address is whitespace-collapsed before storage.
_LEGACY_HDR = re.compile(
    rf"^([A-Z][A-Z /&,]{{4,60}}?){_DASH}(?:([A-Z][A-Z0-9 /&,.'()-]{{3,80}}?){_DASH})?"
    rf"(\d+[A-Z0-9 .'-]{{0,60}}(?:\n[A-Z][A-Z0-9 .'-]{{2,60}})?)\s*$",
    re.M,
)

# type/subtype keywords -> application_type vocabulary. Checked in order against
# the concatenated type+subtype string; first hit wins. 'arb' is not a stored
# type — it marks items the entitlement filter drops.
_TYPE_MAP: list[tuple[re.Pattern[str], str]] = [
    (re.compile(r"conditional\s+use|CUP\b", re.I), "conditional_use"),
    (re.compile(r"variance", re.I), "variance"),
    (re.compile(r"re-?zon|zoning\s+map", re.I), "rezoning"),
    (
        re.compile(
            r"subdivision|lot\s+split|consolidation\s+plat|resubdivision|record\s+plat", re.I
        ),
        "subdivision",
    ),
    (
        re.compile(
            r"text\s+amendment|UDC|unified\s+development|ordinance\s+amendment|comprehensive\s+plan",
            re.I,
        ),
        "text_amendment",
    ),
    (re.compile(r"site\s+plan", re.I), "site_plan"),
    # ARB-only work: out of v1 scope by operator decision. Both spellings occur
    # in the corpus: "Architectural Review" (2017, 2026) and "ARCHITECTURE
    # REVIEW" (2022-era uppercase variant, doc 1319).
    (
        re.compile(
            r"architectur(?:al|e)\s+review|signage|sign\b|exterior\s+alteration|awning|fence|landscap",
            re.I,
        ),
        "arb",
    ),
]


@dataclass(frozen=True)
class BusinessItem:
    """One agenda/business item as it appears in a minutes document."""

    address_raw: str
    type_raw: str
    subtype_raw: str | None
    video_timestamp: str | None  # "HH:MM:SS" into the meeting video, modern era only
    application_type: str  # vocabulary value, or 'arb' (excluded) / 'other'
    body: str  # the item's narrative text, verbatim
    start: int  # character span of the whole item in the document text,
    end: int  # so downstream extraction can verify source quotes against it


def classify(type_raw: str, subtype_raw: str | None) -> str:
    """Map an item's type/subtype strings onto the application_type vocabulary."""
    hay = f"{type_raw} {subtype_raw or ''}"
    for pat, app_type in _TYPE_MAP:
        if pat.search(hay):
            return app_type
    return "other"


def _items_from_matches(
    text: str, matches: list[tuple[re.Match, str, str, str | None, str | None]]
) -> list[BusinessItem]:
    """Build items from header matches; each body runs to the next header."""
    items: list[BusinessItem] = []
    for i, (m, address, type_raw, subtype, ts) in enumerate(matches):
        end = matches[i + 1][0].start() if i + 1 < len(matches) else len(text)
        items.append(
            BusinessItem(
                address_raw=" ".join(address.split()),
                type_raw=type_raw.strip(),
                subtype_raw=subtype.strip() if subtype else None,
                video_timestamp=ts,
                application_type=classify(type_raw, subtype),
                body=text[m.start() : end].strip(),
                start=m.start(),
                end=end,
            )
        )
    return items


def segment_items(text: str) -> list[BusinessItem]:
    """All business items in a minutes document, whichever era's grammar it uses.

    The modern grammar is tried first; when it finds nothing the legacy grammar
    runs. They are never mixed within one document — a modern doc's condition
    lists ("1. The applicant shall…") would false-positive weaker patterns, which
    is why the modern header requires the address-first shape and the legacy
    header requires the all-caps type-first shape.
    """
    modern: list = []
    for m in _MODERN_HDR.finditer(text):
        _num, address, type_raw, subtype, ts = m.groups()
        # Condition lists inside an item ("1. The applicant shall provide…") can
        # match numerically; requiring the address group to start with a digit
        # already filters most, and a type group that parses as prose (lowercase
        # sentence) filters the rest.
        modern.append((m, address, type_raw, subtype, ts))
    if modern:
        return _items_from_matches(text, modern)

    legacy: list = []
    for m in _LEGACY_HDR.finditer(text):
        type_raw, subtype, address = m.groups()
        legacy.append((m, address, type_raw, subtype, None))
    return _items_from_matches(text, legacy)


def entitlement_items(text: str) -> list[BusinessItem]:
    """Only the items in v1 scope: entitlements, never ARB-only work.

    'other' is kept — an unclassified entitlement should surface for review
    rather than vanish; 'arb' is dropped so no row for a homeowner's exterior
    alteration is ever created (docs/architecture/land-use-cases.md, scope).
    """
    return [it for it in segment_items(text) if it.application_type != "arb"]
