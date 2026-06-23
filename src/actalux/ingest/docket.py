"""Extract the agenda "docket" (item list) from a CivicPlus agenda packet PDF.

A CivicPlus "Final Agenda" PDF bundles the docket (a few high-signal pages: the
agenda items, bills, hearings, ending at "Adjournment") with appended attachments
(staff reports, ordinances, exhibits — the bulk). We want only the docket as
verbatim searchable text and link the full packet separately, WITHOUT a fragile
fixed-page cutoff and WITHOUT silently ingesting a mis-extracted docket.

Approach (multi-signal, graded; see notes 2026-06-22, codex-consulted + probed a
real Clayton packet): Clayton's PDFs carry no forward internal links (codex's
preferred primary signal), so the boundary is the terminal **"Adjournment"** item,
validated by per-page agenda-marker density. Every extraction is graded
HIGH/MEDIUM/LOW/FAILED with metadata; only HIGH/MEDIUM are ingested — LOW/FAILED
are quarantined (link the PDF, flag for review) rather than silently clipped.
"""

from __future__ import annotations

import re
from dataclasses import dataclass

import fitz  # PyMuPDF

from actalux.ingest.parser import _ocr_page

# Agenda-ness signals: procedural headers + legislative item types.
_MARKER_RE = re.compile(
    r"bill no\.|ordinance|resolution|public hearing|consent agenda|adjourn|roll call|call to order",
    re.IGNORECASE,
)
_ITEM_RE = re.compile(r"^\s{0,4}\d{1,2}\.\s", re.MULTILINE)  # "1. ", "12. "
_ADJOURN_RE = re.compile(r"adjourn", re.IGNORECASE)

# A docket longer than this is implausible for a council/commission agenda — treat
# as a failed boundary rather than swallowing attachment pages.
_MAX_DOCKET_PAGES = 15
_MIN_DOCKET_CHARS = 200
_OCR_PAGE_MIN_CHARS = 40  # below this a leading page is treated as scanned -> OCR it


@dataclass(frozen=True)
class DocketResult:
    """The extracted docket text plus a confidence grade and audit metadata.

    ``text`` is empty unless ``confidence`` is "high"/"medium" (the only grades a
    caller should ingest); "low"/"failed" mean quarantine the packet (link only).
    """

    text: str
    confidence: str  # "high" | "medium" | "low" | "failed"
    boundary_page: int | None
    metadata: dict


def _page_markers(text: str) -> int:
    return len(_MARKER_RE.findall(text)) + len(_ITEM_RE.findall(text))


def extract_docket(pdf_bytes: bytes) -> DocketResult:
    """Grade and (when confident) extract the docket from an agenda packet PDF."""
    doc = fitz.open(stream=pdf_bytes, filetype="pdf")
    try:
        n = doc.page_count
        texts = [str(doc[i].get_text("text", sort=True)) for i in range(n)]
        scan = min(n, _MAX_DOCKET_PAGES)
        # Scanned/image-only agenda: the leading window has almost no extractable
        # text, so a real docket would be quarantined as "no markers". OCR those
        # pages first. Degrades to the native (empty) text when tesseract is absent,
        # so a host without an OCR toolchain still grades, just without recovery.
        if sum(len(t) for t in texts[:scan]) < _MIN_DOCKET_CHARS:
            for i in range(scan):
                if len(texts[i].strip()) < _OCR_PAGE_MIN_CHARS:
                    ocr = _ocr_page(doc[i])
                    if ocr.strip():
                        texts[i] = ocr
    finally:
        doc.close()

    marks = [_page_markers(t) for t in texts]
    warnings: list[str] = []

    def fail(reason: str) -> DocketResult:
        warnings.append(reason)
        return DocketResult(
            "",
            "failed",
            None,
            {
                "pdf_page_count": n,
                "boundary_method": "none",
                "confidence": "failed",
                "warnings": warnings,
            },
        )

    # Not an agenda, or scanned/un-extractable.
    if sum(marks[:scan]) < 3:
        return fail("no agenda markers in leading pages")
    if sum(len(t) for t in texts[:scan]) < _MIN_DOCKET_CHARS:
        return fail("leading pages have almost no extractable text (scanned?)")

    # Primary boundary: first "Adjournment" on an agenda-ish page within the window.
    adjourn = next(
        (
            i
            for i in range(scan)
            if _ADJOURN_RE.search(texts[i]) and (marks[i] > 0 or (i and marks[i - 1] > 0))
        ),
        None,
    )
    if adjourn is not None:
        boundary, method = adjourn, "adjournment"
        after = marks[boundary + 1 : boundary + 4]
        docket_marks = sum(marks[: boundary + 1])
        peak = max(marks[: boundary + 1]) or 1
        clear_drop = boundary + 1 >= n or not after or max(after) <= max(1, peak // 3)
        confidence = "high" if docket_marks >= 5 and clear_drop else "medium"
        if not clear_drop:
            warnings.append("agenda-marker density does not drop cleanly after the boundary")
    else:
        # Fallback: the contiguous run of marker-bearing pages from the front.
        boundary, method = 0, "marker-run"
        while boundary + 1 < n and boundary + 1 < _MAX_DOCKET_PAGES and marks[boundary + 1] > 0:
            boundary += 1
        # A strong, self-delimiting marker run (no terminal "Adjournment" printed,
        # but plenty of agenda markers that drop off cleanly after the run) is a real
        # docket -> medium, so it is ingested rather than quarantined. A weak or
        # ambiguous run stays low. This is signal-based, not a blind page cutoff.
        docket_marks = sum(marks[: boundary + 1])
        after = marks[boundary + 1 : boundary + 4]
        peak = max(marks[: boundary + 1]) or 1
        clear_drop = boundary + 1 >= n or not after or max(after) <= max(1, peak // 3)
        if docket_marks >= 5 and clear_drop:
            confidence = "medium"
            warnings.append("no Adjournment marker; boundary from a clean marker run")
        else:
            confidence = "low"
            warnings.append("no Adjournment marker; boundary inferred from marker run")

    if boundary + 1 > _MAX_DOCKET_PAGES:
        return fail(f"docket implausibly long ({boundary + 1} pages)")

    text = "\n".join(texts[: boundary + 1]).strip()
    if len(text) < _MIN_DOCKET_CHARS:
        return fail("extracted docket text too short")

    meta = {
        "pdf_page_count": n,
        "docket_page_count": boundary + 1,
        "boundary_page": boundary,
        "boundary_method": method,
        "confidence": confidence,
        "docket_marker_count": sum(marks[: boundary + 1]),
        "has_adjournment": adjourn is not None,
        "warnings": warnings,
    }
    return DocketResult(text, confidence, boundary, meta)
