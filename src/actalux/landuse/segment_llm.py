"""LLM segmentation for minutes neither regex grammar can parse.

Two document families fail the deterministic segmenter for the same reason —
no regex-able header. BoA minutes are prose ("An appeal from Sanford Talley,
Applicant, on behalf of Dawn Kotva, Owner of 7451 Bland Avenue, for the
following variance…", doc 1796); Zoom-era PC minutes are table-layout text
whose PDF extraction yields pipe-separated cells (doc 1370). Together they are
the 46/56 unparsed BoA docs and the 47-doc PC 2019-21 cluster
(docs/architecture/land-use-cases-gap-closing.md, G1).

The discipline matches the narrative extractor: the LLM proposes items, but
each item is admitted only by evidence — its ``opening_quote`` must locate
verbatim in the document (whitespace-tolerant), and the located positions, not
the LLM's say-so, define the item spans. Each body runs from its own located
opening to the next item's, so downstream verbatim verification works against
real document text. An opening that fails to locate kills that item to the
review file; it never becomes a guessed span.
"""

from __future__ import annotations

import logging
import re

from actalux.landuse.extract import ExtractError, LlmFn, _parse_json
from actalux.landuse.segment import BusinessItem, classify

logger = logging.getLogger(__name__)

SEGMENT_SYSTEM_PROMPT = """\
You segment municipal meeting minutes into their business items. You are given
the verbatim text of ONE minutes document. It may be prose, or messy
pipe-separated table text. Answer in strict JSON only, no markdown.

Find each BUSINESS item — an application, appeal, variance, permit, plan or
hearing concerning a property or ordinance. Skip roll call, approval of prior
minutes, adjournment, and announcements.

For each item report:
- "opening_quote": the item's first ~10-25 words, copied EXACTLY from the text
  (this is how the item is located; a paraphrase makes the item unusable).
- "address": the street address or property the item concerns, exactly as
  written, or null for a non-property item (e.g. a text amendment).
- "type": the application type as the text states it (e.g. "variance",
  "conditional use permit", "architectural review"), or null.
- "subtype": a stated sub-description, or null.

JSON shape: {"items": [{"opening_quote": "...", "address": "...",
"type": "...", "subtype": "..."}]}
"""

_WS_RUN = re.compile(r"\s+")


def _locate(quote: str, text: str, start_at: int = 0) -> int | None:
    """Index of ``quote`` in ``text`` at/after ``start_at``, whitespace-tolerant.

    Builds a regex from the quote's non-space runs joined by ``\\s+``, so a quote
    the PDF text layer wrapped mid-phrase still locates — the same tolerance
    ``extract.quote_in`` gives verification, but returning the position, which
    span construction needs and a boolean cannot provide.
    """
    tokens = [re.escape(t) for t in _WS_RUN.split(quote.strip()) if t]
    if not tokens:
        return None
    m = re.compile(r"\s+".join(tokens), re.IGNORECASE).search(text, start_at)
    return m.start() if m else None


def llm_segment_items(text: str, llm: LlmFn) -> tuple[list[BusinessItem], list[str]]:
    """Segment an unparseable minutes document via the LLM, spans located verbatim.

    Returns ``(items, rejected_quotes)``. Items are ordinary ``BusinessItem``s —
    everything downstream (classification filter, narrative extraction, linking)
    is unchanged and cannot tell which segmenter produced its input. Rejected
    quotes are openings that failed to locate; the caller surfaces them to QA.

    Items are located in document order, each search starting after the previous
    item's opening, so a repeated phrase cannot bind two items to one position.
    """
    try:
        data = _parse_json(llm(SEGMENT_SYSTEM_PROMPT, text))
    except ExtractError:
        raise
    raw_items = data.get("items")
    if not isinstance(raw_items, list):
        raise ExtractError(f"expected an items list, got {type(raw_items).__name__}")

    located: list[tuple[int, dict]] = []
    rejected: list[str] = []
    cursor = 0
    for it in raw_items:
        if not isinstance(it, dict):
            continue
        quote = it.get("opening_quote") or ""
        pos = _locate(quote, text, cursor)
        if pos is None:
            # One retry from the top: the LLM sometimes reports items out of
            # document order, which the forward-only cursor would misread as a
            # failure to locate.
            pos = _locate(quote, text)
        if pos is None:
            rejected.append(quote[:120])
            continue
        located.append((pos, it))
        cursor = max(cursor, pos + 1)

    located.sort(key=lambda x: x[0])
    items: list[BusinessItem] = []
    for i, (pos, it) in enumerate(located):
        end = located[i + 1][0] if i + 1 < len(located) else len(text)
        body = text[pos:end].strip()
        if not body:
            # Two openings located at the same position (duplicate items from the
            # LLM) or an opening at EOF yield an empty span. An event's
            # source_quote is NOT NULL by CHECK — the constraint that caught this
            # on the first full run — so an empty body can never become an item.
            rejected.append((it.get("opening_quote") or "")[:120])
            continue
        type_raw = (it.get("type") or "").strip()
        subtype = (it.get("subtype") or "").strip() or None
        items.append(
            BusinessItem(
                address_raw=" ".join((it.get("address") or "").split()),
                type_raw=type_raw,
                subtype_raw=subtype,
                video_timestamp=None,
                application_type=classify(type_raw, subtype),
                body=body,
                start=pos,
                end=end,
            )
        )
    if rejected:
        logger.info("llm segmenter rejected %d unlocatable openings", len(rejected))
    return items, rejected
