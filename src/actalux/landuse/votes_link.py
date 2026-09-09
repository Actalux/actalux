"""Link business items to their vote rows by verbatim position (G4).

Motion text alone cannot link votes to items: Clayton minutes decide different
items with byte-identical sentences ("Helen DiFate made a motion to approve as
submitted" appears three times in doc 1250, as three distinct votes on three
items). What distinguishes them is *where* each occurrence sits.

The pairing is positional and exact: a vote's ``source_quote`` is a verbatim
passage of the document (the vote parsers guarantee it), and votes parse in
document order (ascending id). So the k-th occurrence of a repeated quote in
the document belongs to the k-th vote carrying that quote, giving every vote a
character position; an item then links to the votes positioned inside its own
span. When several votes land in one item's span — a procedural motion
("open the public hearing") followed by the decision — the last one wins:
minutes record an item's decisive motion after its procedure. No fuzzy
matching anywhere; an unlocatable quote simply leaves its vote unlinked.
"""

from __future__ import annotations

from actalux.landuse.segment import BusinessItem
from actalux.landuse.segment_llm import _locate


def position_votes(doc_content: str, doc_votes: list[dict]) -> dict[int, int]:
    """Character position of each vote's source_quote, k-th occurrence to k-th vote.

    ``doc_votes`` are one document's vote rows (needing ``id`` and
    ``source_quote``). Votes sharing an identical quote are ordered by id —
    parse order, which is document order — and consume successive occurrences.
    A quote that cannot be located (or runs out of occurrences) leaves its vote
    out of the map.
    """
    by_quote: dict[str, list[dict]] = {}
    for v in sorted(doc_votes, key=lambda v: v["id"]):
        # The motion is the anchor, not source_quote: measured on the full
        # corpus (2026-09-01), every one of 698 votes' motions locates in its
        # document, while 357 source_quotes do not — the longer passage crosses
        # interleaved page footers the vote parsers strip but the raw text keeps.
        quote = v.get("motion") or v.get("source_quote") or ""
        if quote.strip():
            by_quote.setdefault(quote, []).append(v)

    positions: dict[int, int] = {}
    for quote, votes in by_quote.items():
        cursor = 0
        for v in votes:
            pos = _locate(quote, doc_content, cursor)
            if pos is None:
                break
            positions[v["id"]] = pos
            cursor = pos + 1
    return positions


def link_votes(
    doc_content: str, items: list[BusinessItem], doc_votes: list[dict]
) -> dict[int, int]:
    """Map item start offsets to vote ids, ``{item.start: vote_id}``.

    An item links to the last vote positioned inside its span (see module
    docstring for why last). Items whose span holds no positioned vote are
    absent from the map — null vote_id, never a guess.
    """
    positions = position_votes(doc_content, doc_votes)
    links: dict[int, int] = {}
    for item in items:
        inside = [(pos, vid) for vid, pos in positions.items() if item.start <= pos < item.end]
        if inside:
            links[item.start] = max(inside)[1]
    return links
