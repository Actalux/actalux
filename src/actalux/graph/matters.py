"""Council matters (bills & resolutions): extract references, project matter edges.

A *matter* is a council bill or resolution identified by its stable number — the
number is the durable identity that threads the matter across meetings ("Bill No.
7156" introduced, postponed, then passed). Routine procedural motions (approve the
agenda / consent agenda / minutes) carry no number, so they yield no matter — which
is exactly the line the design draws (connections-graph §13: procedural motions are
not matters).

Pure module: no DB access. scripts/project_member_votes.py mints the matter subjects
(store.upsert_matters) and writes the edges; this module is the deterministic core —
the number regexes and the matter->vote edge shape — so it is unit-testable without a
database. Every edge is ``status='cited'`` and carries the vote's durable identity
``(vote_document_id, vote_ref)`` + citation, so nothing is asserted without a source.
"""

from __future__ import annotations

import re
from collections.abc import Iterable
from dataclasses import dataclass

from actalux.graph.project import quote_hash

# Clayton bills are 4-digit (~6400-7200); resolutions are "YYYY-NN" or "YY-NN" or a
# bare number. Both require the keyword, so a stray number elsewhere is not a matter.
_BILL_RE = re.compile(r"\bbill\s+(?:no\.?\s*)?#?\s*([0-9]{3,5})\b", re.I)
_RESOLUTION_RE = re.compile(
    r"\bresolution\s+(?:no\.?\s*)?#?\s*([0-9]{2,4}(?:-[0-9]{1,4})?)\b", re.I
)
# A bill/resolution's title follows its number as ", an Ordinance ..." / "a Resolution
# ...". Best-effort: used for the directory label; the cited motions carry the full text.
_TITLE_RE = re.compile(r"^\s*,?\s+(an?\s+(?:ordinance|resolution)\b[^.;]{5,200})", re.I)


@dataclass(frozen=True)
class MatterRef:
    """A matter reference parsed from a motion."""

    kind: str  # 'bill' | 'resolution'
    number: str  # '7156' | '2024-19'
    canonical: str  # 'Bill No. 7156'
    slug: str  # 'bill-7156'
    title: str | None  # 'an Ordinance Amending Chapter 405 ...' (best-effort)


def _title_after(text: str, end: int) -> str | None:
    """The ordinance/resolution title immediately following a matter number, if any."""
    m = _TITLE_RE.match(text[end:])
    return " ".join(m.group(1).split()) if m else None


def extract_matter_refs(motion: str) -> list[MatterRef]:
    """Distinct bill/resolution references in one motion, in first-seen order.

    A motion that references several bills yields one ref each; a procedural motion
    with no number yields an empty list.
    """
    text = motion or ""
    refs: dict[str, MatterRef] = {}
    for m in _BILL_RE.finditer(text):
        num = m.group(1)
        slug = f"bill-{num}"
        if slug not in refs:
            title = _title_after(text, m.end())
            refs[slug] = MatterRef("bill", num, f"Bill No. {num}", slug, title)
    for m in _RESOLUTION_RE.finditer(text):
        num = m.group(1)
        slug = f"resolution-{num.lower()}"
        if slug not in refs:
            title = _title_after(text, m.end())
            refs[slug] = MatterRef("resolution", num, f"Resolution No. {num}", slug, title)
    return list(refs.values())


def collect_matter_refs(texts: Iterable[str]) -> dict[str, MatterRef]:
    """Slug -> MatterRef across many texts, keeping the richest title seen.

    The title appears mainly in the introduction ("Bill No. 7156, an Ordinance ...");
    other references ("postpone Bill No. 7156") omit it. So across all references to a
    matter, keep the longest title so the matter directory has a usable label.
    """
    best: dict[str, MatterRef] = {}
    for text in texts:
        for ref in extract_matter_refs(text or ""):
            cur = best.get(ref.slug)
            if cur is None:
                best[ref.slug] = ref
            elif ref.title and (cur.title is None or len(ref.title) > len(cur.title)):
                best[ref.slug] = ref
    return best


def collect_matters(votes: list[dict]) -> dict[str, MatterRef]:
    """Slug -> MatterRef across many votes (over each vote's motion text)."""
    return collect_matter_refs(vote.get("motion") or "" for vote in votes)


# Recent bills not yet voted sit just above the highest voted number (bill numbers are
# assigned sequentially), so the plausible window extends this far past the voted max.
BILL_NUMBER_MARGIN = 100


def select_mintable_matters(
    voted: dict[str, MatterRef],
    candidates: dict[str, MatterRef],
    *,
    bill_margin: int = BILL_NUMBER_MARGIN,
) -> dict[str, MatterRef]:
    """Merge trusted voted matters with never-voted candidates that look real.

    ``voted`` (slug -> MatterRef from vote motions) is authoritative — a numbered motion
    the body acted on — so every voted matter is minted. ``candidates`` (slug -> MatterRef
    from authoritative agenda/minutes text) also catches bills only *scheduled*, never
    voted — but raw document text yields stray hits too (a year like "2026", a 5-digit
    OCR mash, a 3-digit page number). A candidate not already voted is minted only if its
    number looks like a real matter *for this place*, judged against the place's own voted
    matters — no hardcoded ranges, so the guard travels to any jurisdiction:

    - bill: same digit-length as the voted bills AND within ``[min_voted, max_voted +
      bill_margin]``.
    - resolution: only the distinctive hyphenated "YY-NN" / "YYYY-NN" form; a bare number
      is too easily a stray hit.

    With no voted bills to calibrate against, no new bills are minted (conservative).
    Titles merge richest-wins across both sources. Returns the full mintable set.
    """
    merged: dict[str, MatterRef] = dict(voted)
    # A voted matter may carry a richer title in the agenda/minutes text than in its motion.
    for slug, ref in candidates.items():
        cur = merged.get(slug)
        if cur is not None and ref.title and (cur.title is None or len(ref.title) > len(cur.title)):
            merged[slug] = ref

    bill_numbers = [
        int(r.number) for r in voted.values() if r.kind == "bill" and r.number.isdigit()
    ]
    lengths = {len(str(n)) for n in bill_numbers}
    lo = min(bill_numbers) if bill_numbers else None
    hi = max(bill_numbers) if bill_numbers else None

    for slug, ref in candidates.items():
        if slug in voted:
            continue
        if ref.kind == "bill":
            if lo is None or hi is None or not ref.number.isdigit():
                continue
            n = int(ref.number)
            if len(ref.number) in lengths and lo <= n <= hi + bill_margin:
                merged[slug] = ref
        elif ref.kind == "resolution" and "-" in ref.number:
            merged[slug] = ref
    return merged


def derive_matter_edges(votes: list[dict], matter_ids: dict[str, int]) -> list[dict]:
    """matter->vote 'considered' edges for one document's votes.

    ``matter_ids`` maps a matter slug to its (already-minted) subject id. A vote with
    no vote_ref, or one referencing a matter not in the index, yields no edge.
    Deduped per (matter, vote), mirroring migrate_031's partial unique index.
    """
    edges: list[dict] = []
    seen: set[tuple] = set()
    for vote in votes:
        if not vote.get("vote_ref"):
            continue
        qhash = quote_hash(vote.get("source_quote") or "")
        for ref in extract_matter_refs(vote.get("motion") or ""):
            subject_id = matter_ids.get(ref.slug)
            if subject_id is None:
                continue
            key = (vote["document_id"], subject_id, vote["vote_ref"])
            if key in seen:
                continue
            seen.add(key)
            edges.append(
                {
                    "from_subject": subject_id,
                    "vote_document_id": vote["document_id"],
                    "vote_ref": vote["vote_ref"],
                    "source_document_id": vote["document_id"],
                    "type": "considered",
                    "status": "cited",
                    "chunk_id": vote.get("chunk_id"),
                    "citation_id": vote.get("citation_id"),
                    "source_quote": vote.get("source_quote"),
                    "quote_hash": qhash,
                    "as_of_date": vote.get("meeting_date"),
                    "as_of_date_source": "vote",
                    "projection_complete": True,
                }
            )
    return edges


def derive_document_matter_mentions(
    doc_id: int, chunks: list[dict], matter_ids: dict[str, int]
) -> list[dict]:
    """`mentions`-table rows for one document: each chunk that references a minted
    matter's bill/resolution number is a cited occurrence of that matter.

    ``matter_ids`` maps a matter slug to its already-minted subject id, so a chunk
    whose number resolves to no real matter (a stray regex hit) yields nothing —
    which is why this never mints junk matters. A chunk with no ``citation_id`` is
    skipped: the mentions key ``(subject_id, document_id, citation_id)`` requires it
    (connections-graph §4.2). Deduped per (matter, chunk); ``source_quote`` is the
    verbatim chunk text so ``quote_hash`` re-resolves against chunk content (§4.4).
    """
    mentions: list[dict] = []
    seen: set[tuple[int, str]] = set()
    for chunk in chunks:
        citation_id = chunk.get("citation_id")
        if not citation_id:
            continue
        text = chunk.get("content") or ""
        for ref in extract_matter_refs(text):
            subject_id = matter_ids.get(ref.slug)
            if subject_id is None:
                continue
            key = (subject_id, citation_id)
            if key in seen:
                continue
            seen.add(key)
            mentions.append(
                {
                    "subject_id": subject_id,
                    "document_id": doc_id,
                    "chunk_id": chunk.get("id"),
                    "citation_id": citation_id,
                    "source_quote": text,
                    "quote_hash": quote_hash(text),
                    "projection_complete": True,
                }
            )
    return mentions
