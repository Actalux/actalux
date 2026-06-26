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


def collect_matters(votes: list[dict]) -> dict[str, MatterRef]:
    """Slug -> MatterRef across many votes, keeping the richest title seen.

    The title appears mainly in the introduction motion; other motions ("postpone
    Bill No. 7156") omit it. So across all references to a matter, keep the longest
    title so the matter directory has a usable label.
    """
    best: dict[str, MatterRef] = {}
    for vote in votes:
        for ref in extract_matter_refs(vote.get("motion") or ""):
            cur = best.get(ref.slug)
            if cur is None:
                best[ref.slug] = ref
            elif ref.title and (cur.title is None or len(ref.title) > len(cur.title)):
                best[ref.slug] = ref
    return best


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
