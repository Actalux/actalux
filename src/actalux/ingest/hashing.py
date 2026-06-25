"""Content hashing for document change detection and stable citation ids."""

from __future__ import annotations

import hashlib

# Length (hex chars) of a chunk's stable citation id. 8 hex = 32 bits: a compact
# token (#qa3f91c08) that is collision-safe at the corpus scale (~15k chunks ->
# ~2.6% chance of a single collided pair anywhere; tolerated by routing, which
# prefers the current-version chunk and logs ambiguity).
CITATION_ID_LEN = 8


def content_hash(text: str) -> str:
    """SHA-256 hex digest of document content, whitespace-normalized."""
    normalized = " ".join(text.split())
    return hashlib.sha256(normalized.encode()).hexdigest()


def _normalize_for_citation(text: str) -> str:
    """Whitespace-collapsed, case-folded text for content-addressed hashing.

    Matches ``db._normalize_chunk_text`` so the citation id absorbs the cosmetic
    reflow differences between a PDF twin and an HTML twin without changing which
    passage it identifies.
    """
    return " ".join((text or "").split()).casefold()


def doc_stable_key(source_ref: str, content_hash_value: str, source_file: str) -> str:
    """The most stable available document identity for citation hashing.

    Prefers ``source_ref`` (normalized origin URL — survives a filename change or
    a PDF/HTML twin), then ``content_hash`` (stable for unchanged content), then
    the filename. Empty only for a document carrying none of the three.
    """
    return source_ref or content_hash_value or source_file or ""


def compute_citation_id(doc_key: str, content: str, dup_ordinal: int = 0) -> str:
    """Stable, content-addressed citation id for a chunk (``CITATION_ID_LEN`` hex).

    Derived from the document's stable identity (``doc_key``) and the chunk's
    normalized content, so re-ingesting an unchanged document reproduces the same
    id even though the SERIAL row id is reassigned. ``dup_ordinal`` disambiguates
    a passage that repeats verbatim within the same document (0 for the first
    occurrence, 1 for the next, ...); keyed on appearance order so it stays stable
    across re-ingest, unlike ``chunk_index`` (which shifts when any earlier chunk
    is added or removed).
    """
    payload = doc_key + "\n" + _normalize_for_citation(content)
    if dup_ordinal:
        payload += f"\n#{dup_ordinal}"
    return hashlib.sha256(payload.encode()).hexdigest()[:CITATION_ID_LEN]


def assign_citation_ids(doc_key: str, contents: list[str]) -> list[str]:
    """Citation ids for one document's chunks, in order, disambiguating repeats.

    The only place ``dup_ordinal`` is derived: a verbatim passage repeated within
    the document gets 0, 1, 2, ... by appearance order, so each repeat earns a
    distinct (still stable) id while a unique passage keeps the plain content hash.
    """
    seen: dict[str, int] = {}
    out: list[str] = []
    for content in contents:
        norm = _normalize_for_citation(content)
        ordinal = seen.get(norm, 0)
        seen[norm] = ordinal + 1
        out.append(compute_citation_id(doc_key, content, ordinal))
    return out


def compute_vote_ref(citation_id: str, ordinal_within_chunk: int) -> str:
    """Stable, content-addressed id for one vote, for graph edges to reference.

    A vote is anchored to its citing chunk's ``citation_id`` (itself stable across
    re-ingest, see ``compute_citation_id``) plus its appearance order among the
    votes that resolve to the same ``citation_id`` within the document — so two
    motions sharing one chunk earn distinct refs. The SERIAL ``votes.id`` cannot
    serve this role: ``extract_votes`` delete/reinserts vote rows every run, which
    reassigns it. ``citation_id`` must be non-empty; the caller refuses a vote
    whose chunk lacks one rather than collide every such vote onto ``sha256(":N")``.
    """
    if not citation_id:
        raise ValueError("compute_vote_ref requires a non-empty citation_id")
    return hashlib.sha256(f"{citation_id}:{ordinal_within_chunk}".encode()).hexdigest()
