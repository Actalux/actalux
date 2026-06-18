"""Turn a change digest into a cited, nonpartisan draft post (themed roundup).

For each topic that gained documents, the drafter pulls those documents' own
passages as citeable evidence and runs the *same* citation-verified
``generate_summary`` the site uses — so a claim that cannot cite a source is
dropped, and a topic paragraph appears only when it carries verified citations.
The ``[#qXXXX]`` markers are then linked to the exact source passage on the live
site. Nothing here publishes; the result is markdown for human review.
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from datetime import date, datetime

from supabase import Client

from actalux.db import get_document_chunks
from actalux.digest.change_digest import ChangedDoc, ChangeDigest
from actalux.errors import SummaryError
from actalux.models import chunk_hash_id
from actalux.search.summarize import DEFAULT_MODEL, Summary, generate_summary

logger = logging.getLogger(__name__)

# Leading chunks pulled per document, and the cap per topic, so a long document
# (or a busy week) cannot blow the summary prompt. Leading chunks favour a
# document's opening/overview, which is what a "what's new" paragraph wants.
MAX_CHUNKS_PER_DOC = 4
MAX_CHUNKS_PER_THEME = 16

# Reuse the site's citation token shape (#q + >=4 hex), matched inside brackets.
_CITATION_RE = re.compile(r"\[(#q[0-9a-f]{4,})\]")


@dataclass(frozen=True)
class DraftPost:
    """A review-ready draft: an email subject, the markdown body, and stats."""

    subject: str
    markdown: str
    doc_count: int
    theme_count: int
    citations_verified: int
    citations_dropped: int


def _human_date(value: str) -> str:
    """Render an ISO date/timestamp as e.g. "June 11, 2026"; pass through on failure."""
    if not value:
        return ""
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00")).date()
    except ValueError:
        try:
            parsed = date.fromisoformat(value[:10])
        except ValueError:
            return value
    return f"{parsed:%B} {parsed.day}, {parsed.year}"


def _oneline(text: str) -> str:
    """Collapse whitespace so a stored summary stays on a single markdown bullet."""
    return " ".join((text or "").split())


def _evidence_for_docs(
    client: Client,
    docs: list[ChangedDoc],
    *,
    max_per_doc: int,
    max_per_theme: int,
) -> list[dict[str, object]]:
    """Build citeable evidence rows from a topic's documents' own passages.

    Mirrors the dict shape ``generate_summary`` reads (hash_id, content,
    meeting_date, meeting_title, section) plus ``cite_ref`` for linking, drawing
    each chunk's stable ``citation_id`` so a citation resolves to the same passage
    the rest of the site links to. Capped per document and per topic.
    """
    evidence: list[dict[str, object]] = []
    for doc in docs:
        if len(evidence) >= max_per_theme:
            break
        chunks = get_document_chunks(client, doc.id, limit=max_per_doc)
        for chunk in chunks:
            if len(evidence) >= max_per_theme:
                break
            citation_id = chunk.get("citation_id") or ""
            cite_ref = citation_id or chunk["id"]
            evidence.append(
                {
                    "chunk_id": chunk["id"],
                    "cite_ref": cite_ref,
                    "hash_id": chunk_hash_id(cite_ref),
                    "content": chunk.get("content", ""),
                    "section": chunk.get("section", ""),
                    "meeting_date": doc.meeting_date,
                    "meeting_title": doc.title,
                }
            )
    return evidence


def _link_citations(text: str, evidence: list[dict[str, object]], site_base_url: str) -> str:
    """Rewrite ``[#qXXXX]`` markers into markdown links to the live source passage.

    Uses the same ``cite_ref`` routing as the web app's citation linker, so the
    link target is the durable ``/chunk/{ref}/source`` page. A marker with no
    matching evidence row (should not happen post-verification) is left as text.
    """
    id_map = {row["hash_id"]: row["cite_ref"] for row in evidence}
    base = site_base_url.rstrip("/")

    def repl(match: re.Match[str]) -> str:
        hash_id = match.group(1)
        cite_ref = id_map.get(hash_id)
        if cite_ref is None:
            return match.group(0)
        return f"([{hash_id}]({base}/chunk/{cite_ref}/source))"

    return _CITATION_RE.sub(repl, text)


def _doc_bullet(doc: ChangedDoc, site_base_url: str) -> str:
    """One markdown list item for a changed document: label, meta, summary, link."""
    base = site_base_url.rstrip("/")
    meta = [doc.status, doc.source_portal, _human_date(doc.meeting_date)]
    meta_line = " · ".join(p for p in meta if p)
    summary = _oneline(doc.summary)
    tail = f" {summary}" if summary else ""
    return (
        f"- **{doc.title}** — *{meta_line}*.{tail} [Open the original →]({base}/document/{doc.id})"
    )


def draft_post(
    client: Client,
    digest: ChangeDigest,
    api_key: str,
    *,
    generated_on: str | None = None,
    model: str = DEFAULT_MODEL,
    base_url: str | None = None,
    site_base_url: str = "https://actalux.org",
    reasoning_effort: str = "minimal",
    max_chunks_per_doc: int = MAX_CHUNKS_PER_DOC,
    max_chunks_per_theme: int = MAX_CHUNKS_PER_THEME,
) -> DraftPost:
    """Assemble a themed-roundup draft from a change digest.

    Each topic that gained documents gets a citation-verified summary paragraph
    (omitted when no claim could be cited) followed by the list of its documents.
    Citations are linked to the live site. The post never publishes — it is a draft
    for human review, and the header says so. Raises nothing for an empty digest;
    callers should check ``digest.is_empty`` first.
    """
    by_theme = digest.by_theme()
    since_label = _human_date(digest.since)
    on_label = _human_date(generated_on) if generated_on else ""

    verified_total = 0
    dropped_total = 0
    body: list[str] = []

    for theme, docs in by_theme.items():
        body.append(f"## {theme}")
        evidence = _evidence_for_docs(
            client,
            docs,
            max_per_doc=max_chunks_per_doc,
            max_per_theme=max_chunks_per_theme,
        )
        if evidence:
            query = (
                f"new and updated {theme.lower()} in the Clayton school district "
                f"public record" + (f" since {since_label}" if since_label else "")
            )
            try:
                summary: Summary = generate_summary(
                    query,
                    evidence,
                    api_key,
                    model=model,
                    base_url=base_url,
                    reasoning_effort=reasoning_effort,
                )
            except SummaryError:
                logger.warning("draft summary failed for theme %r; listing docs only", theme)
            else:
                verified_total += summary.citations_verified
                dropped_total += summary.citations_dropped
                if summary.citations_verified > 0:
                    body.append(_link_citations(summary.text, evidence, site_base_url))
        for doc in docs:
            body.append(_doc_bullet(doc, site_base_url))
        body.append("")

    doc_count = len(digest.docs)
    theme_count = len(by_theme)
    when = f" ({on_label})" if on_label else ""
    plural = "s" if doc_count != 1 else ""
    subject = f"Actalux draft: {doc_count} new Clayton schools record{plural}{when}"

    header = _build_header(doc_count, since_label, on_label)
    markdown = "\n".join([*header, "", *body]).rstrip() + "\n"

    return DraftPost(
        subject=subject,
        markdown=markdown,
        doc_count=doc_count,
        theme_count=theme_count,
        citations_verified=verified_total,
        citations_dropped=dropped_total,
    )


def _build_header(doc_count: int, since_label: str, on_label: str) -> list[str]:
    """The post title, a factual one-line lede, and the review/nonpartisan banner."""
    noun = "document" if doc_count == 1 else "documents"
    window = f" since {since_label}" if since_label else ""
    stamp = f" as of {on_label}" if on_label else ""
    return [
        "# What changed in the Clayton schools public record",
        "",
        f"*{doc_count} new or updated {noun}{window}{stamp}.* "
        "Every statement below cites a source passage in the Actalux archive.",
        "",
        "> **Draft for human review — not published.** Assembled automatically from "
        "newly ingested public records. Actalux is independent and nonpartisan: before "
        "publishing, confirm no advocacy or campaign framing slipped in, and that "
        "content stays within board and administration policy.",
    ]
