"""Citation-backed LLM summaries.

Every AI-generated sentence must cite a chunk by its hash ID (e.g., #q003f).
After generation, each citation is verified against the actual search results.
Sentences with invalid citations are dropped entirely.
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from typing import Any

import anthropic

from actalux.errors import SummaryError

logger = logging.getLogger(__name__)

HASH_ID_RE = re.compile(r"#q[0-9a-f]{4,5}")
MODEL = "claude-sonnet-4-20250514"
MAX_TOKENS = 1024

SYSTEM_PROMPT = """\
You are a civic records assistant for Actalux, a nonprofit that makes local \
government records searchable and citable. You summarize search results from \
Clayton, MO school district board meetings.

Rules:
1. Every factual statement MUST cite a source quote by its hash ID (e.g., #q003f).
2. Place citations inline at the end of the sentence, like: "The board approved \
the budget unanimously. [#q003f]"
3. If you cannot cite a source for a claim, DO NOT include it.
4. Do not editorialize, express opinions, or speculate about intent.
5. Use plain language. Write 3-6 sentences.
6. If the quotes are not relevant to the query, say so briefly.
"""

USER_PROMPT_TEMPLATE = """\
Query: {query}

Here are relevant quotes from official Clayton school district documents, \
ordered from most recent to oldest. Each quote has a hash ID for citation.

{quotes_block}

Write a brief summary of what these quotes tell us about "{query}". \
Prioritize the most recent information. \
Cite every factual claim with the quote's hash ID in brackets, like [#q003f]. \
Do NOT list citation IDs separately — place them inline within sentences.
"""


@dataclass(frozen=True)
class Summary:
    """A citation-backed summary with verified citations."""

    text: str
    citations_found: int
    citations_verified: int
    citations_dropped: int


def generate_summary(
    query: str,
    results: list[dict[str, Any]],
    api_key: str,
) -> Summary:
    """Generate a citation-backed summary from search results.

    Each sentence in the output cites a hash ID. After generation,
    citations are verified against the actual result set. Sentences
    with invalid citations are removed.
    """
    if not results:
        return Summary(
            text="No matching records found for this query.",
            citations_found=0,
            citations_verified=0,
            citations_dropped=0,
        )

    # Build the valid hash ID set from results
    valid_ids = {r["hash_id"] for r in results}

    # Build the quotes block for the prompt
    quotes_block = _build_quotes_block(results)

    # Call Claude
    raw_text = _call_llm(query, quotes_block, api_key)

    # Verify citations
    verified_text, stats = _verify_citations(raw_text, valid_ids)

    return Summary(
        text=verified_text,
        citations_found=stats["found"],
        citations_verified=stats["verified"],
        citations_dropped=stats["dropped"],
    )


def _build_quotes_block(results: list[dict[str, Any]]) -> str:
    """Format search results as a quotes block for the LLM prompt.

    Results are sorted by meeting_date descending (most recent first).
    """
    sorted_results = sorted(
        results,
        key=lambda r: r.get("meeting_date", ""),
        reverse=True,
    )
    lines: list[str] = []
    for r in sorted_results:
        header_parts = [r["hash_id"]]
        if r.get("meeting_date"):
            header_parts.append(str(r["meeting_date"]))
        if r.get("meeting_title"):
            header_parts.append(r["meeting_title"])
        if r.get("section"):
            header_parts.append(r["section"])

        header = " | ".join(header_parts)
        lines.append(f"[{header}]")
        lines.append(r["content"])
        lines.append("")

    return "\n".join(lines)


def _call_llm(query: str, quotes_block: str, api_key: str) -> str:
    """Call Claude to generate a citation-backed summary."""
    user_message = USER_PROMPT_TEMPLATE.format(
        query=query,
        quotes_block=quotes_block,
    )

    try:
        client = anthropic.Anthropic(api_key=api_key)
        response = client.messages.create(
            model=MODEL,
            max_tokens=MAX_TOKENS,
            system=SYSTEM_PROMPT,
            messages=[{"role": "user", "content": user_message}],
        )
        block = response.content[0]
        if block.type != "text":
            raise SummaryError("LLM returned non-text content block")
        return block.text  # type: ignore[union-attr]
    except Exception as exc:
        raise SummaryError(f"LLM call failed: {exc}") from exc


def _verify_citations(text: str, valid_ids: set[str]) -> tuple[str, dict[str, int]]:
    """Verify every citation in the text against the valid set.

    Splits text into sentences. Any sentence containing an invalid
    citation is dropped entirely. Returns the cleaned text and stats.
    """
    sentences = _split_sentences(text)
    verified_sentences: list[str] = []

    total_found = 0
    total_verified = 0
    total_dropped = 0

    for sentence in sentences:
        citations = HASH_ID_RE.findall(sentence)
        total_found += len(citations)

        # Drop bare citation fragments (e.g., "[#q003f] [#q0042]" with no prose)
        text_without_citations = HASH_ID_RE.sub("", sentence).strip()
        text_without_citations = re.sub(r"[\[\]\s]+", " ", text_without_citations).strip()
        if not text_without_citations:
            continue

        if not citations:
            # Sentence with no citations — keep it only if it's
            # transitional/structural (short, no factual claims)
            if len(sentence.split()) <= 8:
                verified_sentences.append(sentence)
            continue

        # Check all citations in this sentence
        all_valid = all(cid in valid_ids for cid in citations)
        if all_valid:
            total_verified += len(citations)
            verified_sentences.append(sentence)
        else:
            total_dropped += len(citations)
            bad = [c for c in citations if c not in valid_ids]
            logger.warning("Dropped sentence with invalid citations %s: %.80s", bad, sentence)

    verified_text = " ".join(verified_sentences).strip()

    if not verified_text:
        verified_text = "Could not generate a verified summary for this query."

    return verified_text, {
        "found": total_found,
        "verified": total_verified,
        "dropped": total_dropped,
    }


def _split_sentences(text: str) -> list[str]:
    """Split text into sentences, keeping trailing citations attached.

    A citation like [#q003f] after a period stays with its sentence.
    Split only when a new uppercase word starts a new factual claim.
    """
    # Split on sentence boundaries: period (optionally followed by
    # citation brackets) then space and uppercase letter starting a new sentence.
    # The citation block stays with the preceding sentence.
    parts = re.split(r"(?<=[\].])\s+(?=[A-Z])", text)
    return [p.strip() for p in parts if p.strip()]


def extract_citation_ids(text: str) -> list[str]:
    """Extract all hash IDs from text. Useful for testing."""
    return HASH_ID_RE.findall(text)
