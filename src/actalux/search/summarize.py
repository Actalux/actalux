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

from openai import OpenAI

from actalux.errors import SummaryError

logger = logging.getLogger(__name__)

HASH_ID_RE = re.compile(r"#q[0-9a-f]{4,}")
DEFAULT_MODEL = "gpt-5-mini"
MAX_TOKENS = 1024  # results summary budget
DOC_SUMMARY_MAX_TOKENS = 120  # one-sentence per-document summary

SYSTEM_PROMPT = """\
You are a civic records assistant for Actalux, an independent, nonpartisan \
service that makes local government records searchable and citable. You \
summarize search results from \
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
    model: str = DEFAULT_MODEL,
    *,
    base_url: str | None = None,
    reasoning_effort: str = "minimal",
) -> Summary:
    """Generate a citation-backed summary from search results.

    Each sentence in the output cites a hash ID. After generation,
    citations are verified against the actual result set. Sentences
    with invalid citations are removed. `base_url` targets an OpenAI-compatible
    gateway (e.g. OpenRouter) for model A/B; `reasoning_effort` tunes OpenAI
    reasoning models.
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

    # Call the LLM
    raw_text = _call_llm(query, quotes_block, api_key, model, base_url, reasoning_effort)

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


def _completion_kwargs(
    model: str,
    messages: list[dict[str, str]],
    max_tokens: int,
    reasoning_effort: str = "minimal",
) -> dict[str, Any]:
    """Build chat-completion kwargs, normalizing across model families.

    OpenAI GPT-5 / o-series are reasoning models: they take `max_completion_tokens`
    plus `reasoning_effort` (without minimal effort they spend the whole budget on
    hidden reasoning and return empty content on short tasks). Every other model --
    gpt-4o-mini, and Claude/Gemini reached via OpenRouter -- takes plain
    `max_tokens` and rejects `reasoning_effort`. The "provider/" prefix
    (OpenRouter's "openai/gpt-5-mini") is stripped before the family check.
    """
    is_openai_reasoning = model.split("/")[-1].lower().startswith(("gpt-5", "o1", "o3", "o4"))
    kwargs: dict[str, Any] = {"model": model, "messages": messages}
    if is_openai_reasoning:
        kwargs["max_completion_tokens"] = max_tokens
        kwargs["reasoning_effort"] = reasoning_effort
    else:
        kwargs["max_tokens"] = max_tokens
    return kwargs


def _call_llm(
    query: str,
    quotes_block: str,
    api_key: str,
    model: str,
    base_url: str | None = None,
    reasoning_effort: str = "minimal",
) -> str:
    """Call the LLM to generate a citation-backed summary.

    `base_url` lets the OpenAI-SDK client target an OpenAI-compatible gateway
    (e.g. OpenRouter) so the summary model can be swapped without a rewrite.
    """
    user_message = USER_PROMPT_TEMPLATE.format(
        query=query,
        quotes_block=quotes_block,
    )

    try:
        client = OpenAI(api_key=api_key, base_url=base_url)
        messages = [
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": user_message},
        ]
        response = client.chat.completions.create(
            **_completion_kwargs(model, messages, MAX_TOKENS, reasoning_effort)
        )
        text = response.choices[0].message.content
        if not text:
            raise SummaryError("LLM returned empty content")
        return text
    except SummaryError:
        raise
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


# --- Card-sized summaries (per-document and per-match) ----------------

DOC_SUMMARY_SYSTEM = """\
You describe Clayton, MO school district public records in one sentence \
for a citizen-facing search archive. Be factual and neutral. Say what the \
document is (its kind, scope, and time frame), not what it argues. Do not \
editorialize. Do not speculate.\
"""

DOC_SUMMARY_USER = """\
Document title: {title}
Document type: {doc_type}
Date: {date}
Source portal: {portal}

Excerpts from the start of the document:

{excerpts}

In one sentence (under 25 words), say what this document is. Use plain \
language. No citations needed (this is descriptive of the document itself).\
"""


def generate_doc_summary(
    title: str,
    doc_type: str,
    date: str,
    portal: str,
    excerpts: list[str],
    api_key: str,
    model: str = DEFAULT_MODEL,
) -> str:
    """One-sentence description of a document. Stored on the document row."""
    excerpts_block = "\n\n".join(e.strip() for e in excerpts if e and e.strip())[:6000]
    user_message = DOC_SUMMARY_USER.format(
        title=title or "(untitled)",
        doc_type=doc_type or "(unknown)",
        date=date or "(undated)",
        portal=portal or "(unknown)",
        excerpts=excerpts_block or "(no excerpts available)",
    )
    try:
        client = OpenAI(api_key=api_key)
        messages = [
            {"role": "system", "content": DOC_SUMMARY_SYSTEM},
            {"role": "user", "content": user_message},
        ]
        response = client.chat.completions.create(
            **_completion_kwargs(model, messages, DOC_SUMMARY_MAX_TOKENS)
        )
        text = (response.choices[0].message.content or "").strip()
        if not text:
            raise SummaryError("doc summary returned empty")
        return text
    except SummaryError:
        raise
    except Exception as exc:
        raise SummaryError(f"doc summary call failed: {exc}") from exc
