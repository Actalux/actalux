"""Citation-backed LLM summaries.

Every AI-generated sentence must cite a chunk by its hash ID (e.g., #q003f).
After generation, each citation is verified against the actual search results.
Sentences with invalid citations are dropped entirely.
"""

from __future__ import annotations

import logging
import re
from collections.abc import Iterator
from dataclasses import dataclass
from typing import Any

from openai import OpenAI

from actalux.errors import SummaryError

logger = logging.getLogger(__name__)

HASH_ID_RE = re.compile(r"#q[0-9a-f]{4,}")

# Baseline-dependent district framing about *prospective* tax/levy/rate impact —
# "no/zero tax rate increase", "without increasing the (debt) levy", "would not
# raise the levy", "tax-rate-neutral". Whether such a change is an "increase"
# depends on the chosen baseline (an expiring bond lowers the levy; a new bond
# restores it), a political judgment Actalux does not make. The answer model
# attributes rather than omits this even when instructed, so a sentence carrying
# it is dropped post-generation (see _verify_sentence). Deliberately matches only
# the NEGATION / "neutral" slogans, never a factual recorded change ("the levy
# rose to $0.51"), which is an affirmative statement and must survive.
_LEVY_FRAMING_RE = re.compile(
    r"(?:no|zero)\s+(?:tax|levy|rate)\b[\w\s\-]{0,14}increase"
    r"|no\s+increase\s+(?:in|to)\b[\w\s]{0,14}(?:tax|levy|rate|debt)"
    r"|without\s+(?:any\s+|an\s+)?increas\w*[\w\s]{0,20}(?:tax|levy|rate|debt)"
    r"|(?:will|would|does|did)\s+not\s+(?:increase|raise|change)\b[\w\s]{0,20}(?:tax|levy|rate|debt)"
    r"|(?:won|wouldn|doesn|didn)['’]?t\s+(?:increase|raise|change)\b[\w\s]{0,20}(?:tax|levy|rate|debt)"
    r"|(?:tax|levy|rate)[\s\-]*(?:rate[\s\-]*)?neutral",
    re.I,
)

DEFAULT_MODEL = "gpt-5-mini"
MAX_TOKENS = 1024  # results summary budget
DOC_SUMMARY_MAX_TOKENS = 256  # short (2-4 sentence) per-document content summary

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
5. Separate facts from the district's framing. State plain facts, figures, dates, \
and recorded decisions directly. For tax, levy, or rate impact, do NOT characterize \
a change as an increase, a decrease, or "no increase": that comparison depends on a \
contested baseline (for example, an expiring bond would lower the levy and a new bond \
restores it), which is a political judgment Actalux does not make. Report ONLY the \
actual levy or rate figures stated in the sources — e.g. the debt-service levy per \
$100 of assessed valuation — each with its citation. Do NOT repeat or quote source \
phrasing such as "no tax rate increase", "zero tax rate increase", or "without \
increasing the debt levy" — not even as an attributed claim; omit it and give the \
figures instead. If the sources state no specific rate, say so rather than describing \
the impact. Attribute any other promotional or evaluative framing ("necessary", \
"premier", "fiscally responsible") to the source rather than stating it yourself.
6. Use plain language. Write 3-6 sentences.
7. If the quotes are not relevant to the query, say so briefly.
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


def _verify_sentence(sentence: str, valid_ids: set[str]) -> tuple[str | None, dict[str, int]]:
    """Verify one sentence's citations; return (kept_sentence_or_None, stats).

    The single source of truth for "does this sentence survive?", shared by the
    batch verifier and the streaming generator so they can never diverge:
      - A bare citation fragment (no prose) is dropped.
      - A citation-free sentence is kept only if it's short/transitional (<=8
        words), never if it makes an uncited factual claim.
      - A cited sentence is kept only if EVERY hash ID it cites is valid.
    """
    citations = HASH_ID_RE.findall(sentence)
    found = len(citations)

    # Drop any sentence carrying baseline-dependent tax/levy/rate framing. The
    # model attributes ("the district says no increase") rather than omitting even
    # when told to, so this is the hard guarantee: state the rate, never "increase
    # / no increase". See _LEVY_FRAMING_RE.
    if _LEVY_FRAMING_RE.search(sentence):
        logger.info("Dropped sentence with baseline-dependent tax/levy framing: %.80s", sentence)
        return None, {"found": found, "verified": 0, "dropped": found}

    # Drop bare citation fragments (e.g., "[#q003f] [#q0042]" with no prose).
    text_without_citations = HASH_ID_RE.sub("", sentence).strip()
    text_without_citations = re.sub(r"[\[\]\s]+", " ", text_without_citations).strip()
    if not text_without_citations:
        return None, {"found": found, "verified": 0, "dropped": 0}

    if not citations:
        # No citations: keep only if transitional/structural (short, no claim).
        if len(sentence.split()) <= 8:
            return sentence, {"found": 0, "verified": 0, "dropped": 0}
        return None, {"found": 0, "verified": 0, "dropped": 0}

    if all(cid in valid_ids for cid in citations):
        return sentence, {"found": found, "verified": found, "dropped": 0}

    bad = [c for c in citations if c not in valid_ids]
    logger.warning("Dropped sentence with invalid citations %s: %.80s", bad, sentence)
    return None, {"found": found, "verified": 0, "dropped": found}


def _verify_citations(text: str, valid_ids: set[str]) -> tuple[str, dict[str, int]]:
    """Verify every citation in the text against the valid set.

    Splits text into sentences and applies :func:`_verify_sentence` to each; any
    sentence with an invalid citation is dropped entirely. Returns the cleaned
    text and stats.
    """
    verified_sentences: list[str] = []
    totals = {"found": 0, "verified": 0, "dropped": 0}

    for sentence in _split_sentences(text):
        kept, stats = _verify_sentence(sentence, valid_ids)
        for key in totals:
            totals[key] += stats[key]
        if kept is not None:
            verified_sentences.append(kept)

    verified_text = " ".join(verified_sentences).strip()
    if not verified_text:
        verified_text = "Could not generate a verified summary for this query."

    return verified_text, totals


def _drain_complete_sentences(buffer: str) -> tuple[list[str], str]:
    """Split a streaming buffer into (complete_sentences, trailing_partial).

    Uses the same boundary as :func:`_split_sentences` (a sentence ends at "." or
    "]" followed by whitespace and a capitalized next word). The final segment is
    always returned as the still-incomplete partial, since more tokens may extend
    it — the caller flushes it when the stream ends.
    """
    parts = re.split(r"(?<=[\].])\s+(?=[A-Z])", buffer)
    if len(parts) <= 1:
        return [], buffer
    *complete, partial = parts
    return [p.strip() for p in complete if p.strip()], partial


def generate_summary_stream(
    query: str,
    results: list[dict[str, Any]],
    api_key: str,
    model: str = DEFAULT_MODEL,
    *,
    base_url: str | None = None,
    reasoning_effort: str = "minimal",
) -> Iterator[str | Summary]:
    """Stream a citation-backed summary, one verified sentence at a time.

    Yields each verified sentence (``str``) as it completes, then a final
    :class:`Summary` (full verified text + stats) as the last item. A sentence is
    revealed only once it is complete AND passes the same per-sentence citation
    check as :func:`generate_summary` (:func:`_verify_sentence`), so a claim is
    never shown and then retracted. The answer model is unchanged; only delivery
    differs. Raises :class:`SummaryError` on stream failure.
    """
    if not results:
        yield Summary(
            text="No matching records found for this query.",
            citations_found=0,
            citations_verified=0,
            citations_dropped=0,
        )
        return

    valid_ids = {r["hash_id"] for r in results}
    user_message = USER_PROMPT_TEMPLATE.format(
        query=query, quotes_block=_build_quotes_block(results)
    )

    kept: list[str] = []
    totals = {"found": 0, "verified": 0, "dropped": 0}

    def _emit(sentence: str) -> Iterator[str]:
        decided, stats = _verify_sentence(sentence, valid_ids)
        for key in totals:
            totals[key] += stats[key]
        if decided is not None:
            kept.append(decided)
            yield decided

    try:
        client = OpenAI(api_key=api_key, base_url=base_url)
        messages = [
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": user_message},
        ]
        stream = client.chat.completions.create(
            stream=True, **_completion_kwargs(model, messages, MAX_TOKENS, reasoning_effort)
        )
        buffer = ""
        for chunk in stream:
            choices = getattr(chunk, "choices", None)
            delta = choices[0].delta.content if choices else None
            if not delta:
                continue
            buffer += delta
            complete, buffer = _drain_complete_sentences(buffer)
            for sentence in complete:
                yield from _emit(sentence)
        tail = buffer.strip()
        if tail:
            yield from _emit(tail)
    except Exception as exc:
        raise SummaryError(f"LLM stream failed: {exc}") from exc

    text = " ".join(kept).strip() or "Could not generate a verified summary for this query."
    yield Summary(
        text=text,
        citations_found=totals["found"],
        citations_verified=totals["verified"],
        citations_dropped=totals["dropped"],
    )


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


def strip_framing_sentences(text: str) -> str:
    """Drop sentences carrying baseline-dependent tax/levy/rate framing.

    The doc-summary path doesn't run citation verification, so it applies this
    guard directly. Same rule as the answer path (:data:`_LEVY_FRAMING_RE`): state
    the rate, never "increase / no increase". If every sentence is framing (rare —
    a summary is multi-topic), the original is kept rather than returning empty.
    """
    kept = [s for s in _split_sentences(text) if not _LEVY_FRAMING_RE.search(s)]
    return " ".join(kept).strip() or text


def extract_citation_ids(text: str) -> list[str]:
    """Extract all hash IDs from text. Useful for testing."""
    return HASH_ID_RE.findall(text)


# --- Conversational query condensation --------------------------------------
# A multi-turn chat (the Ask page) resolves a follow-up against its history into a
# standalone retrieval query, so "what about the year before?" still retrieves the
# right passages. This is the only conversation-aware step; the answer itself is
# generated by generate_summary on the standalone query, reusing its citation
# verification unchanged.

CONDENSE_MAX_TOKENS = 256

CONDENSE_SYSTEM = """\
You rewrite a follow-up question into a single standalone search query for a \
civic-records archive of the Clayton, MO school district. Use the prior \
conversation ONLY to resolve references (pronouns, "that year", "the same fund"). \
Do not answer the question, add facts, or change its meaning. Output ONLY the \
rewritten standalone question, nothing else. If the follow-up is already \
standalone, return it unchanged.\
"""


def condense_question(
    history: list[dict[str, str]],
    question: str,
    api_key: str,
    model: str = DEFAULT_MODEL,
    *,
    base_url: str | None = None,
    reasoning_effort: str = "minimal",
) -> str:
    """Rewrite a follow-up + prior turns into a standalone retrieval query.

    ``history`` is the prior conversation as ``[{"role", "content"}]`` (oldest
    first). With no history, returns ``question`` unchanged (no LLM call). On any
    LLM failure it falls back to the raw question rather than failing the turn, so
    a degraded condense never blocks an answer.
    """
    if not history:
        return question
    convo = "\n".join(f"{t.get('role', '')}: {t.get('content', '')}" for t in history)
    user_message = (
        f"Conversation so far:\n{convo}\n\nFollow-up question: {question}\n\nStandalone question:"
    )
    try:
        client = OpenAI(api_key=api_key, base_url=base_url)
        messages = [
            {"role": "system", "content": CONDENSE_SYSTEM},
            {"role": "user", "content": user_message},
        ]
        response = client.chat.completions.create(
            **_completion_kwargs(model, messages, CONDENSE_MAX_TOKENS, reasoning_effort)
        )
        text = (response.choices[0].message.content or "").strip()
        return text or question
    except Exception:
        logger.warning("condense_question failed; using the raw question", exc_info=True)
        return question


# --- Query expansion (widen recall) -----------------------------------

EXPANSION_MAX_TOKENS = 128

# Stray list markers ("1.", "- ", "* ") a model may prepend despite the prompt.
# Requires a separator so a legitimate phrasing like "2024 budget" is untouched.
_LIST_MARKER_RE = re.compile(r"^\s*(?:[-*•]|\d+[.)])\s+")

EXPANSION_SYSTEM = """\
You widen search recall for a civic-records archive of the Clayton, MO school \
district. Given one search query, output alternate phrasings for the SAME \
information need — wording a record might use when the user's terms differ from \
the document's. Swap colloquial and official terms both ways (e.g. "bond measure" \
↔ "Proposition O" / "bond referendum" / "bond issue"; "school board" ↔ "Board of \
Education"; "superintendent contract" ↔ "employment agreement") and vary close \
synonyms. Do NOT change or narrow the information need, add facts or constraints, \
or answer the question. Output one phrasing per line — just the phrasings, no \
numbering, quotes, or commentary. Output nothing if you cannot improve recall.\
"""


def generate_query_variants(
    query: str,
    api_key: str,
    model: str = "gpt-4o-mini",
    *,
    n: int = 3,
    base_url: str | None = None,
    reasoning_effort: str = "minimal",
) -> list[str]:
    """Generate up to ``n`` alternate phrasings of ``query`` to widen recall.

    Each variant is embedded and searched alongside the original, then the
    candidate pools are fused (see ``hybrid_search``). Returns phrasings that
    DIFFER from the original (case-insensitively), deduplicated and capped at
    ``n``. On any LLM failure — or when nothing useful comes back — it returns
    ``[]`` so expansion degrades to plain single-query retrieval rather than
    failing the search.
    """
    if not query.strip():
        return []
    user_message = f"Search query: {query}\n\nAlternate phrasings:"
    try:
        client = OpenAI(api_key=api_key, base_url=base_url)
        messages = [
            {"role": "system", "content": EXPANSION_SYSTEM},
            {"role": "user", "content": user_message},
        ]
        response = client.chat.completions.create(
            **_completion_kwargs(model, messages, EXPANSION_MAX_TOKENS, reasoning_effort)
        )
        text = response.choices[0].message.content or ""
    except Exception:
        logger.warning("generate_query_variants failed; skipping expansion", exc_info=True)
        return []
    return _dedupe_variants(query, text, n)


def _dedupe_variants(query: str, text: str, n: int) -> list[str]:
    """Parse newline-listed phrasings: drop blanks, list markers, the original, dups."""
    seen = {query.strip().lower()}
    variants: list[str] = []
    for line in text.splitlines():
        cand = _LIST_MARKER_RE.sub("", line).strip().strip('"').strip()
        key = cand.lower()
        if not cand or key in seen:
            continue
        seen.add(key)
        variants.append(cand)
        if len(variants) >= n:
            break
    return variants


# --- Card-sized summaries (per-document and per-match) ----------------

DOC_SUMMARY_SYSTEM = """\
You describe Clayton, MO school district public records for a citizen-facing \
search archive. Summarize what the document covers — its kind and time frame, \
its main topics or sections, and any concrete decisions, votes, or figures it \
records. Be factual and neutral: say what the document is and what is in it, \
not what it argues or implies. Do not editorialize, infer intent, or speculate \
beyond the excerpts provided. If the document characterizes tax, levy, or rate \
impact (e.g. "no tax rate increase"), do NOT restate or quote that framing — give \
any actual levy or rate figures it records instead, and describe the document by \
its subject (e.g. "a letter to residents about Proposition O") without repeating \
the framing.\
"""

DOC_SUMMARY_USER = """\
Document title: {title}
Document type: {doc_type}
Date: {date}
Source: {portal}

Excerpts from the document (sampled across it; may be partial):

{excerpts}

In 2-4 plain-language sentences, summarize what this document covers: its kind \
and time frame, the main topics or sections, and any specific decisions, votes, \
or figures present in the excerpts. Stay factual and neutral; do not editorialize \
or speculate beyond what the excerpts show. No citations needed (this describes \
the document itself).\
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
    """Short (2-4 sentence) content summary of a document. Stored on the row."""
    excerpts_block = "\n\n".join(e.strip() for e in excerpts if e and e.strip())[:8000]
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
        # Same neutrality guard as the answer path: never carry baseline-dependent
        # tax/levy framing into a stored summary.
        return strip_framing_sentences(text)
    except SummaryError:
        raise
    except Exception as exc:
        raise SummaryError(f"doc summary call failed: {exc}") from exc
