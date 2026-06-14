"""Sentence-level extractive snippeting for search results and the reader pane.

Two presentation needs share one idea — find the sentence in a passage that
best answers the query:

- The result card "Match" line shows that sentence (with neighbours up to a
  width budget) instead of a blind window around the first keyword hit, which
  routinely landed on boilerplate.
- The reader pane highlights *only* that sentence inside the cited chunk, so the
  archival-yellow motif marks the relevant clause rather than 200 words of solid
  yellow.

Functions here are pure and HTML-aware only where they must be (term marking and
the snippet emit escaped HTML); the FastAPI layer wraps the returned strings in
``Markup``. No per-result LLM call — this is the deliberately-cheap path.
"""

from __future__ import annotations

import re
from html import escape

# Terms worth scoring on: 3+ alphanumerics, minus a small generic stoplist so a
# query like "the budget" scores sentences on "budget", not "the".
_WORD_RE = re.compile(r"[A-Za-z0-9]{3,}")
_SENTENCE_SPLIT_RE = re.compile(r"(?<=[.!?])\s+")
_WHITESPACE_RE = re.compile(r"\s+")
_STOPWORDS = frozenset(
    {
        "the",
        "and",
        "for",
        "with",
        "that",
        "this",
        "from",
        "are",
        "was",
        "were",
        "has",
        "have",
        "had",
        "not",
        "but",
        "all",
        "any",
        "our",
        "their",
        "its",
        "what",
        "which",
        "who",
        "how",
        "when",
        "where",
        "will",
        "would",
        "can",
        "about",
        "into",
        "over",
        "than",
        "then",
        "they",
        "them",
        "you",
        "your",
    }
)


def extract_query_terms(query: str) -> list[str]:
    """Distinct, order-preserving query terms worth highlighting/scoring on."""
    seen: set[str] = set()
    terms: list[str] = []
    for token in _WORD_RE.findall((query or "").lower()):
        if token in _STOPWORDS or token in seen:
            continue
        seen.add(token)
        terms.append(token)
    return terms


def split_sentences(text: str) -> list[str]:
    """Collapse whitespace and split into sentences on terminal punctuation."""
    cleaned = _WHITESPACE_RE.sub(" ", (text or "").strip())
    if not cleaned:
        return []
    return [s for s in _SENTENCE_SPLIT_RE.split(cleaned) if s]


def best_sentence_index(sentences: list[str], terms: list[str]) -> int:
    """Index of the sentence covering the most query terms (ties: most hits).

    Returns -1 when no sentence contains any query term, so callers can fall
    back to a head-truncation rather than highlighting an arbitrary sentence.
    """
    best_index = -1
    best_score = (0, 0)  # (distinct terms matched, total occurrences)
    for index, sentence in enumerate(sentences):
        lowered = sentence.lower()
        distinct = sum(1 for t in terms if t in lowered)
        if distinct == 0:
            continue
        total = sum(lowered.count(t) for t in terms)
        score = (distinct, total)
        if score > best_score:
            best_score = score
            best_index = index
    return best_index


def mark_terms(text: str, terms: list[str]) -> str:
    """Escape ``text`` and wrap query-term occurrences in ``<mark>``. HTML-safe."""
    if not terms:
        return escape(text)
    pattern = re.compile("(" + "|".join(re.escape(t) for t in terms) + ")", re.IGNORECASE)
    out: list[str] = []
    last = 0
    for match in pattern.finditer(text):
        out.append(escape(text[last : match.start()]))
        out.append("<mark>")
        out.append(escape(match.group(0)))
        out.append("</mark>")
        last = match.end()
    out.append(escape(text[last:]))
    return "".join(out)


def extractive_snippet(content: str, query: str, max_chars: int = 220) -> str:
    """Best-sentence snippet with query terms marked. Returns escaped HTML.

    Picks the most query-relevant sentence, then grows outward to neighbouring
    sentences until the ``max_chars`` budget is spent, so the match reads in
    context. Falls back to a head-truncation when no query term matches.
    Ellipses flag truncation at either edge.
    """
    sentences = split_sentences(content)
    if not sentences:
        return ""
    terms = extract_query_terms(query)

    best = best_sentence_index(sentences, terms)
    if best == -1:
        joined = " ".join(sentences)
        core = joined[:max_chars]
        return mark_terms(core, terms) + ("…" if len(joined) > max_chars else "")

    # Grow a window around the best sentence, preferring to read forward first.
    lo = hi = best
    length = len(sentences[best])
    while True:
        grew = False
        if hi + 1 < len(sentences) and length + 1 + len(sentences[hi + 1]) <= max_chars:
            hi += 1
            length += 1 + len(sentences[hi])
            grew = True
        if lo - 1 >= 0 and length + 1 + len(sentences[lo - 1]) <= max_chars:
            lo -= 1
            length += 1 + len(sentences[lo])
            grew = True
        if not grew:
            break

    core = " ".join(sentences[lo : hi + 1])
    prefix_cut = lo > 0
    suffix_cut = hi < len(sentences) - 1

    # A single sentence longer than the budget: window around the first term hit
    # so the match stays visible rather than head-trimming it off.
    if len(core) > max_chars:
        lowered = core.lower()
        positions = [lowered.find(t) for t in terms if t in lowered]
        pivot = min(positions) if positions else 0
        start = max(0, pivot - max_chars // 2)
        end = min(len(core), start + max_chars)
        start = max(0, end - max_chars)
        prefix_cut = prefix_cut or start > 0
        suffix_cut = suffix_cut or end < len(core)
        core = core[start:end]

    return ("…" if prefix_cut else "") + mark_terms(core, terms) + ("…" if suffix_cut else "")


def split_for_highlight(content: str, query: str) -> tuple[str, str, str]:
    """Split a cited chunk into (before, key_sentence, after) for the reader pane.

    The key sentence is the most query-relevant one; the caller wraps only it in
    the cited-passage highlight. When no query term matches (e.g. a citation
    opened without a search), the whole chunk is the key so the highlight still
    marks the cited unit.
    """
    sentences = split_sentences(content)
    if not sentences:
        return ("", content or "", "")
    terms = extract_query_terms(query)
    best = best_sentence_index(sentences, terms)
    if best == -1:
        return ("", " ".join(sentences), "")
    before = " ".join(sentences[:best])
    after = " ".join(sentences[best + 1 :])
    return (before, sentences[best], after)
