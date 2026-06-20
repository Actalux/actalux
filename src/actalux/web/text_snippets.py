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

# Timestamp patterns found in YouTube auto-captions:
#   [mm:ss]       bracket form:   [02:14]
#   HH:MM:SS      plain colon:    0:02:14  or  02:14
# A "standalone" timestamp line is one where the entire stripped line is a
# timestamp (possibly with surrounding brackets/whitespace).  We drop those
# lines rather than inline-timestamp markers that happen to appear mid-sentence.
_STANDALONE_TIMESTAMP_RE = re.compile(
    r"^\s*"
    r"(?:"
    r"\[?\d{1,2}:\d{2}(?::\d{2})?\]?"  # [mm:ss], mm:ss, [HH:MM:SS], HH:MM:SS
    r")"
    r"\s*$",
    re.MULTILINE,
)
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


def normalize_whitespace(text: str) -> str:
    """Collapse runs of whitespace/newlines to single spaces; strip.

    Reflows PDF/transcript extraction (which is full of mid-paragraph line
    breaks) into readable prose WITHOUT changing any characters, so a displayed
    cited passage stays verbatim word-for-word. Deeper extraction artifacts
    (glued tokens like ``2022/23budget``) are an ingest-quality fix, not a
    display-time edit.
    """
    return _WHITESPACE_RE.sub(" ", (text or "").strip())


_BLANK_LINE_RE = re.compile(r"\n\s*\n")


def content_paragraphs(text: str) -> list[str]:
    """Split a document's stored text into readable paragraphs for full display.

    Splits on blank lines (real paragraph breaks) and collapses the single
    newlines inside each block to spaces — so a transcript wrapped at ~60 chars
    mid-sentence reads as flowing prose, while a PDF/markdown body keeps its
    paragraphs. Verbatim words are unchanged (only whitespace is reflowed).
    """
    blocks = _BLANK_LINE_RE.split((text or "").strip())
    return [_WHITESPACE_RE.sub(" ", b).strip() for b in blocks if b.strip()]


def paragraphize_prose(text: str, sentences_per_para: int = 4) -> list[str]:
    """Group unbroken prose into readable paragraphs by sentence count.

    Whisper stores a transcript as one continuous block (segments joined with
    spaces, no newlines), so ``content_paragraphs``/``reflow_transcript`` — which
    split on blank lines — would return a single wall of text. This groups the
    sentences into paragraphs of roughly ``sentences_per_para`` so a full
    transcript reads as prose. Mechanical and verbatim-safe: sentences are
    re-joined with single spaces and only the paragraph grouping is added (no word
    change, no dedup). Returns a list of paragraph strings; empty input yields [].
    """
    sentences = split_sentences(text)
    if not sentences:
        return []
    step = max(1, sentences_per_para)
    return [" ".join(sentences[i : i + step]) for i in range(0, len(sentences), step)]


# ──────────────────────────────────────────────────────────────────────────────
# Transcript-specific presentation helpers (YouTube / auto-caption source only)
# ──────────────────────────────────────────────────────────────────────────────
# ALL transforms here are MECHANICAL — they touch only whitespace, duplicate
# tokens, and timestamp-only lines.  Word content is NEVER changed so that
# a displayed passage remains verbatim for citation purposes.

#: Label shown above a displayed transcript block. Board-meeting transcripts are
#: machine-generated (Whisper), so the text is accurate-but-imperfect — flag that.
TRANSCRIPT_CAPTION_LABEL = "Machine-generated transcript — may contain errors."


def strip_transcript_timestamps(text: str) -> str:
    """Remove lines that consist solely of a timestamp marker.

    Drops lines like ``[02:14]``, ``0:02:14``, ``02:14``, ``[0:12:34]``.
    These appear as standalone caption-navigation markers in YouTube
    auto-captions and add noise without contributing word content.

    Only standalone timestamp lines are removed — timestamps embedded
    mid-sentence are left alone (rare in practice, but verbatim-safe rule).
    """
    # Split → drop matching lines → rejoin with the same newlines.
    lines = text.split("\n")
    kept = [line for line in lines if not _STANDALONE_TIMESTAMP_RE.match(line)]
    return "\n".join(kept)


def dedup_rolling_captions(text: str) -> str:
    """Collapse rolling-caption prefix repetition where the whole prior line repeats.

    YouTube auto-captions sometimes repeat the previous caption line as the
    first words of the next line (a rolling-window overlap).  This function
    handles the case where the ENTIRE previous line is an exact word-for-word
    prefix of the current line, e.g.::

        and the board approved
        and the board approved the budget

    Only exact whole-line-prefix matches are collapsed — not partial overlaps.
    This conservative approach avoids silently removing real repeated speech
    (e.g. "Thank you" followed by "Thank you for coming" is *not* collapsed,
    because the intent cannot be determined mechanically).

    .. warning::
        Not called from :func:`reflow_transcript` because even this conservative
        form can remove intentional repetition (verbatim-safety rule for a citation
        archive).  Available for callers that accept this tradeoff.
    """
    lines = text.split("\n")
    result: list[str] = []
    for line in lines:
        stripped = line.rstrip()
        if not result or not stripped:
            result.append(stripped)
            continue
        prev = result[-1]
        # Require a minimum overlap length to reduce false collapses on short phrases.
        # Compare lowercased to handle case drift in auto-captions.
        prev_words = prev.lower().split()
        cur_words = stripped.lower().split()
        if prev_words and len(prev_words) >= 4 and cur_words[: len(prev_words)] == prev_words:
            # Current line fully contains the previous line as a prefix — replace.
            result[-1] = stripped
        else:
            result.append(stripped)
    return "\n".join(result)


# Minimum word count for a block to be kept as its own paragraph; shorter
# fragments are merged into the previous block when possible.
_MIN_PARAGRAPH_WORDS = 8


def reflow_transcript(text: str) -> list[str]:
    """Reflow YouTube auto-caption text into readable paragraphs.

    Pipeline (all mechanical, no LLM, verbatim-safe):
    1. Strip standalone timestamp lines (no word content; safe to drop).
    2. Split on blank lines to get raw caption blocks.
    3. Collapse intra-block newlines to spaces (60-char hard-wraps → prose).
    4. Merge short trailing fragments into the previous paragraph.

    Rolling-caption deduplication (:func:`dedup_rolling_captions`) is deliberately
    NOT applied here: it could silently remove intentional repeated speech from a
    citation archive — the verbatim-safety risk exceeds the presentation benefit.

    Returns a list of paragraph strings.  Each paragraph retains the original
    verbatim words; only whitespace is changed.
    """
    cleaned = strip_transcript_timestamps(text)
    raw_blocks = _BLANK_LINE_RE.split(cleaned.strip())
    paragraphs: list[str] = []
    for block in raw_blocks:
        para = _WHITESPACE_RE.sub(" ", block).strip()
        if not para:
            continue
        # Merge short fragments into the previous paragraph to avoid
        # one-sentence stubs that read as orphaned bullets.
        if paragraphs and len(para.split()) < _MIN_PARAGRAPH_WORDS:
            paragraphs[-1] = paragraphs[-1] + " " + para
        else:
            paragraphs.append(para)
    return paragraphs


def clean_text_light(text: str) -> str:
    """Collapse runs of whitespace to single spaces; strip leading/trailing.

    A lighter alternative to ``content_paragraphs`` for non-transcript chunks
    in the reader pane.  Does NOT split on blank lines (which would wreck
    tabular/budget content) and does NOT apply any transcript-specific
    deduplication.  Only whitespace is changed; all words are verbatim.
    """
    return _WHITESPACE_RE.sub(" ", (text or "").strip())


# ──────────────────────────────────────────────────────────────────────────────


def split_sentences(text: str) -> list[str]:
    """Collapse whitespace and split into sentences on terminal punctuation."""
    cleaned = normalize_whitespace(text)
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


def marked_paragraphs(content: str, query: str) -> list[str]:
    """Reflow a chunk into paragraphs with only the query's terms wrapped in <mark>.

    For the reader pane: the cited passage reads as clean paragraphs (verbatim
    words, whitespace reflowed) with the matching words highlighted, rather than a
    solid highlight over the whole block — "highlight everything" reads the same
    as "highlight nothing." Each returned string is escaped HTML; the caller wraps
    it in ``<p>``. With no query terms (a citation opened without a search),
    paragraphs are simply escaped and nothing is marked.
    """
    paras = content_paragraphs(content)
    if not paras:
        normalized = normalize_whitespace(content)
        paras = [normalized] if normalized else []
    terms = extract_query_terms(query)
    return [mark_terms(p, terms) for p in paras]


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
        # No query term occurs in this (semantic) match: lead with clean prose
        # rather than whatever bullet/table glyph the extraction left at the head.
        joined = _LEAD_NOISE_RE.sub("", " ".join(sentences)).lstrip()
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


# Leading noise left by PDF/markdown extraction at the head of a displayed quote:
# layout glyphs (bullets, checkbox artifacts, stray brackets, blockquote/heading
# markers), zero-width/format characters, and Private-Use-Area glyphs (U+E000–
# U+F8FF — embedded-font bullets/symbols that extraction emits as PUA code points,
# e.g. U+F0B7). Deliberately excludes letters, digits, quotes, and currency so no
# semantic character is ever stripped.
_LEAD_NOISE_RE = re.compile(
    r"^[\s\u200b-\u200d\u2060\ufeff\ue000-\uf8ff"
    r"\[\]\u2022\u00b7\u25aa\u25e6\u2023*|>#\u2026]+"
)


def lead_sentence(content: str, query: str = "", max_chars: int = 240) -> str:
    """One clean verbatim sentence for a citation list — the most query-relevant
    sentence, whitespace-normalised, free of highlight markup and extraction noise.

    The topic "what X has said" lists lead with the document and show a single
    readable quote rather than the raw windowed snippet (which dumped a run-on,
    ellipsis-bracketed block). Only whitespace and leading layout glyphs are
    removed; the words themselves stay verbatim, and the full passage is one click
    away on the source page. A sentence longer than ``max_chars`` is truncated at
    a word boundary with an ellipsis.
    """
    sentences = split_sentences(content)
    if not sentences:
        return ""
    terms = extract_query_terms(query)
    idx = best_sentence_index(sentences, terms)
    sentence = sentences[idx] if idx != -1 else sentences[0]
    sentence = _LEAD_NOISE_RE.sub("", sentence).strip()
    if len(sentence) > max_chars:
        sentence = sentence[:max_chars].rsplit(" ", 1)[0].rstrip(",;:—– ") + "…"
    return sentence


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
