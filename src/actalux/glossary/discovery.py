"""Discover proper-name manglings by aligning ASR transcripts to authoritative text.

Whisper transcribes meetings well but misspells proper nouns the human record spells
correctly ("Musco Sports Lighting" -> "moscow sports", "Yorg" -> "york"). For a given
meeting the authoritative spellings are knowable from two sources:

1. the place's roster/lexicon of officials (cross-meeting canonical names + aliases), and
2. the proper nouns in *that meeting's own* agenda/minutes (human-typed, per-meeting:
   the streets, businesses, and applicants actually discussed that night).

The cardinal safety property is **we never invent a spelling**. Discovery is anchored on
the authoritative vocabulary: every proposed correction's ``canonical`` side is a name
that demonstrably appears in one of those sources, and every ``mangled`` side is a token
that appears verbatim in the transcript. The output is candidate ``name_corrections``
rows for a human gate — never an edit to stored transcript text (verbatim integrity).

The matcher is deterministic (no LLM): clear multi-word manglings land in the ``high``
bucket; the phonetically-distant or single-token majority land in ``review`` for the
operator to confirm. Confidence thresholds are named constants below and are tunable.
"""

from __future__ import annotations

from collections import Counter, defaultdict
from dataclasses import dataclass, replace
from typing import Any

from rapidfuzz import fuzz
from rapidfuzz.distance import JaroWinkler, Levenshtein

from actalux.graph.resolve import normalize_name

# Fuzzy score is 0..1. Chosen so a clear multi-word corruption auto-qualifies while the
# ambiguous tail is held for human review rather than written blind.
HIGH_SCORE = 0.92
REVIEW_FLOOR = 0.80
SINGLE_TOKEN_REVIEW = 0.88  # single-word manglings are riskier -> higher floor, never auto
AMBIG_EPS = 0.05  # if a rival canonical scores within this of the best, it's ambiguous
MIN_CANONICAL_LEN = 5  # below this a canonical is too short to match without noise
MAX_NAME_TOKENS = 4

# Capitalized tokens that are not proper nouns worth correcting (procedural vocabulary,
# honorifics, calendar words). Compared against the normalized (casefolded) form.
_STOPWORDS = frozenset(
    """
    the a an and or of to in on for with by at from as we i he she they it this that
    board council commission city district school schools meeting minutes agenda
    motion second seconded carried approved approve ayes nays yeas present absent
    mr mrs ms mx dr member members chair chairman chairwoman mayor alderman alderwoman
    commissioner president vice resolution ordinance bill item items public hearing
    monday tuesday wednesday thursday friday saturday sunday
    january february march april may june july august september october november december
    """.split()
)

_TRIM = ".,;:()[]{}\"'’`?!“”‘-–—"
_SENT_END = frozenset(".,;:!?")  # a token ending in these closes a name run (sentence/list break)


def _strip(token: str) -> str:
    return token.strip(_TRIM)


def norm_key(name: str) -> str:
    """The module's normalized match key: ``normalize_name`` plus apostrophe folding.

    Apostrophes are dropped (``Mayor's`` -> ``mayors``, ``County's`` -> ``countys``) so a
    possessive in one source matches its plain form in the other. Shared by the matcher
    and the CLI so a proposal's key and the DB-dedup key are computed identically.
    """
    return normalize_name(name.replace("’", "'").replace("'", ""))


def _alnum(s: str) -> str:
    return "".join(ch for ch in s if ch.isalnum())


def _is_trivial_variant(a: str, b: str) -> bool:
    """True when two normalized names differ only by spacing/hyphenation or a plural 's'.

    "republic services"/"Republic Service" and "multifamily"/"multi-family" are not
    spelling manglings — full-text search already stems them — so they are not worth a
    correction row. A genuine typo ("solberger"/"Sollberger") changes the letters.
    """
    aa, bb = _alnum(a), _alnum(b)
    return aa == bb or aa == bb + "s" or bb == aa + "s"


def _is_stopword_phrase(norm: str) -> bool:
    """True when every token of a normalized phrase is procedural/calendar vocabulary."""
    parts = norm.split()
    return not parts or all(p in _STOPWORDS for p in parts)


def _emit_grams(run: list[str], counts: Counter, max_tokens: int) -> None:
    for n in range(1, max_tokens + 1):
        for i in range(len(run) - n + 1):
            counts[" ".join(run[i : i + n])] += 1


def extract_proper_nouns(text: str, *, max_tokens: int = MAX_NAME_TOKENS) -> Counter:
    """Capitalized n-grams (1..``max_tokens`` words) and their counts.

    Runs of consecutive Title-Case words are collected line by line (a newline or a
    lowercase/punctuation token ends a run), then every n-gram within each run is
    emitted. This keeps "Susan Buse" and "Van't Hof" intact while still surfacing the
    parts of longer spans. Sentence-initial common words are caught too; downstream
    filtering (stopwords, known-name exclusion, the score gate) removes that noise.
    """
    counts: Counter = Counter()
    for line in text.splitlines():
        run: list[str] = []
        for raw in line.split():
            tok = _strip(raw)
            if tok and tok[0].isupper() and any(ch.isalpha() for ch in tok):
                run.append(tok)
                if raw[-1] in _SENT_END:  # "Shaw Park." / "Buse," ends the name span
                    _emit_grams(run, counts, max_tokens)
                    run = []
            else:
                _emit_grams(run, counts, max_tokens)
                run = []
        _emit_grams(run, counts, max_tokens)
    return counts


def _score(a: str, b: str) -> float:
    """Edit-distance similarity (0..1) of two normalized names.

    Word-order-sensitive on purpose: an ASR mangling mis-hears a word *in place*
    (musco -> moscow), so a reordering ("Winston Jordan" vs "Jordan Winston") is NOT a
    mangling and must not score high (token_sort/token_set are avoided for that reason).

    Jaro-Winkler is used only for single-token names, where its prefix weighting catches
    real typos (yorg/york). For multi-word names it is *harmful* — a shared first name
    inflates the score for two different people ("Josh Goodman"/"Josh Corson") or for a
    sub/super-phrase ("Shaw Park"/"Shaw Park Pool") — so multi-word names use plain
    ``ratio``, which penalizes the discriminating (surname) tokens.
    """
    if " " in a or " " in b:
        return fuzz.ratio(a, b) / 100.0
    return max(JaroWinkler.normalized_similarity(a, b), fuzz.ratio(a, b) / 100.0)


def _confidence(mangled_norm: str, canonical_norm: str, score: float) -> str | None:
    """Bucket a candidate as ``high``, ``review``, or ``None`` (discard).

    A multi-word name reaches ``high`` either on a strong ratio or on a small *absolute*
    edit distance — a 1-2 character change ("shah park"/"Shaw Park") is a typo even when
    the short string keeps the ratio modest, whereas a 5+ change ("Shaw Park Pool" vs
    "Shaw Park") is a different entity, not a misspelling. Single-token names never
    auto-qualify: a lone surname is too ambiguous to apply without a human look.
    """
    multiword = len(canonical_norm.split()) >= 2 and len(mangled_norm.split()) >= 2
    if multiword:
        distance = Levenshtein.distance(mangled_norm, canonical_norm)
        if score >= HIGH_SCORE or (distance <= 2 and score >= REVIEW_FLOOR):
            return "high"
        if score >= REVIEW_FLOOR:
            return "review"
        return None
    if score >= SINGLE_TOKEN_REVIEW:
        return "review"
    return None


@dataclass(frozen=True)
class Candidate:
    """A correctly-spelled name and where it is documented."""

    canonical: str  # surface form as printed in the authoritative source
    norm: str  # normalized match key
    source: str  # "lexicon" or "doc <id> (<type>, <date>)"
    category: str  # person | other (best guess; refined by the operator)
    n_tokens: int


@dataclass
class Vocabulary:
    """The authoritative names for one meeting (or place)."""

    candidates: list[Candidate]
    known_norm: set[str]  # normalized names that are already correct -> never a mangling


@dataclass(frozen=True)
class Mangling:
    """A candidate ``mangled -> canonical`` correction grounded in the record."""

    mangled: str  # normalized transcript form (the correction key)
    surface: str  # transcript surface form (for the evidence snippet)
    canonical: str
    score: float
    occurrences: int  # times the mangled form appears in this transcript
    confidence: str  # "high" | "review"
    category: str
    source: str  # where the canonical is documented


def build_vocabulary(
    lexicon_entries: list[dict[str, Any]],
    auth_docs: list[dict[str, Any]],
    *,
    min_len: int = MIN_CANONICAL_LEN,
) -> Vocabulary:
    """Assemble the authoritative vocabulary from officials + per-meeting documents.

    Parameters
    ----------
    lexicon_entries
        ``place_lexicon`` output: officials with ``canonical_name`` and ``aliases``.
        The canonical name is a correction target (category ``person``); aliases are
        only added to the known-correct set so legitimate alias use is not flagged.
    auth_docs
        The meeting's agenda/minutes as ``{id, document_type, meeting_date, text}``.
        Their proper nouns are human-typed, so they are both correction targets
        (category ``other``) and known-correct names.
    """
    by_norm: dict[str, Candidate] = {}
    known: set[str] = set()

    for entry in lexicon_entries:
        name = entry["canonical_name"]
        norm = norm_key(name)
        if norm:
            known.add(norm)
        for alias in entry.get("aliases", []):
            an = norm_key(alias.get("raw", "")) or alias.get("normalized", "")
            if an:
                known.add(an)
        if norm and len(norm) >= min_len and norm not in by_norm:
            by_norm[norm] = Candidate(name, norm, "lexicon", "person", len(norm.split()))

    for doc in auth_docs:
        src = f"doc {doc['id']} ({doc.get('document_type', '?')}, {doc.get('meeting_date', '?')})"
        for surface in extract_proper_nouns(doc.get("text", "")):
            norm = norm_key(surface)
            if not norm:
                continue
            known.add(norm)
            if len(norm) < min_len or _is_stopword_phrase(norm) or norm in by_norm:
                continue
            by_norm[norm] = Candidate(surface, norm, src, "other", len(norm.split()))

    return Vocabulary(list(by_norm.values()), known)


def _transcript_grams_by_token_count(text: str) -> dict[int, list[tuple[str, str, int]]]:
    """Normalized transcript proper-noun n-grams bucketed by token count.

    Returns ``{n_tokens: [(norm, surface, count), ...]}``. Surface form kept for the
    evidence snippet; the most frequent surface wins when several normalize alike.
    """
    merged: dict[str, tuple[str, int]] = {}
    for surface, count in extract_proper_nouns(text).items():
        norm = norm_key(surface)
        if not norm:
            continue
        if norm in merged:
            best_surface, total = merged[norm]
            merged[norm] = (surface if count > total else best_surface, total + count)
        else:
            merged[norm] = (surface, count)

    buckets: dict[int, list[tuple[str, str, int]]] = defaultdict(list)
    for norm, (surface, count) in merged.items():
        buckets[len(norm.split())].append((norm, surface, count))
    return buckets


def find_manglings(
    transcript_text: str,
    vocab: Vocabulary,
    *,
    existing_norm: frozenset[str] = frozenset(),
) -> list[Mangling]:
    """Candidate corrections for one transcript, anchored on ``vocab``.

    For each authoritative candidate, the transcript's proper-noun n-grams of
    comparable length are scored; a near-but-not-exact match that is not itself a
    known-correct name (and not already a logged correction) becomes a candidate. When
    two different canonicals tie for the same mangled form the result is downgraded to
    ``review`` rather than guessing which is right (connections-graph conservatism).
    """
    buckets = _transcript_grams_by_token_count(transcript_text)
    # Individual tokens of every known-correct name: a lone "susan"/"park"/"buse" is a
    # correct name-part, not a mangling, even though it isn't a full known name.
    known_tokens = {tok for kn in vocab.known_norm for tok in kn.split()}
    best: dict[str, Mangling] = {}
    rival: dict[str, float] = {}  # best score from a *different* canonical, for ambiguity

    for cand in vocab.candidates:
        for k in (cand.n_tokens - 1, cand.n_tokens, cand.n_tokens + 1):
            for mnorm, msurface, mcount in buckets.get(k, ()):
                if mnorm == cand.norm or mnorm in vocab.known_norm:
                    continue  # the transcript token is itself a correct spelling
                if mnorm in existing_norm or _is_stopword_phrase(mnorm):
                    continue
                if " " not in mnorm and mnorm in known_tokens:
                    continue  # single token that is part of a known name -> not a mangling
                if _is_trivial_variant(mnorm, cand.norm):
                    continue  # plural/hyphen/spacing variant -> not a spelling mangling
                score = _score(mnorm, cand.norm)
                if score < REVIEW_FLOOR:
                    continue
                conf = _confidence(mnorm, cand.norm, score)
                if conf is None:
                    continue
                prev = best.get(mnorm)
                if prev is None or score > prev.score:
                    if prev is not None and prev.canonical != cand.canonical:
                        rival[mnorm] = max(rival.get(mnorm, 0.0), prev.score)
                    best[mnorm] = Mangling(
                        mnorm,
                        msurface,
                        cand.canonical,
                        score,
                        mcount,
                        conf,
                        cand.category,
                        cand.source,
                    )
                elif prev.canonical != cand.canonical:
                    rival[mnorm] = max(rival.get(mnorm, 0.0), score)

    resolved: list[Mangling] = []
    for mnorm, m in best.items():
        if m.confidence == "high" and rival.get(mnorm, 0.0) >= m.score - AMBIG_EPS:
            m = replace(m, confidence="review")  # a rival canonical is too close to trust
        resolved.append(m)

    # Suppress sub-span manglings: "moscow sports" and "moscow" fall away once
    # "moscow sports lighting" is retained (keep the longest, most specific form).
    resolved.sort(key=lambda m: (len(m.mangled.split()), m.score), reverse=True)
    out: list[Mangling] = []
    for m in resolved:
        if any(m.mangled != k.mangled and m.mangled in k.mangled for k in out):
            continue
        out.append(m)
    return out


def context_snippet(text: str, surface: str, *, width: int = 70) -> str:
    """A one-line transcript excerpt around the first occurrence of ``surface``."""
    idx = text.find(surface)
    if idx < 0:
        return ""
    start = max(0, idx - width)
    end = min(len(text), idx + len(surface) + width)
    snippet = " ".join(text[start:end].split())
    prefix = "…" if start > 0 else ""
    suffix = "…" if end < len(text) else ""
    return f"{prefix}{snippet}{suffix}"
