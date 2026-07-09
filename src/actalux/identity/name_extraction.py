"""Conservative name extraction from self- and presenter-introductions in a turn.

Shared, jurisdiction-agnostic extraction: the surface patterns that turn a spoken
"my name is X" / "I'm X" self-introduction, or an "introduce/recognize/welcome X"
presenter introduction, into a candidate name plus the verbatim sentence that stated
it. This is the single source of truth for that extraction, imported by BOTH the
headroom-measurement script (``scripts/analyze_self_intro_coverage.py``) and the tier-2
participant-naming module (``actalux.identity.participant_names``), so the two never
drift.

The extraction is deliberately precision-biased (honorifics skipped, 1-3 capitalized
tokens, a broad non-name stop-list, presenter names require >= 2 tokens): favouring a
missed introduction over a wrong name. It knows nothing about the roster or the DB —
callers layer roster comparison (:func:`roster_keys`) and persistence on top.
"""

from __future__ import annotations

import string
from collections import defaultdict
from dataclasses import dataclass
from typing import Any

from actalux.graph.resolve import normalize_name
from actalux.identity.resolve import RosterMember

# --- extraction tunables ----------------------------------------------------------
# A self-intro cue qualifies if it opens the turn (within the first N tokens, so a
# "Hi, my name is ..." greeting prefix still counts) OR begins a fresh sentence
# (pyannote under-segmentation glues a later intro into an ongoing turn — requiring a
# sentence boundary keeps that precise rather than matching a mid-sentence "this is").
SELF_INTRO_HEAD_TOKENS = 6
# A name candidate is at most this many capitalized tokens ("Jane Elizabeth Harris").
MAX_NAME_TOKENS = 3
# Determiner/honorific filler tokens skipped between a cue and the name it introduces
# ("recognize Commissioner Jane Harris", "introduce our Jane Harris"); bounded so the
# name must still follow the cue closely (precision over recall).
MAX_FILLER_SKIP = 3
# Words captured verbatim after the name as the person's self-stated role/affiliation.
SNIPPET_WORDS = 15

_SENTENCE_END = frozenset(".?!:")
_EDGE_PUNCT = string.punctuation + "“”‘’—–…"

# Self-intro cue token sequences (matched on the apostrophe/punctuation-stripped lower
# form: "I'm" -> "im", "name's" -> "names"). "this is" is included per the measurement
# spec even though the resolver excludes it as too ambiguous — the name gate carries the
# precision here.
SELF_INTRO_CUES: tuple[tuple[str, ...], ...] = (
    ("my", "name", "is"),
    ("my", "names"),
    ("this", "is"),
    ("i", "am"),
    ("im",),
)
# Presenter/recognition cue verbs (all inflections). Generic English handoff verbs whose
# object is a person; the name gate rejects the non-handoff uses ("present the budget").
PRESENTER_CUES = frozenset(
    """introduce introduces introducing introduced recognize recognizes recognizing
    recognized welcome welcomes welcoming welcomed present presents presenting presented
    invite invites inviting invited""".split()
)

# Filler tokens (determiners + honorifics) that may sit between a cue and the name.
NAME_FILLERS = frozenset(
    """our the a an my your his her their mr mrs ms mx dr doctor prof professor rev
    reverend hon honorable mayor alderman alderwoman alderperson councilmember councilman
    councilwoman commissioner chair chairman chairwoman chairperson president vice
    director superintendent principal secretary treasurer clerk attorney counsel""".split()
)

# Capitalized tokens that are NOT personal names. Kept deliberately broad on the precision
# side (a human triages the NOT-IN-ROSTER output): pronouns/discourse markers, titles/roles,
# procedural + document nouns, address/direction words, days, and months. Place-specific
# tokens (the town and state names) are added at runtime from the resolved place.
STOP_WORDS = frozenset(
    """i im a an the this that these those we you he she it they them my your our his her
    their and but or so well ok okay now here there yeah yes no not going gonna wanna sorry
    thank thanks hi hello hey good great just actually really also again please welcome glad
    pleased happy honored excited sure right all one first second next last today tonight
    tomorrow morning afternoon evening everyone everybody folks oh any as own both
    someone somebody anybody nobody nothing anything everything youre thats whats heres
    theres lets dont cant wont ive hes shes weve youve maybe okay
    mr mrs ms mx dr doctor prof professor rev reverend hon honorable mayor alderman
    mr mrs ms mx dr doctor prof professor rev reverend hon honorable mayor alderman
    alderwoman alderperson councilmember councilman councilwoman council commissioner
    commission chair chairman chairwoman chairperson president vice director superintendent
    principal secretary treasurer clerk attorney counsel member members board city district
    ward staff department office division bureau agency committee subcommittee
    meeting agenda item items motion motions resolution ordinance proposition minutes budget
    report reports plan plans project projects application applications case cases petition
    presentation number section subsection exhibit page order business hearing session public
    comment comments roll call vote votes aye ayes nay nays abstain present approval consent
    street streets avenue avenues road roads boulevard drive lane court way plaza park
    building buildings floor room north south east west northeast northwest southeast
    southwest
    monday tuesday wednesday thursday friday saturday sunday
    january february march april may june july august september october november december""".split()
)

# --- token helpers ----------------------------------------------------------------


def _edge_strip(token: str) -> str:
    """A token with surrounding punctuation/quotes removed (internal ' and - kept)."""
    return token.strip(_EDGE_PUNCT)


def _cue_norm(token: str) -> str:
    """Lowercase, alphabetic-only form of a token for cue matching ("I'm." -> "im")."""
    return "".join(c for c in token.lower() if c.isalpha())


def _ends_sentence(token: str) -> bool:
    """True if ``token`` ends a sentence (last non-quote char is . ? ! :)."""
    stripped = token.rstrip("\"')]”’")
    return bool(stripped) and stripped[-1] in _SENTENCE_END


def _is_number_token(token: str) -> bool:
    """True if ``token`` is a bare number ("6852", "6674") — a bill/ordinance/item id.

    A name immediately followed by a number is almost never a personal name in this corpus
    ("introduce Bill 6852", "present Ordinance 6674"), so the candidate is rejected.
    """
    bare = _edge_strip(token)
    return bool(bare) and bare.isdigit()


def _is_name_token(token: str) -> bool:
    """True if ``token`` looks like one word of a personal name.

    Title-case (leading uppercase letter, not ALL-CAPS), letters plus internal apostrophe
    or hyphen only (O'Brien, Lyss-Lerman), at least two letters, and not a known non-name
    word. Conservative on purpose — favouring precision over recall.
    """
    bare = _edge_strip(token)
    if len(bare) < 2 or not bare[0].isalpha() or not bare[0].isupper():
        return False
    if bare.isupper():  # ALL-CAPS -> acronym, not a name (OK, TV, LRFMP)
        return False
    core = bare.replace("'", "").replace("’", "").replace("-", "")
    if not core.isalpha() or len(core) < 2:
        return False
    return core.lower() not in STOP_WORDS


def _extract_name(
    tokens: list[str], start: int, stops: frozenset[str], *, min_tokens: int = 1
) -> tuple[str, int] | None:
    """The name candidate beginning at/after ``start`` -> (display, index-after-name).

    Skips a bounded run of determiner/honorific fillers, then collects up to
    ``MAX_NAME_TOKENS`` consecutive name-like tokens. Returns ``None`` when fewer than
    ``min_tokens`` name-like tokens follow, or when the name is immediately followed by a
    bare number (a bill/ordinance/item id, not a person). ``stops`` augments the static
    stop-list with place-specific tokens.
    """
    i, n, skipped = start, len(tokens), 0
    while i < n and skipped < MAX_FILLER_SKIP and _cue_norm(tokens[i]) in NAME_FILLERS:
        i += 1
        skipped += 1
    name_parts: list[str] = []
    while i < n and len(name_parts) < MAX_NAME_TOKENS and _is_name_token(tokens[i]):
        if _cue_norm(tokens[i]) in stops:  # place token (town/state) mid-name -> stop
            break
        name_parts.append(_edge_strip(tokens[i]))
        i += 1
    if len(name_parts) < min_tokens:
        return None
    if i < n and _is_number_token(tokens[i]):  # "Bill 6852" / "Ordinance 6674" -> not a name
        return None
    return " ".join(name_parts), i


# --- per-turn hit extraction ------------------------------------------------------


@dataclass(frozen=True)
class Hit:
    """One extracted introduction in a turn."""

    source: str  # "self_intro" | "presenter_intro"
    cue: str
    name: str
    end_index: int  # token index immediately after the name (role snippet starts here)
    start_index: int  # token index where the cue begins (evidence sentence starts at/before)


def _self_intro_hits(tokens: list[str], norms: list[str], stops: frozenset[str]) -> list[Hit]:
    """Self-introductions opening the turn or a fresh sentence within it."""
    hits: list[Hit] = []
    for start in range(len(tokens)):
        at_head = start <= SELF_INTRO_HEAD_TOKENS
        if not (at_head or (start > 0 and _ends_sentence(tokens[start - 1]))):
            continue
        for cue in SELF_INTRO_CUES:
            if tuple(norms[start : start + len(cue)]) != cue:
                continue
            found = _extract_name(tokens, start + len(cue), stops)
            if found:
                hits.append(Hit("self_intro", " ".join(cue), found[0], found[1], start))
            break  # at most one cue family per starting position
    return hits


def _presenter_hits(tokens: list[str], norms: list[str], stops: frozenset[str]) -> list[Hit]:
    """Presenter/recognition introductions: a cue verb followed by a full (>=2 token) name.

    A full name is required (unlike self-intro, which allows a bare surname) to match the
    resolver's own posture — it anchors a presenter introduction only on a full-name / alias
    span, never a bare token — and because a single capitalized word after a generic cue
    verb ("welcome Everyone", "present Both") is a frequent false positive.
    """
    hits: list[Hit] = []
    for i, norm in enumerate(norms):
        if norm not in PRESENTER_CUES:
            continue
        found = _extract_name(tokens, i + 1, stops, min_tokens=2)
        if found:
            hits.append(Hit("presenter_intro", norm, found[0], found[1], i))
    return hits


def turn_hits(text: str, stops: frozenset[str]) -> list[Hit]:
    """Distinct introduction hits in one turn's text (deduped by source+name)."""
    tokens = text.split()
    norms = [_cue_norm(t) for t in tokens]
    seen: set[tuple[str, str]] = set()
    out: list[Hit] = []
    for hit in _self_intro_hits(tokens, norms, stops) + _presenter_hits(tokens, norms, stops):
        key = (hit.source, normalize_name(hit.name))
        if key not in seen:
            seen.add(key)
            out.append(hit)
    return out


def role_snippet(text: str, end_index: int) -> str:
    """Up to ``SNIPPET_WORDS`` verbatim tokens after the name (the self-stated role)."""
    return " ".join(text.split()[end_index : end_index + SNIPPET_WORDS]).strip()


def evidence_sentence(text: str, start_index: int, end_index: int) -> str:
    """The verbatim sentence containing an introduction hit — the source citation.

    Spans from the start of the sentence the cue opens (walk left to just after the
    previous sentence-ending token) through the first sentence-ending token at or after
    the name (so a trailing role clause, "..., a resident of Maryland Avenue.", is kept).
    Tokens are the raw ``text.split()`` forms, so punctuation is preserved and the result
    is verbatim as recorded. ``start_index``/``end_index`` are the cue-start and
    index-after-name from a :class:`Hit`.
    """
    tokens = text.split()
    if not tokens:
        return ""
    left = max(0, min(start_index, len(tokens)))
    while left > 0 and not _ends_sentence(tokens[left - 1]):
        left -= 1
    right = max(left, min(end_index, len(tokens)) - 1)  # last name token, clamped in-range
    while right < len(tokens) and not _ends_sentence(tokens[right]):
        right += 1
    return " ".join(tokens[left : right + 1]).strip()


# --- roster comparison + place stop-tokens ----------------------------------------


def roster_keys(members: list[RosterMember]) -> tuple[set[str], dict[str, list[int]]]:
    """The body's roster comparison keys and a key -> subject_ids index.

    Keys are ``normalize_name`` outputs: every stored alias (which the seeder already
    normalized, and which includes the canonical name) plus the canonical name normalized
    defensively. The index lets an in-roster hit be attributed to the actual official(s).
    """
    key_to_subjects: dict[str, list[int]] = defaultdict(list)
    for m in members:
        keys = set(m.aliases) | {normalize_name(m.canonical_name)}
        for k in keys:
            if k:
                key_to_subjects[k].append(m.subject_id)
    return set(key_to_subjects), dict(key_to_subjects)


def place_stop_tokens(place: dict[str, Any]) -> frozenset[str]:
    """Place-specific stop tokens (town + state names) so the town name isn't read as a name."""
    parts: list[str] = []
    fields = ("state", "slug", "name", "display_name")
    for value in (place.get(f) for f in fields):
        if isinstance(value, str):
            parts.extend(value.replace("-", " ").split())
    return frozenset(_cue_norm(p) for p in parts if _cue_norm(p))
