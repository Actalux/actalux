"""Deterministic parser for Clayton city-body votes in CivicPlus minutes.

Clayton's city-body minutes (``source_portal='civicplus'``) record votes in *prose*,
unlike the line-anchored Diligent school-board minutes parsed by ``votes_parser.py``.
Two bodies publish here in two distinct prose styles, each with its own parse entry
point but sharing the footer/sentence/citation plumbing below:

* **City Council / Board of Aldermen** (``parse_votes``) — voting members carry a
  title (Alderman / Alderwoman / Councilmember / Mayor / Mayor Pro Tem[pore] /
  Acting Mayor). Three motion lead-ins and two result forms, detailed below.
* **Plan Commission–Architectural Review Board** (``parse_votes_pc``) — members are
  named without a title across two eras: name-before (~2021-2026) "<Name> made a
  motion to ... <Name> seconded the motion. The motion carried unanimously [with
  five votes in favor and one opposed]", and name-after (~2016-2021) "<Name> made a
  motion to ... The motion was seconded by <Name> and unanimously approved by the
  Board" (the outcome is appended to the second sentence). No per-member roll calls;
  counts, when present, are word-numbers ("five votes in favor and one opposed").

``extract_votes.py`` routes a CivicPlus document to whichever parser its lead-ins
match (council vs. PC), so neither needs the database's entity-id assignment.

City Council — three motion lead-ins and two result forms across 2015-2026, all
parsed into the shared :class:`ParsedVote`:

  Motion lead-ins
    "Motion made by Councilmember Buse to approve the Consent Agenda."
    "Alderman Boulton moved to approve the December 23, 2014 minutes."
    "Councilmember Buse introduced Bill No. 7157, ... by title only."
  Second (optional — ~88% of motions have one)
    "Councilmember Patel seconded."
  Result
    R1  "The motion passed unanimously on a voice vote."
        "The motion passed 7-0 on a voice vote."  /  "passed unanimously (7-0) ..."
        "The motion passed on a roll call vote: Councilmember Buse - Aye; ...
         and Mayor McAndrew - Aye."
        "The motion failed 2 - Ayes to 5 - Nays (...) on a voice vote."
    R2  (older second readings, no "The motion" lead-in) a bare roll call —
        "Alderman Garnholz - Aye; ...; and Mayor Pro Tempore Harris - Aye." —
        followed by a disposition: "The bill was adopted and became Ordinance
        No. 6352." (passed) or "defeated" / "did not pass" (failed).

Like ``votes_parser.py`` it is rule-based, not LLM-driven: the result word
("passed" / "failed" / "adopted") is read verbatim and a tally is *counted* from
the literal roll call or read from a literal "7-0"; no count is ever inferred. A
roll call with any unparseable member token (OCR garble, e.g. "Yorg - A Nay ye")
drops the count to ``None`` while keeping the stated result — a partial count is
never stored. A motion with no recognizable result is skipped rather than guessed.

The citing chunk is found by the full motion text (unique within a document); both
the anchor and the chunk are footer-normalized before matching, since the PDF
interleaves a running footer ("MM-DD-YYYY BOA Minutes / Month D, YYYY / Page N of
M") that the chunker preserves verbatim.
"""

from __future__ import annotations

import logging
import re

from actalux.ingest.votes_parser import ParsedVote

logger = logging.getLogger(__name__)

# (yes, no, abstain) tallies — each None when no count was recorded.
_Counts = tuple[int | None, int | None, int | None]
# A resolved result: (status, basis, counts, members | None, source clause).
_Result = tuple[str, str, _Counts, list[dict[str, str]] | None, str]

# A voting member's title. Longest/most-specific alternatives first — Python's re
# is leftmost-first-alternative, so "Mayor Pro Tem[pore]" must precede "Mayor". The
# "Tem(?:pore|p)?" also catches the abbreviated "Mayor Pro Tem"/"Pro Temp" forms.
_TITLE = (
    r"(?:Mayor\s+Pro[\s.-]?Tem(?:pore|p)?|Acting\s+Mayor|Councilmember|Council\s+Member|"
    r"Alderman|Alderwoman|Mayor)"
)
# A surname token (single word; hyphenated names like "Meyland-Smith" are one token).
_NAME = r"[A-Z][A-Za-z.'\-]*"

# --- Motion lead-ins --------------------------------------------------------
# "Motion made by <title> <name> to|that ..."
_MOTION_MADE_RE = re.compile(
    rf"(?:A\s+motion\s+was\s+made|Motion\s+(?:was\s+)?made)\s+by\s+"
    rf"(?P<title>{_TITLE})\s+(?P<name>{_NAME})\s+(?:to|that)\b"
)
# "<title> [<name>] moved to|that ..." (the 2015-era form; the name is optional for
# "Mayor Pro Tempore moved that ..."). The title requirement keeps a narrative
# "the discussion moved to ..." from matching.
_MOVED_RE = re.compile(rf"(?P<title>{_TITLE})(?:\s+(?P<name>{_NAME}))?\s+moved\s+(?:to|that)\b")
# "<title> <name> introduced Bill No. <n> ..." (a first/second reading)
_INTRODUCED_RE = re.compile(
    rf"(?P<title>{_TITLE})\s+(?P<name>{_NAME})\s+introduced\s+Bill\s+No\.?\s*(?P<bill>\d+)"
)
_LEAD_INS = (_MOTION_MADE_RE, _MOVED_RE, _INTRODUCED_RE)

# "<title> <name> seconded"
_SECOND_RE = re.compile(rf"(?P<title>{_TITLE})\s+(?P<name>{_NAME})\s+seconded\b")

# --- Result (R1): "[The] motion [<text>] passed|failed|carried|did not pass" -----
# "The" is optional ("Motion to approve the minutes passed ..." also occurs). The
# bounded ``[^.]{0,80}?`` admits the insert ("... to approve the minutes ...") while
# staying inside the sentence (no result sentence in the corpus carries an
# abbreviation period before the status word, verified). The detection runs only on
# the post-second window, so a motion's own "Motion made by ..." text never reaches
# it (and carries no status word anyway).
_MOTION_RESULT_RE = re.compile(
    r"(?:[Tt]he\s+)?[Mm]otion\b[^.]{0,80}?\b"
    r"(?P<status>passed|failed|carried|defeated|did\s+not\s+pass)\b"
)

# --- Result (R2): a bare roll call (no "The motion" lead-in) -----------------
# A vote word a roll-call segment can end on.
_VOTE_WORD = {
    "aye": "aye",
    "ayes": "aye",
    "yes": "aye",
    "nay": "no",
    "nays": "no",
    "no": "no",
    "abstain": "abstain",
    "abstained": "abstain",
    "abstention": "abstain",
    "present": "present",
    "absent": "absent",
}
# Detects where a roll call begins ("Alderman Garnholz - Aye"). Used to find a bare
# (R2) roll call; in an R1 roll-call vote the "The motion passed on a roll call
# vote:" lead-in sits before the first member, so R1 is detected first.
_ROLLCALL_START_RE = re.compile(
    rf"{_TITLE}\s+{_NAME}\s*[–—-]\s*"
    r"(?:[Aa]yes?|[Nn]ays?|[Aa]bstain(?:ed)?|[Pp]resent|[Aa]bsent)\b"
)
# One roll-call segment: "[and] <title> <name> - <vote>". The whole segment must
# match (anchored) — trailing words (OCR garble) reject it, so the count drops.
_ROLLCALL_MEMBER_RE = re.compile(
    rf"^(?:[Aa]nd\s+)?(?P<title>{_TITLE})\s+(?P<name>{_NAME})\s*[–—-]\s*"
    r"(?P<vote>[A-Za-z]+)$"
)
# Disposition that follows a bare roll call and states the outcome verbatim.
_ADOPTED_RE = re.compile(r"\b(?:was\s+)?adopted\b|became\s+Ordinance\b|\bwas\s+approved\b", re.I)
_REJECTED_RE = re.compile(
    r"\b(?:was\s+)?(?:defeated|rejected)\b|\bnot\s+adopted\b|\bdid\s+not\s+pass\b|\bfailed\b", re.I
)

_RESULT_NORM = {
    "passed": "passed",
    "carried": "passed",
    "failed": "failed",
    "defeated": "failed",
    "did not pass": "failed",
}

# --- Plan Commission (PC-ARB) prose ----------------------------------------
# A PC member is named without a title, so the lead-in/second/outcome anchor on the
# verb. A name is 1-3 capitalized tokens immediately before the verb.
_PC_NAME = r"[A-Z][A-Za-z.'\-]+(?:\s+[A-Z][A-Za-z.'\-]+){0,2}"
# "Helen DiFate made a motion to ..." (also "moved to/that", "move to/that").
_PC_LEAD_RE = re.compile(
    rf"(?P<name>{_PC_NAME})\s+(?:made\s+a\s+motion|moved|move)\s+(?:to|that)\b"
)
# Second, two eras: name-before ("Jim Arsenault seconded the motion") and name-after
# ("[The motion was] seconded by Jim Arsenault").
_PC_SECOND_BEFORE_RE = re.compile(rf"(?P<name>{_PC_NAME})\s+seconded(?:\s+the\s+motion)?\b")
_PC_SECOND_AFTER_RE = re.compile(
    rf"(?:[Tt]he\s+motion\s+was\s+)?seconded\s+by\s+(?P<name>{_PC_NAME})"
)
_PC_STATUS = r"(?P<status>carried|passed|failed|approved|adopted|denied|tabled|withdrawn)"
# Standalone outcome (name-before era): "[The] motion [was] carried/approved/...".
# The "motion to <verb>" of the motion text never matches (a "to" sits between).
_PC_OUTCOME_SENTENCE_RE = re.compile(rf"(?:[Tt]he\s+)?[Mm]otion\s+(?:was\s+)?{_PC_STATUS}\b")
# Appended outcome (name-after era): "... and [unanimously] approved/carried ...".
# Only ever applied to the span right after a name-after second, so the loose "and"
# anchor cannot pick up an unrelated clause.
_PC_OUTCOME_APPENDED_RE = re.compile(rf"\band\s+(?:unanimously\s+)?{_PC_STATUS}\b")
_PC_STATUS_NORM = {
    "carried": "passed",
    "passed": "passed",
    "approved": "passed",
    "adopted": "passed",
    "failed": "failed",
    "denied": "failed",  # "the motion was denied" — the motion did not carry
    "tabled": "tabled",
    "withdrawn": "withdrawn",
}
# "... with four votes in favor and two votes opposed" (word-numbers; also digits).
_PC_COUNT_RE = re.compile(
    r"\b(\w+)\s+votes?\s+in\s+favor\s+(?:and|to)\s+(\w+)\s+(?:votes?\s+)?"
    r"(?:opposed|against|in\s+opposition)",
    re.I,
)
_NUM_WORDS = {
    "one": 1,
    "two": 2,
    "three": 3,
    "four": 4,
    "five": 5,
    "six": 6,
    "seven": 7,
    "eight": 8,
    "nine": 9,
    "ten": 10,
    "eleven": 11,
    "twelve": 12,
}


def _num(token: str) -> int | None:
    """A count word ("four") or digit ("4") -> int, else None (never guessed)."""
    token = token.strip().lower()
    if token.isdigit():
        return int(token)
    return _NUM_WORDS.get(token)


# --- Footer stripping (shared by parsing and chunk matching) -----------------
_MONTHS = r"January|February|March|April|May|June|July|August|September|October|November|December"
# The full running footer as one unit ("05-26-2026 BOA Minutes May 26, 2026 Page 1
# of 5"); removing it as a unit also drops the in-footer date so it can't be
# mistaken for a motion date. The "[A-Z][A-Za-z ]*?Minutes" covers BOA / BOA SDS /
# BOA Special Meeting / CC SDS variants.
_FOOTER_UNIT_RE = re.compile(
    rf"\d{{2}}-\d{{2}}-\d{{4}}\s+[A-Z][A-Za-z ]*?Minutes\s+(?:{_MONTHS})\s+\d{{1,2}},\s+\d{{4}}"
    r"\s+Page\s+\d+\s+of\s+\d+"
)
_RUNNING_HEADER_RE = re.compile(r"\d{2}-\d{2}-\d{4}\s+[A-Z][A-Za-z ]*?Minutes")
_PAGE_RE = re.compile(r"Page\s+\d+\s+of\s+\d+")

# How far past a motion's second to look for its result before giving up (a generous
# backstop; the next motion is the real bound). Covers an intervening "City Attorney
# reads Bill ..." narrative between a second reading and its roll call.
_RESULT_WINDOW = 2000

# Sentence-end detection abbreviations (so "Bill No. 7156" / "Res. No. 15-01" don't
# end a motion early when there is no second to bound it).
_ABBREV = {
    "no",
    "nos",
    "res",
    "ord",
    "st",
    "ave",
    "rd",
    "dr",
    "mr",
    "mrs",
    "ms",
    "jr",
    "sr",
    "co",
    "inc",
    "vs",
    "etc",
    "fig",
    "sec",
    "art",
}


def _clean(text: str) -> str:
    """Whitespace-collapse ``text`` and strip the interleaved running footer.

    Applied identically to the document (for parsing) and to each chunk (for
    matching), so a motion that the PDF split across a page break still matches the
    chunk that verbatim-contains the footer.
    """
    flat = " ".join((text or "").split())
    flat = _FOOTER_UNIT_RE.sub(" ", flat)
    flat = _RUNNING_HEADER_RE.sub(" ", flat)
    flat = _PAGE_RE.sub(" ", flat)
    return " ".join(flat.split())


def _sentence_end(text: str, start: int) -> int:
    """Index just past the period that ends the sentence starting at ``start``.

    Skips known abbreviations and single-letter initials so "Bill No. 7156" does
    not terminate a motion mid-clause. Returns ``len(text)`` if none is found.
    """
    i = start
    n = len(text)
    while i < n:
        j = text.find(".", i)
        if j == -1:
            return n
        k = j - 1
        while k >= 0 and (text[k].isalnum() or text[k] in ".'-"):
            k -= 1
        word = text[k + 1 : j].lower().strip(".'-")
        if word in _ABBREV or len(word) <= 1:
            i = j + 1
            continue
        return j + 1
    return n


def _tally(members: list[dict[str, str]]) -> tuple[int, int, int]:
    """Count aye / no / abstain across a roll call (present/absent excluded)."""
    yes = sum(1 for m in members if m["vote"] == "aye")
    no = sum(1 for m in members if m["vote"] == "no")
    abstain = sum(1 for m in members if m["vote"] == "abstain")
    return yes, no, abstain


def _parse_rollcall(text: str) -> list[dict[str, str]] | None:
    """Parse a roll call ("<title> <name> - Aye; ...; and <title> <name> - Aye").

    All-or-nothing: every ``;``-split segment must match cleanly and end on a known
    vote word, else ``None`` (no count) — never a partial tally. Requires >= 2
    members so a stray "X - Aye" fragment is not read as a roll call.
    """
    body = text.strip().rstrip(".").strip()
    segments = [s.strip() for s in body.split(";") if s.strip()]
    if len(segments) < 2:
        return None
    members: list[dict[str, str]] = []
    for seg in segments:
        m = _ROLLCALL_MEMBER_RE.match(seg)
        if not m:
            return None
        word = m.group("vote").lower()
        if word not in _VOTE_WORD:
            return None
        members.append(
            {"name": f"{m.group('title')} {m.group('name')}".strip(), "vote": _VOTE_WORD[word]}
        )
    return members


def _parse_inline_count(clause: str) -> _Counts:
    """Read an explicit ayes/nays count from a non-roll-call result clause.

    Tries, in order: "N - Ayes to N - Nays", a digit-only parenthetical "(7-0[-1])",
    a bare "passed 7-0", and "7-0 on a voice vote". Returns all-``None`` when no
    explicit count is present (e.g. "passed unanimously on a voice vote"); a count
    is never inferred from "unanimously".
    """
    m = re.search(r"(\d+)\s*[–—-]?\s*Ayes?\b.*?\bto\b\s*(\d+)\s*[–—-]?\s*Nays?", clause, re.I)
    if m:
        return int(m.group(1)), int(m.group(2)), None
    m = re.search(r"\((\d+)\s*[–—-]\s*(\d+)(?:\s*[–—-]\s*(\d+))?\)", clause)
    if m:
        return int(m.group(1)), int(m.group(2)), int(m.group(3)) if m.group(3) else None
    m = re.search(r"\b(?:passed|failed|carried)\s+(\d+)\s*[–—-]\s*(\d+)\b", clause, re.I)
    if m:
        return int(m.group(1)), int(m.group(2)), None
    m = re.search(r"\b(\d+)\s*[–—-]\s*(\d+)\s+on\s+a\s+voice\s+vote", clause, re.I)
    if m:
        return int(m.group(1)), int(m.group(2)), None
    return None, None, None


def _r1_counts(clause: str) -> tuple[_Counts, list[dict[str, str]] | None]:
    """Counts + members for an R1 result clause. Roll call (if any) is ground truth.

    A roll call that fails to parse cleanly (OCR garble) yields no count but is not
    an error — the stated result still stands. An inline count that contradicts
    "unanimously" (a non-zero no/abstain) is dropped as a misparse.
    """
    if "roll call" in clause.lower():
        after = re.split(r"roll\s+call\s+vote\s*:?", clause, maxsplit=1, flags=re.I)
        if len(after) == 2:
            members = _parse_rollcall(after[1])
            if members:
                return _tally(members), members
        return (None, None, None), None
    yes, no, abstain = _parse_inline_count(clause)
    if "unanim" in clause.lower() and ((no or 0) > 0 or (abstain or 0) > 0):
        logger.warning(
            "council vote: inline count contradicts 'unanimously'; dropping: %.80s", clause
        )
        return (None, None, None), None
    return (yes, no, abstain), None


def _lead_in_starts(flat: str) -> list[int]:
    """Sorted, de-duplicated start offsets of every motion lead-in in ``flat``."""
    starts: set[int] = set()
    for rx in _LEAD_INS:
        for m in rx.finditer(flat):
            starts.add(m.start())
    return sorted(starts)


def _mover(flat: str, start: int) -> str:
    """The "<title> <name>" that moved, from whichever lead-in matches at ``start``."""
    for rx in _LEAD_INS:
        m = rx.match(flat, start)
        if m:
            name = m.groupdict().get("name") or ""
            return f"{m.group('title')} {name}".strip()
    return ""


def _find_result(flat: str, search_from: int, next_motion: int) -> _Result | None:
    """Find this motion's result between ``search_from`` and the next motion.

    Returns ``(result, basis, (yes, no, abstain), members | None, clause)`` or
    ``None`` when no result is recognizable (the motion is then skipped). Chooses
    whichever of an R1 "The motion ..." sentence or an R2 bare roll call appears
    first; in an R1 roll-call vote the "The motion passed on a roll call vote:"
    lead-in precedes the members, so R1 wins.
    """
    end = min(next_motion, search_from + _RESULT_WINDOW)
    window = flat[search_from:end]

    r1 = _MOTION_RESULT_RE.search(window)
    r2 = _ROLLCALL_START_RE.search(window)
    r1_at = r1.start() if r1 else len(window) + 1
    r2_at = r2.start() if r2 else len(window) + 1

    if r1 and r1_at <= r2_at:
        clause = window[r1.start() : _sentence_end(window, r1.start())].strip()
        status = _RESULT_NORM[re.sub(r"\s+", " ", r1.group("status").lower())]
        counts, members = _r1_counts(clause)
        return status, "stated", counts, members, clause

    if r2:
        rc_start = r2.start()
        rc_end = _sentence_end(window, rc_start)
        rollcall = window[rc_start:rc_end].strip()
        members = _parse_rollcall(rollcall)
        tail = window[rc_end : rc_end + 200]
        if _ADOPTED_RE.search(tail):
            status, basis = "passed", "stated"
        elif _REJECTED_RE.search(tail):
            status, basis = "failed", "stated"
        elif members:
            yes, no, _ = _tally(members)
            if yes > no:
                status, basis = "passed", "derived"
            elif no > yes:
                status, basis = "failed", "derived"
            else:
                return None  # tie with no disposition word — not determinable
        else:
            return None  # unparseable roll call and no disposition — skip
        counts = _tally(members) if members else (None, None, None)
        clause = window[rc_start : rc_end + 120].strip()
        return status, basis, counts, members, clause

    return None


def _parse_one(flat: str, start: int, next_motion: int) -> ParsedVote | None:
    """Parse the motion whose lead-in begins at ``start`` into a :class:`ParsedVote`."""
    moved_by = _mover(flat, start)

    # The motion text runs from the lead-in to its second (the clean boundary); with
    # no second, to the end of the motion sentence.
    sec = _SECOND_RE.search(flat, start, min(next_motion, start + _RESULT_WINDOW))
    if sec:
        motion = flat[start : sec.start()].strip().rstrip(".").strip()
        seconded_by = f"{sec.group('title')} {sec.group('name')}".strip()
        search_from = sec.end()
    else:
        motion = flat[start : _sentence_end(flat, start)].strip().rstrip(".").strip()
        seconded_by = ""
        search_from = start + len(motion)

    if len(motion.split()) < 3:
        return None  # not a real motion (a stray lead-in fragment)

    found = _find_result(flat, search_from, next_motion)
    if found is None:
        return None
    result, basis, (yes, no, abstain), members, clause = found

    source_quote = _clean(f"{motion}. {clause}")
    return ParsedVote(
        motion=motion,
        result=result,
        result_basis=basis,
        vote_count_yes=yes,
        vote_count_no=no,
        vote_count_abstain=abstain,
        moved_by=moved_by,
        seconded_by=seconded_by,
        members=tuple(members or ()),
        source_quote=source_quote,
        anchors=(motion,) if motion else (),
    )


def parse_votes(content: str) -> list[ParsedVote]:
    """Parse all recognizable council/aldermen vote blocks from a minutes document.

    Returns one :class:`ParsedVote` per motion that has both a verbatim motion and
    a recognizable result. Free-prose documents with no motion lead-in yield [].
    """
    flat = _clean(content)
    starts = _lead_in_starts(flat)
    out: list[ParsedVote] = []
    for i, start in enumerate(starts):
        next_motion = starts[i + 1] if i + 1 < len(starts) else len(flat)
        vote = _parse_one(flat, start, next_motion)
        if vote is not None:
            out.append(vote)
    return out


def find_citing_chunk(anchors: tuple[str, ...], chunks: list[dict]) -> dict | None:
    """First chunk whose footer-normalized content contains one of ``anchors``.

    Both sides are run through :func:`_clean` so the running footer the PDF
    interleaves (and the chunker preserves) does not block a match. Returns None
    when none match (the loader then skips the vote — cite or abstain).
    """
    normalized = [(c, _clean(c.get("content", ""))) for c in chunks]
    for anchor in anchors:
        na = _clean(anchor)
        if not na:
            continue
        for chunk, content in normalized:
            if na in content:
                return chunk
    return None


def count_lead_ins(content: str) -> int:
    """How many City Council motion lead-ins the document carries (audit denominator)."""
    return len(_lead_in_starts(_clean(content)))


# --- Plan Commission (PC-ARB) parsing --------------------------------------


def _pc_count(clause: str) -> _Counts:
    """Read a word-number count from a PC outcome clause, else all-``None``.

    "... carried with four votes in favor and two votes opposed" -> (4, 2, None).
    "... carried unanimously" / "... approved with X opposing" carry no parseable
    number, so the tally stays ``None`` (the quorum present is not the full board —
    a count is never inferred from "unanimously" or a named dissent).
    """
    m = _PC_COUNT_RE.search(clause)
    if m:
        yes, no = _num(m.group(1)), _num(m.group(2))
        if yes is not None and no is not None:
            return yes, no, None
    return None, None, None


# Opening-prefix lengths (in words) tried as PC citation fallbacks, longest first.
# PC motions are often conditional/recommendation motions ("... with the following
# conditions: 1. ... 2. ...") whose numbered conditions the chunker splits onto the
# next chunk, so the full motion is in no single chunk. The opening sits in one
# chunk, but the split point varies per motion, so several lengths are tried; each
# is used only via the uniqueness-gated fallback in ``find_citing_chunk_pc`` (cite
# only when the prefix identifies exactly one chunk), so a generic opening shared by
# two motions never mis-attributes a vote.
_PC_PREFIX_WORDS = (24, 16, 10)


def _pc_anchors(motion: str) -> tuple[str, ...]:
    """Citation anchors for a PC motion: the full text, then opening/closing windows.

    When the chunker splits a conditional motion, its opening sits in one chunk and
    its (unique) conditions in the next; trying both ends — each uniqueness-gated —
    recovers whichever landed wholly in a single chunk.
    """
    words = motion.split()
    anchors = [motion]
    for n in _PC_PREFIX_WORDS:
        if len(words) > n:
            anchors.append(" ".join(words[:n]))  # opening (in the first chunk)
    for n in _PC_PREFIX_WORDS:
        if len(words) > n:
            anchors.append(" ".join(words[-n:]))  # closing conditions (in the next chunk)
    return tuple(dict.fromkeys(anchors))  # de-dupe, preserve order


def find_citing_chunk_pc(anchors: tuple[str, ...], chunks: list[dict]) -> dict | None:
    """Find the citing chunk for a PC motion: full text, then a *unique* opening.

    The full motion (``anchors[0]``) is unique within a document, so the first chunk
    that contains it is correct. When it spans a chunk boundary (a conditional motion
    whose numbered conditions were split off), fall back to the opening prefix — but
    accept it only when it identifies exactly one chunk, so an opening shared by two
    motions in the same document yields no (mis-)citation rather than a wrong one.
    """
    normalized = [(c, _clean(c.get("content", ""))) for c in chunks]
    full = _clean(anchors[0]) if anchors else ""
    if full:
        for chunk, content in normalized:
            if full in content:
                return chunk
    for anchor in anchors[1:]:
        na = _clean(anchor)
        if not na:
            continue
        matches = [chunk for chunk, content in normalized if na in content]
        if len(matches) == 1:
            return matches[0]
    return None


def _parse_one_pc(flat: str, start: int, next_motion: int) -> ParsedVote | None:
    """Parse the PC motion whose "<name> made a motion" begins at ``start``."""
    lead = _PC_LEAD_RE.match(flat, start)
    moved_by = lead.group("name").strip() if lead else ""
    window = flat[start : min(next_motion, start + _RESULT_WINDOW)]

    # Second: the earliest of the name-before / name-after forms.
    before = _PC_SECOND_BEFORE_RE.search(window)
    after = _PC_SECOND_AFTER_RE.search(window)
    sec = min((m for m in (before, after) if m), key=lambda m: m.start(), default=None)
    seconded_by = sec.group("name").strip() if sec else ""

    # Outcome: a standalone "[The] motion [was] <status>" sentence (name-before era),
    # else an outcome appended to the second ("... and unanimously approved", name-after).
    status: str | None = None
    clause = ""
    sentence = _PC_OUTCOME_SENTENCE_RE.search(window, sec.end() if sec else 0)
    if sentence:
        status = _PC_STATUS_NORM[sentence.group("status").lower()]
        clause = window[sentence.start() : _sentence_end(window, sentence.start())].strip()
    elif sec is not None:
        appended = _PC_OUTCOME_APPENDED_RE.search(window, sec.end(), sec.end() + 140)
        if appended:
            status = _PC_STATUS_NORM[appended.group("status").lower()]
            clause = window[sec.start() : _sentence_end(window, appended.end())].strip()
    if status is None:
        return None  # no recognizable outcome — skip rather than guess

    cut = sec.start() if sec else (sentence.start() if sentence else len(window))
    motion = window[:cut].strip().rstrip(".").strip()
    if len(motion.split()) < 4:
        return None  # a stray "<name> made a motion" fragment, not a real motion

    yes, no, abstain = _pc_count(clause)
    source_quote = _clean(f"{motion}. {clause}")
    return ParsedVote(
        motion=motion,
        result=status,
        result_basis="stated",
        vote_count_yes=yes,
        vote_count_no=no,
        vote_count_abstain=abstain,
        moved_by=moved_by,
        seconded_by=seconded_by,
        members=(),
        source_quote=source_quote,
        anchors=_pc_anchors(motion),
    )


def _pc_lead_in_starts(flat: str) -> list[int]:
    """Sorted, de-duplicated start offsets of every PC motion lead-in in ``flat``."""
    return sorted({m.start() for m in _PC_LEAD_RE.finditer(flat)})


def parse_votes_pc(content: str) -> list[ParsedVote]:
    """Parse all recognizable Plan Commission (PC-ARB) vote blocks from a document.

    Same shape and guarantees as :func:`parse_votes` (verbatim outcome word, count
    only when explicitly stated, cite-or-abstain), for the PC prose style across
    both eras. Documents with no "<name> made a motion" lead-in yield [].
    """
    flat = _clean(content)
    starts = _pc_lead_in_starts(flat)
    out: list[ParsedVote] = []
    for i, start in enumerate(starts):
        next_motion = starts[i + 1] if i + 1 < len(starts) else len(flat)
        vote = _parse_one_pc(flat, start, next_motion)
        if vote is not None:
            out.append(vote)
    return out


def count_lead_ins_pc(content: str) -> int:
    """How many PC motion lead-ins the document carries (audit denominator)."""
    return len(_pc_lead_in_starts(_clean(content)))
