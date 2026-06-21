"""Deterministic parser for board votes recorded in official minutes.

Clayton's Diligent-exported minutes (2024-09 onward) record each board action in a
machine-regular block anchored on a "Moved by:" line. Three layouts appear across
that span, all parsed here into one shape:

  A (2025-12+)     Move to approve the agenda as posted.
                   Moved by: Ms. Chris Win; seconded by: Mr. Leo Human
                   Votes: Ben Beinfeld-aye, Leo Human-aye, Chris Win-aye
                   Carried

  B (2024-09 ..    Motion to approve the agenda as posted.   (or a bare imperative,
     2025-06)      Moved by: Ms. Chris Win                    "Approve the agenda...")
                   Seconded by: Mr. Jason Growe
                   Aye
                   Ms. Stacy Siwak, Ms. Kim Hurst, ... and Mr. Ben Beinfeld
                   Motion Carries 7-0

  C (hybrid)       Approve the agenda as posted.
                   Moved by: Ms. Chris Win
                   Seconded by: Ms. Kim Hurst
                   Yes: Mr. Ben Beinfeld, Mr. Leo Human, ...
                   Carried

It is intentionally rule-based, not LLM-driven: every field is read verbatim from
the text, so a tally is *counted* from the literal roll call (or read from the
literal "7-0") and a result is the literal "Carried"/"Fails" word — there is no
path by which a count is invented. Blocks without a recognizable motion or result
are skipped rather than guessed at, and older free-prose minutes (no "Moved by:"
structure) yield nothing.

Tallies are ``None`` when the minutes record a result with no countable roll call
or explicit total (the common case — most blocks read "...seconded by: ... /
Carried"), which is distinct from a recorded 0.

Citation is by anchor: a unique line of the block (the motion line, or the roll
call) is matched as a whitespace-normalized substring against the document's
chunks — the same normalization the chunker guarantees (``validate_chunks``) — so
each vote links the verbatim passage it came from.
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass

logger = logging.getLogger(__name__)

# "Moved by: <mover>" line, optionally with an inline "; seconded by: <seconder>".
_MOVED_BY_RE = re.compile(r"^Moved by:\s*(.+?)(?:\s*;\s*seconded by:\s*(.+))?$", re.IGNORECASE)
# Standalone "Seconded by: <seconder>" line (the non-inline variant).
_SECONDED_RE = re.compile(r"^Seconded by:\s*(.+)$", re.IGNORECASE)
# A motion's opening cue. The motion line begins with one of these (the explicit
# "Move to" / "Motion to" of layout A/B, or the bare imperative of layout B/C:
# "Approve ...", "Adopt ...", "That the Board ..."). Requiring a cue is what lets
# the parser tell a real motion from the agenda-item title or narrative line that
# can sit just above a "Moved by:" — a block with no cue line is skipped, not
# guessed at (e.g. an adjournment recorded only as "The meeting adjourned at ...").
_MOTION_CUE_RE = re.compile(
    r"^(?:move|motion|approve|adopt|accept|acknowledge|authorize|ratify|award|set|"
    r"establish|recess|convene|adjourn|direct|reaffirm|re-?approve|appoint|designate|"
    r"confirm|declare|rescind|amend|table|postpone|refer|receive|grant|renew|"
    r"terminate|suspend|elect|nominate|that)\b",
    re.IGNORECASE,
)
# A pure section-number line ("1.", "2.1", "6.01") — a boundary above/below a block.
_SECTION_RE = re.compile(r"^\d+(?:\.\d+)*\.?$")
# Agenda-item labels that precede a motion but are not part of it.
_LABEL_RE = re.compile(
    r"^(?:CONTACT|DESCRIPTION|RECOMMENDATION|First Reading|Second Reading|"
    r"Information|Action|Discussion)\b\s*:?",
    re.IGNORECASE,
)

# A bare result word line, e.g. "Carried" / "Failed".
_RESULT_BARE_RE = re.compile(
    r"^(carried|passed|failed|defeated|tabled|withdrawn)\.?$", re.IGNORECASE
)
# A "Motion Carries 7-0" / "Motion Fails 3-4-1" result line (counts optional).
_RESULT_MOTION_RE = re.compile(
    r"^motion\s+(carries|carried|passes|passed|fails|failed|defeated|tabled|withdrawn)\b"
    r"\s*(?:(\d+)\s*-\s*(\d+)(?:\s*-\s*(\d+))?)?",
    re.IGNORECASE,
)

# Result word -> normalized status stored on the record.
_RESULT_NORM = {
    "carried": "passed",
    "carries": "passed",
    "passed": "passed",
    "passes": "passed",
    "failed": "failed",
    "fails": "failed",
    "defeated": "failed",
    "tabled": "tabled",
    "withdrawn": "withdrawn",
}

# Per-member / header vote word -> normalized vote. absent/present are recorded in
# the member list but never counted in a tally (not a yes/no/abstain).
_VOTE_NORM = {
    "aye": "aye",
    "ayes": "aye",
    "yes": "aye",
    "nay": "no",
    "nays": "no",
    "no": "no",
    "abstain": "abstain",
    "abstained": "abstain",
    "abstention": "abstain",
    "absent": "absent",
    "present": "present",
}
_VOTE_HEADER_RE = re.compile(
    r"^(aye|ayes|yes|nay|nays|no|abstain|abstained|abstention|absent|present)\b\s*:?\s*(.*)$",
    re.IGNORECASE,
)
# A page footer ("Page 1 of 2") — noise the PDF interleaves into a block; skipped
# when collecting a motion so it neither breaks the run nor lands in the text.
_PAGE_FOOTER_RE = re.compile(r"^Page\s+\d+\s+of\s+\d+\s*$", re.IGNORECASE)
# The same footer matched anywhere inline, for normalizing it out before an anchor
# match: a motion collected across a page break has no footer, but the chunk that
# verbatim-contains it does, so it must be stripped from both sides to compare.
_PAGE_FOOTER_INLINE_RE = re.compile(r"\s*Page\s+\d+\s+of\s+\d+\s*", re.IGNORECASE)
# "All aye" / "The votes were all aye" — a unanimous result with no counted roll
# call. The result is determinable (passed) but the tally is not.
_UNANIMOUS_AYE_RE = re.compile(r"(?i)^(?:the votes were )?all ayes?\.?$")
# A narrative adjournment line with no motion ("The meeting adjourned at 8:14
# p.m."). When it is the only text above a moved-by, no motion was recorded.
_ADJOURN_NARRATIVE_RE = re.compile(r"(?i)^the meeting (?:was )?adjourned\b")
# Strong, unambiguous motion verbs for a *mid-line* cue search (recovering a
# motion merged onto a title line). Narrower than the line-start cue set: weak
# words like "that"/"set"/"receive" are excluded so they cannot split a
# declarative sentence mid-stream.
_MID_CUE_RE = re.compile(
    r"\b(?:move|motion|approve|adopt|authorize|adjourn|ratify|rescind|reaffirm|appoint)\b",
    re.IGNORECASE,
)

# How far above a "Moved by:" line to look for its motion, and how far below for
# its roll call + result. Bounded so a block missing one never runs into the next.
_MOTION_LOOKUP = 14
_RESULT_LOOKAHEAD = 30


@dataclass(frozen=True)
class ParsedVote:
    """One board vote read verbatim from a minutes block.

    ``result`` is the normalized status (passed/failed/tabled/withdrawn).
    ``result_basis`` is "stated" when the minutes printed a result word and
    "derived" when passed/failed was computed from the verbatim roll call because
    no result line was printed. Tallies are ``None`` when no countable roll call or
    explicit total was recorded. ``anchors`` are the candidate lines (most specific
    first) used to locate the citing chunk.
    """

    motion: str
    result: str
    result_basis: str
    vote_count_yes: int | None
    vote_count_no: int | None
    vote_count_abstain: int | None
    moved_by: str
    seconded_by: str
    members: tuple[dict[str, str], ...]
    source_quote: str
    anchors: tuple[str, ...]


def _strip_zw(text: str) -> str:
    """Drop zero-width characters PDF extraction sprinkles into section numbers."""
    return text.replace("​", "").replace("﻿", "")


def _norm(text: str) -> str:
    """Collapse all whitespace to single spaces — the chunker's comparison form."""
    return " ".join(_strip_zw(text).split())


def _is_boundary(line: str) -> bool:
    """True for a line that ends a motion's upward/downward scan (number/label)."""
    return bool(_SECTION_RE.match(line) or _LABEL_RE.match(line))


def _match_result(line: str) -> tuple[str, int | None, int | None, int | None] | None:
    """If ``line`` is a result line, return (status, yes, no, abstain) else None.

    Recognizes a bare result word ("Carried") and a "Motion Carries N-N[-N]" line;
    counts come only from the explicit N-N[-N], never inferred.
    """
    bare = _RESULT_BARE_RE.match(line)
    if bare:
        return _RESULT_NORM[bare.group(1).lower()], None, None, None
    mot = _RESULT_MOTION_RE.match(line)
    if mot:
        yes = int(mot.group(2)) if mot.group(2) is not None else None
        no = int(mot.group(3)) if mot.group(3) is not None else None
        abstain = int(mot.group(4)) if mot.group(4) is not None else None
        return _RESULT_NORM[mot.group(1).lower()], yes, no, abstain
    return None


def _split_names(text: str) -> list[str]:
    """Split a roster into clean names.

    Splits on commas and on a conjunction ('and'), so both the Oxford-comma form
    ('A, B, and C') and the bare form ('A, B and C') yield three names — a missed
    split would undercount the roll call and so the derived tally. No board
    member's name contains ' and ', so splitting on it is safe here.
    """
    names: list[str] = []
    for part in re.split(r",|\s+and\s+", text):
        name = re.sub(r"(?i)^and\s+", "", part.strip()).strip()
        if name:
            names.append(name)
    return names


def _members_from_suffix(text: str) -> list[dict[str, str]] | None:
    """Parse a layout-A roll call ('Name-aye, Name-nay, ...') into member votes.

    A leading "Votes:" label is dropped; then every comma token must split on its
    last hyphen (so hyphenated surnames survive) into a name and a known vote word.
    Any unclean token rejects the whole line — the parser records no tally rather
    than a partial one. Requires at least two members.
    """
    text = re.sub(r"(?i)^votes:\s*", "", text).strip()
    if not text:
        return None
    parts = [p.strip() for p in text.split(",") if p.strip()]
    if len(parts) < 2:
        return None
    members: list[dict[str, str]] = []
    for part in parts:
        name, sep, vote = part.rpartition("-")
        word = vote.strip().lower()
        if not sep or not name.strip() or word not in _VOTE_NORM:
            return None
        members.append({"name": name.strip(), "vote": _VOTE_NORM[word]})
    return members


def _members_from_headers(lines: list[str]) -> list[dict[str, str]] | None:
    """Parse layout-B/C vote headers ('Aye' / 'Yes:') plus following name lists.

    A header line names a vote ("Aye", "Nay", "Yes:", "No:"); the names that
    follow it — on the same line after a colon, or on subsequent lines until the
    next header — all cast that vote. Returns None if no header yields any name.
    """
    members: list[dict[str, str]] = []
    current: str | None = None
    pending: list[str] = []

    def flush() -> None:
        if current and pending:
            for name in _split_names(" ".join(pending)):
                members.append({"name": name, "vote": current})
        pending.clear()

    for line in lines:
        header = _VOTE_HEADER_RE.match(line)
        # A line that opens with a vote word is a header ("Aye", "Yes: names");
        # the region is scoped between moved-by and result, so a plain name line
        # (no leading vote word) won't match and accrues to the current header.
        if header:
            flush()
            current = _VOTE_NORM[header.group(1).lower()]
            rest = header.group(2).strip()
            if rest:
                pending.append(rest)
        elif current:
            pending.append(line)
    flush()
    return members or None


def _tally(members: list[dict[str, str]]) -> tuple[int, int, int]:
    """Count yes / no / abstain across a roll call (absent/present excluded)."""
    yes = sum(1 for m in members if m["vote"] == "aye")
    no = sum(1 for m in members if m["vote"] == "no")
    abstain = sum(1 for m in members if m["vote"] == "abstain")
    return yes, no, abstain


def _cue_offset(line: str) -> int | None:
    """Char offset where a motion cue opens in ``line``, or None.

    The line start first (the common imperative "Approve ..."), then a strong
    mid-line verb, which recovers a motion the PDF merged onto its title line
    ("Adjournment Adjourn the meeting." -> "Adjourn the meeting."). The mid-line
    set is deliberately narrow (unambiguous motion verbs only) so a stray verb in
    a declarative sentence does not split it.
    """
    if _MOTION_CUE_RE.match(line):
        return 0
    mid = _MID_CUE_RE.search(line)
    return mid.start() if mid else None


def _looks_like_title(line: str) -> bool:
    """A short agenda-item heading ('Adjournment', 'Adoption of Agenda') that sits
    above a motion but is not part of it: few words, no sentence punctuation, no
    cue."""
    words = line.split()
    return bool(
        1 <= len(words) <= 5
        and not line.endswith((".", ":", ";"))
        and not _MOTION_CUE_RE.match(line)
    )


def _has_motion_shape(text: str) -> bool:
    """A plausible motion: at least three words and starting with a letter (rejects
    a wrap fragment like '$875,000 (proposal cost plus contingency).')."""
    return len(text.split()) >= 3 and text[:1].isalpha()


def _find_motion(stripped: list[str], moved_idx: int) -> tuple[int, str] | None:
    """The motion's (start_index, text) for the moved-by at ``moved_idx``, or None.

    Collects the contiguous run above the moved-by (skipping the blank gap and
    page-footer noise, stopping at a section number / label / another block) and
    drops leading agenda-item titles. The motion is then either cue-anchored (from
    the first line carrying a motion cue to the run's end — capturing wraps and
    title-merged motions) or, absent any cue, the run read as a declarative motion
    ("Receipt of ... is hereby acknowledged ...") — unless it is only an
    adjournment narrative or lacks motion shape, in which case there is no verbatim
    motion to record and it returns None.
    """
    run: list[tuple[int, str]] = []  # (index, line), bottom-up then reversed
    i = moved_idx - 1
    while i >= 0 and (not stripped[i] or _PAGE_FOOTER_RE.match(stripped[i])):
        i -= 1  # skip the blank gap (and any footer) directly above the moved-by
    lo = max(0, moved_idx - _MOTION_LOOKUP)
    while i >= lo and not _is_boundary(stripped[i]):
        if _MOVED_BY_RE.match(stripped[i]) or _SECONDED_RE.match(stripped[i]):
            break
        if not stripped[i] or _PAGE_FOOTER_RE.match(stripped[i]):
            # A contiguous blank/footer gap. Cross it only when it carries a page
            # footer (the PDF split one motion across a page break, e.g. "...for a
            # sum not to exceed" | Page 5 of 7 | "$875,000..."); a gap of plain
            # blank lines separates blocks, so stop there.
            j = i
            saw_footer = False
            while j >= lo and (not stripped[j] or _PAGE_FOOTER_RE.match(stripped[j])):
                saw_footer = saw_footer or bool(_PAGE_FOOTER_RE.match(stripped[j]))
                j -= 1
            if not saw_footer:
                break
            i = j
            continue
        run.append((i, stripped[i]))
        i -= 1
    run.reverse()  # top-to-bottom
    while run and _looks_like_title(run[0][1]):
        run.pop(0)
    if not run:
        return None

    for pos, (idx, line) in enumerate(run):
        off = _cue_offset(line)
        if off is not None:
            text = _norm(" ".join([line[off:], *(b[1] for b in run[pos + 1 :])]))
            return (idx, text) if _has_motion_shape(text) else None

    joined = _norm(" ".join(line for _, line in run))
    if _ADJOURN_NARRATIVE_RE.match(joined) or not _has_motion_shape(joined):
        return None
    return run[0][0], joined


def _scan_result(
    stripped: list[str], start: int
) -> tuple[tuple[str, int | None, int | None, int | None] | None, int, list[tuple[int, str]]]:
    """From ``start``, find the result line and collect the (indexed) lines before it.

    Returns ``((status, yes, no, abstain) | None, result_line_index, between)``
    where ``between`` is the ``(index, line)`` pairs between the moved-by/seconded
    and the result. Stops with no result if a new moved-by/section line appears
    first, so a block missing its own result never borrows the next block's.
    """
    between: list[tuple[int, str]] = []
    end = min(start + _RESULT_LOOKAHEAD, len(stripped))
    for k in range(start, end):
        s = stripped[k]
        if not s or _PAGE_FOOTER_RE.match(s):
            continue
        res = _match_result(s)
        if res:
            return res, k, between
        if _MOVED_BY_RE.match(s) or _SECTION_RE.match(s):
            break
        between.append((k, s))
    return None, -1, between


def _counts(
    res: tuple[str, int | None, int | None, int | None],
    members: list[dict[str, str]] | None,
) -> tuple[int | None, int | None, int | None]:
    """Reconcile the explicit result-line total with the parsed roll call.

    When both are present they must agree: a conflict means one was misparsed, so
    rather than silently pick a side (and risk storing a wrong count) the count is
    dropped to None and logged for audit. When they agree, the roll call is
    preferred (it also supplies abstain that a two-part "N-N" omits). With only one
    source, that source is used. Never infers a number.
    """
    _, rn_yes, rn_no, rn_abs = res
    mt = _tally(members) if members else None
    if rn_yes is not None:
        if mt is None:
            return rn_yes, rn_no, rn_abs
        if mt[0] != rn_yes or mt[1] != rn_no or (rn_abs is not None and mt[2] != rn_abs):
            logger.warning(
                "Vote count conflict: roll call %s vs stated %s-%s-%s; dropping count",
                mt,
                rn_yes,
                rn_no,
                rn_abs,
            )
            return None, None, None
        return mt  # agree; mt also carries abstain when the stated total omits it
    if mt:
        return mt
    return None, None, None


def _resolve_result(
    res: tuple[str, int | None, int | None, int | None] | None,
    result_idx: int,
    members: list[dict[str, str]] | None,
    between: list[tuple[int, str]],
) -> tuple[str | None, str, tuple[int | None, int | None, int | None], int]:
    """Decide a block's result, its basis, counts, and the block's end line.

    Returns ``(result | None, basis, (yes, no, abstain), end_index)``. A ``None``
    result means the block is not recordable (skipped).

    - A stated result line ("Carried" / "Motion Carries N-N") -> basis "stated".
    - No result line but a countable roll call with a strict majority -> the result
      is computed from the verbatim tally (basis "derived"); a tie is not derivable.
    - No result line and no roll call but a unanimous-aye phrase ("All aye") ->
      passed, basis "derived", with no countable tally.
    """
    if res is not None:
        return res[0], "stated", _counts(res, members), result_idx
    end_idx = between[-1][0] if between else -1
    if members:
        yes, no, abstain = _tally(members)
        if yes > no:
            return "passed", "derived", (yes, no, abstain), end_idx
        if no > yes:
            return "failed", "derived", (yes, no, abstain), end_idx
        return None, "", (None, None, None), -1  # tie: not derivable
    if any(_UNANIMOUS_AYE_RE.match(line) for _, line in between):
        return "passed", "derived", (None, None, None), end_idx
    return None, "", (None, None, None), -1


def _parse_block(stripped: list[str], moved_idx: int) -> ParsedVote | None:
    """Parse the one vote block whose 'Moved by:' line is at ``moved_idx``."""
    found = _find_motion(stripped, moved_idx)
    if found is None:
        return None
    motion_idx, motion = found

    moved_match = _MOVED_BY_RE.match(stripped[moved_idx])
    moved_by = (moved_match.group(1).strip() if moved_match else "").strip()
    seconded_by = (moved_match.group(2) or "").strip() if moved_match else ""

    after = moved_idx + 1
    if not seconded_by and after < len(stripped):
        sec = _SECONDED_RE.match(stripped[after])
        if sec:
            seconded_by = sec.group(1).strip()
            after += 1

    res, result_idx, between = _scan_result(stripped, after)
    between_lines = [line for _, line in between]

    members = _members_from_suffix(_norm(" ".join(between_lines))) if between_lines else None
    if members is None and between_lines:
        members = _members_from_headers(between_lines)

    result, result_basis, counts, end_idx = _resolve_result(res, result_idx, members, between)
    if result is None:
        return None
    yes, no, abstain = counts

    block_lines = [s for s in stripped[motion_idx : end_idx + 1] if s]
    source_quote = _norm(" ".join(block_lines))
    # Anchor on the full motion text only. It is unique within a document, so any
    # chunk that verbatim-contains it is the right one (the chunker's overlap may
    # put it in two adjacent chunks, but both hold this motion). The roll-call line
    # is deliberately NOT an anchor — the same members vote on every motion, so it
    # repeats and would misattribute; a motion-prefix fallback is likewise omitted
    # because a shared prefix could match a different motion. A motion that no
    # single chunk contains yields no anchor and the loader skips it (cite or abstain).
    anchors = (motion,) if motion else ()

    return ParsedVote(
        motion=motion,
        result=result,
        result_basis=result_basis,
        vote_count_yes=yes,
        vote_count_no=no,
        vote_count_abstain=abstain,
        moved_by=moved_by,
        seconded_by=seconded_by,
        members=tuple(members or ()),
        source_quote=source_quote,
        anchors=anchors,
    )


def parse_votes(content: str) -> list[ParsedVote]:
    """Parse all recognizable board-vote blocks from a minutes document's text.

    Returns one :class:`ParsedVote` per block that has both a verbatim motion and
    a result. Documents with no "Moved by:" structure (older free-prose minutes)
    yield an empty list.
    """
    stripped = [_strip_zw(ln).strip() for ln in content.split("\n")]
    out: list[ParsedVote] = []
    for i, s in enumerate(stripped):
        if _MOVED_BY_RE.match(s):
            block = _parse_block(stripped, i)
            if block is not None:
                out.append(block)
    return out


def find_citing_chunk(anchors: tuple[str, ...], chunks: list[dict]) -> dict | None:
    """The first chunk whose content contains one of ``anchors`` (normalized).

    Chunks are verbatim whitespace-normalized substrings of the source document
    (``chunker.validate_chunks``), so an anchor taken from the same text matches
    the chunk it lives in. Anchors are tried most-specific first. Returns None when
    none match (the caller then skips the vote rather than cite a passage that does
    not carry it).
    """
    normalized = [(c, _norm(_PAGE_FOOTER_INLINE_RE.sub(" ", c.get("content", "")))) for c in chunks]
    for anchor in anchors:
        na = _norm(_PAGE_FOOTER_INLINE_RE.sub(" ", anchor))
        if not na:
            continue
        for chunk, content in normalized:
            if na in content:
                return chunk
    return None


def build_details(vote: ParsedVote) -> dict | None:
    """The JSONB ``details`` payload for a vote: mover, seconder, roll call."""
    details: dict = {}
    if vote.moved_by:
        details["moved_by"] = vote.moved_by
    if vote.seconded_by:
        details["seconded_by"] = vote.seconded_by
    if vote.members:
        details["members"] = [dict(m) for m in vote.members]
    return details or None
