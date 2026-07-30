"""Compare a meeting's spoken roll call against the attendance its minutes record.

The question this answers: was a member called at the roll without answering, yet listed as
present in the minutes the body later approved? Both sides are quoted verbatim so every reading
is checkable against the record, and neither side is adjudicated — the output states what each
document says, never which one is right.

Three properties keep a silent roll call from becoming a false accusation:

* **Silence is a candidate, not a verdict.** The roll-call reading
  (:func:`~actalux.identity.vote_align.roll_call_attendance`) leaves a call unbound both for a
  genuinely absent member and for one whose "here" the diarizer glued into the clerk's turn. A
  reading is only reported when an independent signal agrees — see :data:`_CORROBORATION`.
* **Late arrival is normal, not a discrepancy.** Minutes routinely seat a member after the roll
  ("arrived at 7:15"). :func:`parse_minutes_attendance` reads those notes so an ordinary late
  arrival never surfaces as a mismatch.
* **The roster bounds it, and ambiguity disqualifies.** Only seated members are ever read, and a
  surname two of them share is dropped rather than guessed — those members become unreadable
  here instead of mis-readable as each other.

Jurisdiction-agnostic: no town's wording is hardcoded. The honorific forms a clerk uses (and the
manglings ASR makes of them) come from the per-place name-corrections glossary, so a body that
says "Alderwoman" and one that says "Councilmember" are both read from data.
"""

from __future__ import annotations

import re
from dataclasses import dataclass

from actalux.identity.resolve import RosterMember
from actalux.identity.vote_align import RollCallAttendance

# Honorifics a clerk reads before a surname. Generic parliamentary English only: a body's own
# forms, and the manglings ASR makes of them, come from the per-place glossary.
BASE_TITLE_FORMS = (
    "council member",
    "councilmember",
    "councilman",
    "councilwoman",
    "alderman",
    "alderwoman",
    "alderperson",
    "mayor",
    "trustee",
    "commissioner",
    "supervisor",
)
# Affirmative answers to a name-call. Deliberately short and generic.
_AFFIRMATIVES = r"(?:here|present|aye|yes)\b"
# A response is looked for between one name-call and the next, so the window needs no fixed size.
# This bounds only the trailing search after the LAST name called.
_TRAILING_RESPONSE_CHARS = 60
# A roll is a run of name-calls close together; a wider gap means ordinary debate resumed.
_MAX_CALL_GAP_CHARS = 120
_MIN_CALLS_FOR_ROLL = 3
# Proximity alone does not make a roll call: "the committee includes Councilmember Smith,
# Councilmember Jones, Councilmember Diaz" is a prose list, and reading it as a roll would report
# all three as silent. A body that convenes has a quorum, so in a real roll a majority of those
# called answer. Requiring that separates a roll being taken from members merely being listed.
_MIN_ANSWERED_SHARE = 0.5
_MIN_ANSWERED_CALLS = 2

# A minutes attendance block: the label, then names up to the next labelled block. Kept
# label-driven rather than position-driven because clerks reorder these freely.
_PRESENT_LABELS = r"in\s*person|present|in\s*attendance|virtually|via\s+zoom"
_ABSENT_LABELS = r"absent|excused"
# The all-caps run is a minutes section heading ("OPEN FORUM"). It stays case-SENSITIVE via the
# inline flag: the enclosing search runs case-insensitive, under which [A-Z]{4,} would match any
# four-letter word and end every block at its first name.
_BLOCK_END = (
    r"(?=\s*(?:staff|absent|excused|in\s*person|present|virtually|via\s+zoom)\s*:"
    r"|\s*(?-i:[A-Z]{4,})|$)"
)
# Minutes seat a member who missed the roll with an explicit arrival note; that is ordinary
# practice, so the note is read and the member is exempted rather than flagged.
_LATE_ARRIVAL = r"([A-Z][\w'’-]+)\s+(?:arrived|joined|entered)\b"
# Silence at the roll only becomes a reading when this many independent signals agree.
_CORROBORATION = 1


@dataclass(frozen=True)
class MinutesAttendance:
    """The attendance one meeting's minutes record, as written."""

    present: frozenset[str]
    absent: frozenset[str]
    arrived_late: frozenset[str]
    source_quote: str


@dataclass(frozen=True)
class AttendanceReading:
    """One member whose roll-call silence and minutes listing disagree, with both sides quoted."""

    subject_id: int
    canonical_name: str
    minutes_quote: str
    corroborating_signals: tuple[str, ...]


def unambiguous_surnames(members: list[RosterMember]) -> dict[str, int]:
    """Map surname -> subject id, dropping every surname two seated members share.

    A shared surname cannot be attributed from a spoken "Councilmember Smith" alone, and guessing
    would attach a reading to the wrong named official. Dropping the key makes those members
    unreadable here rather than mis-readable.
    """
    holders: dict[str, set[int]] = {}
    for member in members:
        if not member.canonical_name:
            continue
        surname = member.canonical_name.split()[-1].lower()
        holders.setdefault(surname, set()).add(member.subject_id)
    return {surname: next(iter(ids)) for surname, ids in holders.items() if len(ids) == 1}


def _surnames(blob: str) -> frozenset[str]:
    """Lowercased surnames in a minutes attendance blob.

    Takes the last token of each comma/and-separated name so "Mayor Bridget McAndrew" and
    "McAndrew" both key on ``mcandrew``.
    """
    names = re.split(r",|\band\b", blob)
    out = set()
    for name in names:
        words = re.findall(r"[A-Za-z][\w'’-]+", name)
        if words:
            out.add(words[-1].lower())
    return frozenset(out)


def parse_minutes_attendance(text: str) -> MinutesAttendance | None:
    """Read the present/absent/late lists out of a minutes document's opening.

    Returns ``None`` when no attendance block is present, which is distinct from an empty one.
    """
    # Every matching block is unioned, not just the first: a preamble ("were in attendance:")
    # can precede the real list, and minutes routinely split the seated members across an
    # in-person block and a virtual one. Both are present.
    present_blocks = [
        m
        for m in re.finditer(
            rf"(?:{_PRESENT_LABELS})\s*:\s*(.{{0,400}}?){_BLOCK_END}", text, re.I | re.S
        )
        if m.group(1).strip()
    ]
    if not present_blocks:
        return None
    absent_blocks = re.finditer(
        rf"(?:{_ABSENT_LABELS})\s*:\s*(.{{0,400}}?){_BLOCK_END}", text, re.I | re.S
    )
    present: set[str] = set()
    for match in present_blocks:
        present |= _surnames(match.group(1))
    absent: set[str] = set()
    for match in absent_blocks:
        absent |= _surnames(match.group(1))
    late = frozenset(m.group(1).lower() for m in re.finditer(_LATE_ARRIVAL, text))
    quote = re.sub(r"\s+", " ", present_blocks[0].group(0)).strip()
    return MinutesAttendance(
        present=frozenset(present),
        absent=frozenset(absent),
        arrived_late=late,
        source_quote=quote,
    )


def roll_call_from_text(
    text: str, members: list[RosterMember], title_forms: tuple[str, ...] = BASE_TITLE_FORMS
) -> RollCallAttendance | None:
    """Read the roll from flat transcript text, for meetings the turn structure cannot serve.

    Diarizers routinely glue a member's "here" into the clerk's own turn, so on most transcripts
    the turn-level reader finds no separable responses at all. The words are still there, so this
    reads the same roll from the text: a run of ``<title> <surname>`` calls, each answered when an
    affirmative appears before the next name is called.

    Biased toward hearing an answer. Members reply "Oh, here" and "Yes, I'm here", so the whole
    inter-call gap is searched rather than its first characters — a false silence about a named
    official is the costly error, and silence alone never becomes a finding anyway.
    """
    if not text or not members:
        return None
    by_surname = unambiguous_surnames(members)
    if not by_surname:
        return None
    titles = "|".join(re.escape(t).replace(r"\ ", r"\s+") for t in title_forms)
    pattern = rf"(?i:{titles})\s+([\w'’-]+(?:\s+[\w'’-]+)?)\s*[\.,\?]?"

    hits = [m for m in re.finditer(pattern, text) if m.group(1).split()[-1].lower() in by_surname]
    if len(hits) < _MIN_CALLS_FOR_ROLL:
        return None
    run = [hits[0]]
    for previous, current in zip(hits, hits[1:]):
        if current.start() - previous.end() > _MAX_CALL_GAP_CHARS:
            break
        run.append(current)
    if len(run) < _MIN_CALLS_FOR_ROLL:
        return None

    called: set[int] = set()
    answered: set[int] = set()
    for index, hit in enumerate(run):
        subject_id = by_surname[hit.group(1).split()[-1].lower()]
        called.add(subject_id)
        if index + 1 < len(run):
            gap = text[hit.end() : run[index + 1].start()]
        else:
            gap = text[hit.end() : hit.end() + _TRAILING_RESPONSE_CHARS]
        if re.search(_AFFIRMATIVES, gap, re.I):
            answered.add(subject_id)
    if len(answered) < _MIN_ANSWERED_CALLS or len(answered) < _MIN_ANSWERED_SHARE * len(called):
        return None  # names listed, nobody answering -> prose, not a roll being taken
    return RollCallAttendance(frozenset(called), frozenset(answered), regions=1)


def _vote_participants(votes: list[dict]) -> tuple[frozenset[str], int]:
    """Surnames recorded casting a vote, and how many votes named their members individually.

    The count matters more than the names. Only about a quarter of parsed votes record who voted;
    the rest carry a tally only. Absence from a tally-only meeting says nothing at all, so the
    caller needs to know whether this meeting's record can support the inference before drawing
    one from it.
    """
    out: set[str] = set()
    itemised = 0
    for vote in votes:
        members = (vote.get("details") or {}).get("members") or []
        if not members:
            continue
        itemised += 1
        for member in members:
            words = re.findall(r"[A-Za-z][\w'’-]+", member.get("name") or "")
            if words:
                out.add(words[-1].lower())
    return frozenset(out), itemised


def compare_attendance(
    roll: RollCallAttendance | None,
    minutes: MinutesAttendance | None,
    members: list[RosterMember],
    votes: list[dict],
) -> list[AttendanceReading]:
    """Members the roll left unanswered whom the minutes nonetheless seat as present.

    Fails closed. A member is only read when their surname is unique on the roster, the minutes
    neither list them absent nor note a late arrival, and this meeting's record can actually
    support the inference — see :func:`_vote_participants`. Silence with nothing to corroborate it
    yields nothing, because the diarizer swallowing a "here" produces exactly the same silence.
    """
    if roll is None or minutes is None:
        return []
    voted, itemised_votes = _vote_participants(votes)
    readable = unambiguous_surnames(members)
    by_id = {m.subject_id: m for m in members}
    readings: list[AttendanceReading] = []
    for subject_id in sorted(roll.unanswered):
        member = by_id.get(subject_id)
        if member is None:
            continue
        surname = member.canonical_name.split()[-1].lower()
        if readable.get(surname) != subject_id:
            continue  # surname shared with another seated member -> not attributable
        if surname not in minutes.present:
            continue  # minutes do not claim them present -> the records agree
        if surname in minutes.absent or surname in minutes.arrived_late:
            continue  # minutes already account for the silence
        signals = []
        if itemised_votes and surname not in voted:
            signals.append(
                f"named in none of the {itemised_votes} votes these minutes record by member"
            )
        if len(signals) >= _CORROBORATION:
            readings.append(
                AttendanceReading(
                    subject_id=subject_id,
                    canonical_name=member.canonical_name,
                    minutes_quote=minutes.source_quote,
                    corroborating_signals=tuple(signals),
                )
            )
    return readings
