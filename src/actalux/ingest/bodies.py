"""Per-body crawl/transcription config.

Identity for each public body lives in the database (``entities``/``places``);
this registry holds the crawl-time tuning that pairs with a body: which YouTube
channel its meetings are published on, which video titles count as that body's
meetings, and the proper-noun bias passed to Whisper. The City of Clayton channel
hosts many bodies (City Council, Plan Commission/ARB, Board of Adjustment,
committees), so each body is selected from the shared channel by its title filter.

Keyed by a short body key (``schools``/``council``) so ``transcribe_meetings``
discovery and ``ingest --body`` agree on one identifier; the ``entity_path``
resolves to the DB ``entities`` row.
"""

from __future__ import annotations

import re
from dataclasses import dataclass

from actalux.ingest.youtube import BOARD_MEETING_RE, CHANNEL


@dataclass(frozen=True)
class TranscriptionBody:
    """How to discover and transcribe one public body's meeting videos."""

    entity_path: str  # "state/place/body" — matches ingest --entity / resolve_entity_id
    channel: str  # YouTube channel URL whose tabs are enumerated
    title_filter: re.Pattern[str]  # video titles that are this body's meetings
    transcribe_prompt: str  # Whisper proper-noun bias (names mis-heard otherwise)
    # Titles that also match ``title_filter`` but belong to another body that owns
    # them — dropped from discovery so the same video isn't transcribed twice.
    exclude_filter: re.Pattern[str] | None = None


# City Council titles: the current "City Council" plus the body's OLD name "Board
# of Aldermen" ("BOA"). Defined once so Plan Commission can reuse it to exclude
# council-owned joint meetings. (Board of ADJUSTMENT spells its name out and never
# uses "BOA", so it stays correctly excluded from council.)
_COUNCIL_TITLE_RE = re.compile(r"city council|board of alderm(?:an|en)|\bboa\b", re.IGNORECASE)


SCHOOLS = TranscriptionBody(
    entity_path="mo/clayton/schools",
    channel=CHANNEL,
    title_filter=BOARD_MEETING_RE,
    transcribe_prompt=(
        "School District of Clayton Board of Education meeting. "
        "Superintendent, Board of Education, Proposition O, levy, agenda, motion carried."
    ),
)

COUNCIL = TranscriptionBody(
    entity_path="mo/clayton/council",
    channel="https://www.youtube.com/@CityofClayton",
    # The city channel hosts several bodies; restrict to City Council (including the
    # old "Board of Aldermen"/"BOA" naming — see _COUNCIL_TITLE_RE).
    title_filter=_COUNCIL_TITLE_RE,
    transcribe_prompt=(
        "City of Clayton, Missouri City Council meeting. "
        "Mayor, City Council, alderman, ordinance, resolution, agenda, motion carried."
    ),
)

PLAN_COMMISSION = TranscriptionBody(
    entity_path="mo/clayton/plan-commission",
    channel="https://www.youtube.com/@CityofClayton",
    # Plan Commission + Architectural Review Board (one body). Titles vary a lot:
    # "PC/ARB", "PC-ARB", "Plan Commission", older "Planning Commission", and a real
    # misspelling "Plan Commision".
    title_filter=re.compile(
        r"pc[\s/-]*arb|plan(?:ning)?\s+comm|architectural review", re.IGNORECASE
    ),
    # Joint "Board of Aldermen & Plan Commission/ARB" meetings match this filter too,
    # but council owns them — exclude council titles so the same video isn't also
    # transcribed under this body.
    exclude_filter=_COUNCIL_TITLE_RE,
    transcribe_prompt=(
        "City of Clayton, Missouri Plan Commission and Architectural Review Board meeting. "
        "Plan Commission, Architectural Review Board, rezoning, variance, site plan, "
        "setback, overlay district, agenda, motion carried."
    ),
)

BODIES: dict[str, TranscriptionBody] = {
    "schools": SCHOOLS,
    "council": COUNCIL,
    "plan-commission": PLAN_COMMISSION,
}


def get_body(key: str) -> TranscriptionBody:
    """Look up a body by its short key (``schools``/``council``), or abort."""
    try:
        return BODIES[key]
    except KeyError:
        raise SystemExit(f"Unknown body {key!r}; choices: {', '.join(BODIES)}") from None
