"""Resolve a raw vote-roll name to a curated roster subject.

The minutes name members inconsistently: school roll calls carry honorifics
("Mr. Ben Beinfeld"), council roll calls are last-name-only with a rotating title
("Alderman Harris", "Mayor Harris", "Mayor Pro Tempore Harris" — one person), and
both carry OCR drift ("Dr Pamela Lyss— Lerman", "Garhnolz"). This module turns a
raw name into a normalized key and resolves that key against a curated roster,
date-bounded so a future same-surname pair (or one alias reused across eras)
disambiguates by who was seated on the meeting date.

Resolution is deliberately conservative (connections-graph.md §4, §7): a name that
matches exactly one roster member resolves; zero or more-than-one returns an
unresolved/ambiguous result the caller QUEUES for review — it never auto-mints or
auto-attributes. ``normalize_name`` is the single normalizer shared by the seeder
(which stores each alias normalized) and this resolver (which normalizes incoming
vote names), so the two always agree.
"""

from __future__ import annotations

import re
from collections.abc import Iterable
from dataclasses import dataclass, field
from datetime import date

# Leading title/honorific tokens stripped before matching. Multi-word phrases come
# first so "mayor pro tempore" is consumed whole; stripping iterates, so even a
# single-token match ("mayor") still lets the next pass take "pro tempore". These
# are roles a name is PREFIXED with in Clayton minutes, not surnames — the roster
# is small and operator-confirmed, so the rare over-strip surfaces as an ambiguous
# resolution (queued), never a silent mis-attribution.
_TITLE = (
    r"(?:mayor pro tempore|pro tempore|pro temp|pro tem|vice president|council member|"
    r"councilmember|councilman|councilwoman|alderwoman|alderperson|alderman|"
    r"mayor|commissioner|chairman|chairwoman|chairperson|chair|president|member|"
    r"mr|mrs|ms|mx|dr)"
)
_LEADING_TITLE_RE = re.compile(rf"^{_TITLE}\b\s+", re.IGNORECASE)

# Dash glyphs OCR emits for a hyphenated surname (em/en/figure/non-breaking/minus).
_DASH_VARIANTS = ("—", "–", "‐", "‑", "−")


def normalize_name(raw: str) -> str:
    """Normalized resolution key for a member name.

    Strips honorifics/titles, folds OCR dash glyphs and spacing around a
    hyphenated surname, drops periods/commas, collapses whitespace, and casefolds.
    Systematic variants ("Mr. X", "Mayor X", "Lyss— Lerman") collapse to one key;
    non-systematic OCR garble ("lVls.Kim Hurst", "Garhnolz") deliberately does NOT
    (it has no rule), so it stays distinct and the resolver leaves it for an
    explicit alias or the review queue rather than guessing.

    Parameters
    ----------
    raw
        The name as written in the minutes (``details.members[*].name`` or a
        mover/seconder string).

    Returns
    -------
    str
        The normalized key (possibly empty for a blank/garbage input).
    """
    n = raw or ""
    for dash in _DASH_VARIANTS:
        n = n.replace(dash, "-")
    n = n.replace(".", " ").replace(",", " ")
    n = re.sub(r"\s*-\s*", "-", n)  # "lyss - lerman" / "lyss- lerman" -> "lyss-lerman"
    n = " ".join(n.split())
    prev = ""
    while prev != n:  # iterate: "Mayor Pro Tempore Winings" -> "Winings"
        prev = n
        n = _LEADING_TITLE_RE.sub("", n).strip()
    return " ".join(n.split()).casefold()


@dataclass(frozen=True)
class Membership:
    """One body-membership window for a subject (an entities.id + a date span).

    ``start_date``/``end_date`` are optional: a member whose term dates we could
    not source carries a NULL-bounded window (covers every date), so they still
    count as on the roster — the window only ever NARROWS, never invents a bound.
    """

    entity_id: int
    start_date: date | None = None
    end_date: date | None = None

    def covers(self, day: date) -> bool:
        """True if ``day`` falls within this window (open where a bound is NULL)."""
        if self.start_date is not None and day < self.start_date:
            return False
        if self.end_date is not None and day > self.end_date:
            return False
        return True


@dataclass(frozen=True)
class RosterSubject:
    """A curated roster member: a subject id, its normalized aliases, its terms."""

    subject_id: int
    aliases: frozenset[str]  # already normalized via normalize_name
    memberships: tuple[Membership, ...] = ()

    def seated_on(self, entity_id: int, day: date) -> bool:
        """True if a membership window for ``entity_id`` covers ``day``."""
        return any(m.entity_id == entity_id and m.covers(day) for m in self.memberships)

    def on_body(self, entity_id: int) -> bool:
        """True if the subject holds any membership in ``entity_id``."""
        return any(m.entity_id == entity_id for m in self.memberships)


@dataclass(frozen=True)
class Resolution:
    """Outcome of resolving one name.

    ``status`` is ``resolved`` (``subject_id`` set), ``unresolved`` (no roster
    match), or ``ambiguous`` (several matched and the date did not break the tie —
    ``candidates`` lists them). The caller mints an edge only for ``resolved``;
    the other two go to ``subject_resolution_queue``.
    """

    status: str
    subject_id: int | None = None
    reason: str = ""
    candidates: tuple[int, ...] = ()


@dataclass
class Roster:
    """An alias index over curated subjects, scoped to one place's bodies."""

    subjects: Iterable[RosterSubject]
    _by_alias: dict[str, list[RosterSubject]] = field(default_factory=dict, init=False)
    _subject_ids: set[int] = field(default_factory=set, init=False)

    def __post_init__(self) -> None:
        for subject in self.subjects:
            self._subject_ids.add(subject.subject_id)
            for alias in subject.aliases:
                self._by_alias.setdefault(alias, []).append(subject)

    def __len__(self) -> int:
        """Number of distinct subjects in the roster."""
        return len(self._subject_ids)

    def resolve(self, raw_name: str, entity_id: int, meeting_date: date) -> Resolution:
        """Resolve ``raw_name`` (seen in ``entity_id`` on ``meeting_date``).

        Matches the normalized name against roster aliases restricted to members of
        ``entity_id``. One match resolves; several are broken by which member was
        seated on ``meeting_date``; an unbroken tie is ambiguous. Zero is
        unresolved. Conservative by construction — never picks arbitrarily.
        """
        key = normalize_name(raw_name)
        if not key:
            return Resolution("unresolved", reason="empty_name")

        candidates = [s for s in self._by_alias.get(key, ()) if s.on_body(entity_id)]
        if not candidates:
            return Resolution("unresolved", reason="no_roster_match")
        if len(candidates) == 1:
            return Resolution("resolved", subject_id=candidates[0].subject_id)

        # More than one member shares this alias (a future same-surname pair, or an
        # alias reused across eras): let the meeting date pick the seated one.
        seated = [s for s in candidates if s.seated_on(entity_id, meeting_date)]
        if len(seated) == 1:
            return Resolution("resolved", subject_id=seated[0].subject_id)
        return Resolution(
            "ambiguous",
            reason="multiple_roster_match",
            candidates=tuple(s.subject_id for s in candidates),
        )
