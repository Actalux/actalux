"""Link extracted business items into cases that span meetings.

A land-use case appears at one meeting or several — heard, continued, continued
again, decided. The linker groups item-appearances into cases deterministically:
same body, same normalized address, same application type, within a rolling
window that a continuance extends. Anything the rules cannot place confidently
goes to a review list, never into a guessed join.
Design: docs/architecture/land-use-cases.md.

Status resolution: the last decisive action wins; a case whose latest action is
a continuance stays open. ``outcome_matches_staff`` — the dataset's core
analytical column — compares the resolved outcome against the staff
recommendation and is null until both exist.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import date, timedelta

# A case heard again within this many days of its last appearance is the same
# case; beyond it, a new application at the same address starts a new case.
# Clayton continuances observed in the corpus resolve within a few months; a
# year of silence at the same address is a new project.
LINK_WINDOW = timedelta(days=365)

# PC recommends these to the City Council (advisory role); everything else the
# hearing body decides itself. The BoA is final on all its business.
ADVISORY_TYPES = frozenset({"conditional_use", "rezoning", "text_amendment"})

DECISIVE_ACTIONS = frozenset(
    {"approved", "approved_with_conditions", "denied", "recommended", "withdrawn"}
)

_ACTION_TO_STATUS = {
    "approved": "approved",
    "approved_with_conditions": "approved_with_conditions",
    "denied": "denied",
    "recommended": "recommended_to_council",
    "withdrawn": "withdrawn",
    "continued": "continued",
    "heard": "pending",
}

# Street-suffix and directional noise words collapsed during address
# normalization so "42 N. Central Ave" and "42 North Central Avenue" link.
_ADDR_SUBS = [
    (re.compile(r"\bnorth\b|\bn\.?\b", re.I), "n"),
    (re.compile(r"\bsouth\b|\bs\.?\b", re.I), "s"),
    (re.compile(r"\beast\b|\be\.?\b", re.I), "e"),
    (re.compile(r"\bwest\b|\bw\.?\b", re.I), "w"),
    (re.compile(r"\bavenue\b|\bave\.?\b", re.I), "av"),
    (re.compile(r"\bboulevard\b|\bblvd\.?\b", re.I), "bl"),
    (re.compile(r"\bdrive\b|\bdr\.?\b", re.I), "dr"),
    (re.compile(r"\broad\b|\brd\.?\b", re.I), "rd"),
    (re.compile(r"\bstreet\b|\bst\.?\b", re.I), "st"),
    (re.compile(r"\blane\b|\bln\.?\b", re.I), "ln"),
    (re.compile(r"\bcourt\b|\bct\.?\b", re.I), "ct"),
    (re.compile(r"\bcircle\b|\bcir\.?\b", re.I), "cr"),
    (re.compile(r"\bparkway\b|\bpkwy\.?\b", re.I), "pw"),
]


def normalize_address(raw: str) -> str:
    """Collapse an address to a linking key. Display always uses the raw form."""
    s = raw.casefold().strip()
    s = re.sub(r"[.,#]", " ", s)
    for pat, rep in _ADDR_SUBS:
        s = pat.sub(rep, s)
    return re.sub(r"\s+", " ", s).strip()


@dataclass(frozen=True)
class Appearance:
    """One business item at one meeting, ready for linking."""

    document_id: int
    entity_id: int
    event_date: date
    address_raw: str
    application_type: str
    subtype_raw: str | None
    action: str | None  # from extraction; None when the extractor rejected it
    staff_recommendation: str | None
    conditions_text: str | None
    video_timestamp: str | None
    source_quote: str
    chunk_id: int | None = None
    vote_id: int | None = None
    parties: tuple = ()


@dataclass
class LinkedCase:
    """A case assembled from one or more appearances, oldest first."""

    entity_id: int
    address_raw: str
    address_norm: str
    application_type: str
    subtype_raw: str | None
    appearances: list[Appearance] = field(default_factory=list)

    @property
    def first_seen(self) -> date:
        return self.appearances[0].event_date

    @property
    def last_seen(self) -> date:
        return self.appearances[-1].event_date

    def status(self) -> str:
        """Last decisive action wins; otherwise the latest action; else pending."""
        for app in reversed(self.appearances):
            if app.action in DECISIVE_ACTIONS:
                return _ACTION_TO_STATUS[app.action]
        for app in reversed(self.appearances):
            if app.action:
                return _ACTION_TO_STATUS[app.action]
        return "pending"

    def resolved_date(self) -> date | None:
        for app in reversed(self.appearances):
            if app.action in DECISIVE_ACTIONS:
                return app.event_date
        return None

    def decision_role(self) -> str:
        return "advisory" if self.application_type in ADVISORY_TYPES else "final"

    def staff_recommendation(self) -> str | None:
        """The latest stated recommendation — staff can change position between hearings."""
        for app in reversed(self.appearances):
            if app.staff_recommendation and app.staff_recommendation != "none_stated":
                return app.staff_recommendation
        if any(a.staff_recommendation == "none_stated" for a in self.appearances):
            return "none_stated"
        return None

    def outcome_matches_staff(self) -> bool | None:
        """The core analytical column: did the body do what staff recommended?

        Null until the case has both a decisive outcome and a stated
        recommendation. Approval matches a recommendation to approve whether or
        not conditions were attached — condition-tinkering is normal practice,
        not a staff override; a denial against approve (or vice versa) is.
        A recommendation to council counts as the PC approving at its stage.
        """
        rec = self.staff_recommendation()
        status = self.status()
        if rec in (None, "none_stated"):
            return None
        approved = status in ("approved", "approved_with_conditions", "recommended_to_council")
        denied = status == "denied"
        if not (approved or denied):
            return None
        if rec in ("approve", "approve_with_conditions"):
            return approved
        return denied  # rec == "deny"


@dataclass(frozen=True)
class ReviewItem:
    """An appearance the rules would not place; a human (or later pass) decides."""

    appearance: Appearance
    reason: str
    candidate_case_index: int | None = None


def link_appearances(appearances: list[Appearance]) -> tuple[list[LinkedCase], list[ReviewItem]]:
    """Group appearances into cases; the unplaceable go to review, never guessed.

    Sorted by date, each appearance joins the most recent open case with the
    same (entity, address_norm, application_type) inside LINK_WINDOW of that
    case's last appearance. A decisive action closes the case to further joins —
    a new application at the same address after a decision is a new case, which
    is exactly the distinction an approval-history buyer cares about.

    Sent to review instead of linked: an appearance with no address (legacy
    headers sometimes truncate to a bare house number under 3 characters), and
    an appearance matching a case that resolved less than 60 days earlier —
    close enough that "new case" versus "rehearing of the old one" needs eyes.
    """
    cases: list[LinkedCase] = []
    review: list[ReviewItem] = []
    open_by_key: dict[tuple[int, str, str], int] = {}
    closed_recency: dict[tuple[int, str, str], tuple[int, date]] = {}

    for app in sorted(appearances, key=lambda a: (a.event_date, a.document_id)):
        norm = normalize_address(app.address_raw)
        if len(norm) < 3:
            review.append(ReviewItem(app, reason=f"unusable address {app.address_raw!r}"))
            continue
        key = (app.entity_id, norm, app.application_type)

        idx = open_by_key.get(key)
        if idx is not None:
            case = cases[idx]
            if app.event_date - case.last_seen <= LINK_WINDOW:
                case.appearances.append(app)
                if app.action in DECISIVE_ACTIONS:
                    del open_by_key[key]
                    closed_recency[key] = (idx, app.event_date)
                continue
            del open_by_key[key]  # window expired: same key, but a fresh case

        if key in closed_recency:
            closed_idx, closed_on = closed_recency[key]
            if app.event_date - closed_on < timedelta(days=60):
                review.append(
                    ReviewItem(
                        app,
                        reason=f"appears {app.event_date - closed_on} after case resolved",
                        candidate_case_index=closed_idx,
                    )
                )
                continue

        case = LinkedCase(
            entity_id=app.entity_id,
            address_raw=app.address_raw,
            address_norm=norm,
            application_type=app.application_type,
            subtype_raw=app.subtype_raw,
            appearances=[app],
        )
        cases.append(case)
        if app.action in DECISIVE_ACTIONS:
            closed_recency[key] = (len(cases) - 1, app.event_date)
        else:
            open_by_key[key] = len(cases) - 1

    return cases, review
