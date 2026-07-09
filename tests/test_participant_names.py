"""Unit tests for tier-2 (named-in-transcript) public-participant naming.

Covers the shared introduction extraction (relocated from the headroom-measurement
script), non-roster detection with correct per-cluster attribution, universal minor
suppression by cue class, and schema-aware persistence under the per-body policy flag —
all pure or against a faked Supabase client, mirroring tests/test_identity_resolve.py.
"""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any

from actalux.identity.name_extraction import (
    STOP_WORDS,
    evidence_sentence,
    place_stop_tokens,
    role_snippet,
    roster_keys,
    turn_hits,
)
from actalux.identity.participant_names import (
    ParticipantNameProposal,
    ParticipantTurn,
    detect_participant_names,
    is_minor_selfid,
    persist_participant_names,
)
from actalux.identity.resolve import RosterMember


def _members() -> list[RosterMember]:
    return [
        RosterMember(1, "jane-harris", "Jane Harris", frozenset({"jane harris", "harris"})),
        RosterMember(2, "bob-stevens", "Bob Stevens", frozenset({"bob stevens", "stevens"})),
        RosterMember(3, "carol-diaz", "Carol Diaz", frozenset({"carol diaz", "diaz"})),
    ]


def _pt(cluster: str, text: str, start: float | None = None) -> ParticipantTurn:
    return ParticipantTurn(cluster, text, start)


# --- shared extraction: turn_hits + evidence_sentence -------------------------------------


def test_self_intro_extraction_basic():
    hits = turn_hits("Hi, my name is Tyler Stevens.", STOP_WORDS)
    assert len(hits) == 1
    assert hits[0].source == "self_intro"
    assert hits[0].name == "Tyler Stevens"


def test_self_intro_im_contraction():
    hits = turn_hits("I'm Casey Nguyen here tonight", STOP_WORDS)
    assert [(h.source, h.name) for h in hits] == [("self_intro", "Casey Nguyen")]


def test_self_intro_skips_leading_honorific():
    hits = turn_hits("My name is Dr. Tyler Stevens", STOP_WORDS)
    assert hits[0].name == "Tyler Stevens"  # "Dr." skipped as filler


def test_self_intro_caps_at_three_name_tokens():
    hits = turn_hits("My name is Jon Elias Vasquez Ramirez", STOP_WORDS)
    assert hits[0].name == "Jon Elias Vasquez"  # 4th token dropped (MAX_NAME_TOKENS)


def test_self_intro_stop_word_is_not_a_name():
    # A procedural/role word after the cue is rejected (not a personal name).
    assert turn_hits("My name is Ordinance Review", STOP_WORDS) == []


def test_self_intro_name_followed_by_number_is_rejected():
    # "Bill 6852" shape: a name immediately followed by a bare number is not a person.
    assert turn_hits("My name is Tyler Stevens 42", STOP_WORDS) == []


def test_presenter_intro_extraction_requires_full_name():
    full = turn_hits("For the next item I would like to introduce Tyler Stevens", STOP_WORDS)
    assert [(h.source, h.name) for h in full] == [("presenter_intro", "Tyler Stevens")]
    # A single capitalized token after a cue verb is too weak (min 2 tokens) -> no hit.
    assert turn_hits("I recognize Marcus", STOP_WORDS) == []


def test_evidence_sentence_is_the_verbatim_intro_sentence():
    text = "Good evening. My name is Tyler Stevens, a resident of Maryland Avenue. Thank you."
    hit = turn_hits(text, STOP_WORDS)[0]
    quote = evidence_sentence(text, hit.start_index, hit.end_index)
    assert quote == "My name is Tyler Stevens, a resident of Maryland Avenue."


def test_role_snippet_returns_words_after_the_name():
    text = "My name is Tyler Stevens, an architect with Core Ten Architecture"
    hit = turn_hits(text, STOP_WORDS)[0]
    assert role_snippet(text, hit.end_index).startswith("an architect")


def test_roster_keys_include_aliases_and_canonical():
    keys, index = roster_keys(_members())
    assert "jane harris" in keys and "harris" in keys
    assert index["jane harris"] == [1]


def test_place_stop_tokens_from_place_fields():
    stops = place_stop_tokens({"state": "mo", "slug": "clayton", "name": "Clayton"})
    assert "clayton" in stops and "mo" in stops


# --- detection: roster exclusion ---------------------------------------------------------


def test_detect_roster_full_name_is_not_a_tier2_proposal():
    # A roster official self-identifying is the TRACKED path — never a tier-2 name.
    turns = [_pt("S0", "Hi, I'm Jane Harris.")]
    assert detect_participant_names(turns, _members(), STOP_WORDS) == []


def test_detect_roster_surname_is_not_a_tier2_proposal():
    assert detect_participant_names([_pt("S0", "I'm Harris")], _members(), STOP_WORDS) == []


def test_detect_nonroster_self_intro_emits_proposal():
    turn = _pt("SPEAKER_04", "Hi, my name is Tyler Stevens, a local architect.", 91.5)
    props = detect_participant_names([turn], _members(), STOP_WORDS)
    assert len(props) == 1
    p = props[0]
    assert p.cluster_label == "SPEAKER_04"
    assert p.display_name == "Tyler Stevens"
    assert p.basis == "self_intro"
    assert p.start_seconds == 91.5
    assert "Tyler Stevens" in p.evidence_quote


# --- detection: presenter introductions are deferred (P1 = self-intro only) ---------------


def test_detect_presenter_intro_is_deferred_no_proposal():
    # A third-party presenter introduction produces NO tier-2 proposal in P1 — even when a
    # different cluster clearly takes the floor afterward. The introducer is never labeled.
    turns = [
        _pt("CHAIR", "For the next item I would like to introduce Tyler Stevens", 5.0),
        _pt("SPEAKER_02", "Thank you. " + " ".join(["budget"] * 60), 10.0),
    ]
    assert detect_participant_names(turns, _members(), STOP_WORDS) == []


def test_detect_ambiguous_cluster_two_names_is_dropped():
    # One cluster self-introduces as two different people -> ambiguous -> drop (precision).
    turns = [
        _pt("SPEAKER_04", "I'm Tyler Stevens"),
        _pt("SPEAKER_04", "I'm Casey Nguyen"),
    ]
    assert detect_participant_names(turns, _members(), STOP_WORDS) == []


# --- universal minor suppression ---------------------------------------------------------


def test_detect_suppresses_a_minor_self_id():
    turn = _pt("SPEAKER_04", "My name is Jordan Alvarez and I'm a senior at the high school.")
    assert detect_participant_names([turn], _members(), STOP_WORDS) == []


def test_minor_cue_bare_class_standing_sophomore_freshman():
    # Only unambiguous bare class-standing suppresses on its own.
    assert is_minor_selfid("I'm a sophomore here to speak")
    assert is_minor_selfid("I am a freshman at Clayton")


def test_minor_cue_senior_junior_suppressed_only_with_school_context():
    # "senior"/"junior" are NOT minor cues on their own (adult senses), but a co-occurring
    # high-school or grade cue still catches the actual minor.
    assert is_minor_selfid("I'm a senior at Clayton High School")
    assert is_minor_selfid("I'm a junior in 11th grade")


def test_minor_cue_student_self_identification():
    assert is_minor_selfid("I'm a student at Wydown")
    assert is_minor_selfid("I'm here with the student council")


def test_minor_cue_numeric_grade_level():
    assert is_minor_selfid("I'm in 9th grade")
    assert is_minor_selfid("a 12th grade project")


def test_minor_cue_spelled_grade_level():
    assert is_minor_selfid("I am in twelfth grade")


def test_minor_cue_school_level_nouns():
    assert is_minor_selfid("I go to the high school")
    assert is_minor_selfid("over at the middle school")
    assert is_minor_selfid("at our elementary school")


def test_minor_cue_youth_governance():
    assert is_minor_selfid("I represent the youth council")


def test_minor_cue_does_not_suppress_an_adult_professional():
    assert not is_minor_selfid("I'm an architect with Core Ten Architecture")
    assert not is_minor_selfid("I represent the Chamber of Commerce")


def test_minor_cue_senior_junior_professional_titles_are_named():
    # The fix: adult "senior"/"junior" titles must NOT be suppressed — these are exactly the
    # plan-commission architects/developers/associates the feature exists to name.
    assert not is_minor_selfid("I'm a senior architect at the firm")
    assert not is_minor_selfid("I'm a senior partner")
    assert not is_minor_selfid("I'm a junior associate")


# --- persistence: a faked Supabase client keyed per table --------------------------------


class _FakeTable:
    def __init__(self, name: str, store: dict[str, list[dict]], log: list[dict]) -> None:
        self._name = name
        self._store = store
        self._log = log
        self._op: str | None = None
        self._payload: Any = None
        self._on_conflict: str | None = None

    def select(self, _cols: str) -> _FakeTable:
        self._op = "select"
        return self

    def eq(self, _col: str, _val: Any) -> _FakeTable:
        return self

    def limit(self, _n: int) -> _FakeTable:
        return self

    def upsert(self, payload: Any, on_conflict: str | None = None) -> _FakeTable:
        self._op, self._payload, self._on_conflict = "upsert", payload, on_conflict
        return self

    def execute(self) -> SimpleNamespace:
        if self._op == "select":
            return SimpleNamespace(data=list(self._store.get(self._name, [])))
        self._log.append(
            {"table": self._name, "payload": self._payload, "on_conflict": self._on_conflict}
        )
        return SimpleNamespace(data=[])


class _FakeClient:
    def __init__(self, store: dict[str, list[dict]] | None = None) -> None:
        self.store = store or {}
        self.log: list[dict] = []

    def table(self, name: str) -> _FakeTable:
        return _FakeTable(name, self.store, self.log)


def _proposal(cluster: str = "SPEAKER_04", name: str = "Tyler Stevens") -> ParticipantNameProposal:
    return ParticipantNameProposal(cluster, name, "self_intro", f"I'm {name}.", 12.0)


def _upserts(client: _FakeClient) -> list[dict]:
    return [e for e in client.log if e["table"] == "transcript_speaker_names"]


def test_persist_off_writes_nothing():
    client = _FakeClient({"entities": [{"public_participant_naming": "off"}]})
    assert persist_participant_names(client, 7, 1, [_proposal()]) == 0
    assert client.log == []


def test_persist_default_flag_is_off():
    # No flag row -> the safe default is 'off' -> nothing written.
    client = _FakeClient({})
    assert persist_participant_names(client, 7, 1, [_proposal()]) == 0
    assert client.log == []


def test_persist_auto_inserts_approved_without_subject_id():
    client = _FakeClient({"entities": [{"public_participant_naming": "auto"}]})
    written = persist_participant_names(client, 7, 1, [_proposal()])
    assert written == 1
    up = _upserts(client)
    assert len(up) == 1
    assert up[0]["on_conflict"] == "document_id,cluster_label"
    row = up[0]["payload"][0]
    assert row["status"] == "approved"
    assert row["document_id"] == 7 and row["cluster_label"] == "SPEAKER_04"
    assert "subject_id" not in row  # tier 2 is non-tracked by construction


def test_persist_review_inserts_proposed():
    client = _FakeClient({"entities": [{"public_participant_naming": "review"}]})
    written = persist_participant_names(client, 7, 1, [_proposal()])
    assert written == 1
    assert _upserts(client)[0]["payload"][0]["status"] == "proposed"


def test_persist_skips_cluster_with_a_tracked_identity():
    # SPEAKER_04 is already a tracked official (tier 1) -> no tier-2 row for it.
    client = _FakeClient(
        {
            "entities": [{"public_participant_naming": "auto"}],
            "speaker_identities": [{"cluster_label": "SPEAKER_04", "subject_id": 9}],
        }
    )
    assert persist_participant_names(client, 7, 1, [_proposal()]) == 0
    assert _upserts(client) == []


def test_persist_leaves_a_human_rejected_row():
    client = _FakeClient(
        {
            "entities": [{"public_participant_naming": "auto"}],
            "transcript_speaker_names": [{"cluster_label": "SPEAKER_04", "status": "rejected"}],
        }
    )
    assert persist_participant_names(client, 7, 1, [_proposal()]) == 0
    assert _upserts(client) == []


def test_persist_leaves_a_human_approved_row_on_a_review_body():
    # On a 'review' body the machine writes 'proposed'; an 'approved' row is a human action.
    client = _FakeClient(
        {
            "entities": [{"public_participant_naming": "review"}],
            "transcript_speaker_names": [{"cluster_label": "SPEAKER_04", "status": "approved"}],
        }
    )
    assert persist_participant_names(client, 7, 1, [_proposal()]) == 0
    assert _upserts(client) == []


def test_persist_overwrites_a_matching_machine_status_row():
    # An existing 'approved' row on an 'auto' body is the machine default -> idempotent re-write.
    client = _FakeClient(
        {
            "entities": [{"public_participant_naming": "auto"}],
            "transcript_speaker_names": [{"cluster_label": "SPEAKER_04", "status": "approved"}],
        }
    )
    assert persist_participant_names(client, 7, 1, [_proposal()]) == 1
    assert len(_upserts(client)) == 1


def test_to_row_shape_carries_null_start_seconds():
    p = ParticipantNameProposal(
        "SPEAKER_00", "Dana Kim", "presenter_intro", "welcome Dana Kim", None
    )
    assert p.to_row(7, "approved") == {
        "document_id": 7,
        "cluster_label": "SPEAKER_00",
        "display_name": "Dana Kim",
        "basis": "presenter_intro",
        "evidence_quote": "welcome Dana Kim",
        "start_seconds": None,
        "status": "approved",
    }


def test_analysis_script_still_imports_after_extraction_refactor():
    # The headroom script shares the relocated extraction — it must still import + expose its API.
    import scripts.analyze_self_intro_coverage as asic

    assert callable(asic.scan_body) and callable(asic.run)
