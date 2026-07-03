"""Unit tests for the confirm/deny CLI: ordering + decision payloads (no DB/GPU)."""

from __future__ import annotations

import scripts.confirm_speaker as cs


def _cand(
    *,
    identity_id=1,
    person_id=1,
    subject_id=10,
    official_name="Kami Waldman",
    basis="rollcall",
    confidence="inferred_high",
    meeting_key="vid1",
    seconds=100.0,
    cluster_label="SPEAKER_00",
    document_id=5,
    excerpts=(),
) -> cs.Candidate:
    return cs.Candidate(
        identity_id=identity_id,
        document_id=document_id,
        cluster_label=cluster_label,
        person_id=person_id,
        subject_id=subject_id,
        official_name=official_name,
        basis=basis,
        confidence=confidence,
        meeting_key=meeting_key,
        video_id=meeting_key,
        meeting_title="Regular Meeting",
        meeting_date="2025-03-19",
        seconds=seconds,
        excerpts=excerpts,
    )


# --- decision payloads (the y/n state transitions) --------------------------------


def test_confirm_payload_keeps_basis_for_provenance():
    payload = cs.confirm_payload(_cand(basis="rollcall"))
    assert payload == {"confidence": "confirmed"}  # basis kept -> not in the update


def test_confirm_payload_rewrites_voiceprint_basis_to_manual():
    # A biometric-basis row is never enrollable even when confirmed, so it is rewritten to
    # 'manual' to make the confirmation eligible for the gallery.
    payload = cs.confirm_payload(_cand(basis="voiceprint"))
    assert payload == {"confidence": "confirmed", "basis": "manual"}


def test_reject_payload_only_flips_confidence():
    # A denial records nothing about the true voice — subject_id/basis are untouched, only
    # the tier moves to 'rejected'.
    assert cs.reject_payload(_cand(basis="self_intro")) == {"confidence": "rejected"}


def test_youtube_cue_url_matches_source_pane_pattern():
    assert cs.youtube_cue_url("abc123", 412) == "https://www.youtube.com/watch?v=abc123&t=412s"


# --- excerpt selection ------------------------------------------------------------


def _turn(label, start, end, words):
    return {
        "cluster_label": label,
        "start_seconds": start,
        "end_seconds": end,
        "words": [{"word": w} for w in words],
    }


def test_cluster_excerpts_longest_first_with_own_cue():
    turns = [
        _turn("SPEAKER_00", 10, 12, ["short", "one"]),
        _turn("SPEAKER_00", 30, 40, ["a", "much", "longer", "excerpt", "here"]),
        _turn("SPEAKER_01", 5, 9, ["other", "speaker", "ignored"]),
        _turn("SPEAKER_00", 50, 51, []),  # empty -> dropped
    ]
    got = cs.cluster_excerpts(turns, "SPEAKER_00", limit=3)
    assert got == [
        (30, "a much longer excerpt here"),
        (10, "short one"),
    ]  # longest first, own start


def test_cluster_excerpts_respects_limit():
    turns = [_turn("SPEAKER_00", i, i + 1, ["w"] * (5 - i)) for i in range(4)]
    got = cs.cluster_excerpts(turns, "SPEAKER_00", limit=2)
    assert len(got) == 2
    assert got[0][0] == 0 and got[1][0] == 1  # the two wordiest turns, longest first


# --- candidate ordering (minimize operator time to real recall) -------------------


def test_order_candidates_puts_least_confirmed_official_first():
    # Official A (person 1) has zero confirmed meetings; official B (person 2) already has two.
    a = _cand(identity_id=1, person_id=1, official_name="A", meeting_key="a1")
    b = _cand(identity_id=2, person_id=2, official_name="B", meeting_key="b1")
    confirmed = {2: {"bx", "by"}}  # B already covered in two meetings
    order = cs.order_candidates([b, a], confirmed)
    assert [c.person_id for c in order] == [1, 2]  # fewest-confirmed official leads


def test_order_candidates_prefers_distinct_meetings_then_speech():
    # One official, three candidate clusters: two in meeting m1 (30s, 90s) and one in m2 (50s).
    # Distinct-meeting coverage comes first: the top cluster of each fresh meeting (m1's 90s,
    # then m2's 50s), then the extra m1 cluster (30s).
    m1_big = _cand(identity_id=1, meeting_key="m1", seconds=90.0, cluster_label="SPEAKER_01")
    m1_small = _cand(identity_id=2, meeting_key="m1", seconds=30.0, cluster_label="SPEAKER_02")
    m2 = _cand(identity_id=3, meeting_key="m2", seconds=50.0, cluster_label="SPEAKER_03")
    order = cs.order_candidates([m1_small, m2, m1_big], {})
    assert [c.identity_id for c in order] == [1, 3, 2]  # m1-top, m2-top, then m1-extra


def test_order_candidates_deprioritizes_already_confirmed_meetings():
    # A candidate in a meeting the official is already confirmed in is least useful (that meeting
    # is covered) -> it sorts after a candidate from a fresh meeting even with less speech.
    fresh = _cand(identity_id=1, meeting_key="fresh", seconds=20.0)
    covered = _cand(identity_id=2, meeting_key="done", seconds=200.0)
    order = cs.order_candidates([covered, fresh], {1: {"done"}})
    assert [c.identity_id for c in order] == [1, 2]  # fresh meeting first despite less speech


# --- membership guard: only roster officials are confirmable (Option B scope) ------


class _FakeQuery:
    """Minimal PostgREST-shaped query that applies eq/in_ filters over canned rows."""

    def __init__(self, rows: list[dict]) -> None:
        self._rows = rows
        self._filters: list[tuple[str, str, object]] = []

    def select(self, _cols: str) -> _FakeQuery:
        return self

    def order(self, *_args, **_kwargs) -> _FakeQuery:
        return self

    def range(self, *_args, **_kwargs) -> _FakeQuery:
        return self

    def eq(self, col: str, val: object) -> _FakeQuery:
        self._filters.append(("eq", col, val))
        return self

    def in_(self, col: str, vals: list) -> _FakeQuery:
        self._filters.append(("in", col, list(vals)))
        return self

    def execute(self):
        from types import SimpleNamespace

        rows = self._rows
        for kind, col, val in self._filters:
            if kind == "eq":
                rows = [r for r in rows if r.get(col) == val]
            else:
                rows = [r for r in rows if r.get(col) in val]
        return SimpleNamespace(data=rows)


class _FakeClient:
    def __init__(self, tables: dict[str, list[dict]]) -> None:
        self._tables = tables

    def table(self, name: str) -> _FakeQuery:
        return _FakeQuery(self._tables.get(name, []))


def test_load_candidates_only_offers_roster_members():
    # Two publishable person subjects hypothesized on doc 5: subject 10 is a MEMBER of the body
    # (entity 100), subject 11 is a publishable non-member. Only the member is a candidate.
    docs_by_id = {
        5: {
            "id": 5,
            "video_id": "vid1",
            "entity_id": 100,
            "replaces_id": None,
            "meeting_title": "Regular Meeting",
            "meeting_date": "2025-01-01",
        }
    }
    subjects_by_id = {
        10: {"id": 10, "person_id": 1000, "publishable": True, "canonical_name": "Member Official"},
        11: {
            "id": 11,
            "person_id": 1001,
            "publishable": True,
            "canonical_name": "Public Nonmember",
        },
    }
    tables = {
        "speaker_identities": [
            {"id": 1, "document_id": 5, "cluster_label": "SPEAKER_00", "subject_id": 10,
             "confidence": "inferred_high", "basis": "rollcall"},
            {"id": 2, "document_id": 5, "cluster_label": "SPEAKER_01", "subject_id": 11,
             "confidence": "inferred_high", "basis": "rollcall"},
        ],
        "memberships": [{"subject_id": 10, "entity_id": 100}],  # only subject 10 is a member
        "diarization_turns": [
            {"cluster_label": "SPEAKER_00", "start_seconds": 10.0, "end_seconds": 40.0,
             "words": [{"word": w} for w in "the member speaks at some length here".split()]},
            {"cluster_label": "SPEAKER_01", "start_seconds": 50.0, "end_seconds": 80.0,
             "words": [{"word": w} for w in "the nonmember also speaks here".split()]},
        ],
    }  # fmt: skip
    candidates, _confirmed = cs._load_candidates(_FakeClient(tables), docs_by_id, subjects_by_id)
    assert [(c.subject_id, c.person_id, c.cluster_label) for c in candidates] == [
        (10, 1000, "SPEAKER_00")
    ]  # the non-member publishable person (subject 11) is never offered
