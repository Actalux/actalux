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
    roster_title=None,
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
        roster_title=roster_title,
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


def test_load_candidates_confirmed_voiceprint_row_is_not_coverage():
    # enrollment.py never enrolls a confirmed basis='voiceprint' row, so it is not real Gate-A
    # coverage. It must not seed confirmed_meetings, or the batch done-gate could skip an official
    # who still lacks enrollable meetings. A confirmed 'manual' row on the same official does count.
    docs_by_id = {
        5: {"id": 5, "video_id": "vidA", "entity_id": 100, "replaces_id": None,
            "meeting_title": "M1", "meeting_date": "2025-01-01"},
        6: {"id": 6, "video_id": "vidB", "entity_id": 100, "replaces_id": None,
            "meeting_title": "M2", "meeting_date": "2025-02-01"},
    }  # fmt: skip
    subjects_by_id = {
        10: {"id": 10, "person_id": 1000, "publishable": True, "canonical_name": "Member"},
    }
    tables = {
        "speaker_identities": [
            {"id": 1, "document_id": 5, "cluster_label": "SPEAKER_00", "subject_id": 10,
             "confidence": "confirmed", "basis": "voiceprint"},  # non-enrollable -> not coverage
            {"id": 2, "document_id": 6, "cluster_label": "SPEAKER_00", "subject_id": 10,
             "confidence": "confirmed", "basis": "manual"},  # enrollable -> coverage
        ],
        "memberships": [{"subject_id": 10, "entity_id": 100, "role": "Mayor"}],
        "diarization_turns": [],
    }  # fmt: skip
    _candidates, confirmed = cs._load_candidates(_FakeClient(tables), docs_by_id, subjects_by_id)
    assert confirmed[1000] == {"vidB"}  # only the manual-basis meeting, not the voiceprint one


# --- batch mode: input grammar ----------------------------------------------------


def _parsed(raw: str, listed_count: int = 5) -> tuple[str, tuple[int, ...], tuple[int, ...]] | None:
    """Flatten a parse result to (action, confirm, reject) for terse asserts; None stays None."""
    d = cs.parse_batch_input(raw, listed_count)
    return None if d is None else (d.action, d.confirm, d.reject)


def test_parse_batch_input_confirm_numbers():
    assert _parsed("1,3") == ("apply", (1, 3), ())


def test_parse_batch_input_confirm_all_expands_to_every_position():
    assert _parsed("a", 4) == ("apply", (1, 2, 3, 4), ())


def test_parse_batch_input_reject_clause_only():
    assert _parsed("n 2") == ("apply", (), (2,))


def test_parse_batch_input_confirm_and_reject_combined():
    assert _parsed("1,3 n 2") == ("apply", (1, 3), (2,))


def test_parse_batch_input_reject_number_adjacent_to_n():
    assert _parsed("n2") == ("apply", (), (2,))  # 'n2' without a space still parses


def test_parse_batch_input_comma_and_space_separators_both_work():
    assert _parsed("1, 3") == ("apply", (1, 3), ())


def test_parse_batch_input_dedupes_and_sorts_positions():
    assert _parsed("3,1,1") == ("apply", (1, 3), ())


def test_parse_batch_input_skip_variants():
    assert _parsed("") == ("skip", (), ())
    assert _parsed("   ") == ("skip", (), ())
    assert _parsed("s") == ("skip", (), ())


def test_parse_batch_input_quit():
    assert _parsed("q") == ("quit", (), ())


def test_parse_batch_input_out_of_range_is_garbage():
    assert _parsed("9", 3) is None  # position beyond the listed rows
    assert _parsed("0", 3) is None  # positions are 1-based
    assert _parsed("n 9", 3) is None


def test_parse_batch_input_overlap_is_garbage():
    # Confirming and rejecting the same row is contradictory -> re-prompt, never a wrong write.
    assert _parsed("1 n 1", 3) is None


def test_parse_batch_input_all_cannot_combine_with_reject():
    # 'a' already confirms every row, so an 'n' clause overlaps it entirely -> garbage.
    assert _parsed("a n 2", 3) is None


def test_parse_batch_input_garbage_returns_none():
    assert _parsed("xyz", 3) is None
    assert _parsed("1 z", 3) is None
    assert _parsed("n", 3) is None  # 'n' with no numbers to reject


# --- batch mode: write path (recording client) ------------------------------------


class _RecordingWrite:
    """Captures one ``update(payload).eq(col, val).execute()`` chain to a shared sink."""

    def __init__(self, sink: list[tuple[dict, tuple[str, object]]]) -> None:
        self._sink = sink
        self._payload: dict | None = None
        self._eq: tuple[str, object] | None = None

    def update(self, payload: dict) -> _RecordingWrite:
        self._payload = payload
        return self

    def eq(self, col: str, val: object) -> _RecordingWrite:
        self._eq = (col, val)
        return self

    def execute(self) -> None:
        self._sink.append((self._payload, self._eq))


class _RecordingClient:
    def __init__(self) -> None:
        self.writes: list[tuple[dict, tuple[str, object]]] = []

    def table(self, _name: str) -> _RecordingWrite:
        return _RecordingWrite(self.writes)


def _group(listed, *, person_id=1, official_name="A", roster_title="Alderman", total=None):
    return cs.OfficialGroup(
        person_id=person_id,
        official_name=official_name,
        roster_title=roster_title,
        listed=tuple(listed),
        total=len(listed) if total is None else total,
    )


def test_apply_batch_decision_maps_display_positions_to_the_right_rows():
    # The catastrophic bug is an off-by-one between the numbers shown and the row ids written.
    # Displayed 1/2/3 -> listed[0]/[1]/[2] -> identity_ids 101/102/103. Confirm 1,3 / reject 2.
    listed = [
        _cand(identity_id=101, cluster_label="SPEAKER_00"),
        _cand(identity_id=102, cluster_label="SPEAKER_01"),
        _cand(identity_id=103, cluster_label="SPEAKER_02"),
    ]
    client = _RecordingClient()
    tally = cs._apply_batch_decision(
        client, _group(listed), cs.BatchInput("apply", confirm=(1, 3), reject=(2,)), {}
    )
    assert client.writes == [
        ({"confidence": "confirmed"}, ("id", 101)),  # position 1
        ({"confidence": "confirmed"}, ("id", 103)),  # position 3
        ({"confidence": "rejected"}, ("id", 102)),  # position 2
    ]
    assert (tally.confirmed, tally.denied, tally.skipped) == (2, 1, 0)


def test_batch_confirm_write_matches_clip_confirm_write():
    # Same decision, same row -> byte-identical payload + target as the clip-by-clip path, including
    # the voiceprint->manual basis rewrite.
    cand = _cand(identity_id=55, basis="voiceprint")
    clip = _RecordingClient()
    cs._apply_decision(clip, cand, "y")
    batch = _RecordingClient()
    cs._apply_batch_decision(batch, _group([cand]), cs.BatchInput("apply", confirm=(1,)), {})
    assert clip.writes == batch.writes
    assert clip.writes == [({"confidence": "confirmed", "basis": "manual"}, ("id", 55))]


def test_batch_reject_write_matches_clip_reject_write():
    cand = _cand(identity_id=77, basis="self_intro")
    clip = _RecordingClient()
    cs._apply_decision(clip, cand, "n")
    batch = _RecordingClient()
    cs._apply_batch_decision(batch, _group([cand]), cs.BatchInput("apply", reject=(1,)), {})
    assert clip.writes == batch.writes == [({"confidence": "rejected"}, ("id", 77))]


def test_apply_batch_decision_empty_writes_nothing_and_counts_all_skipped():
    listed = [_cand(identity_id=1), _cand(identity_id=2, cluster_label="SPEAKER_01")]
    client = _RecordingClient()
    tally = cs._apply_batch_decision(client, _group(listed), cs.BatchInput("apply"), {})
    assert client.writes == []
    assert (tally.confirmed, tally.denied, tally.skipped) == (0, 0, 2)


def test_apply_batch_decision_partial_counts_remainder_as_skipped():
    listed = [
        _cand(identity_id=1),
        _cand(identity_id=2, cluster_label="SPEAKER_01"),
        _cand(identity_id=3, cluster_label="SPEAKER_02"),
    ]
    client = _RecordingClient()
    tally = cs._apply_batch_decision(
        client, _group(listed), cs.BatchInput("apply", confirm=(1,)), {}
    )
    assert (tally.confirmed, tally.denied, tally.skipped) == (1, 0, 2)


def test_apply_batch_decision_confirm_updates_live_coverage():
    cand = _cand(identity_id=1, person_id=7, meeting_key="m1")
    live: dict[int, set[str]] = {}
    cs._apply_batch_decision(
        _RecordingClient(), _group([cand], person_id=7), cs.BatchInput("apply", confirm=(1,)), live
    )
    assert live == {7: {"m1"}}


# --- batch mode: grouping ---------------------------------------------------------


def test_build_official_groups_orders_least_confirmed_first_and_skips_done():
    # Official 1: 0 confirmed meetings. Official 2: already 3 distinct confirmed meetings (done,
    # dropped). Official 3: 1 confirmed meeting. Result: [1, 3] (fewest-confirmed first, 2 gone).
    a = _cand(identity_id=1, person_id=1, official_name="A", meeting_key="a1")
    b = _cand(identity_id=2, person_id=2, official_name="B", meeting_key="b1")
    c = _cand(identity_id=3, person_id=3, official_name="C", meeting_key="c1")
    confirmed = {2: {"x", "y", "z"}, 3: {"p"}}
    groups = cs.build_official_groups([a, b, c], confirmed)
    assert [g.person_id for g in groups] == [1, 3]


def test_build_official_groups_caps_screen_and_keeps_top_speech():
    # Ten clusters for one official; only the eight with the most speech list; total keeps all.
    cands = [
        _cand(
            identity_id=i,
            person_id=1,
            meeting_key=f"m{i}",
            seconds=float(i),
            cluster_label=f"SPEAKER_{i:02d}",
        )
        for i in range(1, 11)
    ]
    groups = cs.build_official_groups(cands, {}, cap=8)
    assert len(groups) == 1
    assert groups[0].total == 10
    assert [c.identity_id for c in groups[0].listed] == [10, 9, 8, 7, 6, 5, 4, 3]


def test_build_official_groups_carries_name_and_roster_title():
    cand = _cand(identity_id=1, person_id=1, official_name="Jane Doe", roster_title="Mayor")
    groups = cs.build_official_groups([cand], {})
    assert (groups[0].official_name, groups[0].roster_title) == ("Jane Doe", "Mayor")


# --- batch mode: interactive session (input monkeypatched) ------------------------


def _feed(monkeypatch, responses):
    """Feed scripted responses to the batch prompt's ``input()`` calls."""
    it = iter(responses)
    monkeypatch.setattr("builtins.input", lambda _prompt="": next(it))


def test_prompt_batch_reprompts_until_valid(monkeypatch):
    _feed(monkeypatch, ["huh?", "9", "1"])  # garbage, out-of-range, then a valid confirm
    decision = cs._prompt_batch(3)
    assert (decision.action, decision.confirm, decision.reject) == ("apply", (1,), ())


def test_run_batch_session_skip_writes_nothing(monkeypatch):
    _feed(monkeypatch, ["s"])
    client = _RecordingClient()
    tallies, enablement = cs._run_batch_session(
        client, [_group([_cand(identity_id=1)], official_name="A")], {}
    )
    assert client.writes == []
    assert tallies["A"].skipped == 1
    assert enablement == [("A", 0, False)]


def test_run_batch_session_confirm_all_marks_newly_enabled(monkeypatch):
    # One official, two clusters in two meetings; 'a' confirms both, crossing the >=2-meeting bar.
    c1 = _cand(identity_id=11, person_id=5, official_name="E", meeting_key="m1", seconds=90.0)
    c2 = _cand(
        identity_id=12,
        person_id=5,
        official_name="E",
        meeting_key="m2",
        seconds=80.0,
        cluster_label="SPEAKER_01",
    )
    groups = cs.build_official_groups([c1, c2], {})
    _feed(monkeypatch, ["a"])
    client = _RecordingClient()
    tallies, enablement = cs._run_batch_session(client, groups, {})
    assert {w[1][1] for w in client.writes} == {11, 12}  # both rows written
    assert tallies["E"].confirmed == 2
    assert enablement == [("E", 2, True)]  # newly crossed the enablement bar


def test_run_batch_session_quit_stops_before_writing(monkeypatch):
    g1 = _group([_cand(identity_id=1)], person_id=1, official_name="A")
    g2 = _group([_cand(identity_id=2, cluster_label="SPEAKER_01")], person_id=2, official_name="B")
    _feed(monkeypatch, ["q"])
    client = _RecordingClient()
    tallies, enablement = cs._run_batch_session(client, [g1, g2], {})
    assert client.writes == []
    assert tallies == {}
    assert enablement == []
