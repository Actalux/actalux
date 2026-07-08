"""Unit tests for the blind speaker-ID sheet: selection logic, no-leak HTML, answer parser.

No DB — the pure selection/render/parse helpers are exercised directly with synthetic inputs.
The load-bearing invariant is that the emitted HTML never reveals the machine's per-clip
hypothesis (that would un-blind the sheet), so that gets the most scrutiny.
"""

from __future__ import annotations

import re

import pytest

import scripts.build_id_sheet as bs

# --- fixtures -----------------------------------------------------------------------


def _cand(document_id, cluster_label, video_id, seconds, subject_id=None):
    return bs._ClusterCand(document_id, cluster_label, video_id, seconds, subject_id)


def _clip(
    subject_id, *, document_id=1, cluster_label="SPEAKER_00", video_id="vid", kind="hypothesis"
):
    return bs.Clip(
        document_id=document_id,
        cluster_label=cluster_label,
        video_id=video_id,
        start_seconds=10,
        end_seconds=22,
        kind=kind,
        hypothesis_subject_id=subject_id,
    )


def _turn(cluster_label, start, end):
    return {"cluster_label": cluster_label, "start_seconds": start, "end_seconds": end}


# --- seed + window ------------------------------------------------------------------


def test_seed_from_path_is_stable_and_path_specific():
    assert bs.seed_from_path("mo/clayton/schools") == bs.seed_from_path("mo/clayton/schools")
    assert bs.seed_from_path("mo/clayton/schools") != bs.seed_from_path("mo/clayton/council")


def test_longest_turn_window_picks_longest_turn():
    turns = [
        _turn("SPEAKER_00", 5.0, 9.0),  # 4s
        _turn("SPEAKER_00", 100.4, 140.9),  # 40.5s — longest
        _turn("SPEAKER_01", 0.0, 500.0),  # different cluster, ignored
    ]
    assert bs.longest_turn_window(turns, "SPEAKER_00") == (100, 112)  # int start, +12s


def test_longest_turn_window_none_when_cluster_absent():
    assert bs.longest_turn_window([_turn("SPEAKER_09", 0.0, 10.0)], "SPEAKER_00") is None


# --- hypothesis selection: distinct-meeting constraint ------------------------------


def test_pick_subject_reps_one_per_meeting_prefers_more_speech():
    cands = [
        _cand(1, "SPEAKER_00", "vidA", 200.0),
        _cand(1, "SPEAKER_01", "vidA", 90.0),  # same meeting, less speech -> not the rep
        _cand(2, "SPEAKER_00", "vidB", 150.0),
        _cand(3, "SPEAKER_00", "vidC", 120.0),
    ]
    reps = bs.pick_subject_meeting_reps(cands)
    assert [(r.video_id, r.cluster_label) for r in reps] == [
        ("vidA", "SPEAKER_00"),
        ("vidB", "SPEAKER_00"),
        ("vidC", "SPEAKER_00"),
    ]
    assert len({r.video_id for r in reps}) == len(reps)  # all distinct meetings


def test_pick_subject_reps_caps_at_three_meetings():
    cands = [_cand(i, "SPEAKER_00", f"vid{i}", 100.0 + i) for i in range(5)]
    reps = bs.pick_subject_meeting_reps(cands)
    assert len(reps) == bs.HYP_MAX_CLIPS_PER_SUBJECT == 3
    # highest speech first (i=4 -> 104s, i=3 -> 103s, i=2 -> 102s)
    assert [r.video_id for r in reps] == ["vid4", "vid3", "vid2"]


def test_pick_subject_reps_requires_two_distinct_meetings():
    # Two clusters but the SAME meeting -> only one distinct meeting -> not eligible.
    cands = [_cand(1, "SPEAKER_00", "vidA", 300.0), _cand(1, "SPEAKER_01", "vidA", 200.0)]
    assert bs.pick_subject_meeting_reps(cands) == []


def test_pick_subject_reps_prefers_sixty_second_clusters_first():
    cands = [
        _cand(1, "SPEAKER_00", "vidA", 500.0),  # >=60 (rank by speech)
        _cand(2, "SPEAKER_00", "vidB", 30.0),  # <60 -> deprioritized despite being present
        _cand(3, "SPEAKER_00", "vidC", 65.0),  # >=60
    ]
    reps = bs.pick_subject_meeting_reps(cands)
    # both >=60 meetings come before the <60 one
    assert [r.video_id for r in reps] == ["vidA", "vidC", "vidB"]


# --- unknown selection --------------------------------------------------------------


def test_pick_unknown_reps_spreads_across_meetings_then_caps():
    cands = [
        _cand(1, "SPEAKER_00", "vidA", 500.0),
        _cand(1, "SPEAKER_01", "vidA", 480.0),  # same meeting as the biggest -> deferred
        _cand(2, "SPEAKER_00", "vidB", 300.0),
        _cand(3, "SPEAKER_00", "vidC", 200.0),
    ]
    reps = bs.pick_unknown_reps(cands, limit=3)
    assert [r.video_id for r in reps] == ["vidA", "vidB", "vidC"]  # distinct meetings first


def test_pick_unknown_reps_backfills_when_meetings_exhausted():
    cands = [
        _cand(1, "SPEAKER_00", "vidA", 500.0),
        _cand(1, "SPEAKER_01", "vidA", 480.0),
    ]
    reps = bs.pick_unknown_reps(cands, limit=5)
    assert len(reps) == 2  # only two clusters exist; backfill can't invent more
    assert {r.cluster_label for r in reps} == {"SPEAKER_00", "SPEAKER_01"}


# --- assembly caps ------------------------------------------------------------------


def test_assemble_caps_unknown_and_admits_whole_subject_sets():
    # budget: total 6, unknown 2. hyp budget = 4.
    hyp_sets = [
        [_clip(10), _clip(10), _clip(10)],  # 3 -> fits (0+3<=4)
        [_clip(20), _clip(20)],  # 2 -> 3+2=5 > 4 -> skipped whole
        [_clip(30)],  # 1 -> 3+1=4 <= 4 -> admitted
    ]
    unknown = [_clip(None, kind="unknown") for _ in range(4)]  # 4 -> capped to 2
    out = bs.assemble_clips(hyp_sets, unknown, max_total=6, max_unknown=2)
    assert len(out) == 6
    assert sum(1 for c in out if c.kind == "unknown") == 2
    hyp_subjects = [c.hypothesis_subject_id for c in out if c.kind == "hypothesis"]
    assert hyp_subjects == [10, 10, 10, 30]  # set {20} skipped, never split


def test_assemble_never_splits_a_subject_set():
    hyp_sets = [[_clip(10), _clip(10), _clip(10)]]  # 3 clips, budget only 2
    out = bs.assemble_clips(hyp_sets, [], max_total=2, max_unknown=8)
    assert out == []  # the set doesn't fit whole, so none of it is admitted


# --- deterministic ordering + no-adjacency ------------------------------------------


def test_deterministic_order_is_stable_for_a_seed():
    clips = [_clip(1), _clip(2), _clip(3), _clip(None, kind="unknown")]
    seed = 12345
    first = bs.deterministic_order(clips, seed)
    second = bs.deterministic_order(clips, seed)
    assert [(c.hypothesis_subject_id, c.cluster_label) for c in first] == [
        (c.hypothesis_subject_id, c.cluster_label) for c in second
    ]


def test_deterministic_order_never_adjacent_same_subject():
    # 3 of subject A, plus B, C, and one unknown -> spreadable (3 fillers for 3 A's).
    clips = [
        _clip(1, cluster_label="a1"),
        _clip(1, cluster_label="a2"),
        _clip(1, cluster_label="a3"),
        _clip(2, cluster_label="b1"),
        _clip(3, cluster_label="c1"),
        _clip(None, cluster_label="u1", kind="unknown"),
    ]
    for seed in range(50):  # every seed must satisfy the invariant
        ordered = bs.deterministic_order(clips, seed)
        for a, b in zip(ordered, ordered[1:]):
            if a.hypothesis_subject_id is not None:
                assert a.hypothesis_subject_id != b.hypothesis_subject_id


# --- HTML no-hypothesis-leak invariant ----------------------------------------------


def _roster():
    return [
        bs.RosterEntry(900001, "zaphod-beeblebrox", "Zaphod Beeblebrox", 5001, True),
        bs.RosterEntry(900002, "trillian-astra", "Trillian Astra", 5002, True),
        bs.RosterEntry(900003, "ford-prefect", "Ford Prefect", 5003, True),
    ]


def test_render_html_hides_the_per_clip_hypothesis():
    roster = _roster()
    # Each clip's hypothesis points at a specific roster subject; unknown carries None.
    clips = [
        bs.Clip(11, "SPEAKER_00", "vidAAA", 30, 42, "hypothesis", 900001),
        bs.Clip(12, "SPEAKER_01", "vidBBB", 60, 72, "hypothesis", 900003),
        bs.Clip(13, "SPEAKER_02", "vidCCC", 90, 102, "unknown", None),
    ]
    doc = bs.render_html(clips, roster, title="Sheet", subtitle="listen")

    # No HTML comment could smuggle a hypothesis.
    assert "<!--" not in doc
    # The ONLY data attribute is data-clip (the clip id, never a subject).
    assert set(re.findall(r"data-(\w+)=", doc)) == {"clip"}
    # Every data-clip value is a clipNN id, nothing else.
    for val in re.findall(r'data-clip="([^"]*)"', doc):
        assert re.fullmatch(r"clip\d\d", val), val

    # Strip the dropdowns (the ONE allowed home for names/slugs); nothing must remain.
    without_selects = re.sub(r"<select\b.*?</select>", "", doc, flags=re.DOTALL)
    for r in roster:
        assert r.canonical_name not in without_selects, r.canonical_name
        assert r.slug not in without_selects, r.slug
        assert str(r.subject_id) not in without_selects, r.subject_id
    # The subject ids never appear even inside the dropdown (values are slugs, labels names).
    for r in roster:
        assert str(r.subject_id) not in doc


def test_render_html_dropdown_is_identical_for_every_clip():
    roster = _roster()
    clips = [
        bs.Clip(11, "SPEAKER_00", "vidAAA", 30, 42, "hypothesis", 900001),
        bs.Clip(12, "SPEAKER_01", "vidBBB", 60, 72, "unknown", None),
    ]
    doc = bs.render_html(clips, roster, title="Sheet", subtitle="listen")
    selects = re.findall(r"<select\b.*?</select>", doc, flags=re.DOTALL)
    assert len(selects) == 2
    # Same option markup on both -> the dropdown reveals nothing clip-specific.
    opts = [re.sub(r'\sdata-clip="[^"]*"', "", s) for s in selects]
    assert opts[0] == opts[1]
    # All three officials + the three fixed choices are present.
    for r in roster:
        assert f'value="{r.slug}"' in selects[0]
    for value, _label in bs.FIXED_CHOICES:
        assert f'value="{value}"' in selects[0]


# --- answer-string parser -----------------------------------------------------------


def test_parse_answers_valid_tokens_and_clip_number_normalization():
    answers = bs.parse_answers("clip01=zaphod-beeblebrox\nclip7=citizen clip12=unsure")
    assert answers == {
        "clip01": "zaphod-beeblebrox",
        "clip07": "citizen",  # clip7 normalized to clip07
        "clip12": "unsure",
    }


def test_parse_answers_accepts_fixed_choices():
    answers = bs.parse_answers("clip01=other clip02=citizen clip03=unsure")
    assert set(answers.values()) == {"other", "citizen", "unsure"}


def test_parse_answers_rejects_junk():
    with pytest.raises(ValueError, match="unparseable"):
        bs.parse_answers("clip01=jane hello-world clip02=citizen")


def test_parse_answers_rejects_bad_token_shape():
    with pytest.raises(ValueError, match="unparseable"):
        bs.parse_answers("clip=jane")  # no clip number
    with pytest.raises(ValueError, match="unparseable"):
        bs.parse_answers("clip01=UPPERCASE")  # slugs are lowercase


def test_parse_answers_rejects_duplicate_clip_ids():
    with pytest.raises(ValueError, match="duplicate"):
        bs.parse_answers("clip01=jane-doe clip1=john-roe")  # clip01 and clip1 collide


# --- write-payload semantics (mirrors confirm_speaker) ------------------------------


def test_confirm_update_keeps_basis_but_rewrites_voiceprint():
    assert bs.confirm_update("rollcall") == {"confidence": "confirmed"}
    assert bs.confirm_update("discourse") == {"confidence": "confirmed"}
    assert bs.confirm_update("voiceprint") == {"confidence": "confirmed", "basis": "manual"}
    assert bs.confirm_update(None) == {"confidence": "confirmed"}


def test_denovo_confirm_row_is_a_manual_confirmed_row():
    row = bs.denovo_confirm_row(42, "SPEAKER_03", 900001)
    assert row == {
        "document_id": 42,
        "cluster_label": "SPEAKER_03",
        "subject_id": 900001,
        "confidence": "confirmed",
        "basis": "manual",
    }


def test_agreement_label():
    assert bs._agreement_label(7, None) == "net-new"
    assert bs._agreement_label(7, 7) == "agree"
    assert bs._agreement_label(7, 8) == "disagree"


# --- integration: select_clips end to end (synthetic turns) -------------------------


def test_select_clips_end_to_end():
    # Two hypothesis subjects (each across 2 meetings) + some unknown clusters.
    turns_by_doc = {
        1: [_turn("SPEAKER_00", 10, 130), _turn("SPEAKER_09", 200, 260)],
        2: [_turn("SPEAKER_00", 10, 100), _turn("SPEAKER_08", 300, 380)],
        3: [_turn("SPEAKER_01", 5, 120)],
        4: [_turn("SPEAKER_01", 5, 90)],
    }
    hyp_by_subject = {
        900001: [
            _cand(1, "SPEAKER_00", "vid1", 120.0, 900001),
            _cand(2, "SPEAKER_00", "vid2", 90.0, 900001),
        ],
        900002: [
            _cand(3, "SPEAKER_01", "vid3", 115.0, 900002),
            _cand(4, "SPEAKER_01", "vid4", 85.0, 900002),
        ],
    }
    unknown = [
        _cand(1, "SPEAKER_09", "vid1", 60.0),
        _cand(2, "SPEAKER_08", "vid2", 80.0),
    ]
    clips = bs.select_clips(hyp_by_subject, unknown, turns_by_doc)
    hyp = [c for c in clips if c.kind == "hypothesis"]
    unk = [c for c in clips if c.kind == "unknown"]
    assert len(hyp) == 4  # 2 subjects x 2 meetings
    assert {c.hypothesis_subject_id for c in hyp} == {900001, 900002}
    assert len(unk) == 2
    # Every hypothesis clip's window is its cluster's longest turn (int start, +12s).
    starts = {(c.document_id, c.cluster_label): (c.start_seconds, c.end_seconds) for c in hyp}
    assert starts[(1, "SPEAKER_00")] == (10, 22)


# --- guarded write path (fake Supabase client, no DB) -------------------------------


class _FakeExec:
    def __init__(self, data):
        self.data = data


class _FakeQuery:
    """Records insert/update payloads; returns the client's preset row on a select."""

    def __init__(self, client):
        self._client = client
        self._op = "select"
        self._payload = None

    def select(self, *a, **k):
        self._op = "select"
        return self

    def insert(self, payload):
        self._op, self._payload = "insert", payload
        return self

    def update(self, payload):
        self._op, self._payload = "update", payload
        return self

    def eq(self, *a, **k):
        return self

    def limit(self, *a, **k):
        return self

    def execute(self):
        if self._op == "select":
            return _FakeExec(list(self._client.existing))
        self._client.writes.append((self._op, self._payload))
        return _FakeExec([])


class _FakeClient:
    def __init__(self, existing=None):
        self.existing = existing or []  # rows _existing_identity should see
        self.writes = []  # (op, payload) tuples recorded on insert/update

    def table(self, _name):
        return _FakeQuery(self)


_TARGET = bs.RosterEntry(900001, "zaphod-beeblebrox", "Zaphod Beeblebrox", 5001, True)
_ENTRY = {"document_id": 7, "cluster_label": "SPEAKER_02", "hypothesis_subject_id": 900001}


def _apply_one_person(existing, *, write=True, target=_TARGET, entry=_ENTRY):
    client = _FakeClient(existing)
    tally, flags = bs.ApplyTally(), []
    bs._apply_person(client, "clip01", entry, target, tally, flags, write=write)
    return client, tally, flags


def test_apply_person_inserts_denovo_when_no_row():
    client, tally, flags = _apply_one_person([])
    assert tally.inserted == 1 and not flags
    assert client.writes == [("insert", bs.denovo_confirm_row(7, "SPEAKER_02", 900001))]


def test_apply_person_confirms_matching_inferred_row_in_place():
    existing = [
        {"id": 3, "subject_id": 900001, "confidence": "inferred_medium", "basis": "discourse"}
    ]
    client, tally, flags = _apply_one_person(existing)
    assert tally.confirmed_in_place == 1 and not flags
    assert client.writes == [("update", {"confidence": "confirmed"})]


def test_apply_person_is_idempotent_on_already_confirmed():
    existing = [{"id": 3, "subject_id": 900001, "confidence": "confirmed", "basis": "manual"}]
    client, tally, flags = _apply_one_person(existing)
    assert tally.already_confirmed == 1
    assert client.writes == []  # no-op -> re-applying the same answers writes nothing


def test_apply_person_flags_disagreement_and_never_writes():
    # The row names a DIFFERENT official than the operator picked -> never confirmed.
    existing = [{"id": 3, "subject_id": 555, "confidence": "inferred_high", "basis": "rollcall"}]
    client, tally, flags = _apply_one_person(existing)
    assert tally.disagreement == 1
    assert client.writes == []
    assert "DISAGREEMENT" in flags[0]


def test_apply_person_flags_conflict_on_rejected_row_and_never_writes():
    # A prior human denial (rejected) must not be flipped to confirmed by a matching pick.
    existing = [{"id": 3, "subject_id": 900001, "confidence": "rejected", "basis": "discourse"}]
    client, tally, flags = _apply_one_person(existing)
    assert tally.conflict == 1
    assert client.writes == []
    assert "REJECTED" in flags[0]


def test_apply_person_dry_run_writes_nothing_but_tallies():
    client, tally, _ = _apply_one_person([], write=False)
    assert tally.inserted == 1
    assert client.writes == []


def _apply_one_citizen(existing, *, write=True):
    client = _FakeClient(existing)
    tally, flags = bs.ApplyTally(), []
    bs._apply_citizen(client, "clip01", _ENTRY, tally, flags, write=write)
    return client, tally, flags


def test_apply_citizen_rejects_an_inferred_official_row():
    existing = [{"id": 3, "subject_id": 555, "confidence": "inferred_medium", "basis": "discourse"}]
    client, tally, flags = _apply_one_citizen(existing)
    assert tally.rejected == 1 and not flags
    assert client.writes == [("update", {"confidence": "rejected"})]


def test_apply_citizen_on_no_row_is_a_noop_already_anonymous():
    client, tally, flags = _apply_one_citizen([])
    assert tally.citizen_anonymous == 1
    assert client.writes == []


def test_apply_citizen_conflicts_with_a_confirmed_row():
    existing = [{"id": 3, "subject_id": 555, "confidence": "confirmed", "basis": "manual"}]
    client, tally, flags = _apply_one_citizen(existing)
    assert tally.conflict == 1
    assert client.writes == []
    assert "CONFLICT" in flags[0]


def test_apply_citizen_is_idempotent_on_already_rejected():
    existing = [{"id": 3, "subject_id": 555, "confidence": "rejected", "basis": "discourse"}]
    client, tally, flags = _apply_one_citizen(existing)
    assert tally.already_rejected == 1
    assert client.writes == []
