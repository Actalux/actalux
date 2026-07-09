"""Unit tests for recalibration helpers: negative selection + privacy-safe reporting.

Also covers the dual-embedder A/B plumbing (WS4): per-model Sample separation, primary-only
persistence, and the report['ab'] shape — all exercised through stubbed embed results, since the
Modal GPU boundary cannot run locally.
"""

from __future__ import annotations

from types import SimpleNamespace

import pytest

import scripts.recalibrate_voiceprints as rc
from actalux.diarization.enrollment import EnrollableCluster
from actalux.diarization.matching import CURVE_PRECISION_BARS, Metrics, Sample
from actalux.errors import ActaluxError


def _turn(label, start, end):
    return {"cluster_label": label, "start_seconds": start, "end_seconds": end}


# --- dual-embedder A/B (WS4) ------------------------------------------------------------------

# Six orthogonal unit vectors so the primary and alternate embedders can be told apart by value:
# officials/negatives get distinct directions per model, and pooling two identical turns returns
# the same unit vector, so a persisted vector is exactly one of these tuples.
_E = [tuple(1.0 if j == i else 0.0 for j in range(6)) for i in range(6)]
_A, _B, _C = _E[0], _E[1], _E[2]  # primary: official 1, official 2, negative
_P, _Q, _R = _E[3], _E[4], _E[5]  # alternate: official 1, official 2, negative


def _turns(vec, *, n=2, secs=30.0):
    """A cluster's per-turn ``(vector, seconds)`` list; >=2 turns clears POOL_PARAMS min core."""
    return [(tuple(vec), secs) for _ in range(n)]


def _ec(person_id, label, name, meeting, *, confidence="inferred_high", basis="rollcall"):
    return EnrollableCluster(
        person_id=person_id,
        source_subject_id=10 * person_id,
        source_identity_id=1000 * person_id + meeting,
        document_id=meeting,
        cluster_label=label,
        source_basis=basis,
        canonical_name=name,
        confidence=confidence,
    )


def test_build_meeting_samples_single_model_officials_then_negatives():
    # A default single-embedder meeting: officials pooled first (cluster order), then negatives;
    # confidence tier is carried; pooled_officials mirrors the officials, negatives excluded.
    ec1 = _ec(1, "SPEAKER_00", "Alice", 1, confidence="confirmed")
    ec2 = _ec(2, "SPEAKER_01", "Bob", 1)
    tbm = {"wp": {"SPEAKER_00": _turns(_A), "SPEAKER_01": _turns(_B), "SPEAKER_09": _turns(_C)}}
    per_model, pooled_officials = rc.build_meeting_samples(
        tbm, [ec1, ec2], ["SPEAKER_09"], "vid1", min_seconds=10.0, primary_model="wp"
    )
    assert [s.person_id for s in per_model["wp"]] == [1, 2, None]
    assert (per_model["wp"][0].confidence, per_model["wp"][1].confidence) == (
        "confirmed",
        "inferred_high",
    )
    assert [ec for ec, _ in pooled_officials] == [ec1, ec2]  # negatives never pooled for persist


def test_build_meeting_samples_separates_models_and_pools_primary_only():
    # Two embedders over the same spans: each model gets its own Sample vectors, but the
    # persistable pooled_officials come from the PRIMARY model alone — no alternate vector enrolls.
    ec = _ec(1, "SPEAKER_00", "Alice", 1)
    tbm = {
        "wp": {"SPEAKER_00": _turns(_A), "SPEAKER_09": _turns(_C)},
        "ecapa": {"SPEAKER_00": _turns(_P), "SPEAKER_09": _turns(_R)},
    }
    per_model, pooled_officials = rc.build_meeting_samples(
        tbm, [ec], ["SPEAKER_09"], "vid1", min_seconds=10.0, primary_model="wp"
    )
    assert {s.person_id for s in per_model["wp"]} == {1, None}
    assert {s.person_id for s in per_model["ecapa"]} == {1, None}
    prim_off = next(s for s in per_model["wp"] if s.person_id == 1)
    alt_off = next(s for s in per_model["ecapa"] if s.person_id == 1)
    assert prim_off.embedding == _A and alt_off.embedding == _P  # embedder isolated
    assert len(pooled_officials) == 1
    (poff_ec, poff_pooled) = pooled_officials[0]
    assert poff_ec is ec and poff_pooled.vector == _A  # primary vector, never the alternate _P
    alt_vectors = {s.embedding for s in per_model["ecapa"]}
    assert poff_pooled.vector not in alt_vectors


def test_build_meeting_samples_drops_short_clusters():
    # A cluster whose pooled speech is under min_seconds is not sampled (Gate B / floor), for every
    # model — so a too-short official is a miss, not a silent short-sample enrollment.
    ec = _ec(1, "SPEAKER_00", "Alice", 1)
    tbm = {"wp": {"SPEAKER_00": _turns(_A, secs=2.0)}}  # 2 turns * 2s = 4s < 10
    per_model, pooled_officials = rc.build_meeting_samples(
        tbm, [ec], [], "vid1", min_seconds=10.0, primary_model="wp"
    )
    assert per_model["wp"] == [] and pooled_officials == []


def test_ab_report_empty_when_only_primary():
    # A single-model run has no alternate, so there is no ab block at all -> the persisted report is
    # byte-identical to the pre-A/B path (regression guard).
    samples = [Sample(1, "m1", _A), Sample(1, "m2", _A)]
    ab = rc._ab_report(
        {"wp": samples}, primary_model="wp", precision_bar=0.9, curve_bars=(0.8, 0.9, 0.98)
    )
    assert ab == {}


def test_ab_report_shape_for_alternate_model():
    # Clean separation so the alternate harness produces a real nested verdict; each official has
    # two evidence families across four meetings (consensus-eligible even under LOMO holdout). The
    # report carries the nested summary + curve + n_enabled + recall split, keyed by model, primary
    # excluded.
    def _fam(person, mk, vec):
        return Sample(person, f"m{mk}", vec, basis=("rollcall" if mk % 2 else "vote_anchor"))

    alt = [
        *(_fam(1, mk, _A) for mk in (1, 2, 3, 4)),
        *(_fam(2, mk, _B) for mk in (5, 6, 7, 8)),
        Sample(None, "m9", _C),
    ]
    curve_bars = tuple(sorted({0.9, *CURVE_PRECISION_BARS}))
    ab = rc._ab_report(
        {"wp": [Sample(1, "m1", _A), Sample(1, "m2", _A)], "ecapa": alt},
        primary_model="wp",
        precision_bar=0.9,
        curve_bars=curve_bars,
    )
    assert set(ab) == {"ecapa"}  # primary is measured+persisted elsewhere, not duplicated here
    entry = ab["ecapa"]
    assert {"macro_precision", "recall", "n_enabled", "recall_by_confidence", "curve"} <= set(entry)
    assert set(entry["curve"]) == {"nested_by_bar", "pareto"}
    assert entry["n_enabled"] >= 1


def test_ab_report_alternate_with_no_positives_reports_note():
    ab = rc._ab_report(
        {"wp": [Sample(1, "m1", _A)], "ecapa": [Sample(None, "m1", _R)]},
        primary_model="wp",
        precision_bar=0.9,
        curve_bars=tuple(sorted({0.9, *CURVE_PRECISION_BARS})),
    )
    assert ab["ecapa"] == {"n_enabled": 0, "note": "no official voiceprints pooled"}


def test_parse_embedders_blank_defaults_to_primary():
    assert rc._parse_embedders("", primary="wp") == ["wp"]
    assert rc._parse_embedders("  ", primary="wp") == ["wp"]


def test_parse_embedders_requires_primary_first():
    # A non-primary head would try to persist an off-dimension gallery; reject it loudly.
    with pytest.raises(ActaluxError, match="first entry must be the primary"):
        rc._parse_embedders("ecapa,wp", primary="wp")


def test_parse_embedders_dedups_preserving_order():
    assert rc._parse_embedders("wp, ecapa , wp", primary="wp") == ["wp", "ecapa"]


# --- _finish persistence: primary only, ab is measurement only --------------------------------


class _FakeQuery:
    """Records writes and satisfies the read-back chains _finish/_delete_stale/_prune use."""

    def __init__(self, table, recorder):
        self._table = table
        self._recorder = recorder
        self._op = None

    def insert(self, row):
        self._op = "insert"
        self._recorder["inserts"].append((self._table, row))
        return self

    def upsert(self, rows, on_conflict=None):
        self._op = "upsert"
        self._recorder["upserts"].append((self._table, rows))
        return self

    def delete(self):
        self._op = "delete"
        return self

    def select(self, _cols):
        self._op = "select"
        return self

    def in_(self, _col, _vals):
        return self

    def eq(self, _col, _val):
        return self

    def order(self, *_a, **_k):
        return self

    def range(self, *_a, **_k):
        return self

    def execute(self):
        if self._op == "insert":
            return SimpleNamespace(data=[{"id": 777}])
        return SimpleNamespace(data=[])  # select -> no existing gallery rows; delete -> no-op


class _FakeClient:
    def __init__(self):
        self.recorder = {"inserts": [], "upserts": []}

    def table(self, name):
        return _FakeQuery(name, self.recorder)


def _clean_dual_model(primary, alt):
    """Four clean meetings for two officials, embedded by two models, via build_meeting_samples.

    Primary gets orthogonal vectors _A/_B (+ neg _C); the alternate gets _P/_Q (+ neg _R). Each
    official is anchored by two evidence families (adjacency + vote) alternating by meeting, across
    FOUR meetings — so under nested LOMO (one held out) three remain and clear the 3-meeting
    consensus floor, both officials enable, and the primary gallery is actually written.
    pooled_officials is accumulated primary-only exactly as _apply does.
    """
    samples_by_model = {primary: [], alt: []}
    pooled_officials = []
    for mk in (1, 2, 3, 4):
        basis = "rollcall" if mk % 2 else "vote_anchor"  # two families across the four meetings
        ec1 = _ec(1, "SPEAKER_00", "Alice", mk, basis=basis)
        ec2 = _ec(2, "SPEAKER_01", "Bob", mk, basis=basis)
        tbm = {
            primary: {"SPEAKER_00": _turns(_A), "SPEAKER_01": _turns(_B), "SPEAKER_09": _turns(_C)},
            alt: {"SPEAKER_00": _turns(_P), "SPEAKER_01": _turns(_Q), "SPEAKER_09": _turns(_R)},
        }
        per_model, pooled = rc.build_meeting_samples(
            tbm, [ec1, ec2], ["SPEAKER_09"], f"vid{mk}", min_seconds=10.0, primary_model=primary
        )
        for m, s in per_model.items():
            samples_by_model[m].extend(s)
        pooled_officials.extend(pooled)
    return samples_by_model, pooled_officials


def test_finish_persists_only_primary_vectors_and_reports_ab():
    primary, alt = "wp", "ecapa"
    samples_by_model, pooled_officials = _clean_dual_model(primary, alt)
    client = _FakeClient()
    rc._finish(
        client,
        place_id=5,
        entity_id=None,
        models=[primary, alt],
        samples_by_model=samples_by_model,
        pooled_officials=pooled_officials,
        processed_docs={1, 2, 3},
        superseded=set(),
        args=SimpleNamespace(precision_bar=0.9),
    )

    (_table, cal_row) = client.recorder["inserts"][0]
    assert cal_row["status"] == "candidate"  # clean separation clears the bar
    assert set(cal_row["report"]["ab"]) == {alt}  # alternate measured, keyed by model id
    assert "macro_precision" in cal_row["report"]["ab"][alt]

    # The ONLY embeddings written are the primary pooled vectors; no alternate vector is persisted.
    primary_vectors = {p.vector for _, p in pooled_officials}
    alt_vectors = {s.embedding for s in samples_by_model[alt]}
    assert client.recorder["upserts"], "clean data should enroll the primary gallery"
    for _table, rows in client.recorder["upserts"]:
        for row in rows:
            emb = tuple(row["embedding"])
            assert emb in primary_vectors
            assert emb not in alt_vectors


def test_finish_single_model_writes_no_ab_block():
    # The default single-embedder run: the persisted report carries no ab block, and the verdict is
    # reached from the primary samples alone (regression guard for byte-identical behavior).
    primary = "wp"
    samples_by_model, pooled_officials = _clean_dual_model(primary, "ecapa")
    single = {primary: samples_by_model[primary]}
    client = _FakeClient()
    rc._finish(
        client,
        place_id=5,
        entity_id=None,
        models=[primary],
        samples_by_model=single,
        pooled_officials=pooled_officials,
        processed_docs={1, 2, 3},
        superseded=set(),
        args=SimpleNamespace(precision_bar=0.9),
    )
    (_table, cal_row) = client.recorder["inserts"][0]
    assert cal_row["status"] == "candidate"
    assert "ab" not in cal_row["report"]
    assert client.recorder["upserts"], "single-model run still persists the primary gallery"


def test_finish_report_carries_audit_delta_and_trusted_recall():
    # The persisted report gains the Phase-C blocks: trusted-tier headline recall, the per-official
    # audit (families + agreement + enable path), and the run-over-run enablement delta.
    primary = "wp"
    samples_by_model, pooled_officials = _clean_dual_model(primary, "ecapa")
    client = _FakeClient()
    rc._finish(
        client,
        place_id=5,
        entity_id=None,
        models=[primary],
        samples_by_model={primary: samples_by_model[primary]},
        pooled_officials=pooled_officials,
        processed_docs={1, 2, 3, 4},
        superseded=set(),
        args=SimpleNamespace(precision_bar=0.9),
    )
    (_t, cal_row) = client.recorder["inserts"][0]
    report = cal_row["report"]
    assert set(report["trusted_recall"]) == {"positives", "recalled", "recall"}
    audit = report["audit"]
    assert set(audit) == {"1", "2"}
    for entry in audit.values():
        assert entry["enabled"] and entry["enable_path"] == "consensus"
        assert set(entry["family_agreement"]["agreeing_families"]) == {"adjacency", "vote"}
    delta = report["delta"]
    assert {g["person_id"] for g in delta["gained"]} == {1, 2}  # no previous row -> both gained
    assert delta["lost"] == [] and delta["previous_calibration_id"] is None


def test_build_audit_enabled_flag_follows_run_not_bare_gate():
    # A not_cleared run: an official may clear Gate-A consensus, but the RUN enabled nobody
    # (enabled=set()). The audit's `enabled` flag must follow the persisted verdict, not the bare
    # gate pass, so the sheet/report never claim an official is enabled while nothing was stored.
    from actalux.diarization.matching import Sample, gate_officials

    two_family = [
        Sample(1, "m1", _A, basis="rollcall"),
        Sample(1, "m2", _A, basis="vote_anchor"),
        Sample(1, "m3", _A, basis="rollcall"),
    ]
    decisions = gate_officials(two_family, core_floor=0.3, min_core=2, collapse_bound=0.85)
    assert decisions[1].enabled  # the official clears consensus Gate A
    # the RUN enabled nobody (not_cleared): the audit's enabled flag must follow the run
    audit, _reasons = rc._build_audit(decisions, {1: "Alice"}, {}, enabled=set())
    assert audit["1"]["enabled"] is False  # audit agrees with the (empty) run enablement
    assert audit["1"]["enable_path"] == "consensus"  # path/reason still explain the gate pass


def test_top_evidence_excludes_discarded_family_clips():
    # A rollcall (adjacency) clip and a discourse clip. When the coherent voice is adjacency-only
    # (discourse was discarded by the gate), the audit must NOT cue the discarded discourse clip.
    cues = [
        {
            "document_id": 1, "video_id": "v1", "cluster_label": "S0", "basis": "rollcall",
            "confidence": "inferred_high", "start_seconds": 10.0, "end_seconds": 20.0,
        },
        {
            "document_id": 2, "video_id": "v2", "cluster_label": "S1", "basis": "discourse",
            "confidence": "inferred_medium", "start_seconds": 5.0, "end_seconds": 15.0,
        },
    ]  # fmt: skip
    picked = rc._top_evidence(cues, {"adjacency"})
    assert [c["document_id"] for c in picked] == [1]  # discourse clip (discarded family) excluded
    assert len(rc._top_evidence(cues, None)) == 2  # confirmed-waiver: no family filter, keep both


def test_render_audit_sheet_has_embeds_metric_block_and_delta():
    report = {
        "trusted_recall": {"positives": 3, "recalled": 2, "recall": 0.667},
        "macro_precision": 1.0,
        "recall": 0.5,
        "fp_negatives": 0,
        "audit": {
            "1": {
                "name": "Alice",
                "enabled": True,
                "enable_path": "consensus",
                "reason": "consensus: 2 families agree across 4 meetings",
                "families": {"adjacency": 2, "vote": 2},
                "family_agreement": {
                    "agreeing_families": ["adjacency", "vote"],
                    "in_core": {"adjacency": 2, "vote": 2},
                    "core_meetings": 4,
                    "discarded": {},
                },
                "confirmed_meetings": 0,
                "evidence": [
                    {
                        "document_id": 12,
                        "video_id": "abc123",
                        "cluster_label": "SPEAKER_00",
                        "basis": "rollcall",
                        "start_seconds": 90.0,
                        "end_seconds": 105.0,
                    }
                ],
            },
            "2": {  # a not-enabled official gets no row on the sheet
                "name": "Bob",
                "enabled": False,
                "enable_path": "not_enabled",
                "reason": "single family",
                "families": {"adjacency": 3},
                "family_agreement": {
                    "agreeing_families": ["adjacency"],
                    "in_core": {"adjacency": 3},
                    "core_meetings": 3,
                    "discarded": {},
                },
                "confirmed_meetings": 0,
            },
        },
        "delta": {
            "gained": [{"person_id": 1, "name": "Alice", "reason": "consensus"}],
            "lost": [{"person_id": 9, "name": "Zed", "reason": "single family (adjacency)"}],
            "previous_calibration_id": 7,
        },
    }
    out = rc.render_audit_sheet(
        title="Voiceprint audit — mo/clayton", calibration_id=8, status="candidate", report=report
    )
    assert "<!doctype html>" in out.lower()
    # the enabled official's evidence is a cued watch LINK (embeds are dead — YouTube error 153),
    # opening at the clip second; no iframe/embed is emitted
    assert "youtube.com/watch?v=abc123&amp;t=90s" in out
    assert "Play @ 1:30" in out
    assert "iframe" not in out and "nocookie" not in out
    assert "Alice" in out and "Bob" not in out  # only enabled officials get a row
    # metric block + delta (including the demotion)
    assert "trusted-tier recall" in out
    assert "Zed" in out and "lost (demoted)" in out


def test_build_meeting_samples_stamps_audit_provenance():
    # Officials carry their cluster's own doc/label; negatives get the meeting's document_id —
    # the receipt a quarantined sample needs to be cued for human review.
    ec = _ec(1, "SPEAKER_00", "Alice", 1)
    tbm = {"wp": {"SPEAKER_00": _turns(_A), "SPEAKER_09": _turns(_C)}}
    per_model, _ = rc.build_meeting_samples(
        tbm, [ec], ["SPEAKER_09"], "vid1", min_seconds=10.0, primary_model="wp", document_id=77
    )
    official = next(s for s in per_model["wp"] if s.person_id == 1)
    negative = next(s for s in per_model["wp"] if s.person_id is None)
    assert (official.document_id, official.cluster_label) == (1, "SPEAKER_00")
    assert (negative.document_id, negative.cluster_label) == (77, "SPEAKER_09")


def test_finish_quarantines_twin_negative_and_reports_clean_fp():
    # A "citizen" negative that IS an official's voice (the cal-15 poison) must leave the
    # metric with a receipt: clean fp_negatives stays 0, the quarantine is counted, and the
    # would-match disclosure says the deployed operating point would have named it.
    primary = "wp"
    samples_by_model, pooled_officials = _clean_dual_model(primary, "ecapa")
    twin = Sample(None, "vid5", _A, document_id=5, cluster_label="SPEAKER_07")
    single = {primary: [*samples_by_model[primary], twin]}
    client = _FakeClient()
    rc._finish(
        client,
        place_id=5,
        entity_id=None,
        models=[primary],
        samples_by_model=single,
        pooled_officials=pooled_officials,
        processed_docs={1, 2, 3, 4},
        superseded=set(),
        args=SimpleNamespace(precision_bar=0.9),
    )
    (_t, cal_row) = client.recorder["inserts"][0]
    report = cal_row["report"]
    assert cal_row["status"] == "candidate"
    assert report["fp_negatives"] == 0  # the twin never reaches the citizen-FP metric
    hyg = report["hygiene"]
    assert hyg["quarantined_negatives"] == 1
    assert hyg["quarantined_would_match_at_refit"] == 1  # the FP exposure quarantine hides
    assert hyg["alien_positives"] == 0
    # aggregate counts only — the quarantined negative's identifiers never persist
    assert "SPEAKER_07" not in str(report["hygiene"])


def test_finish_alien_positive_never_persists():
    # The Patel-doc2549 shape end-to-end: a confirmed official's wrong-voice inferred anchor
    # is vetted out — counted in the report, absent from the persisted gallery rows.
    primary = "wp"
    samples: list[Sample] = []
    pooled_officials = []
    for mk in (1, 2, 3, 4):
        ec = _ec(1, "SPEAKER_00", "Alice", mk, confidence="confirmed")
        tbm = {primary: {"SPEAKER_00": _turns(_A), "SPEAKER_09": _turns(_C)}}
        per_model, pooled = rc.build_meeting_samples(
            tbm, [ec], ["SPEAKER_09"], f"vid{mk}",
            min_seconds=10.0, primary_model=primary, document_id=mk,
        )  # fmt: skip
        samples.extend(per_model[primary])
        pooled_officials.extend(pooled)
    alien_vec = _E[4]  # a different voice wearing Alice's name in meeting 5
    ec5 = _ec(1, "SPEAKER_04", "Alice", 5, confidence="inferred_medium", basis="discourse")
    per_model5, pooled5 = rc.build_meeting_samples(
        {primary: {"SPEAKER_04": _turns(alien_vec)}}, [ec5], [], "vid5",
        min_seconds=10.0, primary_model=primary, document_id=5,
    )  # fmt: skip
    samples.extend(per_model5[primary])
    pooled_officials.extend(pooled5)

    client = _FakeClient()
    rc._finish(
        client,
        place_id=5,
        entity_id=None,
        models=[primary],
        samples_by_model={primary: samples},
        pooled_officials=pooled_officials,
        processed_docs={1, 2, 3, 4, 5},
        superseded=set(),
        args=SimpleNamespace(precision_bar=0.9),
    )
    (_t, cal_row) = client.recorder["inserts"][0]
    assert cal_row["status"] == "candidate"  # Alice enables via confirmed waiver
    assert cal_row["report"]["hygiene"]["alien_positives"] == 1
    persisted = [tuple(r["embedding"]) for _t2, rows in client.recorder["upserts"] for r in rows]
    assert persisted and alien_vec not in persisted  # 4 genuine rows, never the alien


def test_render_audit_sheet_hygiene_queues_and_collapse_pairs():
    report = {
        "trusted_recall": {"positives": 4, "recalled": 3, "recall": 0.75},
        "macro_precision": 1.0,
        "recall": 0.75,
        "predictions": 6,
        "fp_negatives": 0,
        "hygiene": {"quarantined_negatives": 1, "near_band_negatives": 0, "alien_positives": 1},
        "audit": {
            "1": {
                "name": "Alice",
                "enabled": False,
                "enable_path": "not_enabled",
                "reason": "collapse: one voice anchored under multiple names",
                "families": {"human": 2},
                "family_agreement": {},
                "confirmed_meetings": 2,
                "collapse_pairs": [
                    {
                        "person_a": 1,
                        "name_a": "Alice",
                        "person_b": 2,
                        "name_b": "Bob",
                        "cosine": 0.91,
                        "meeting_a": "vidA",
                        "meeting_b": "vidB",
                        "basis_a": "manual",
                        "basis_b": "discourse",
                        "confidence_a": "confirmed",
                        "confidence_b": "inferred_medium",
                        "cue_a": 30.0,
                        "cue_b": 60.0,
                    }
                ],
            },
        },
        "delta": {"gained": [], "lost": [], "previous_calibration_id": None},
    }
    hygiene_details = {
        "quarantined_negatives": [
            {
                "document_id": 2559,
                "video_id": "QTM",
                "cluster_label": "SPEAKER_07",
                "person_id": 23,
                "person_name": "Stacy Siwak",
                "score": 0.824,
                "start_seconds": 933.0,
            }
        ],
        "near_band_negatives": [],
        "alien_positives": [
            {
                "document_id": 2549,
                "video_id": "ELP",
                "cluster_label": "SPEAKER_03",
                "person_id": 100,
                "person_name": "Nisha Patel",
                "score": 0.061,
                "start_seconds": 4969.0,
            }
        ],
    }
    out = rc.render_audit_sheet(
        title="Voiceprint audit — mo/clayton/schools",
        calibration_id=16,
        status="candidate",
        report=report,
        hygiene_details=hygiene_details,
    )
    # collapse pairs: both sides named + cued so a human can break the veto by ear
    assert "Collapse pairs (1)" in out
    assert "youtube.com/watch?v=vidA&amp;t=30s" in out
    assert "youtube.com/watch?v=vidB&amp;t=60s" in out
    # quarantine queues, each row cued
    assert "Quarantined negatives" in out and "Stacy Siwak" in out
    assert "youtube.com/watch?v=QTM&amp;t=933s" in out
    assert "Quarantined positives" in out and "Nisha Patel" in out
    # metric block carries the honest support numbers
    assert "nested predictions" in out and "quarantined negatives (unresolved)" in out


def test_negative_labels_excludes_officials_and_short_clusters_longest_first():
    turns = [
        _turn("SPEAKER_00", 0, 30),  # official -> excluded
        _turn("SPEAKER_01", 0, 20),  # negative, 20s
        _turn("SPEAKER_02", 0, 5),  # negative but 5s < min -> dropped
        _turn("SPEAKER_03", 0, 40),  # negative, 40s
    ]
    labels = rc.negative_labels(turns, {"SPEAKER_00"}, min_seconds=10.0, cap=2)
    assert labels == ["SPEAKER_03", "SPEAKER_01"]  # longest first, capped, official excluded


def test_confusion_report_drops_negative_identifiers():
    metrics = Metrics(
        macro_precision=0.8,
        recall=0.6,
        predictions=5,
        per_person_precision={1: 1.0, 2: 0.5},
        confusions=[(None, 1), (3, 2)],  # (negative->official), (official->official)
    )
    report = rc._confusion_report(metrics)
    assert report["fp_negatives"] == 1
    assert report["official_confusions"] == [[3, 2]]  # the (None, 1) negative pair is dropped
    assert report["per_official_precision"] == {"1": 1.0, "2": 0.5}
