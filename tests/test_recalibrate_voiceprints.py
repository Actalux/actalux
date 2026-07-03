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


def _ec(person_id, label, name, meeting, *, confidence="inferred_high"):
    return EnrollableCluster(
        person_id=person_id,
        source_subject_id=10 * person_id,
        source_identity_id=1000 * person_id + meeting,
        document_id=meeting,
        cluster_label=label,
        source_basis="rollcall",
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
    # Clean separation so the alternate harness produces a real nested verdict; the report carries
    # the nested summary + curve + n_enabled + recall split, keyed by model id, primary excluded.
    alt = [
        Sample(1, "m1", _A), Sample(1, "m2", _A), Sample(1, "m3", _A),
        Sample(2, "m4", _B), Sample(2, "m5", _B), Sample(2, "m6", _B),
        Sample(None, "m7", _C),
    ]  # fmt: skip
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
    """Three clean meetings for two officials, embedded by two models, via build_meeting_samples.

    Primary gets orthogonal vectors _A/_B (+ neg _C); the alternate gets _P/_Q (+ neg _R). Clean
    separation clears the bar, so both officials enable and the primary gallery is actually written.
    pooled_officials is accumulated primary-only exactly as _apply does.
    """
    samples_by_model = {primary: [], alt: []}
    pooled_officials = []
    for mk in (1, 2, 3):
        ec1 = _ec(1, "SPEAKER_00", "Alice", mk)
        ec2 = _ec(2, "SPEAKER_01", "Bob", mk)
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
