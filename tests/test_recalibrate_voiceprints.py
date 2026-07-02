"""Unit tests for recalibration helpers: negative selection + privacy-safe reporting."""

from __future__ import annotations

import scripts.recalibrate_voiceprints as rc
from actalux.diarization.matching import Metrics


def _turn(label, start, end):
    return {"cluster_label": label, "start_seconds": start, "end_seconds": end}


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
