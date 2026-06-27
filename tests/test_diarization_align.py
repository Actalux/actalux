"""Unit tests for the diarization seam + overlap alignment (no GPU/Modal)."""

from __future__ import annotations

from actalux.diarization.align import assign_clusters, chunk_windows
from actalux.diarization.backend import SpeakerTimeline, SpeakerTurn


def _timeline() -> SpeakerTimeline:
    return SpeakerTimeline(
        turns=[
            SpeakerTurn("SPEAKER_00", 0.0, 10.0),
            SpeakerTurn("SPEAKER_01", 10.0, 25.0),
            SpeakerTurn("SPEAKER_00", 25.0, 30.0),
        ],
        num_speakers=2,
        source_model="test",
    )


def test_from_segments_counts_distinct_speakers() -> None:
    tl = SpeakerTimeline.from_segments(
        [
            {"speaker": "SPEAKER_00", "start": 0, "end": 5},
            {"speaker": "SPEAKER_01", "start": 5, "end": 9},
            {"speaker": "SPEAKER_00", "start": 9, "end": 12},
        ],
        "pyannote/test",
    )
    assert tl.num_speakers == 2
    assert tl.turns[0].cluster_label == "SPEAKER_00"
    assert tl.source_model == "pyannote/test"


def test_assign_clusters_by_max_overlap() -> None:
    out = assign_clusters(_timeline(), [(1, 1.0, 9.0), (2, 11.0, 20.0), (3, 9.0, 12.0)])
    assert out[1] == "SPEAKER_00"  # fully inside the first turn
    assert out[2] == "SPEAKER_01"  # fully inside the second turn
    assert out[3] == "SPEAKER_01"  # straddles 9-12: 1s of _00 vs 2s of _01 -> _01


def test_assign_clusters_omits_unattributed_chunk() -> None:
    out = assign_clusters(_timeline(), [(9, 100.0, 110.0)])  # past every turn
    assert 9 not in out


def test_chunk_windows_uses_next_start_as_end_and_drops_untimed() -> None:
    windows = chunk_windows(
        [
            {"id": 1, "start_seconds": 0.0},
            {"id": 2, "start_seconds": 12.5},
            {"id": 3, "start_seconds": None},  # untimed -> dropped
        ]
    )
    assert (1, 0.0, 12.5) in windows
    assert windows[-1][0] == 2 and windows[-1][2] == float("inf")
    assert all(cid != 3 for cid, _, _ in windows)
