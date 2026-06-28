"""Unit tests for the diarization seam + overlap alignment (no GPU/Modal)."""

from __future__ import annotations

from actalux.diarization.align import (
    AttributedTurn,
    assign_clusters,
    attribute_words,
    chunk_windows,
)
from actalux.diarization.backend import SpeakerTimeline, SpeakerTurn
from actalux.transcription.backend import Word


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


def _three_turn_timeline() -> SpeakerTimeline:
    return SpeakerTimeline(
        turns=[
            SpeakerTurn("SPEAKER_00", 0.0, 10.0),
            SpeakerTurn("SPEAKER_01", 10.0, 20.0),
            SpeakerTurn("SPEAKER_00", 20.0, 30.0),
        ],
        num_speakers=2,
        source_model="pyannote/test",
    )


def test_attribute_words_merges_runs_and_splits_on_speaker_change() -> None:
    words = [
        Word("Hello", 1.0, 2.0),
        Word("there", 3.0, 4.0),  # both inside SPEAKER_00
        Word("colleagues", 11.0, 12.0),  # SPEAKER_01
        Word("today", 21.0, 22.0),  # SPEAKER_00 again -> a separate run
    ]
    turns = attribute_words(words, _three_turn_timeline())
    assert [(t.cluster_label, t.text) for t in turns] == [
        ("SPEAKER_00", "Hello there"),
        ("SPEAKER_01", "colleagues"),
        ("SPEAKER_00", "today"),
    ]
    assert turns[0].start_s == 1.0 and turns[0].end_s == 4.0


def test_attribute_words_gap_word_falls_back_to_nearest_turn() -> None:
    # A word overlapping no turn attaches to the temporally nearest turn (never dropped).
    timeline = SpeakerTimeline(
        turns=[SpeakerTurn("SPEAKER_00", 0.0, 10.0), SpeakerTurn("SPEAKER_01", 20.0, 30.0)],
        num_speakers=2,
        source_model="pyannote/test",
    )
    near_first = attribute_words([Word("gap", 14.0, 15.0)], timeline)
    assert [t.cluster_label for t in near_first] == ["SPEAKER_00"]  # 4s from _00 vs 6s from _01
    near_second = attribute_words([Word("gap", 16.0, 17.0)], timeline)
    assert [t.cluster_label for t in near_second] == ["SPEAKER_01"]  # 4s from _01 vs 6s from _00


def test_attribute_words_gap_fallback_uses_word_end_not_just_start() -> None:
    # Word 10.5-10.9 sits in the 10-11 gap. By interval distance it is 0.5s after
    # SPEAKER_00 (ends 10.0) but only 0.1s before SPEAKER_01 (starts 11.0) -> _01.
    timeline = SpeakerTimeline(
        turns=[SpeakerTurn("SPEAKER_00", 0.0, 10.0), SpeakerTurn("SPEAKER_01", 11.0, 12.0)],
        num_speakers=2,
        source_model="pyannote/test",
    )
    turns = attribute_words([Word("between", 10.5, 10.9)], timeline)
    assert [t.cluster_label for t in turns] == ["SPEAKER_01"]


def test_attribute_words_empty_inputs() -> None:
    assert attribute_words([], _three_turn_timeline()) == []
    empty_tl = SpeakerTimeline(turns=[], num_speakers=0, source_model="x")
    assert attribute_words([Word("hi", 1.0, 2.0)], empty_tl) == []


def test_attributed_turn_to_row_shape() -> None:
    turn = AttributedTurn(
        "SPEAKER_00", 1.0, 4.0, [Word("Hello", 1.0, 2.0), Word("there", 3.0, 4.0)]
    )
    assert turn.to_row(7, "pyannote/speaker-diarization-3.1") == {
        "document_id": 7,
        "cluster_label": "SPEAKER_00",
        "start_seconds": 1.0,
        "end_seconds": 4.0,
        "words": [
            {"word": "Hello", "start": 1.0, "end": 2.0},
            {"word": "there", "start": 3.0, "end": 4.0},
        ],
        "source_model": "pyannote/speaker-diarization-3.1",
    }
