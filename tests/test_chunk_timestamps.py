"""Tests for Whisper-sidecar chunk-timestamp alignment.

The exact path: a transcript's chunks are aligned to seconds from the same
Whisper segments the transcript was built from (near-100% coverage, vs the fuzzy
caption fallback).
"""

from __future__ import annotations

import json

import scripts.backfill_chunk_timestamps as mod
from scripts.backfill_chunk_timestamps import (
    align_chunk,
    build_timed_index_from_segments,
    load_segment_sidecar,
)

_SEGMENTS = [
    {"start": 0.0, "end": 5.0, "text": "good evening everyone welcome to the board meeting"},
    {"start": 5.0, "end": 12.0, "text": "first item on the agenda is the budget resolution"},
    {"start": 12.0, "end": 20.0, "text": "all in favor say aye the motion carries unanimously"},
]


class TestBuildTimedIndexFromSegments:
    def test_text_and_char_ms_aligned(self) -> None:
        text, char_ms = build_timed_index_from_segments(_SEGMENTS)
        assert len(char_ms) == len(text)
        assert "good evening everyone" in text
        assert char_ms[0] == 0  # first char is from the 0.0s segment

    def test_later_segment_carries_its_start_ms(self) -> None:
        text, char_ms = build_timed_index_from_segments(_SEGMENTS)
        idx = text.index("first item")
        assert char_ms[idx] == 5000  # second segment starts at 5.0s

    def test_blank_segments_skipped(self) -> None:
        text, _ = build_timed_index_from_segments([{"start": 0.0, "end": 1.0, "text": "  "}])
        assert text == ""


class TestAlignChunkFromSidecar:
    def test_chunk_aligns_to_its_segment_second(self) -> None:
        text, char_ms = build_timed_index_from_segments(_SEGMENTS)
        chunk = "first item on the agenda is the budget resolution"
        assert align_chunk(chunk, text, char_ms) == 5

    def test_third_segment_chunk(self) -> None:
        text, char_ms = build_timed_index_from_segments(_SEGMENTS)
        chunk = "all in favor say aye the motion carries unanimously"
        assert align_chunk(chunk, text, char_ms) == 12


class TestLoadSegmentSidecar:
    def test_missing_returns_none(self) -> None:
        assert load_segment_sidecar("") is None
        assert load_segment_sidecar("no-such-file.txt") is None

    def test_loads_existing_sidecar(self, tmp_path, monkeypatch) -> None:
        monkeypatch.setattr(mod, "SEGMENTS_DIR", tmp_path)
        (tmp_path / "Meeting.segments.json").write_text(json.dumps(_SEGMENTS))
        loaded = load_segment_sidecar("Meeting.txt")
        assert loaded is not None
        assert loaded[1]["start"] == 5.0
