"""Tests for Whisper-sidecar chunk-timestamp alignment.

The exact path: a transcript's chunks are aligned to seconds from the same
Whisper segments the transcript was built from (near-100% coverage, vs the fuzzy
caption fallback).
"""

from __future__ import annotations

import json
import os

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
    def test_missing_returns_none(self, tmp_path, monkeypatch) -> None:
        monkeypatch.setattr(mod, "SEGMENTS_DIR", tmp_path)
        assert load_segment_sidecar("", "") is None
        assert load_segment_sidecar("nope123", "no-such-file.txt") is None

    def test_matches_by_video_id_despite_drifted_source_file(self, tmp_path, monkeypatch) -> None:
        # The regression: the sidecar stem is <title>_<video_id>, but the doc's stored
        # source_file has a *different* title (drifted across a re-ingest). Matching by
        # source_file stem would miss it; matching by video_id finds it.
        monkeypatch.setattr(mod, "SEGMENTS_DIR", tmp_path)
        (tmp_path / "Old_Board_Meeting_abc123XYZ00.segments.json").write_text(json.dumps(_SEGMENTS))
        loaded = load_segment_sidecar("abc123XYZ00", "New Board Meeting Title_abc123XYZ00.txt")
        assert loaded is not None
        assert loaded[1]["start"] == 5.0

    def test_falls_back_to_source_file_stem_without_video_id(self, tmp_path, monkeypatch) -> None:
        # Older rows predating the video_id column: match by the source_file stem.
        monkeypatch.setattr(mod, "SEGMENTS_DIR", tmp_path)
        (tmp_path / "Meeting.segments.json").write_text(json.dumps(_SEGMENTS))
        loaded = load_segment_sidecar("", "Meeting.txt")
        assert loaded is not None
        assert loaded[1]["start"] == 5.0

    def test_multiple_matches_use_newest(self, tmp_path, monkeypatch) -> None:
        # Two title-drifted sidecars for one meeting accumulate locally; the newest
        # (the current run's) wins.
        monkeypatch.setattr(mod, "SEGMENTS_DIR", tmp_path)
        old = tmp_path / "Old_vid00000001.segments.json"
        new = tmp_path / "New_vid00000001.segments.json"
        old.write_text(json.dumps([{"start": 99.0, "end": 100.0, "text": "stale"}]))
        new.write_text(json.dumps(_SEGMENTS))
        os.utime(old, (1_000_000, 1_000_000))  # force old to be older than new
        os.utime(new, (2_000_000, 2_000_000))
        loaded = load_segment_sidecar("vid00000001", "")
        assert loaded is not None and loaded[0]["text"].startswith("good evening")
