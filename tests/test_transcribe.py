"""Tests for the Whisper transcription module.

The pure helpers (window planning, segment merge/offset) and the orchestration
are tested without ffmpeg or network by mocking the encode + API boundaries.
"""

from __future__ import annotations

from unittest.mock import patch

import pytest

from actalux.ingest.transcribe import (
    Segment,
    Transcript,
    _merge_windows,
    _plan_windows,
    transcribe_audio,
)


class TestPlanWindows:
    def test_zero_duration(self) -> None:
        assert _plan_windows(0, 600) == [(0.0, 0.0)]

    def test_shorter_than_window_is_one(self) -> None:
        assert _plan_windows(180, 600) == [(0.0, 180.0)]

    def test_exact_multiple(self) -> None:
        assert _plan_windows(1200, 600) == [(0.0, 600.0), (600.0, 600.0)]

    def test_remainder_window(self) -> None:
        assert _plan_windows(1500, 600) == [(0.0, 600.0), (600.0, 600.0), (1200.0, 300.0)]

    def test_tiny_trailing_remainder_folds_into_previous(self) -> None:
        # A 0.05s tail (end-of-meeting rounding) would 400 as "audio too short";
        # it is folded into the prior window instead of sent alone.
        windows = _plan_windows(180.05, 90)
        assert len(windows) == 2
        assert windows[0] == (0.0, 90.0)
        assert windows[1][0] == 90.0
        assert windows[1][1] == pytest.approx(90.05)


class TestMergeWindows:
    def test_offsets_segments_onto_continuous_timeline(self) -> None:
        results = [
            (0.0, "first window", [Segment(0.0, 5.0, "a"), Segment(5.0, 9.0, "b")]),
            (60.0, "second window", [Segment(1.0, 4.0, "c")]),
        ]
        merged = _merge_windows(results)
        assert isinstance(merged, Transcript)
        assert merged.text == "first window second window"
        assert [(s.start, s.end, s.text) for s in merged.segments] == [
            (0.0, 5.0, "a"),
            (5.0, 9.0, "b"),
            (61.0, 64.0, "c"),  # window-2 segment shifted by its 60s offset
        ]

    def test_skips_empty_text(self) -> None:
        merged = _merge_windows([(0.0, "", []), (60.0, "only this", [])])
        assert merged.text == "only this"


class TestTranscribeAudioOrchestration:
    def test_stitches_three_windows(self, tmp_path) -> None:
        audio = tmp_path / "meeting.m4a"
        audio.write_bytes(b"fake")

        def fake_transcribe_one(client, path, model, language, prompt):
            # Each window returns the same window-relative segment; merge offsets it.
            return "win", [Segment(0.0, 10.0, "seg")]

        with (
            patch("actalux.ingest.transcribe._probe_duration", return_value=150.0),
            patch("actalux.ingest.transcribe._encode_window") as enc,
            patch("actalux.ingest.transcribe._transcribe_one", side_effect=fake_transcribe_one),
            patch("actalux.ingest.transcribe.OpenAI"),
        ):
            result = transcribe_audio(audio, "sk-test", window_seconds=60)

        # 150s / 60s -> 3 windows -> 3 encode calls, 3 segments at offsets 0/60/120.
        assert enc.call_count == 3
        assert [s.start for s in result.segments] == [0.0, 60.0, 120.0]
        assert result.text == "win win win"
