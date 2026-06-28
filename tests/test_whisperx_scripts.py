"""Tests for the WhisperX staging/persist scripts' pure logic (no GPU/DB/network)."""

from __future__ import annotations

import json
from types import SimpleNamespace
from typing import Any

from actalux.diarization.align import AttributedTurn
from actalux.ingest.youtube import BoardMeeting
from actalux.transcription.backend import Word
from actalux.transcription.pipeline import SpeakerLayer
from scripts.persist_whisperx import current_transcript_ids, stem_from_source_file
from scripts.transcribe_whisperx import meeting_stem, stage_meeting

DIAR_MODEL = "pyannote/speaker-diarization-3.1"


def _layer() -> SpeakerLayer:
    words = [Word("Hello", 0.0, 1.0), Word("all", 1.0, 2.0)]
    return SpeakerLayer(
        "Hello all", "Hello all", [], [AttributedTurn("SPEAKER_00", 0.0, 2.0, words)], DIAR_MODEL
    )


def test_stem_from_source_file():
    assert stem_from_source_file("2026-06-27 Council.txt") == "2026-06-27 Council"
    assert stem_from_source_file("noext") == "noext"


def test_stage_meeting_writes_artifacts_and_roundtrips(tmp_path):
    meeting = BoardMeeting(
        video_id="abc123",
        title="2026-06-27 City Council",
        meeting_date="2026-06-27",
        url="https://www.youtube.com/watch?v=abc123",
    )
    layer = _layer()
    segments = [{"start": 0.0, "end": 2.0, "text": "Hello all"}]
    entry = stage_meeting(meeting, layer, segments, entity_id=3, out_dir=tmp_path)

    stem = meeting_stem(meeting)
    assert stem.endswith("_abc123")  # video id keeps same-title meetings distinct
    assert (tmp_path / f"{stem}.txt").read_text() == layer.canonical_text
    assert json.loads((tmp_path / f"{stem}.segments.json").read_text()) == segments

    att = json.loads((tmp_path / f"{stem}.attribution.json").read_text())
    assert att["video_id"] == "abc123"
    assert att["entity_id"] == 3
    assert att["source_url"] == meeting.url
    assert SpeakerLayer.from_dict(att["layer"]) == layer  # round-trips losslessly

    assert entry["source_file"] == f"{stem}.txt"
    assert entry["document_type"] == "transcript"
    assert entry["video_id"] == "abc123"


class _Query:
    def __init__(self, data: list[dict[str, Any]]) -> None:
        self._data = data

    def select(self, _cols: str) -> _Query:
        return self

    def eq(self, _col: str, _val: Any) -> _Query:
        return self

    def execute(self) -> SimpleNamespace:
        return SimpleNamespace(data=self._data)


class _Client:
    def __init__(self, data: list[dict[str, Any]]) -> None:
        self._data = data

    def table(self, _name: str) -> _Query:
        return _Query(self._data)


def test_current_transcript_ids_returns_single_live_doc():
    client = _Client(
        [
            {"id": 1, "replaces_id": 2},  # superseded
            {"id": 3, "replaces_id": None},  # the one current version
        ]
    )
    assert current_transcript_ids(client, "abc", 3) == [3]


def test_current_transcript_ids_surfaces_duplicate_anomaly():
    # Two live rows for one meeting is an anomaly the caller must refuse to guess on.
    client = _Client([{"id": 3, "replaces_id": None}, {"id": 5, "replaces_id": None}])
    assert current_transcript_ids(client, "abc", 3) == [3, 5]


def test_current_transcript_ids_empty_when_all_superseded_or_missing():
    assert current_transcript_ids(_Client([]), "abc", 3) == []
    assert current_transcript_ids(_Client([{"id": 1, "replaces_id": 9}]), "abc", 3) == []
