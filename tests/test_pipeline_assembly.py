"""Unit tests for the speaker-attribution assembly mapping (no GPU/DB)."""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any

from actalux.diarization.backend import SpeakerTimeline, SpeakerTurn
from actalux.glossary.canonicalize import CorrectionRule
from actalux.transcription.backend import WordTranscript
from actalux.transcription.pipeline import (
    SpeakerLayer,
    assemble_speaker_layer,
    media_asset_row,
    persist_speaker_layer,
    transcribe_and_attribute,
)

DIAR_MODEL = "pyannote/speaker-diarization-3.1"


def _transcript() -> WordTranscript:
    payload = {
        "language": "en",
        "segments": [
            {
                "start": 0.0,
                "end": 4.0,
                "text": "Mr York moved",
                "words": [
                    {"word": "Mr", "start": 0.0, "end": 0.5},
                    {"word": "York", "start": 1.0, "end": 1.5},
                    {"word": "moved", "start": 2.0, "end": 2.5},
                ],
            },
            {
                "start": 11.0,
                "end": 13.0,
                "text": "I second",
                "words": [
                    {"word": "I", "start": 11.0, "end": 11.2},
                    {"word": "second", "start": 11.3, "end": 11.8},
                ],
            },
        ],
    }
    return WordTranscript.from_payload(payload, "whisperx/large-v3")


def _timeline() -> SpeakerTimeline:
    return SpeakerTimeline(
        turns=[
            SpeakerTurn("SPEAKER_00", 0.0, 10.0),
            SpeakerTurn("SPEAKER_01", 10.0, 20.0),
        ],
        num_speakers=2,
        source_model=DIAR_MODEL,
    )


def _rules() -> list[CorrectionRule]:
    return [CorrectionRule("york", "Yorg", "lexicon")]


def test_assemble_produces_raw_and_canonical_text():
    layer = assemble_speaker_layer(_transcript(), _timeline(), _rules())
    assert layer.raw_text == "Mr York moved I second"
    assert layer.canonical_text == "Mr Yorg moved I second"
    assert layer.diarization_model == DIAR_MODEL


def test_assemble_canonicalization_audit():
    layer = assemble_speaker_layer(_transcript(), _timeline(), _rules())
    assert len(layer.canonicalizations) == 1
    c = layer.canonicalizations[0]
    assert c.raw_token == "York"
    assert c.canonical == "Yorg"
    assert c.char_start == layer.raw_text.index("York")


def test_assemble_turns_are_verbatim_and_speaker_split():
    layer = assemble_speaker_layer(_transcript(), _timeline(), _rules())
    # Turns carry RAW words (verbatim) — "York", not the canonical "Yorg".
    assert [(t.cluster_label, t.text) for t in layer.turns] == [
        ("SPEAKER_00", "Mr York moved"),
        ("SPEAKER_01", "I second"),
    ]


def test_canonicalization_rows_shape():
    layer = assemble_speaker_layer(_transcript(), _timeline(), _rules())
    rows = layer.canonicalization_rows(5)
    assert rows == [
        {
            "document_id": 5,
            "char_start": layer.raw_text.index("York"),
            "raw_token": "York",
            "canonical": "Yorg",
            "source": "lexicon",
            "score": None,
        }
    ]


def test_turn_rows_shape():
    layer = assemble_speaker_layer(_transcript(), _timeline(), _rules())
    rows = layer.turn_rows(5)
    assert len(rows) == 2
    first = rows[0]
    assert first["document_id"] == 5
    assert first["cluster_label"] == "SPEAKER_00"
    assert first["start_seconds"] == 0.0
    assert first["end_seconds"] == 2.5
    assert first["source_model"] == DIAR_MODEL
    assert first["words"][0] == {"word": "Mr", "start": 0.0, "end": 0.5}
    assert first["words"][1]["word"] == "York"  # verbatim in the words JSONB


def test_assemble_with_no_corrections_leaves_text_unchanged():
    layer = assemble_speaker_layer(_transcript(), _timeline(), [])
    assert layer.canonical_text == layer.raw_text
    assert layer.canonicalizations == []


def test_media_asset_row_shape():
    row = media_asset_row(
        7, "https://www.youtube.com/watch?v=abc", entity_id=3, duration_seconds=3600.0
    )
    assert row == {
        "document_id": 7,
        "entity_id": 3,
        "source_url": "https://www.youtube.com/watch?v=abc",
        "kind": "video",
        "duration_seconds": 3600.0,
        "content_hash": None,
    }


class _StubTranscriber:
    def __init__(self, transcript: WordTranscript) -> None:
        self._transcript = transcript

    def transcribe(self, audio_uri: str) -> WordTranscript:
        return self._transcript


class _StubDiarizer:
    def __init__(self, timeline: SpeakerTimeline) -> None:
        self._timeline = timeline

    def run(
        self,
        audio_uri: str,
        *,
        hint_num_speakers: int | None = None,
        return_embeddings: bool = False,
    ) -> SpeakerTimeline:
        return self._timeline


def test_transcribe_and_attribute_orchestrates_backends():
    layer = transcribe_and_attribute(
        "audio.mp3", _StubTranscriber(_transcript()), _StubDiarizer(_timeline()), _rules()
    )
    assert layer.canonical_text == "Mr Yorg moved I second"
    assert [t.cluster_label for t in layer.turns] == ["SPEAKER_00", "SPEAKER_01"]


def test_speaker_layer_dict_roundtrip():
    layer = assemble_speaker_layer(_transcript(), _timeline(), _rules())
    assert SpeakerLayer.from_dict(layer.to_dict()) == layer


class _RecordingTable:
    """Records the supabase-style call made against one table."""

    def __init__(self, name: str, log: list[dict[str, Any]]) -> None:
        self.name = name
        self.log = log
        self._op: str | None = None
        self._payload: Any = None
        self._filters: list[tuple[str, Any]] = []
        self._on_conflict: str | None = None

    def update(self, payload: Any) -> _RecordingTable:
        self._op, self._payload = "update", payload
        return self

    def insert(self, payload: Any) -> _RecordingTable:
        self._op, self._payload = "insert", payload
        return self

    def delete(self) -> _RecordingTable:
        self._op = "delete"
        return self

    def upsert(self, payload: Any, on_conflict: str | None = None) -> _RecordingTable:
        self._op, self._payload, self._on_conflict = "upsert", payload, on_conflict
        return self

    def eq(self, column: str, value: Any) -> _RecordingTable:
        self._filters.append((column, value))
        return self

    def execute(self) -> SimpleNamespace:
        self.log.append(
            {
                "table": self.name,
                "op": self._op,
                "payload": self._payload,
                "filters": self._filters,
                "on_conflict": self._on_conflict,
            }
        )
        return SimpleNamespace(data=[])


class _RecordingClient:
    def __init__(self) -> None:
        self.log: list[dict[str, Any]] = []

    def table(self, name: str) -> _RecordingTable:
        return _RecordingTable(name, self.log)


def test_persist_speaker_layer_writes_all_tables_idempotently():
    layer = assemble_speaker_layer(_transcript(), _timeline(), _rules())
    client = _RecordingClient()
    persist_speaker_layer(
        client, 5, layer, media_url="https://yt/abc", entity_id=3, duration_seconds=3600.0
    )
    ops = [(e["table"], e["op"]) for e in client.log]
    # The whole prior layer is cleared first — INCLUDING speaker_identities (stale
    # cluster->subject after a re-diarize) — then re-inserted; raw_content written last.
    cleared = ("diarization_turns", "name_canonicalizations", "speaker_identities", "media_assets")
    for table in cleared:
        assert (table, "delete") in ops
    assert ops.index(("diarization_turns", "delete")) < ops.index(("diarization_turns", "insert"))
    assert ops.index(("media_assets", "delete")) < ops.index(("media_assets", "insert"))
    assert ops[-1] == ("documents", "update")  # raw_content is the final write
    by = {(e["table"], e["op"]): e for e in client.log}
    assert by[("documents", "update")]["payload"] == {"raw_content": layer.raw_text}
    assert by[("documents", "update")]["filters"] == [("id", 5)]
    assert len(by[("diarization_turns", "insert")]["payload"]) == 2
    assert len(by[("name_canonicalizations", "insert")]["payload"]) == 1


def test_persist_speaker_layer_skips_empty_inserts():
    empty = SpeakerLayer("raw text", "raw text", [], [], DIAR_MODEL)
    client = _RecordingClient()
    persist_speaker_layer(client, 9, empty, media_url="https://yt/x")
    ops = [(e["table"], e["op"]) for e in client.log]
    assert ("diarization_turns", "insert") not in ops
    assert ("name_canonicalizations", "insert") not in ops
    # deletes (idempotency) + the media asset + the raw_content update still happen
    assert ("speaker_identities", "delete") in ops
    assert ("media_assets", "insert") in ops
    assert ops[-1] == ("documents", "update")
