"""Unit tests for the diarization domain types + wire mapping (no GPU)."""

from __future__ import annotations

from actalux.diarization.backend import ClusterEmbedding, SpeakerTimeline

DIAR_MODEL = "pyannote/speaker-diarization-3.1"


def _segments() -> list[dict]:
    return [
        {"speaker": "SPEAKER_00", "start": 0.0, "end": 4.0},
        {"speaker": "SPEAKER_01", "start": 4.0, "end": 9.0},
        {"speaker": "SPEAKER_00", "start": 9.0, "end": 12.0},
    ]


def test_from_segments_has_no_embeddings():
    tl = SpeakerTimeline.from_segments(_segments(), DIAR_MODEL)
    assert tl.num_speakers == 2
    assert len(tl.turns) == 3
    assert tl.embeddings == {}


def test_from_remote_bare_list_is_backward_compatible():
    # A not-yet-redeployed backend still returns a bare segment list.
    tl = SpeakerTimeline.from_remote(_segments(), DIAR_MODEL)
    assert tl.num_speakers == 2
    assert [t.cluster_label for t in tl.turns] == ["SPEAKER_00", "SPEAKER_01", "SPEAKER_00"]
    assert tl.embeddings == {}


def test_from_remote_dict_parses_embeddings():
    payload = {
        "segments": _segments(),
        "embeddings": [
            {
                "cluster_label": "SPEAKER_00",
                "vector": [0.1, 0.2, 0.3],
                "seconds": 7.0,
                "model": "pyannote/wespeaker-voxceleb-resnet34-LM",
            },
            {
                "cluster_label": "SPEAKER_01",
                "vector": [0.9, 0.0, -0.1],
                "seconds": 5.0,
                "model": "pyannote/wespeaker-voxceleb-resnet34-LM",
            },
        ],
    }
    tl = SpeakerTimeline.from_remote(payload, DIAR_MODEL)
    assert set(tl.embeddings) == {"SPEAKER_00", "SPEAKER_01"}
    e0 = tl.embeddings["SPEAKER_00"]
    assert isinstance(e0, ClusterEmbedding)
    assert e0.vector == (0.1, 0.2, 0.3)
    assert e0.seconds == 7.0
    assert e0.model == "pyannote/wespeaker-voxceleb-resnet34-LM"


def test_from_remote_dict_without_embeddings_key():
    tl = SpeakerTimeline.from_remote({"segments": _segments()}, DIAR_MODEL)
    assert tl.num_speakers == 2
    assert tl.embeddings == {}
