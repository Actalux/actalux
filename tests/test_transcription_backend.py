"""Unit tests for the word-level transcription seam (no GPU/Modal)."""

from __future__ import annotations

from actalux.transcription.backend import WordTranscript

PAYLOAD = {
    "language": "en",
    "segments": [
        {
            "start": 0.0,
            "end": 2.0,
            "text": "Good evening.",
            "words": [
                {"word": "Good", "start": 0.0, "end": 0.6},
                {"word": "evening.", "start": 0.7, "end": 2.0},
            ],
        },
        {
            "start": 2.5,
            "end": 4.0,
            "text": "Roll call.",
            # a word missing timing is dropped (alignment can fail on a token)
            "words": [
                {"word": "Roll", "start": 2.5, "end": 3.0},
                {"word": "call.", "start": None, "end": None},
            ],
        },
    ],
}


def test_from_payload_parses_segments_and_words() -> None:
    tx = WordTranscript.from_payload(PAYLOAD, "whisperx/large-v3")
    assert tx.language == "en"
    assert tx.source_model == "whisperx/large-v3"
    assert len(tx.segments) == 2
    assert tx.segments[0].words[0].text == "Good"
    assert tx.segments[0].words[1].end_s == 2.0


def test_text_joins_segments() -> None:
    tx = WordTranscript.from_payload(PAYLOAD, "whisperx/large-v3")
    assert tx.text == "Good evening. Roll call."


def test_all_words_flattens_in_order_and_drops_untimed() -> None:
    tx = WordTranscript.from_payload(PAYLOAD, "whisperx/large-v3")
    words = tx.all_words()
    # 2 from the first segment + 1 timed from the second (untimed "call." dropped)
    assert [w.text for w in words] == ["Good", "evening.", "Roll"]
