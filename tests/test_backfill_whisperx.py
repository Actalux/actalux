"""Tests for the parallel WhisperX backfill orchestration (no GPU/DB/network).

Fakes stand in for the Modal runners (``spawn``/``collect``) and the WARP download,
so the three phases — concurrent download+spawn, serial reconnect retry, and
collect+stage — are exercised without touching Modal or YouTube.
"""

from __future__ import annotations

import json
from pathlib import Path

from actalux.diarization.backend import SpeakerTimeline
from actalux.errors import ActaluxError
from actalux.glossary.canonicalize import CorrectionRule
from actalux.ingest.youtube import BoardMeeting
from actalux.transcription.backend import TranscriptSegment, Word, WordTranscript
from scripts import backfill_whisperx as bf
from scripts.backfill_whisperx import (
    POOL_DOWNLOAD_RETRIES,
    _Pending,
    collect_and_stage,
    download_and_spawn,
    make_downloader,
    retry_serial,
)
from scripts.transcribe_whisperx import meeting_stem

DIAR_MODEL = "pyannote/speaker-diarization-3.1"
TX_MODEL = "whisperx/large-v3"


def _meeting(vid: str, date: str = "2026-06-09") -> BoardMeeting:
    return BoardMeeting(
        video_id=vid,
        title=f"{date} City Council",
        meeting_date=date,
        url=f"https://www.youtube.com/watch?v={vid}",
    )


class FakeTranscriber:
    """Records spawned bytes + cancels; ``collect`` returns a canned transcript with a mangling."""

    def __init__(self) -> None:
        self.spawned: list[bytes] = []
        self.cancelled: list[object] = []

    def spawn(self, audio_bytes: bytes) -> dict:
        self.spawned.append(audio_bytes)
        return {"kind": "tx", "n": len(self.spawned) - 1}

    def collect(self, _handle: object) -> WordTranscript:
        # "York" is the as-heard mangling; the rule canonicalizes it to "Jeffery Yorg".
        words = [Word("Councilmember", 0.0, 0.5), Word("York", 0.5, 1.0)]
        seg = TranscriptSegment("Councilmember York", 0.0, 1.0, words)
        return WordTranscript([seg], "en", TX_MODEL)

    def cancel(self, handle: object) -> None:
        self.cancelled.append(handle)


class FakeDiarizer:
    def __init__(self) -> None:
        self.spawned: list[bytes] = []
        self.cancelled: list[object] = []

    def spawn(self, audio_bytes: bytes) -> dict:
        self.spawned.append(audio_bytes)
        return {"kind": "di"}

    def collect(self, _handle: object) -> SpeakerTimeline:
        return SpeakerTimeline.from_segments(
            [{"speaker": "SPEAKER_00", "start": 0.0, "end": 1.0}], DIAR_MODEL
        )

    def cancel(self, handle: object) -> None:
        self.cancelled.append(handle)


def _make_download(tmp_path: Path, *, fail_on_pool=frozenset(), fail_always=frozenset()):
    """A fake ``download(video_id, reconnect)`` that writes a stub mp3 (or raises)."""

    def download(video_id: str, reconnect: bool) -> Path:
        if video_id in fail_always:
            raise ActaluxError(f"permanent failure for {video_id}")
        if not reconnect and video_id in fail_on_pool:
            raise ActaluxError(f"pool (no-rotate) failure for {video_id}")
        path = tmp_path / f"{video_id}.mp3"
        path.write_bytes(f"audio-{video_id}".encode())
        return path

    return download


def test_download_and_spawn_all_succeed(tmp_path):
    meetings = [_meeting("a"), _meeting("b"), _meeting("c")]
    tx, di = FakeTranscriber(), FakeDiarizer()
    pending, deferred = download_and_spawn(meetings, _make_download(tmp_path), tx, di, workers=2)
    assert deferred == []
    assert {p.meeting.video_id for p in pending} == {"a", "b", "c"}
    # Both GPU jobs were spawned for every meeting, with the downloaded bytes.
    assert set(tx.spawned) == {b"audio-a", b"audio-b", b"audio-c"}
    assert set(di.spawned) == set(tx.spawned)
    # The transient audio is deleted once spawned (only the text is kept).
    assert list(tmp_path.glob("*.mp3")) == []


def test_pool_failure_defers_then_serial_retry_succeeds(tmp_path):
    meetings = [_meeting("a"), _meeting("b"), _meeting("c")]
    tx, di = FakeTranscriber(), FakeDiarizer()
    # "b" fails on the no-rotate pool path but a rotated retry would succeed.
    download = _make_download(tmp_path, fail_on_pool={"b"})
    pending, deferred = download_and_spawn(meetings, download, tx, di, workers=3)
    assert {p.meeting.video_id for p in pending} == {"a", "c"}
    assert [m.video_id for m in deferred] == ["b"]

    retried = retry_serial(deferred, download, tx, di)
    assert [p.meeting.video_id for p in retried] == ["b"]


def test_permanent_download_failure_is_dropped(tmp_path):
    meetings = [_meeting("a"), _meeting("b")]
    tx, di = FakeTranscriber(), FakeDiarizer()
    download = _make_download(tmp_path, fail_on_pool={"b"}, fail_always={"b"})
    pending, deferred = download_and_spawn(meetings, download, tx, di, workers=2)
    assert {p.meeting.video_id for p in pending} == {"a"}
    assert [m.video_id for m in deferred] == ["b"]
    # The serial retry also fails -> dropped, not raised.
    assert retry_serial(deferred, download, tx, di) == []


def test_collect_and_stage_writes_canonical_artifacts(tmp_path):
    meeting = _meeting("vid42")
    tx, di = FakeTranscriber(), FakeDiarizer()
    rules = [CorrectionRule("york", "Jeffery Yorg", "lexicon")]
    pending = [_Pending(meeting, transcribe_call=None, diarize_call=None)]

    entries = collect_and_stage(
        pending, entity_id=3, rules=rules, transcriber=tx, diarizer=di, out_dir=tmp_path
    )

    stem = meeting_stem(meeting)
    assert len(entries) == 1
    assert entries[0]["source_file"] == f"{stem}.txt"
    assert entries[0]["source_portal"] == "youtube"
    assert entries[0]["video_id"] == "vid42"

    # Canonical (displayed/embedded) text has the corrected spelling...
    canonical = (tmp_path / f"{stem}.txt").read_text()
    assert "Jeffery Yorg" in canonical and "York" not in canonical
    # ...the canonical segment sidecar matches...
    segments = json.loads((tmp_path / f"{stem}.segments.json").read_text())
    assert segments[0]["text"] == "Councilmember Jeffery Yorg"
    # ...and the attribution sidecar keeps the RAW verbatim + the audit + the turns.
    att = json.loads((tmp_path / f"{stem}.attribution.json").read_text())
    assert att["video_id"] == "vid42" and att["entity_id"] == 3
    assert "York" in att["layer"]["raw_text"]
    assert att["layer"]["canonicalizations"]  # at least one fix recorded
    assert att["layer"]["turns"]  # word-level speaker turns present


def test_transcribe_collect_failure_cancels_paired_diarize(tmp_path):
    class Boom(FakeTranscriber):
        def collect(self, _handle: object) -> WordTranscript:
            raise RuntimeError("modal collect failed")

    di = FakeDiarizer()
    di_handle = {"kind": "di", "id": 7}
    pending = [_Pending(_meeting("a"), transcribe_call=None, diarize_call=di_handle)]
    entries = collect_and_stage(
        pending, entity_id=3, rules=[], transcriber=Boom(), diarizer=di, out_dir=tmp_path
    )
    assert entries == []
    assert list(tmp_path.glob("*.txt")) == []
    # The sibling diarize job must be cancelled, not left orphaned.
    assert di.cancelled == [di_handle]


def test_spawn_failure_on_diarize_cancels_transcribe(tmp_path):
    class BoomDiarizer(FakeDiarizer):
        def spawn(self, audio_bytes: bytes) -> dict:
            raise RuntimeError("modal spawn failed")

    tx = FakeTranscriber()
    pending, deferred = download_and_spawn(
        [_meeting("a")], _make_download(tmp_path), tx, BoomDiarizer(), workers=1
    )
    # A spawn failure is dropped (not deferred — a re-download can't fix it)...
    assert pending == [] and deferred == []
    # ...and the already-spawned transcribe job is cancelled, not orphaned.
    assert tx.cancelled == [{"kind": "tx", "n": 0}]
    # The transient audio is still cleaned up on the failure path.
    assert list(tmp_path.glob("*.mp3")) == []


def test_stage_failure_is_skipped(monkeypatch, tmp_path):
    def boom_stage(*_a, **_k):
        raise OSError("disk full")

    monkeypatch.setattr(bf, "stage_meeting", boom_stage)
    pending = [_Pending(_meeting("a"), None, None)]
    entries = collect_and_stage(
        pending,
        entity_id=3,
        rules=[],
        transcriber=FakeTranscriber(),
        diarizer=FakeDiarizer(),
        out_dir=tmp_path,
    )
    assert entries == []  # the whole run is not crashed by one stage failure


def test_collect_and_stage_is_deterministically_ordered(tmp_path):
    # Pending arrives in download-completion order; output must be (meeting_date, video_id).
    pending = [
        _Pending(_meeting("c", "2026-03-01"), None, None),
        _Pending(_meeting("a", "2026-01-01"), None, None),
        _Pending(_meeting("b", "2026-02-01"), None, None),
    ]
    entries = collect_and_stage(
        pending,
        entity_id=3,
        rules=[],
        transcriber=FakeTranscriber(),
        diarizer=FakeDiarizer(),
        out_dir=tmp_path,
    )
    assert [e["video_id"] for e in entries] == ["a", "b", "c"]


def test_make_downloader_pool_never_rotates_warp(monkeypatch, tmp_path):
    """The concurrent pool must download without rotating the (global) WARP egress."""
    calls: list[dict] = []

    def recorder(video_id, dest_dir, *, proxy=None, retries=1, on_retry=None):
        calls.append(
            {"video_id": video_id, "proxy": proxy, "retries": retries, "on_retry": on_retry}
        )
        path = Path(dest_dir) / f"{video_id}.mp3"
        path.write_bytes(b"x")
        return path

    monkeypatch.setattr(bf, "download_audio", recorder)
    download = make_downloader(tmp_path, proxy="socks5h://127.0.0.1:40000")

    download("v1", False)  # pool path
    assert calls[-1]["on_retry"] is None  # never rotates mid-pool
    assert calls[-1]["retries"] == POOL_DOWNLOAD_RETRIES

    download("v2", True)  # serial retry path
    assert calls[-1]["on_retry"] is bf.reconnect_warp  # rotates between attempts
    assert calls[-1]["retries"] == bf.WARP_DOWNLOAD_RETRIES


def test_make_downloader_local_has_no_proxy_no_rotation(monkeypatch, tmp_path):
    calls: list[dict] = []

    def recorder(video_id, dest_dir, *, proxy=None, retries=1, on_retry=None):
        calls.append({"proxy": proxy, "retries": retries, "on_retry": on_retry})
        path = Path(dest_dir) / f"{video_id}.mp3"
        path.write_bytes(b"x")
        return path

    monkeypatch.setattr(bf, "download_audio", recorder)
    download = make_downloader(tmp_path, proxy=None)
    download("v1", True)  # even "reconnect" is a no-op without a proxy
    assert calls[-1] == {"proxy": None, "retries": 1, "on_retry": None}
