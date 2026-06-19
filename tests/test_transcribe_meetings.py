"""Tests for the transcription orchestrator's pure selection/staging logic."""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import patch

from actalux.ingest.youtube import BoardMeeting
from scripts.transcribe_meetings import manifest_entry, safe_stem, select_meetings


def _meeting(vid: str, date: str, title: str | None = None) -> BoardMeeting:
    title = title or f"{date} Board of Education Meeting"
    return BoardMeeting(vid, title, date, f"https://www.youtube.com/watch?v={vid}")


def _args(**kw) -> SimpleNamespace:
    base = dict(
        video_id=None, title=None, date=None, since=None, limit=None, force=False, proxy=None
    )
    base.update(kw)
    return SimpleNamespace(**base)


class TestSafeStem:
    def test_strips_path_chars(self) -> None:
        assert safe_stem('6/3/26 Board: "Meeting"') == "6_3_26 Board_ _Meeting_"


class TestManifestEntry:
    def test_youtube_transcript_shape(self) -> None:
        entry = manifest_entry(_meeting("abc", "2026-06-03"), "6-3-26 BOE.txt")
        assert entry["source_portal"] == "youtube"
        assert entry["document_type"] == "transcript"
        assert entry["video_id"] == "abc"
        assert entry["meeting_date"] == "2026-06-03"
        assert entry["date_source"] == "filename"

    def test_undated_meeting_is_manual(self) -> None:
        entry = manifest_entry(_meeting("abc", "", title="BOE Special"), "BOE Special.txt")
        assert entry["date_source"] == "manual"
        assert entry["meeting_date"] == ""


class TestSelectMeetings:
    def test_explicit_video_id(self) -> None:
        out = select_meetings(
            _args(video_id="vid9", date="2026-06-03", title="June Meeting"), None, set()
        )
        assert len(out) == 1
        assert out[0].video_id == "vid9"
        assert out[0].meeting_date == "2026-06-03"

    def test_discover_skips_dates_already_in_db(self, tmp_path) -> None:
        staged = _meeting("a", "2026-06-03")
        fresh = _meeting("b", "2026-05-13")
        with patch("scripts.transcribe_meetings.list_board_meetings", return_value=[staged, fresh]):
            out = select_meetings(_args(), tmp_path, {"2026-06-03"})
        assert [m.video_id for m in out] == ["b"]  # already-ingested date 'a' skipped

    def test_discover_skips_already_staged_file(self, tmp_path) -> None:
        staged = _meeting("a", "2026-06-03")
        fresh = _meeting("b", "2026-05-13")
        (tmp_path / f"{safe_stem(staged.title)}.txt").write_text("done")
        with patch("scripts.transcribe_meetings.list_board_meetings", return_value=[staged, fresh]):
            out = select_meetings(_args(), tmp_path, set())
        assert [m.video_id for m in out] == ["b"]  # local file 'a' skipped

    def test_discover_force_keeps_all(self, tmp_path) -> None:
        m = _meeting("a", "2026-06-03")
        (tmp_path / f"{safe_stem(m.title)}.txt").write_text("done")
        with patch("scripts.transcribe_meetings.list_board_meetings", return_value=[m]):
            out = select_meetings(_args(force=True), tmp_path, {"2026-06-03"})
        assert [x.video_id for x in out] == ["a"]

    def test_discover_since_and_limit(self, tmp_path) -> None:
        meetings = [
            _meeting("a", "2026-06-03"),
            _meeting("b", "2026-05-13"),
            _meeting("c", "2026-01-01"),
        ]
        with patch("scripts.transcribe_meetings.list_board_meetings", return_value=meetings):
            out = select_meetings(_args(since="2026-05-01", limit=1), tmp_path, set())
        assert [m.video_id for m in out] == ["a"]  # 'c' filtered by since, limit caps to 1
