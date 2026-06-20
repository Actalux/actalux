"""Tests for the shared YouTube helpers (date parsing + channel listing)."""

from __future__ import annotations

import subprocess
from types import SimpleNamespace
from unittest.mock import patch

import pytest

from actalux.errors import IngestError
from actalux.ingest.youtube import (
    BoardMeeting,
    download_audio,
    list_board_meetings,
    parse_date_from_title,
)


class TestParseDateFromTitle:
    def test_slash_two_digit_year(self) -> None:
        assert parse_date_from_title("2/19/26 Board of Education Meeting") == "2026-02-19"

    def test_slash_older_year(self) -> None:
        assert parse_date_from_title("10/8/25 Board of Education") == "2025-10-08"

    def test_slash_four_digit_year(self) -> None:
        assert parse_date_from_title("12/13/2023 Board of Education Meeting") == "2023-12-13"

    def test_month_name(self) -> None:
        assert parse_date_from_title("Nov. 13, 2019 BOE Meeting") == "2019-11-13"

    def test_full_month_name(self) -> None:
        assert parse_date_from_title("August 17, 2022 Board of Education Meeting") == "2022-08-17"
        assert parse_date_from_title("June 1, 2022 Board of Education Meeting") == "2022-06-01"

    def test_eight_digit(self) -> None:
        assert parse_date_from_title("BOE Meeting 11132019") == "2019-11-13"

    def test_no_date(self) -> None:
        assert parse_date_from_title("Board of Education Meeting") is None

    def test_implausible_year_rejected(self) -> None:
        # A mis-parsed title must not yield a bogus far-future date.
        assert parse_date_from_title("3/31/3021 Board of Education Meeting") is None


class TestListBoardMeetings:
    def _run(self, stdout: str):
        with patch("actalux.ingest.youtube.subprocess.run") as run:
            run.return_value = SimpleNamespace(stdout=stdout, returncode=0)
            return list_board_meetings()

    def test_filters_to_board_meetings_and_parses_dates(self) -> None:
        stdout = (
            "id1|2/19/26 Board of Education Meeting\n"
            "id2|Clayton High School Pep Rally\n"  # not a board meeting -> dropped
            "id3|BOE Meeting 10/8/25\n"
        )
        meetings = self._run(stdout)
        assert [m.video_id for m in meetings] == ["id1", "id3"]  # sorted newest first
        assert all(isinstance(m, BoardMeeting) for m in meetings)
        assert meetings[0].meeting_date == "2026-02-19"
        assert meetings[1].meeting_date == "2025-10-08"
        assert meetings[0].url == "https://www.youtube.com/watch?v=id1"

    def test_undated_meeting_kept_with_empty_date(self) -> None:
        meetings = self._run("idX|Board of Education Special Meeting\n")
        assert len(meetings) == 1
        assert meetings[0].meeting_date == ""

    def test_blank_lines_ignored(self) -> None:
        assert self._run("\n\n") == []


class TestDownloadAudioRetry:
    def test_retries_then_succeeds_rotating_egress(self, tmp_path) -> None:
        calls = {"n": 0}

        def fake_run(cmd, **kw):
            calls["n"] += 1
            if calls["n"] == 1:  # first WARP IP is flagged
                raise subprocess.CalledProcessError(
                    1, cmd, stderr="Sign in to confirm you're not a bot"
                )
            (tmp_path / "vid.mp3").write_text("audio")  # second IP is clean
            return SimpleNamespace(returncode=0)

        rotated = {"n": 0}
        with patch("actalux.ingest.youtube.subprocess.run", side_effect=fake_run):
            out = download_audio(
                "vid",
                tmp_path,
                proxy="socks5h://x",
                retries=4,
                on_retry=lambda: rotated.__setitem__("n", rotated["n"] + 1),
            )
        assert out == tmp_path / "vid.mp3"
        assert calls["n"] == 2  # failed once, succeeded on the rotated IP
        assert rotated["n"] == 1  # rotated the egress once between attempts

    def test_raises_after_exhausting_retries(self, tmp_path) -> None:
        def always_blocked(cmd, **kw):
            raise subprocess.CalledProcessError(1, cmd, stderr="not a bot")

        rotated = {"n": 0}
        with patch("actalux.ingest.youtube.subprocess.run", side_effect=always_blocked):
            with pytest.raises(IngestError):
                download_audio(
                    "vid",
                    tmp_path,
                    proxy="socks5h://x",
                    retries=3,
                    on_retry=lambda: rotated.__setitem__("n", rotated["n"] + 1),
                )
        assert rotated["n"] == 2  # rotated between each of the 3 attempts (not after the last)
