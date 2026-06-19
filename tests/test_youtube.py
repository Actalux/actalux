"""Tests for the shared YouTube helpers (date parsing + channel listing)."""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import patch

from actalux.ingest.youtube import BoardMeeting, list_board_meetings, parse_date_from_title


class TestParseDateFromTitle:
    def test_slash_two_digit_year(self) -> None:
        assert parse_date_from_title("2/19/26 Board of Education Meeting") == "2026-02-19"

    def test_slash_older_year(self) -> None:
        assert parse_date_from_title("10/8/25 Board of Education") == "2025-10-08"

    def test_month_name(self) -> None:
        assert parse_date_from_title("Nov. 13, 2019 BOE Meeting") == "2019-11-13"

    def test_eight_digit(self) -> None:
        assert parse_date_from_title("BOE Meeting 11132019") == "2019-11-13"

    def test_no_date(self) -> None:
        assert parse_date_from_title("Board of Education Meeting") is None


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
