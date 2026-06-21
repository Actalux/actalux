"""Tests for the shared YouTube helpers (date parsing + channel listing)."""

from __future__ import annotations

import subprocess
from types import SimpleNamespace
from unittest.mock import patch

import pytest

from actalux.errors import IngestError
from actalux.ingest.bodies import COUNCIL, PLAN_COMMISSION
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

    def test_dash_date_city_council(self) -> None:
        # City of Clayton titles use MM-DD-YYYY (dashes) — must parse, not skip.
        assert parse_date_from_title("06-09-2026 City Council Meeting") == "2026-06-09"
        assert parse_date_from_title("05-26-2026 Clayton City Council Meeting") == "2026-05-26"

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

    def test_one_tab_missing_is_skipped(self) -> None:
        # A channel that never livestreams has no /streams tab (yt-dlp errors);
        # discovery must skip it and still list /videos rather than aborting.
        def fake_run(cmd, **kw):
            if cmd[-1].endswith("/streams"):
                raise subprocess.CalledProcessError(1, cmd, stderr="tab not available")
            return SimpleNamespace(stdout="v1|06-09-2026 City Council Meeting\n", returncode=0)

        with patch("actalux.ingest.youtube.subprocess.run", side_effect=fake_run):
            meetings = list_board_meetings(title_filter=COUNCIL.title_filter)
        assert [m.video_id for m in meetings] == ["v1"]

    def test_all_tabs_failing_raises(self) -> None:
        def always_fail(cmd, **kw):
            raise subprocess.CalledProcessError(1, cmd, stderr="blocked")

        with patch("actalux.ingest.youtube.subprocess.run", side_effect=always_fail):
            with pytest.raises(IngestError):
                list_board_meetings()

    def test_custom_title_filter_selects_one_body(self) -> None:
        # The city channel hosts many bodies; the council filter keeps only council
        # meetings — including the old "Board of Aldermen"/"BOA" naming — and drops
        # the rest. Board of ADJUSTMENT (a different body) must NOT be swept in.
        stdout = (
            "c1|06-09-2026 City Council Meeting\n"
            "c2|04-17-2026 City Council Strategic Discussion Session\n"
            "c3|07-22-2025 Board of Alderman Meeting\n"  # old name -> council
            "c4|09-19-2025 BOA Strategic Discussion Session\n"  # old abbrev -> council
            "p1|06-15-2026 PC/ARB Meeting\n"  # plan commission -> dropped
            "b1|06-04-2026 Board of Adjustment\n"  # different body -> dropped
        )
        with patch("actalux.ingest.youtube.subprocess.run") as run:
            run.return_value = SimpleNamespace(stdout=stdout, returncode=0)
            meetings = list_board_meetings(title_filter=COUNCIL.title_filter)
        assert {m.video_id for m in meetings} == {"c1", "c2", "c3", "c4"}

    def test_plan_commission_filter_handles_naming_variants(self) -> None:
        # PC/ARB titles vary: "PC/ARB", "PC-ARB", "Plan Commission", older
        # "Planning Commission", and a real typo "Plan Commision". Council and
        # Board of Adjustment must NOT be swept in.
        stdout = (
            "p1|06-15-2026 PC/ARB Meeting\n"
            "p2|PC-ARB 06/01/2026\n"
            "p3|03-04-2024 Plan Commision/ARB Meeting\n"  # typo "Commision"
            "p4|December 19, 2016 Planning Commission/ARB Meeting\n"  # older naming
            "c1|06-09-2026 City Council Meeting\n"  # council -> dropped
            "b1|06-04-2026 Board of Adjustment\n"  # different body -> dropped
        )
        with patch("actalux.ingest.youtube.subprocess.run") as run:
            run.return_value = SimpleNamespace(stdout=stdout, returncode=0)
            meetings = list_board_meetings(title_filter=PLAN_COMMISSION.title_filter)
        assert {m.video_id for m in meetings} == {"p1", "p2", "p3", "p4"}


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
