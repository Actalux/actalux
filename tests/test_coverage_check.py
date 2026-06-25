"""Tests for the minutes-coverage gap finder."""

from __future__ import annotations

import importlib.util
import sys
from datetime import date, timedelta
from pathlib import Path

# The script lives in scripts/ (not an installed package); load it by path.
_path = Path(__file__).resolve().parent.parent / "scripts" / "check_minutes_coverage.py"
_spec = importlib.util.spec_from_file_location("check_minutes_coverage", _path)
cmc = importlib.util.module_from_spec(_spec)
# Register before exec so dataclass annotation resolution can find the module.
sys.modules[_spec.name] = cmc
_spec.loader.exec_module(cmc)

TODAY = date(2026, 6, 24)
NAMES = {2: "Clayton City Council"}


def _row(entity_id: int, doc_type: str, days_ago: int) -> dict:
    return {
        "entity_id": entity_id,
        "document_type": doc_type,
        "meeting_date": (TODAY - timedelta(days=days_ago)).isoformat(),
        "replaces_id": None,
    }


def _gaps(rows):
    return cmc.find_coverage_gaps(rows, NAMES, TODAY, lag_days=35, window_days=180)


def test_meeting_in_window_without_minutes_is_flagged():
    gaps = _gaps([_row(2, "agenda", 60)])
    assert len(gaps) == 1
    assert gaps[0].missing == [(TODAY - timedelta(days=60)).isoformat()]


def test_meeting_with_minutes_is_not_flagged():
    rows = [_row(2, "agenda", 60), _row(2, "minutes", 60)]
    assert _gaps(rows) == []


def test_too_recent_meeting_is_within_lag_and_skipped():
    # 10 days ago: minutes legitimately not published yet.
    assert _gaps([_row(2, "transcript", 10)]) == []


def test_too_old_meeting_is_outside_window():
    # 300 days ago: historical backlog, not chased.
    assert _gaps([_row(2, "agenda", 300)]) == []


def test_multiple_missing_dates_sorted_newest_first():
    rows = [_row(2, "agenda", 40), _row(2, "transcript", 90)]
    gaps = _gaps(rows)
    assert gaps[0].missing == [
        (TODAY - timedelta(days=40)).isoformat(),
        (TODAY - timedelta(days=90)).isoformat(),
    ]
