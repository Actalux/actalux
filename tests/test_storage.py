"""Tests for actalux.web.storage public-bucket URL helpers.

All tests are env-free: ``build_stored_file_url`` is pure, and ``stored_file_url``
is exercised with an explicit Config so no environment variables are required.
"""

from __future__ import annotations

from dataclasses import dataclass

from actalux.web.storage import BUCKET, build_stored_file_url, stored_file_url

_BASE = "https://abc.supabase.co"
_PREFIX = f"{_BASE}/storage/v1/object/public/{BUCKET}"


class TestBuildStoredFileUrl:
    def test_plain_filename(self) -> None:
        url = build_stored_file_url(_BASE, "report.pdf")
        assert url == f"{_PREFIX}/report.pdf"

    def test_encodes_spaces(self) -> None:
        url = build_stored_file_url(_BASE, "April 24 Meeting.pdf")
        assert url == f"{_PREFIX}/April%2024%20Meeting.pdf"
        assert " " not in url

    def test_encodes_comma_and_specials(self) -> None:
        url = build_stored_file_url(_BASE, "April 24, 2024 (final) #2.pdf")
        # comma, parens, and hash must all be percent-encoded in a path segment
        assert "," not in url
        assert "(" not in url and ")" not in url
        assert "#" not in url
        assert url == (f"{_PREFIX}/April%2024%2C%202024%20%28final%29%20%232.pdf")

    def test_does_not_double_encode(self) -> None:
        # A key that already contains a percent-escape is left intact ("%" safe).
        already = "April%2024.pdf"
        assert build_stored_file_url(_BASE, already) == f"{_PREFIX}/{already}"

    def test_keeps_slash_in_nested_key(self) -> None:
        url = build_stored_file_url(_BASE, "minutes/April 24.pdf")
        assert url == f"{_PREFIX}/minutes/April%2024.pdf"

    def test_trailing_slash_on_base_tolerated(self) -> None:
        assert build_stored_file_url(_BASE + "/", "x.pdf") == f"{_PREFIX}/x.pdf"

    def test_empty_source_file_returns_empty(self) -> None:
        assert build_stored_file_url(_BASE, "") == ""


@dataclass(frozen=True)
class _Cfg:
    """Minimal stand-in carrying just the field stored_file_url reads."""

    supabase_url: str = _BASE


class TestStoredFileUrl:
    def test_uses_config_base(self) -> None:
        url = stored_file_url("April 24 Meeting.pdf", cfg=_Cfg())
        assert url == f"{_PREFIX}/April%2024%20Meeting.pdf"

    def test_empty_short_circuits_without_config(self) -> None:
        # No cfg passed and none needed: empty key returns "" before any load.
        assert stored_file_url("") == ""
