"""Unit tests for routine PR fingerprint matching (offline)."""

from scripts.routine import find_open_duplicate_pr as dedupe


def test_routine_branch_regex() -> None:
    assert dedupe._ROUTINE_BRANCH.match("routine/abstraction-police-2026-11-10")
    assert not dedupe._ROUTINE_BRANCH.match("cursor/foo-1867")


def test_matches_fingerprint_by_path_overlap() -> None:
    fp = {
        "paths": [
            "src/actalux/ingest/local_effort_xml.py",
            "src/actalux/ingest/ssrs_xml.py",
        ],
        "title_substrings": ["dese"],
    }
    pull = {
        "number": 1,
        "title": "unrelated",
        "body": "",
        "head": {"ref": "routine/abstraction-police-2026-09-29"},
        "html_url": "https://example.test/pull/1",
    }

    files = [
        "src/actalux/ingest/local_effort_xml.py",
        "src/actalux/ingest/ssrs_xml.py",
    ]

    def fake_pr_files(repo: str, token: str, number: int) -> list[str]:
        return files

    orig = dedupe._pr_files
    dedupe._pr_files = fake_pr_files  # type: ignore[method-assign]
    try:
        assert dedupe._matches_fingerprint(pull, fp, "Actalux/actalux", "token")
    finally:
        dedupe._pr_files = orig  # type: ignore[method-assign]
