"""Tests for scripts/restore_source_urls.py planning logic.

All tests use synthetic manifest entries and DB row dicts — no DB, no network.
The planning functions are pure: they take manifest entries + rows and return a
RestorePlan, so the unique-key/duplicate-key/corroboration branches are testable
in isolation.
"""

from __future__ import annotations

import json
from pathlib import Path

import scripts.restore_source_urls as mod
from scripts.restore_source_urls import ManifestEntry

_BUCKET = "https://abc.supabase.co/storage/v1/object/public/documents/budget.pdf"


def _entry(
    source_file: str,
    source_url: str,
    *,
    portal: str = "diligent",
) -> ManifestEntry:
    return ManifestEntry(
        source_file=source_file,
        source_url=source_url,
        source_portal=portal,
    )


def _row(
    *,
    id: int = 1,
    source_file: str,
    source_url: str = "",
    portal: str = "diligent",
    content_hash: str = "",
) -> dict:
    return {
        "id": id,
        "source_file": source_file,
        "source_url": source_url,
        "source_portal": portal,
        "content_hash": content_hash,
    }


class TestLoadManifestEntries:
    def test_reads_list_manifests_and_skips_incomplete(self, tmp_path: Path) -> None:
        (tmp_path / "a.json").write_text(
            json.dumps(
                [
                    {"source_file": "x.pdf", "source_url": "u1", "source_portal": "diligent"},
                    {"source_file": "y.pdf"},  # no url -> skipped
                    {"source_url": "u2"},  # no file -> skipped
                ]
            )
        )
        # A non-list manifest is ignored entirely.
        (tmp_path / "b.json").write_text(json.dumps({"not": "a list"}))

        entries = mod.load_manifest_entries(tmp_path)

        assert [e.source_file for e in entries] == ["x.pdf"]
        assert entries[0].source_url == "u1"


class TestBuildUniqueOriginMap:
    def test_unique_kept_duplicate_dropped(self) -> None:
        entries = [
            _entry("unique.pdf", "uU"),
            _entry("dup.pdf", "uA"),
            _entry("dup.pdf", "uB"),  # same file, different url -> ambiguous
        ]
        unique = mod.build_unique_origin_map(entries)
        assert set(unique) == {"unique.pdf"}
        assert unique["unique.pdf"].source_url == "uU"


class TestComputeLocalHashes:
    def test_hashes_present_files_and_omits_missing(self, monkeypatch, tmp_path: Path) -> None:
        # Stub the parse+hash path so the test does not depend on a real PDF.
        monkeypatch.setattr(mod, "parse_file", lambda p: f"PARSED:{p.name}")
        monkeypatch.setattr(mod, "content_hash", lambda t: f"H:{t}")
        (tmp_path / "present.pdf").write_text("x")
        # absent.pdf intentionally not written

        hashes = mod.compute_local_hashes({"present.pdf", "absent.pdf"}, tmp_path)

        assert hashes == {"present.pdf": "H:PARSED:present.pdf"}


class TestPlanRestore:
    # The local-file content hash corroborates a row; the manifests carry none,
    # so it always comes from compute_local_hashes (here supplied synthetically).
    def test_unique_corroborated_is_restored(self) -> None:
        entries = [_entry("unique.pdf", "https://diligent.com/document/guid", portal="diligent")]
        rows = [
            _row(
                id=5,
                source_file="unique.pdf",
                source_url=_BUCKET,
                portal="diligent",
                content_hash="HASH_A",
            )
        ]

        plan = mod.plan_restore(rows, entries, {"unique.pdf": "HASH_A"})

        assert len(plan.to_apply) == 1
        item = plan.to_apply[0]
        assert item["id"] == "5"
        assert item["new_url"] == "https://diligent.com/document/guid"
        assert plan.review == []

    def test_empty_source_url_is_restored(self) -> None:
        entries = [_entry("u.pdf", "https://origin", portal="diligent")]
        rows = [
            _row(id=7, source_file="u.pdf", source_url="", portal="diligent", content_hash="HASH_A")
        ]

        plan = mod.plan_restore(rows, entries, {"u.pdf": "HASH_A"})

        assert [c["id"] for c in plan.to_apply] == ["7"]

    def test_duplicate_key_goes_to_review(self) -> None:
        entries = [
            _entry("dup.pdf", "https://origin/a", portal="diligent"),
            _entry("dup.pdf", "https://origin/b", portal="diligent"),
        ]
        rows = [_row(id=9, source_file="dup.pdf", source_url=_BUCKET, portal="diligent")]

        plan = mod.plan_restore(rows, entries, {})

        assert plan.to_apply == []
        assert len(plan.review) == 1
        assert plan.review[0]["reason"] == "duplicate-manifest-key"

    def test_portal_mismatch_blocks_restore(self) -> None:
        # Unique key, but the manifest portal disagrees with the row -> review.
        entries = [_entry("u.pdf", "https://origin", portal="claytonschools")]
        rows = [
            _row(
                id=11,
                source_file="u.pdf",
                source_url=_BUCKET,
                portal="diligent",
                content_hash="HASH_A",
            )
        ]

        plan = mod.plan_restore(rows, entries, {"u.pdf": "HASH_A"})

        assert plan.to_apply == []
        assert len(plan.review) == 1
        assert plan.review[0]["reason"] == "portal-mismatch"

    def test_content_hash_mismatch_blocks(self) -> None:
        # Local file hash disagrees with the row's stored hash -> never restore.
        entries = [_entry("u.pdf", "https://origin", portal="diligent")]
        rows = [
            _row(
                id=12,
                source_file="u.pdf",
                source_url=_BUCKET,
                portal="diligent",
                content_hash="HASH_B",
            )
        ]

        plan = mod.plan_restore(rows, entries, {"u.pdf": "HASH_A"})

        assert plan.to_apply == []
        assert plan.review[0]["reason"] == "content-hash-mismatch"

    def test_missing_local_file_blocks(self) -> None:
        # Unique + portal match, but no local file to hash -> cannot corroborate.
        entries = [_entry("u.pdf", "https://origin", portal="diligent")]
        rows = [
            _row(
                id=16,
                source_file="u.pdf",
                source_url=_BUCKET,
                portal="diligent",
                content_hash="HASH_A",
            )
        ]

        plan = mod.plan_restore(rows, entries, {})  # no local hash for u.pdf

        assert plan.to_apply == []
        assert plan.review[0]["reason"] == "local-file-missing"

    def test_no_manifest_origin_left_unchanged(self) -> None:
        rows = [_row(id=14, source_file="orphan.pdf", source_url=_BUCKET, portal="diligent")]

        plan = mod.plan_restore(rows, entries=[], local_hashes={})

        assert plan.to_apply == []
        assert plan.review == []
        assert len(plan.unchanged) == 1
        assert plan.unchanged[0]["reason"] == "no-manifest-origin"

    def test_real_origin_url_is_not_touched(self) -> None:
        # Row already has a genuine origin (not bucket/empty) -> never a candidate.
        entries = [_entry("u.pdf", "https://manifest-origin", portal="diligent")]
        rows = [
            _row(id=15, source_file="u.pdf", source_url="https://real-origin", portal="diligent")
        ]

        plan = mod.plan_restore(rows, entries, {"u.pdf": "HASH_A"})

        assert plan.to_apply == []
        assert plan.review == []
        assert plan.unchanged[0]["reason"] == "already-has-origin"


class TestWriteReviewCsv:
    def test_writes_header_and_rows(self, tmp_path: Path) -> None:
        out = tmp_path / "review.csv"
        mod.write_review_csv(
            [
                {
                    "id": "9",
                    "source_file": "dup.pdf",
                    "old_url": _BUCKET,
                    "portal": "diligent",
                    "reason": "duplicate-manifest-key",
                }
            ],
            out,
        )
        text = out.read_text()
        assert text.splitlines()[0] == "id,source_file,old_url,portal,reason"
        assert "dup.pdf" in text
        assert "duplicate-manifest-key" in text

    def test_empty_review_writes_header_only(self, tmp_path: Path) -> None:
        out = tmp_path / "review.csv"
        mod.write_review_csv([], out)
        assert out.read_text().strip() == "id,source_file,old_url,portal,reason"
