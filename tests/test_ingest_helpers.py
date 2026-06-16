"""Tests for ingest orchestration helpers."""

from datetime import date
from pathlib import Path

import pytest

import scripts.ingest as ingest
from scripts.ingest import (
    _find_existing_document,
    _ingest_with_dedup,
    normalize_source_ref,
    resolve_entity_id,
    subject_header,
)


class TestSubjectHeader:
    """Canva curriculum maps get their subject restored; nothing else does."""

    def test_canva_map_returns_subject_without_artifact(self) -> None:
        header = subject_header(
            "canva_1-5_Spanish_Curriculum_Map.txt", "canva 1 5 Spanish Curriculum Map"
        )
        assert header == "1 5 Spanish Curriculum Map"

    def test_canva_map_case_insensitive(self) -> None:
        assert subject_header("canva_K-5_Art_Curriculum_Map.txt", "canva K 5 Art Curriculum Map")

    def test_non_canva_file_returns_empty(self) -> None:
        header = subject_header("May 15 2024 Meeting Minutes.pdf", "May 15 2024 Meeting Minutes")
        assert header == ""

    def test_canva_non_map_returns_empty(self) -> None:
        # A canva file that isn't a curriculum map is not in scope.
        assert subject_header("canva_Board_Photo.txt", "canva Board Photo") == ""


class TestResolveEntityId:
    """Ingest must resolve a real entity_id up front; a bad path aborts the run."""

    def test_resolves_known_path(self, monkeypatch) -> None:
        monkeypatch.setattr(
            ingest, "get_entity_by_path", lambda _c, s, p, b: {"id": 7} if (s, p, b) else None
        )
        assert resolve_entity_id(object(), "mo/clayton/schools") == 7

    def test_leading_slash_tolerated(self, monkeypatch) -> None:
        monkeypatch.setattr(ingest, "get_entity_by_path", lambda _c, *_p: {"id": 7})
        assert resolve_entity_id(object(), "/mo/clayton/schools") == 7

    def test_malformed_path_aborts(self) -> None:
        with pytest.raises(SystemExit):
            resolve_entity_id(object(), "clayton")

    def test_unknown_entity_aborts(self, monkeypatch) -> None:
        monkeypatch.setattr(ingest, "get_entity_by_path", lambda _c, *_p: None)
        with pytest.raises(SystemExit):
            resolve_entity_id(object(), "mo/nowhere/schools")


class TestNormalizeSourceRef:
    """The stable id is the canonical origin URL, query/fragment stripped."""

    def test_strips_query_and_fragment(self) -> None:
        # Canva share URLs carry rotating utm_* params; the design path is stable.
        url = (
            "https://www.canva.com/design/DAGipd2ozhE/ko0kSwso/view"
            "?utm_content=DAGipd2ozhE&utm_campaign=designshare#x"
        )
        assert normalize_source_ref(url) == (
            "https://www.canva.com/design/DAGipd2ozhE/ko0kSwso/view"
        )

    def test_lowercases_scheme_and_host_and_strips_trailing_slash(self) -> None:
        url = "HTTPS://Example.TEST/document/abc/"
        assert normalize_source_ref(url) == "https://example.test/document/abc"

    def test_diligent_guid_path_preserved(self) -> None:
        url = "https://claytonschools.community.diligentoneplatform.com/document/f8c12dca"
        assert normalize_source_ref(url) == url

    def test_empty_url_returns_empty(self) -> None:
        assert normalize_source_ref("") == ""

    def test_unparseable_url_returns_empty(self) -> None:
        # No netloc -> no stable origin to key on; caller falls back to filename.
        assert normalize_source_ref("not a url") == ""


class _CallRecorder:
    """Records which dedup lookups ran and in what order."""

    def __init__(self, hits: dict[str, dict | None]) -> None:
        self._hits = hits
        self.order: list[str] = []

    def by_source_ref(self, _client, _portal, source_ref):
        self.order.append("source_ref")
        return self._hits.get("source_ref") if source_ref else None

    def by_content_hash(self, _client, content_hash, _portal):
        self.order.append("content_hash")
        return self._hits.get("content_hash") if content_hash else None

    def by_source(self, _client, _filename):
        self.order.append("source_file")
        return self._hits.get("source_file")


def _patch_lookups(monkeypatch, recorder: _CallRecorder) -> None:
    monkeypatch.setattr(ingest, "find_document_by_source_ref", recorder.by_source_ref)
    monkeypatch.setattr(ingest, "find_document_by_content_hash", recorder.by_content_hash)
    monkeypatch.setattr(ingest, "find_document_by_source", recorder.by_source)


class TestFindExistingDocument:
    """Dedup must prefer the stable external id, then content hash, then filename."""

    def test_source_ref_match_short_circuits(self, monkeypatch) -> None:
        rec = _CallRecorder({"source_ref": {"id": 1}, "content_hash": {"id": 2}})
        _patch_lookups(monkeypatch, rec)

        existing = _find_existing_document(
            object(),
            source_ref="https://example.test/document/abc",
            file_hash="deadbeef",
            portal="diligent",
            filename="resolution.pdf",
        )

        assert existing == {"id": 1}
        # source_ref hit ends the search before content_hash/filename are queried.
        assert rec.order == ["source_ref"]

    def test_falls_through_to_content_hash(self, monkeypatch) -> None:
        rec = _CallRecorder({"source_ref": None, "content_hash": {"id": 2}})
        _patch_lookups(monkeypatch, rec)

        existing = _find_existing_document(
            object(),
            source_ref="https://example.test/document/abc",
            file_hash="deadbeef",
            portal="diligent",
            filename="resolution.pdf",
        )

        assert existing == {"id": 2}
        assert rec.order == ["source_ref", "content_hash"]

    def test_falls_through_to_filename(self, monkeypatch) -> None:
        rec = _CallRecorder({"source_ref": None, "content_hash": None, "source_file": {"id": 3}})
        _patch_lookups(monkeypatch, rec)

        existing = _find_existing_document(
            object(),
            source_ref="https://example.test/document/abc",
            file_hash="deadbeef",
            portal="diligent",
            filename="resolution.pdf",
        )

        assert existing == {"id": 3}
        assert rec.order == ["source_ref", "content_hash", "source_file"]

    def test_empty_source_ref_skips_first_tier(self, monkeypatch) -> None:
        # Legacy/origin-less docs have no source_ref: go straight to content_hash.
        rec = _CallRecorder({"content_hash": {"id": 2}})
        _patch_lookups(monkeypatch, rec)

        existing = _find_existing_document(
            object(),
            source_ref="",
            file_hash="deadbeef",
            portal="manual",
            filename="resolution.pdf",
        )

        assert existing == {"id": 2}
        assert rec.order == ["content_hash"]


class TestUnchangedSkipBackfillsSourceRef:
    """Re-ingesting an unchanged legacy row must self-heal its missing source_ref.

    Otherwise a later PDF/HTML twin (different content + filename) of a row that
    predates source_ref can never match by the stable id, and a second current
    row recurs -- the exact regression A1 #4 exists to close.
    """

    def _setup(self, monkeypatch, existing: dict) -> list[tuple]:
        backfills: list[tuple] = []
        monkeypatch.setattr(ingest, "parse_file", lambda _p: "body text")
        monkeypatch.setattr(ingest, "content_hash", lambda _t: "samehash")
        monkeypatch.setattr(ingest, "_find_existing_document", lambda *_a, **_k: existing)
        monkeypatch.setattr(ingest, "update_document_checked", lambda *_a, **_k: None)
        monkeypatch.setattr(
            ingest,
            "backfill_document_source_ref",
            lambda _c, doc_id, ref: backfills.append((doc_id, ref)),
        )
        return backfills

    def test_backfills_when_legacy_row_lacks_source_ref(self, monkeypatch) -> None:
        backfills = self._setup(
            monkeypatch, {"id": 5, "content_hash": "samehash", "source_ref": ""}
        )

        result = _ingest_with_dedup(
            client=object(),
            path=Path("Resolution.pdf"),
            meeting_date=date(2020, 8, 4),
            meeting_title="Resolution",
            config=object(),
            source_url="https://example.test/document/abc",
            source_portal="diligent",
        )

        assert result["status"] == "skipped"
        assert backfills == [(5, "https://example.test/document/abc")]

    def test_no_backfill_when_source_ref_present(self, monkeypatch) -> None:
        backfills = self._setup(
            monkeypatch,
            {
                "id": 5,
                "content_hash": "samehash",
                "source_ref": "https://example.test/document/abc",
            },
        )

        result = _ingest_with_dedup(
            client=object(),
            path=Path("Resolution.pdf"),
            meeting_date=date(2020, 8, 4),
            meeting_title="Resolution",
            config=object(),
            source_url="https://example.test/document/abc",
            source_portal="diligent",
        )

        assert result["status"] == "skipped"
        assert backfills == []

    def test_no_backfill_when_no_source_url(self, monkeypatch) -> None:
        backfills = self._setup(
            monkeypatch, {"id": 5, "content_hash": "samehash", "source_ref": ""}
        )

        result = _ingest_with_dedup(
            client=object(),
            path=Path("Resolution.pdf"),
            meeting_date=date(2020, 8, 4),
            meeting_title="Resolution",
            config=object(),
            source_portal="diligent",
        )

        assert result["status"] == "skipped"
        assert backfills == []
