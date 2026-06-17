"""Tests for the pure detection functions in scripts/audit_corpus.py.

All tests use synthetic rows (dicts) — no DB, no network. This is possible
because every detection function is pure: it takes a row dict (or a list of
them) and returns a result without any I/O.

The test for run_audit() exercises the full aggregation path so that the
per-category lists are confirmed to be wired correctly.
"""

from __future__ import annotations

import scripts.audit_corpus as mod

# ---------------------------------------------------------------------------
# Helpers — synthetic row factories
# ---------------------------------------------------------------------------


def _row(
    *,
    id: int = 1,
    meeting_date: str = "2024-04-10",
    created_at: str = "2024-02-01T12:00:00Z",
    date_source: str | None = None,
    source_url: str = "https://diligent.com/doc/abc",
    source_portal: str = "diligent",
    document_type: str = "minutes",
    entity_id: int | None = 1,
    content_hash: str = "aabbcc",
    meeting_title: str = "April 10, 2024 Meeting Minutes",
    source_file: str = "April 10, 2024 BOE MM signed.pdf",
    content: str = "The board approved the FY2024 budget.",
) -> dict:
    """Return a minimal synthetic document row."""
    row = {
        "id": id,
        "meeting_date": meeting_date,
        "created_at": created_at,
        "source_url": source_url,
        "source_portal": source_portal,
        "document_type": document_type,
        "entity_id": entity_id,
        "content_hash": content_hash,
        "meeting_title": meeting_title,
        "source_file": source_file,
        "content": content,
    }
    if date_source is not None:
        row["date_source"] = date_source
    return row


# ---------------------------------------------------------------------------
# is_suspected_default_date
# ---------------------------------------------------------------------------


class TestIsSuspectedDefaultDate:
    def test_clean_row_is_not_flagged(self) -> None:
        row = _row(meeting_date="2024-04-10", created_at="2024-06-15T14:00:00Z")
        assert not mod.is_suspected_default_date(row)

    def test_date_source_default_is_flagged(self) -> None:
        row = _row(
            meeting_date="2024-04-10", created_at="2026-04-11T14:00:00Z", date_source="default"
        )
        assert mod.is_suspected_default_date(row)

    def test_date_source_filename_is_not_flagged(self) -> None:
        # Even if meeting_date == created_at date, an explicit date_source value
        # of 'filename' means the date was parsed from the filename — not a fallback.
        # (Matching date is just a coincidence here; date_source takes precedence.)
        row = _row(
            meeting_date="2026-04-11",
            created_at="2026-04-11T09:00:00Z",
            date_source="filename",
        )
        assert not mod.is_suspected_default_date(row)

    def test_meeting_date_equals_created_at_date_flagged(self) -> None:
        # Classic ingest-day fallback: date.today() was used.
        row = _row(meeting_date="2026-04-11", created_at="2026-04-11T14:23:07Z")
        assert mod.is_suspected_default_date(row)

    def test_missing_created_at_not_flagged(self) -> None:
        row = _row(meeting_date="2024-04-10", created_at="")
        assert not mod.is_suspected_default_date(row)

    def test_none_created_at_not_flagged(self) -> None:
        row = _row()
        row["created_at"] = None
        assert not mod.is_suspected_default_date(row)

    def test_missing_date_source_key_still_works(self) -> None:
        # Before the A3 migration, date_source is absent entirely.
        row = {
            "id": 1,
            "meeting_date": "2026-04-11",
            "created_at": "2026-04-11T10:00:00Z",
        }
        assert mod.is_suspected_default_date(row)

    def test_date_source_unknown_falls_through_to_heuristic(self) -> None:
        # 'unknown' is the column default for legacy rows ingested before A3.
        # It should NOT suppress the meeting_date == created_at heuristic — a
        # row tagged 'unknown' whose date matches the ingest day is still suspect.
        row = _row(
            meeting_date="2026-04-11",
            created_at="2026-04-11T10:00:00Z",
            date_source="unknown",
        )
        assert mod.is_suspected_default_date(row)

    def test_date_source_unknown_non_matching_date_not_flagged(self) -> None:
        # 'unknown' but the date doesn't match the ingest day — not flagged.
        row = _row(
            meeting_date="2024-04-10",
            created_at="2026-04-11T10:00:00Z",
            date_source="unknown",
        )
        assert not mod.is_suspected_default_date(row)


# ---------------------------------------------------------------------------
# is_bucket_url_issue
# ---------------------------------------------------------------------------


class TestIsBucketUrlIssue:
    def test_good_diligent_url_passes(self) -> None:
        row = _row(source_url="https://diligent.com/meeting/document/abc123")
        assert mod.is_bucket_url_issue(row) is None

    def test_empty_url_flagged(self) -> None:
        row = _row(source_url="")
        assert mod.is_bucket_url_issue(row) == "empty"

    def test_none_url_flagged(self) -> None:
        row = _row()
        row["source_url"] = None
        assert mod.is_bucket_url_issue(row) == "empty"

    def test_storage_bucket_url_flagged(self) -> None:
        url = "https://abc.supabase.co/storage/v1/object/public/documents/budget.pdf"
        row = _row(source_url=url)
        assert mod.is_bucket_url_issue(row) == "storage-bucket-url"

    def test_partial_storage_marker_not_flagged(self) -> None:
        # A URL containing /storage/ but not the full marker is a different service.
        url = "https://my-drive.example.com/storage/files/budget.pdf"
        row = _row(source_url=url)
        assert mod.is_bucket_url_issue(row) is None


# ---------------------------------------------------------------------------
# dedup_cluster_key
# ---------------------------------------------------------------------------


class TestDedupClusterKey:
    def test_same_doc_same_key(self) -> None:
        row = _row()
        assert mod.dedup_cluster_key(row) == mod.dedup_cluster_key(row)

    def test_pdf_and_html_same_key(self) -> None:
        # PDF and HTML versions of the same meeting minutes should cluster.
        r1 = _row(
            id=1,
            meeting_title="April 10, 2024 Meeting Minutes",
            source_file="April 10, 2024 Meeting Minutes.pdf",
        )
        r2 = _row(
            id=2,
            meeting_title="April 10, 2024 Meeting Minutes",
            source_file="April 10, 2024 Meeting Minutes.html",
        )
        assert mod.dedup_cluster_key(r1) == mod.dedup_cluster_key(r2)

    def test_different_dates_different_keys(self) -> None:
        r1 = _row(id=1, meeting_date="2024-04-10")
        r2 = _row(id=2, meeting_date="2024-05-15")
        assert mod.dedup_cluster_key(r1) != mod.dedup_cluster_key(r2)

    def test_different_types_different_keys(self) -> None:
        r1 = _row(id=1, document_type="minutes")
        r2 = _row(id=2, document_type="agenda")
        assert mod.dedup_cluster_key(r1) != mod.dedup_cluster_key(r2)

    def test_suffix_variants_cluster(self) -> None:
        # "Budget Final" and "Budget Approved" should reduce to the same stem.
        r1 = _row(
            id=1,
            document_type="budget",
            meeting_title="2024-2025 Budget Final",
            source_file="2024-2025 Budget Final.pdf",
        )
        r2 = _row(
            id=2,
            document_type="budget",
            meeting_title="2024-2025 Budget Approved",
            source_file="2024-2025 Budget Approved.pdf",
        )
        assert mod.dedup_cluster_key(r1) == mod.dedup_cluster_key(r2)

    def test_different_entities_different_keys(self) -> None:
        r1 = _row(id=1, entity_id=1)
        r2 = _row(id=2, entity_id=2)
        assert mod.dedup_cluster_key(r1) != mod.dedup_cluster_key(r2)


# ---------------------------------------------------------------------------
# find_duplicate_candidates
# ---------------------------------------------------------------------------


class TestFindDuplicateCandidates:
    def test_no_duplicates(self) -> None:
        rows = [
            _row(id=1, meeting_date="2024-04-10", content_hash="hash001"),
            _row(id=2, meeting_date="2024-05-08", content_hash="hash002"),
        ]
        assert mod.find_duplicate_candidates(rows) == []

    def test_cluster_key_collision(self) -> None:
        # Same entity/date/type/title -> should be one cluster.
        r1 = _row(
            id=1,
            content_hash="aaa111",
            source_file="April 10 Minutes.pdf",
            meeting_title="April 10, 2024 Meeting Minutes",
        )
        r2 = _row(
            id=2,
            content_hash="bbb222",
            source_file="April 10 Minutes.html",
            meeting_title="April 10, 2024 Meeting Minutes",
        )
        clusters = mod.find_duplicate_candidates([r1, r2])
        key_clusters = [c for c in clusters if c["reason"] == "cluster-key"]
        assert len(key_clusters) == 1
        assert sorted(key_clusters[0]["doc_ids"]) == [1, 2]

    def test_content_hash_collision(self) -> None:
        # Different titles but same content -> hash-collision cluster.
        r1 = _row(id=1, content_hash="deadbeef", meeting_title="Doc A")
        r2 = _row(id=2, content_hash="deadbeef", meeting_title="Doc B")
        clusters = mod.find_duplicate_candidates([r1, r2])
        hash_clusters = [c for c in clusters if c["reason"] == "content-hash-collision"]
        assert len(hash_clusters) == 1
        assert sorted(hash_clusters[0]["doc_ids"]) == [1, 2]

    def test_three_way_hash_cluster(self) -> None:
        r1 = _row(id=1, content_hash="cafebabe")
        r2 = _row(id=2, content_hash="cafebabe")
        r3 = _row(id=3, content_hash="cafebabe")
        clusters = mod.find_duplicate_candidates([r1, r2, r3])
        hash_clusters = [c for c in clusters if c["reason"] == "content-hash-collision"]
        assert len(hash_clusters) == 1
        assert sorted(hash_clusters[0]["doc_ids"]) == [1, 2, 3]

    def test_empty_content_hash_not_clustered(self) -> None:
        # Empty content_hash is common for very old rows; should not cluster.
        r1 = _row(id=1, content_hash="")
        r2 = _row(id=2, content_hash="")
        clusters = mod.find_duplicate_candidates([r1, r2])
        hash_clusters = [c for c in clusters if c["reason"] == "content-hash-collision"]
        assert hash_clusters == []

    def test_single_row_no_cluster(self) -> None:
        assert mod.find_duplicate_candidates([_row(id=1)]) == []


# ---------------------------------------------------------------------------
# check_extraction_health
# ---------------------------------------------------------------------------


class TestCheckExtractionHealth:
    def test_clean_english_content_healthy(self) -> None:
        row = _row(content="The board approved the FY2024 budget with 5 votes in favour.")
        assert mod.check_extraction_health(row) == []

    def test_control_char_flagged(self) -> None:
        # A raw 0x08 (backspace) character from broken PDF extraction.
        row = _row(content="Planning\x08 document")
        issues = mod.check_extraction_health(row)
        assert "control-char-noise" in issues

    def test_tab_newline_not_flagged(self) -> None:
        # Tab and newline are excluded from the control-char check.
        row = _row(content="Item 1:\tApproved\nItem 2:\tDenied")
        assert mod.check_extraction_health(row) == []

    def test_mojibake_flagged(self) -> None:
        # Cyrillic-block glyphs are the broken-font mojibake we want to detect.
        row = _row(content="ҨҨ ьы҂ эыэѐ" * 5)
        issues = mod.check_extraction_health(row)
        assert any("exotic-char-ratio" in i for i in issues)

    def test_smart_quotes_not_flagged(self) -> None:
        # Curly quotes (“”) and em dash (—) are in the General
        # Punctuation block; exotic_char_ratio excludes them.
        row = _row(content="The “levy” passed — all five members voted yes.")
        assert mod.check_extraction_health(row) == []

    def test_empty_content_not_flagged(self) -> None:
        row = _row(content="")
        assert mod.check_extraction_health(row) == []

    def test_none_content_not_flagged(self) -> None:
        row = _row()
        row["content"] = None
        assert mod.check_extraction_health(row) == []

    def test_both_issues_reported(self) -> None:
        # Both a control char and mojibake present.
        row = _row(content="\x08" + "ҨҨ ьы҂ эыэѐ" * 5)
        issues = mod.check_extraction_health(row)
        assert "control-char-noise" in issues
        assert any("exotic-char-ratio" in i for i in issues)


# ---------------------------------------------------------------------------
# check_classification_anomaly
# ---------------------------------------------------------------------------


class TestCheckClassificationAnomaly:
    def test_youtube_transcript_ok(self) -> None:
        row = _row(source_portal="youtube", document_type="transcript")
        assert mod.check_classification_anomaly(row) is None

    def test_youtube_minutes_flagged(self) -> None:
        row = _row(source_portal="youtube", document_type="minutes")
        result = mod.check_classification_anomaly(row)
        assert result is not None
        assert "youtube" in result and "minutes" in result

    def test_youtube_budget_flagged(self) -> None:
        row = _row(source_portal="youtube", document_type="budget")
        assert mod.check_classification_anomaly(row) is not None

    def test_diligent_minutes_ok(self) -> None:
        row = _row(source_portal="diligent", document_type="minutes")
        assert mod.check_classification_anomaly(row) is None

    def test_diligent_transcript_flagged(self) -> None:
        row = _row(source_portal="diligent", document_type="transcript")
        result = mod.check_classification_anomaly(row)
        assert result is not None
        assert "transcript" in result

    def test_diligent_curriculum_flagged(self) -> None:
        row = _row(source_portal="diligent", document_type="curriculum")
        assert mod.check_classification_anomaly(row) is not None

    def test_claytonschools_curriculum_ok(self) -> None:
        row = _row(source_portal="claytonschools", document_type="curriculum")
        assert mod.check_classification_anomaly(row) is None

    def test_claytonschools_transcript_flagged(self) -> None:
        row = _row(source_portal="claytonschools", document_type="transcript")
        assert mod.check_classification_anomaly(row) is not None

    def test_unknown_portal_not_flagged(self) -> None:
        # A portal not yet in _VALID_PORTAL_TYPES should not be false-positively
        # flagged — new portals must be addable without updating the auditor.
        row = _row(source_portal="futureportal", document_type="resolution")
        assert mod.check_classification_anomaly(row) is None

    def test_empty_portal_not_flagged(self) -> None:
        row = _row(source_portal="", document_type="minutes")
        assert mod.check_classification_anomaly(row) is None


# ---------------------------------------------------------------------------
# check_summary_advocacy (Actalux-generated copy must stay neutral)
# ---------------------------------------------------------------------------


class TestCheckSummaryAdvocacy:
    def test_clean_summary_not_flagged(self) -> None:
        row = {"summary": "A Feb 2025 board presentation on the facilities plan."}
        assert mod.check_summary_advocacy(row) is None

    def test_tax_neutral_framing_flagged(self) -> None:
        row = {"summary": "Borrowing up to $90M without increasing the levy."}
        assert mod.check_summary_advocacy(row) is not None

    def test_campaign_url_flagged(self) -> None:
        row = {"summary": "For approved projects visit www.claytonpropo.org today."}
        assert "claytonpropo" in (mod.check_summary_advocacy(row) or "")

    def test_vote_yes_flagged(self) -> None:
        row = {"summary": "Residents were urged to vote yes on the measure."}
        assert mod.check_summary_advocacy(row) is not None

    def test_hyphenated_forms_flagged(self) -> None:
        # Hyphenated campaign copy must be caught too (separators are [\s-]+).
        assert mod.check_summary_advocacy({"summary": "A vote-yes push for the bond."})
        assert mod.check_summary_advocacy({"summary": "Touted as no-tax-increase."})

    def test_advocacy_in_content_only_not_flagged(self) -> None:
        # Targets the GENERATED summary, not verbatim content: a transcript that
        # quotes "vote yes" must not be flagged when its summary is neutral.
        row = {
            "summary": "Board meeting transcript covering the bond discussion.",
            "content": "a board member said people will vote yes on the bond",
        }
        assert mod.check_summary_advocacy(row) is None

    def test_missing_summary_not_flagged(self) -> None:
        assert mod.check_summary_advocacy({}) is None


# ---------------------------------------------------------------------------
# run_audit (integration: wires all per-row checks + duplicate clustering)
# ---------------------------------------------------------------------------


class TestRunAudit:
    def test_clean_corpus_has_no_issues(self) -> None:
        rows = [
            _row(
                id=1,
                meeting_date="2024-04-10",
                created_at="2024-06-01T00:00:00Z",
                content_hash="clean001",
            ),
            _row(
                id=2,
                meeting_date="2024-05-08",
                created_at="2024-06-01T00:00:00Z",
                content_hash="clean002",
            ),
        ]
        report = mod.run_audit(rows)
        assert report["doc_count"] == 2
        assert report["suspected_default_dates"] == []
        assert report["bucket_url_issues"] == []
        assert report["duplicate_clusters"] == []
        assert report["extraction_issues"] == []
        assert report["classification_anomalies"] == []

    def test_all_categories_populated(self) -> None:
        rows = [
            # Suspected default date (meeting_date == created_at date)
            _row(
                id=1,
                meeting_date="2026-04-11",
                created_at="2026-04-11T14:00:00Z",
                content_hash="unique1",
            ),
            # Bucket URL issue
            _row(
                id=2,
                meeting_date="2024-01-01",
                created_at="2024-01-02T00:00:00Z",
                source_url="https://x.supabase.co/storage/v1/object/public/documents/b.pdf",
                content_hash="unique2",
            ),
            # Extraction health (control char)
            _row(
                id=3,
                meeting_date="2023-05-15",
                created_at="2023-06-01T00:00:00Z",
                content="Planning\x08 document",
                content_hash="unique3",
            ),
            # Classification anomaly (youtube non-transcript)
            _row(
                id=4,
                meeting_date="2023-06-01",
                created_at="2023-07-01T00:00:00Z",
                source_portal="youtube",
                document_type="minutes",
                content_hash="unique4",
            ),
            # Duplicate pair (same content_hash)
            _row(
                id=5,
                meeting_date="2022-03-09",
                created_at="2022-05-01T00:00:00Z",
                content_hash="duplicate_hash",
                source_file="doc_a.pdf",
                meeting_title="March 9 2022 Minutes",
            ),
            _row(
                id=6,
                meeting_date="2022-03-09",
                created_at="2022-05-01T00:00:00Z",
                content_hash="duplicate_hash",
                source_file="doc_b.pdf",
                meeting_title="March 9 2022 Minutes",
            ),
        ]
        report = mod.run_audit(rows)
        assert report["doc_count"] == 6
        assert any(r["id"] == 1 for r in report["suspected_default_dates"])
        assert any(r["id"] == 2 for r in report["bucket_url_issues"])
        assert any(r["id"] == 3 for r in report["extraction_issues"])
        assert any(r["id"] == 4 for r in report["classification_anomalies"])
        # Rows 5 and 6 share both cluster-key and content_hash.
        dup_ids = {id_ for cluster in report["duplicate_clusters"] for id_ in cluster["doc_ids"]}
        assert 5 in dup_ids and 6 in dup_ids

    def test_empty_corpus(self) -> None:
        report = mod.run_audit([])
        assert report["doc_count"] == 0
        assert all(v == [] for k, v in report.items() if k != "doc_count")

    def test_summary_advocacy_flagged_in_report(self) -> None:
        # A clean row whose generated summary carries advocacy framing is flagged
        # under summary_advocacy and nowhere else.
        row = _row(id=9, content_hash="adv9")
        row["summary"] = "Bonds up to $90M without increasing the levy."
        report = mod.run_audit([row])
        assert any(r["id"] == 9 for r in report["summary_advocacy"])
        # "nowhere else": the row is otherwise clean.
        assert report["suspected_default_dates"] == []
        assert report["bucket_url_issues"] == []
        assert report["extraction_issues"] == []
        assert report["classification_anomalies"] == []
        assert report["duplicate_clusters"] == []


class TestFetchRowsBuilderOrder:
    """Guard the supabase-py call order: .select() MUST precede filters like
    .is_(). The fake table builder below has no .is_() (mirroring the real
    SyncRequestBuilder), so filtering before selecting would AttributeError."""

    class _Exec:
        def __init__(self, data: list[dict]) -> None:
            self.data = data

    class _Query:
        def __init__(self, data: list[dict]) -> None:
            self._data = data

        def is_(self, _col: str, _val: str) -> TestFetchRowsBuilderOrder._Query:
            return self

        def execute(self) -> TestFetchRowsBuilderOrder._Exec:
            return TestFetchRowsBuilderOrder._Exec(self._data)

    class _Table:
        # Deliberately exposes only .select() (not .is_()), exactly like the real
        # builder that crashed when _fetch_rows filtered before selecting.
        def __init__(self, data: list[dict]) -> None:
            self._data = data

        def select(self, _cols: str) -> TestFetchRowsBuilderOrder._Query:
            return TestFetchRowsBuilderOrder._Query(self._data)

    class _Client:
        def __init__(self, data: list[dict]) -> None:
            self._data = data

        def table(self, _name: str) -> TestFetchRowsBuilderOrder._Table:
            return TestFetchRowsBuilderOrder._Table(self._data)

    def test_fetch_rows_selects_before_filtering(self) -> None:
        client = self._Client([{"id": 1, "meeting_date": "2024-01-01", "source_url": ""}])
        rows = mod._fetch_rows(client)
        assert rows and rows[0]["id"] == 1
