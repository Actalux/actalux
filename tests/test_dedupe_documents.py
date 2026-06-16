"""Tests for scripts/dedupe_documents.py clustering + canonical-pick logic.

All tested logic is pure (no DB access); synthetic document rows only. Verifies
the three clustering signals, conservative non-clustering of distinct records,
and the prefer-PDF canonical pick.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import scripts.dedupe_documents as dedupe

# A long shared body so the twin (text-overlap) signal has enough tokens to be
# trusted (>= _MIN_TOKENS_FOR_OVERLAP).
_BUDGET_BODY = (
    "The School District of Clayton adopted the budget for fiscal year 2024 2025 "
    "with total revenue of twenty four million dollars and total expenditure of "
    "twenty three million dollars across the general fund the capital projects fund "
    "and the debt service fund as approved by the board of education at the regular "
    "meeting after public review and reconciliation of all line items and reserves."
)


def _row(doc_id: int, **extra: Any) -> dict[str, Any]:
    base: dict[str, Any] = {
        "id": doc_id,
        "source_file": f"doc{doc_id}.pdf",
        "source_url": "",
        "source_portal": "diligent",
        "source_ref": "",
        "content_hash": "",
        "content": "",
        "entity_id": 1,
        "meeting_date": "2024-06-01",
        "document_type": "budget",
        "meeting_title": f"Doc {doc_id}",
        "video_id": "",
    }
    base.update(extra)
    return base


class TestClusteringSourceRef:
    def test_same_source_ref_clusters(self) -> None:
        rows = [
            _row(1, source_ref="https://diligent.test/document/abc"),
            _row(2, source_ref="https://diligent.test/document/abc"),
        ]
        clusters = dedupe.cluster_documents(rows)
        assert len(clusters) == 1
        assert {r["id"] for r in clusters[0].rows} == {1, 2}
        assert "source_ref" in clusters[0].reasons

    def test_empty_source_ref_does_not_cluster(self) -> None:
        # Two rows with empty source_ref must not collapse on the empty key.
        rows = [_row(1, source_ref=""), _row(2, source_ref="")]
        assert dedupe.cluster_documents(rows) == []

    def test_source_ref_scoped_to_portal(self) -> None:
        rows = [
            _row(1, source_ref="https://x.test/document/abc", source_portal="diligent"),
            _row(2, source_ref="https://x.test/document/abc", source_portal="claytonschools"),
        ]
        # Same ref but different portals — not the same identity.
        assert dedupe.cluster_documents(rows) == []


class TestClusteringContentHash:
    def test_same_content_hash_clusters(self) -> None:
        rows = [_row(1, content_hash="deadbeef"), _row(2, content_hash="deadbeef")]
        clusters = dedupe.cluster_documents(rows)
        assert len(clusters) == 1
        assert "content_hash" in clusters[0].reasons

    def test_empty_content_hash_does_not_cluster(self) -> None:
        rows = [_row(1, content_hash=""), _row(2, content_hash="")]
        assert dedupe.cluster_documents(rows) == []


class TestClusteringTextOverlapTwins:
    def test_pdf_html_twins_cluster_on_text_overlap(self) -> None:
        # Different stems, different extensions, no source_ref/hash match — only
        # the text-overlap + same entity/date/type signal should catch them.
        rows = [
            _row(1, source_file="2024-2025 Budget.pdf", content=_BUDGET_BODY, content_hash="h1"),
            _row(
                2,
                source_file="2024-2025 School District of Clayton Budget.html",
                content=_BUDGET_BODY + " Minor HTML footer text.",
                content_hash="h2",
            ),
        ]
        clusters = dedupe.cluster_documents(rows)
        assert len(clusters) == 1
        assert {r["id"] for r in clusters[0].rows} == {1, 2}
        assert "text-overlap" in clusters[0].reasons

    def test_distinct_records_same_bucket_do_not_cluster(self) -> None:
        # Same entity/date/type but genuinely different content — must NOT cluster.
        rows = [
            _row(1, content=_BUDGET_BODY, content_hash="h1"),
            _row(
                2,
                content=(
                    "The board approved the curriculum map for high school mathematics "
                    "covering algebra geometry and statistics units across the academic year "
                    "with pacing guides and assessment checkpoints for each grade level group."
                ),
                content_hash="h2",
            ),
        ]
        assert dedupe.cluster_documents(rows) == []

    def test_short_overlap_not_trusted(self) -> None:
        # Below the min-token floor, a few shared words must not trigger a twin.
        rows = [
            _row(1, content="budget approved tonight", content_hash="h1"),
            _row(2, content="budget approved tonight", content_hash="h2"),
        ]
        assert dedupe.cluster_documents(rows) == []

    def test_text_overlap_requires_same_bucket(self) -> None:
        # Identical text but different meeting_date — different records, no cluster.
        rows = [
            _row(1, content=_BUDGET_BODY, content_hash="h1", meeting_date="2024-06-01"),
            _row(2, content=_BUDGET_BODY, content_hash="h2", meeting_date="2023-06-01"),
        ]
        assert dedupe.cluster_documents(rows) == []

    def test_empty_bucket_fields_never_twin_on_text_alone(self) -> None:
        rows = [
            _row(
                1,
                entity_id=None,
                meeting_date="",
                document_type="",
                content=_BUDGET_BODY,
                content_hash="h1",
            ),
            _row(
                2,
                entity_id=None,
                meeting_date="",
                document_type="",
                content=_BUDGET_BODY,
                content_hash="h2",
            ),
        ]
        assert dedupe.cluster_documents(rows) == []

    def test_partial_bucket_fields_never_twin_on_text_alone(self) -> None:
        # entity_id matches but meeting_date AND document_type are blank — the
        # bucket is too coarse to twin on text alone (A2 requires all three).
        rows = [
            _row(
                1,
                entity_id=1,
                meeting_date="",
                document_type="",
                content=_BUDGET_BODY,
                content_hash="h1",
            ),
            _row(
                2,
                entity_id=1,
                meeting_date="",
                document_type="",
                content=_BUDGET_BODY,
                content_hash="h2",
            ),
        ]
        assert dedupe.cluster_documents(rows) == []

    def test_missing_document_type_blocks_text_twin(self) -> None:
        # entity_id + meeting_date match, document_type blank -> not a twin.
        rows = [
            _row(
                1,
                entity_id=1,
                meeting_date="2024-06-01",
                document_type="",
                content=_BUDGET_BODY,
                content_hash="h1",
            ),
            _row(
                2,
                entity_id=1,
                meeting_date="2024-06-01",
                document_type="",
                content=_BUDGET_BODY,
                content_hash="h2",
            ),
        ]
        assert dedupe.cluster_documents(rows) == []


class TestTransitiveClustering:
    def test_three_signals_merge_into_one_cluster(self) -> None:
        # 1 & 2 share a content_hash; 2 & 3 are text-overlap twins -> all one cluster.
        rows = [
            _row(1, content=_BUDGET_BODY, content_hash="same"),
            _row(2, content=_BUDGET_BODY, content_hash="same", source_file="2.html"),
            _row(
                3, content=_BUDGET_BODY + " extra tail.", content_hash="other", source_file="3.html"
            ),
        ]
        clusters = dedupe.cluster_documents(rows)
        assert len(clusters) == 1
        assert {r["id"] for r in clusters[0].rows} == {1, 2, 3}


class TestCanonicalPick:
    def test_prefers_pdf_over_html(self) -> None:
        cluster = dedupe.Cluster(
            rows=[
                _row(1, source_file="budget.html", content=_BUDGET_BODY),
                _row(2, source_file="budget.pdf", content=_BUDGET_BODY),
            ],
            reasons=["text-overlap"],
        )
        canonical, others = dedupe.pick_canonical(cluster)
        assert canonical["id"] == 2  # the PDF
        assert [r["id"] for r in others] == [1]

    def test_prefers_video_doc_over_text(self) -> None:
        cluster = dedupe.Cluster(
            rows=[
                _row(1, source_file="transcript.txt", content=_BUDGET_BODY, video_id=""),
                _row(2, source_file="transcript.txt", content=_BUDGET_BODY, video_id="vid123"),
            ],
            reasons=["content_hash"],
        )
        canonical, _others = dedupe.pick_canonical(cluster)
        assert canonical["id"] == 2  # the one with a video embed

    def test_tie_breaks_on_richer_then_lower_id(self) -> None:
        # Both non-embeddable: richer content wins, then lower id.
        cluster = dedupe.Cluster(
            rows=[
                _row(3, source_file="a.html", content="short"),
                _row(5, source_file="b.html", content=_BUDGET_BODY),
            ],
            reasons=["text-overlap"],
        )
        canonical, _others = dedupe.pick_canonical(cluster)
        assert canonical["id"] == 5  # longer content


class TestPlanDedupe:
    def test_plan_emits_supersession_edges_to_canonical(self) -> None:
        rows = [
            _row(1, source_file="budget.html", content=_BUDGET_BODY, content_hash="h"),
            _row(2, source_file="budget.pdf", content=_BUDGET_BODY, content_hash="h"),
        ]
        plan = dedupe.plan_dedupe(rows)
        assert len(plan.to_supersede) == 1
        edge = plan.to_supersede[0]
        assert edge["canonical_id"] == 2
        assert edge["non_canonical_id"] == 1

    def test_no_clusters_no_edges(self) -> None:
        rows = [_row(1, content_hash="a"), _row(2, content_hash="b")]
        plan = dedupe.plan_dedupe(rows)
        assert plan.to_supersede == []
        assert plan.clusters == []

    def test_review_csv_marks_canonical_and_candidates(self, tmp_path: Path) -> None:
        rows = [
            _row(1, source_file="budget.html", content=_BUDGET_BODY, content_hash="h"),
            _row(2, source_file="budget.pdf", content=_BUDGET_BODY, content_hash="h"),
        ]
        plan = dedupe.plan_dedupe(rows)
        out = tmp_path / "review.csv"
        dedupe.write_review_csv(plan, out)
        text = out.read_text()
        assert "canonical" in text
        assert "superseded-candidate" in text
        # Both ids appear in the per-document rows.
        assert ",1," in text or text.strip().endswith(",1")
