"""Tests for the change-digest → cited Substack draft pipeline.

Covers the topic mapping, change detection (new vs. updated, grouping), the
drafter (cited paragraph + linked citations, degradation when nothing is
citeable, the human-review banner), and SMTP delivery — all with the DB and LLM
patched so no network call is made.
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock, Mock, patch

from actalux.digest.change_digest import build_change_digest
from actalux.digest.delivery import send_draft_email
from actalux.digest.drafter import _human_date, _link_citations, _oneline, draft_post
from actalux.digest.themes import (
    THEME_BUDGET,
    THEME_MEETINGS,
    THEME_ORDER,
    THEME_OTHER,
    group_by_theme,
    theme_for,
)
from actalux.errors import SummaryError
from actalux.search.summarize import Summary


class TestThemes:
    def test_known_types_map_to_topics(self) -> None:
        assert theme_for("minutes") == THEME_MEETINGS
        assert theme_for("budget") == THEME_BUDGET
        assert theme_for("audit") == THEME_BUDGET
        assert theme_for("facilities_plan") == "Facilities"
        assert theme_for("curriculum_map") == "Curriculum & instruction"

    def test_unknown_type_falls_to_other(self) -> None:
        assert theme_for("something_new") == THEME_OTHER
        assert theme_for("") == THEME_OTHER

    def test_type_is_normalized(self) -> None:
        assert theme_for("  MINUTES ") == THEME_MEETINGS

    def test_group_by_theme_orders_and_drops_empties(self) -> None:
        docs = [
            SimpleNamespace(theme=THEME_BUDGET),
            SimpleNamespace(theme=THEME_MEETINGS),
            SimpleNamespace(theme=THEME_BUDGET),
        ]
        grouped = group_by_theme(docs)
        # Meetings precedes Budget in THEME_ORDER; Facilities/Curriculum/Other absent.
        assert list(grouped.keys()) == [THEME_MEETINGS, THEME_BUDGET]
        assert len(grouped[THEME_BUDGET]) == 2
        assert THEME_MEETINGS in THEME_ORDER  # sanity on the constant


def _row(**over: object) -> dict[str, object]:
    base = {
        "id": 1,
        "document_type": "minutes",
        "meeting_date": "2026-06-03",
        "meeting_title": "minutes.pdf",
        "source_portal": "diligent",
        "source_url": "https://example.org/minutes.pdf",
        "summary": "Board meeting minutes.",
        "version": 1,
    }
    base.update(over)
    return base


class TestChangeDigest:
    def test_labels_new_vs_updated_and_assigns_theme(self) -> None:
        rows = [
            _row(id=1, document_type="minutes", version=1),
            _row(id=2, document_type="budget", version=2, meeting_date="2026-07-01"),
            _row(id=3, document_type="mystery", version=1),
        ]
        with patch(
            "actalux.digest.change_digest.list_documents_changed_since", return_value=rows
        ) as listed:
            digest = build_change_digest(Mock(), "2026-06-01T00:00:00+00:00", entity_id=1)

        listed.assert_called_once()
        assert not digest.is_empty
        by_id = {d.id: d for d in digest.docs}
        assert by_id[1].status == "new"
        assert by_id[2].status == "updated"
        assert by_id[1].theme == THEME_MEETINGS
        assert by_id[2].theme == THEME_BUDGET
        assert by_id[3].theme == THEME_OTHER

    def test_empty_when_nothing_changed(self) -> None:
        with patch("actalux.digest.change_digest.list_documents_changed_since", return_value=[]):
            digest = build_change_digest(Mock(), "2026-06-01T00:00:00+00:00")
        assert digest.is_empty
        assert digest.by_theme() == {}


def _digest_with_one_minutes_doc():
    rows = [_row(id=1, document_type="minutes")]
    with patch("actalux.digest.change_digest.list_documents_changed_since", return_value=rows):
        return build_change_digest(Mock(), "2026-06-01T00:00:00+00:00", entity_id=1)


_CHUNKS = [
    {"id": 10, "content": "The board met on June 3.", "section": "", "citation_id": "a3f91c08"},
]


class TestDrafter:
    def test_human_date(self) -> None:
        assert _human_date("2026-06-11T09:00:00+00:00") == "June 11, 2026"
        assert _human_date("2026-06-11") == "June 11, 2026"
        assert _human_date("") == ""
        assert _human_date("not-a-date") == "not-a-date"

    def test_oneline_collapses_whitespace(self) -> None:
        assert _oneline("a\n  b\t c") == "a b c"

    def test_link_citations_rewrites_known_marker(self) -> None:
        evidence = [{"hash_id": "#qa3f91c08", "cite_ref": "a3f91c08"}]
        out = _link_citations("Approved. [#qa3f91c08]", evidence, "https://actalux.org")
        assert "([#qa3f91c08](https://actalux.org/chunk/a3f91c08/source))" in out

    def test_link_citations_leaves_unknown_marker_literal(self) -> None:
        out = _link_citations("Approved. [#qdeadbeef]", [], "https://actalux.org")
        assert "[#qdeadbeef]" in out
        assert "/chunk/" not in out

    def test_draft_post_includes_cited_paragraph_and_links(self) -> None:
        digest = _digest_with_one_minutes_doc()
        summary = Summary(
            text="The board approved the agenda. [#qa3f91c08]",
            citations_found=1,
            citations_verified=1,
            citations_dropped=0,
        )
        with (
            patch("actalux.digest.drafter.get_document_chunks", return_value=_CHUNKS),
            patch("actalux.digest.drafter.generate_summary", return_value=summary) as gen,
        ):
            draft = draft_post(Mock(), digest, "test-key", generated_on="2026-06-18")

        gen.assert_called_once()
        md = draft.markdown
        assert "# What changed in the Clayton schools public record" in md
        assert "Draft for human review — not published." in md
        assert "## Board meetings" in md
        assert "(https://actalux.org/chunk/a3f91c08/source)" in md
        assert "[Open the original →](https://actalux.org/document/1)" in md
        assert draft.subject == "Actalux draft: 1 new Clayton schools record (June 18, 2026)"
        assert draft.doc_count == 1
        assert draft.citations_verified == 1

    def test_draft_post_no_verified_citations_lists_docs_only(self) -> None:
        digest = _digest_with_one_minutes_doc()
        summary = Summary(
            text="Could not generate a verified summary for this query.",
            citations_found=0,
            citations_verified=0,
            citations_dropped=0,
        )
        with (
            patch("actalux.digest.drafter.get_document_chunks", return_value=_CHUNKS),
            patch("actalux.digest.drafter.generate_summary", return_value=summary),
        ):
            draft = draft_post(Mock(), digest, "test-key", generated_on="2026-06-18")

        assert "Could not generate a verified summary" not in draft.markdown
        assert "[Open the original →](https://actalux.org/document/1)" in draft.markdown
        assert draft.citations_verified == 0

    def test_draft_post_degrades_on_summary_error(self) -> None:
        digest = _digest_with_one_minutes_doc()
        with (
            patch("actalux.digest.drafter.get_document_chunks", return_value=_CHUNKS),
            patch("actalux.digest.drafter.generate_summary", side_effect=SummaryError("boom")),
        ):
            draft = draft_post(Mock(), digest, "test-key", generated_on="2026-06-18")

        # No crash; the document is still listed, just without a cited paragraph.
        assert "[Open the original →](https://actalux.org/document/1)" in draft.markdown
        assert draft.citations_verified == 0

    def test_draft_post_skips_summary_when_no_chunks(self) -> None:
        digest = _digest_with_one_minutes_doc()
        with (
            patch("actalux.digest.drafter.get_document_chunks", return_value=[]),
            patch("actalux.digest.drafter.generate_summary") as gen,
        ):
            draft = draft_post(Mock(), digest, "test-key", generated_on="2026-06-18")

        gen.assert_not_called()
        assert "## Board meetings" in draft.markdown
        assert "[Open the original →](https://actalux.org/document/1)" in draft.markdown


class TestDelivery:
    def test_unconfigured_returns_false(self) -> None:
        sent = send_draft_email(
            "subj", "body", host="", port=587, user="", password="", email_from="", email_to=""
        )
        assert sent is False

    def test_starttls_path_sends(self) -> None:
        server = MagicMock()
        smtp_cm = MagicMock()
        smtp_cm.__enter__.return_value = server
        with patch("actalux.digest.delivery.smtplib.SMTP", return_value=smtp_cm) as smtp:
            sent = send_draft_email(
                "subj",
                "body",
                host="smtp.example.org",
                port=587,
                user="u",
                password="p",
                email_from="from@example.org",
                email_to="a@example.org, b@example.org",
            )

        assert sent is True
        smtp.assert_called_once_with("smtp.example.org", 587)
        server.starttls.assert_called_once()
        server.login.assert_called_once_with("u", "p")
        server.send_message.assert_called_once()

    def test_smtps_path_uses_ssl_and_skips_login_without_user(self) -> None:
        server = MagicMock()
        smtp_cm = MagicMock()
        smtp_cm.__enter__.return_value = server
        with patch("actalux.digest.delivery.smtplib.SMTP_SSL", return_value=smtp_cm) as smtps:
            sent = send_draft_email(
                "subj",
                "body",
                host="smtp.example.org",
                port=465,
                user="",
                password="",
                email_from="from@example.org",
                email_to="a@example.org",
            )

        assert sent is True
        smtps.assert_called_once()
        server.login.assert_not_called()
        server.send_message.assert_called_once()
