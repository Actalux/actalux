"""Tests for FastAPI web endpoints.

Tests static pages (no DB required) and verifies template rendering.
"""

from unittest.mock import patch

from fastapi.testclient import TestClient

from actalux.web.app import _render_citation_links, app

client = TestClient(app, raise_server_exceptions=False)


class TestStaticPages:
    """Pages that render without database access."""

    def test_home_page(self) -> None:
        response = client.get("/")
        assert response.status_code == 200
        assert "Clayton School District" in response.text
        assert "Search" in response.text

    def test_methodology_page(self) -> None:
        response = client.get("/methodology")
        assert response.status_code == 200
        assert "How Actalux works" in response.text
        assert "citation" in response.text.lower()
        assert "verbatim" in response.text.lower()

    def test_home_has_search_form(self) -> None:
        response = client.get("/")
        assert 'hx-post="/search"' in response.text

    def test_methodology_has_correction_info(self) -> None:
        response = client.get("/methodology")
        assert "Report an error" in response.text


class TestSearchEndpoint:
    """Search endpoint behavior (mocked DB)."""

    def test_empty_query_returns_empty(self) -> None:
        response = client.post("/search", data={"q": ""})
        assert response.status_code == 200

    def test_empty_query_no_results(self) -> None:
        response = client.post("/search", data={"q": "   "})
        assert response.status_code == 200


class TestDocumentEndpoint:
    """Document view (mocked DB)."""

    @patch("actalux.web.app._get_db")
    @patch("actalux.web.app.get_document", return_value=None)
    def test_missing_document_returns_404(self, mock_doc, mock_db) -> None:
        response = client.get("/document/99999")
        assert response.status_code == 404


class TestChunkSourceEndpoint:
    """Chunk source context (mocked DB)."""

    @patch("actalux.web.app._get_db")
    @patch("actalux.web.app.get_chunk_with_context", return_value={"chunk": None, "context": []})
    def test_missing_chunk_returns_404(self, mock_ctx, mock_db) -> None:
        response = client.get("/chunk/99999/source")
        assert response.status_code == 404


class TestReportError:
    """Error reporting endpoint."""

    def test_empty_description_returns_400(self) -> None:
        response = client.post(
            "/report-error",
            data={"chunk_id": "1", "description": "  ", "email": ""},
        )
        assert response.status_code == 400


class TestCitationLinks:
    """Summary citation link rendering."""

    def test_known_citation_becomes_link(self) -> None:
        html = _render_citation_links(
            "The budget was approved. [#q003f]",
            [{"hash_id": "#q003f", "chunk_id": 63}],
        )

        assert '<a href="/chunk/63/source" class="source-link">[#q003f]</a>' in html

    def test_unknown_citation_stays_plain(self) -> None:
        html = _render_citation_links(
            "The budget was approved. [#qffff]",
            [{"hash_id": "#q003f", "chunk_id": 63}],
        )

        assert "[#qffff]" in html
        assert "/chunk/" not in html

    def test_non_citation_text_is_escaped(self) -> None:
        html = _render_citation_links(
            "<script>alert(1)</script> [#q10000]",
            [{"hash_id": "#q10000", "chunk_id": 65536}],
        )

        assert "&lt;script&gt;alert(1)&lt;/script&gt;" in html
        assert "<script>" not in html
        assert '<a href="/chunk/65536/source" class="source-link">[#q10000]</a>' in html
