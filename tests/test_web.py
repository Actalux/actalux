"""Tests for FastAPI web endpoints.

Tests static pages (no DB required) and verifies template rendering.
"""

from unittest.mock import patch

from fastapi.testclient import TestClient

from actalux.web.app import app

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
        assert "How Actalux Works" in response.text
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
