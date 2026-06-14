"""Tests for FastAPI web endpoints.

Tests static pages (no DB required) and verifies template rendering.
"""

from unittest.mock import patch

from fastapi.testclient import TestClient

from actalux.web.app import _render_citation_links, app

client = TestClient(app, raise_server_exceptions=False)

_FAKE_ENTITY = {
    "id": 1,
    "body_slug": "schools",
    "type": "school_district",
    "display_name": "Clayton School District",
    "place": {"state": "mo", "slug": "clayton", "display_name": "Clayton"},
}


class TestCanonicalHostRedirect:
    """www.actalux.org redirects to the apex; other hosts pass through."""

    def test_www_redirects_to_apex_preserving_path_and_query(self) -> None:
        response = client.get(
            "/search?q=budget",
            headers={"host": "www.actalux.org"},
            follow_redirects=False,
        )
        assert response.status_code == 301
        assert response.headers["location"] == "https://actalux.org/search?q=budget"

    def test_apex_host_not_redirected(self) -> None:
        response = client.get("/healthz", headers={"host": "actalux.org"}, follow_redirects=False)
        assert response.status_code == 200

    def test_fly_host_not_redirected(self) -> None:
        response = client.get(
            "/healthz", headers={"host": "actalux.fly.dev"}, follow_redirects=False
        )
        assert response.status_code == 200


class TestStaticPages:
    """Pages that render without database access."""

    def test_healthz_is_db_free_and_ok(self) -> None:
        # The platform health check must not depend on config or the DB, so a
        # paused Supabase free tier can't mark the app unhealthy.
        response = client.get("/healthz")
        assert response.status_code == 200
        assert response.json() == {"status": "ok"}

    @patch("actalux.web.app._get_db")
    @patch("actalux.web.app.get_entity_by_path", return_value=_FAKE_ENTITY)
    def test_home_page(self, mock_ent, mock_db) -> None:
        response = client.get("/mo/clayton/schools")
        assert response.status_code == 200
        assert "Clayton School District" in response.text
        assert "Search" in response.text

    @patch("actalux.web.app._get_db")
    @patch("actalux.web.app.get_entity_by_path", return_value=_FAKE_ENTITY)
    def test_methodology_page(self, mock_ent, mock_db) -> None:
        response = client.get("/mo/clayton/schools/methodology")
        assert response.status_code == 200
        assert "How Actalux works" in response.text
        assert "citation" in response.text.lower()
        assert "verbatim" in response.text.lower()

    @patch("actalux.web.app._get_db")
    @patch("actalux.web.app.get_entity_by_path", return_value=_FAKE_ENTITY)
    def test_home_has_search_form(self, mock_ent, mock_db) -> None:
        response = client.get("/mo/clayton/schools")
        assert 'hx-post="/mo/clayton/schools/search"' in response.text

    @patch("actalux.web.app._get_db")
    @patch("actalux.web.app.get_entity_by_path", return_value=_FAKE_ENTITY)
    def test_methodology_has_correction_info(self, mock_ent, mock_db) -> None:
        response = client.get("/mo/clayton/schools/methodology")
        assert "Report an error" in response.text


class TestSearchEndpoint:
    """Search endpoint behavior (mocked DB)."""

    @patch("actalux.web.app._get_db")
    @patch("actalux.web.app.get_entity_by_path", return_value=_FAKE_ENTITY)
    def test_empty_query_returns_empty(self, mock_ent, mock_db) -> None:
        response = client.post("/mo/clayton/schools/search", data={"q": ""})
        assert response.status_code == 200

    @patch("actalux.web.app._get_db")
    @patch("actalux.web.app.get_entity_by_path", return_value=_FAKE_ENTITY)
    def test_empty_query_no_results(self, mock_ent, mock_db) -> None:
        response = client.post("/mo/clayton/schools/search", data={"q": "   "})
        assert response.status_code == 200


class TestJurisdictionRouting:
    """Entity-scoped routing, redirects from legacy flat paths, and 404s."""

    def test_apex_redirects_to_default_body(self) -> None:
        r = client.get("/", follow_redirects=False)
        assert r.status_code == 307
        assert r.headers["location"] == "/mo/clayton/schools"

    def test_legacy_search_redirects_preserving_query(self) -> None:
        r = client.get("/search?q=board+meeting", follow_redirects=False)
        assert r.status_code == 301
        assert r.headers["location"] == "/mo/clayton/schools/search?q=board+meeting"

    def test_legacy_budget_redirects(self) -> None:
        r = client.get("/budget", follow_redirects=False)
        assert r.status_code == 301
        assert r.headers["location"] == "/mo/clayton/schools/budget"

    def test_legacy_methodology_redirects(self) -> None:
        r = client.get("/methodology", follow_redirects=False)
        assert r.status_code == 301
        assert r.headers["location"] == "/mo/clayton/schools/methodology"

    def test_legacy_topic_budget_redirects(self) -> None:
        r = client.get("/topic/budget", follow_redirects=False)
        assert r.status_code == 301
        assert r.headers["location"] == "/mo/clayton/schools/budget"

    @patch("actalux.web.app._get_db")
    @patch("actalux.web.app.get_entity_by_path", return_value=_FAKE_ENTITY)
    def test_entity_home_renders(self, mock_ent, mock_db) -> None:
        r = client.get("/mo/clayton/schools")
        assert r.status_code == 200
        assert "Clayton School District" in r.text
        # internal links are entity-scoped
        assert "/mo/clayton/schools/search" in r.text

    @patch("actalux.web.app._get_db")
    @patch("actalux.web.app.get_entity_by_path", return_value=_FAKE_ENTITY)
    def test_entity_methodology_renders(self, mock_ent, mock_db) -> None:
        r = client.get("/mo/clayton/schools/methodology")
        assert r.status_code == 200

    @patch("actalux.web.app._get_db")
    @patch("actalux.web.app.get_entity_by_path", return_value=None)
    def test_unknown_jurisdiction_404(self, mock_ent, mock_db) -> None:
        r = client.get("/zz/nowhere/schools")
        assert r.status_code == 404

    @patch("actalux.web.app._get_db")
    @patch("actalux.web.app.list_entities", return_value=[_FAKE_ENTITY])
    def test_place_hub_redirects_to_body(self, mock_list, mock_db) -> None:
        r = client.get("/mo/clayton", follow_redirects=False)
        assert r.status_code == 307
        assert r.headers["location"] == "/mo/clayton/schools"


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
