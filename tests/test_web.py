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
    def test_home_search_form_is_get_navigation(self, mock_ent, mock_db) -> None:
        # The top-bar search must work from any page (incl. home, which has no
        # #search-results target), so it GET-navigates to the search route
        # rather than HTMX-swapping into an element that may not exist.
        response = client.get("/mo/clayton/schools")
        assert 'action="/mo/clayton/schools/search"' in response.text
        assert 'method="get"' in response.text
        assert "hx-post" not in response.text

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

    def test_legacy_facilities_plan_redirects(self) -> None:
        r = client.get("/facilities-plan", follow_redirects=False)
        assert r.status_code == 301
        assert r.headers["location"] == "/mo/clayton/schools/facilities-plan"

    def test_legacy_topic_facilities_redirects(self) -> None:
        r = client.get("/topic/facilities-plan", follow_redirects=False)
        assert r.status_code == 301
        assert r.headers["location"] == "/mo/clayton/schools/facilities-plan"

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


_FAKE_DOC = {
    "id": 195,
    "meeting_title": "February 1, 2023 Business Meeting Minutes",
    "document_type": "minutes",
    "meeting_date": "2023-02-01",
    "summary": "Signed minutes from the February 1, 2023 board meeting.",
    "source_url": "https://example.test/storage/minutes.pdf",
    "source_portal": "diligent",
    "video_id": "",
    "entity_id": 1,
}


class TestBrowse:
    """Browse-by-type: chronological document listings, not search."""

    @patch("actalux.web.app._get_db")
    @patch("actalux.web.app.get_entity_by_path", return_value=_FAKE_ENTITY)
    @patch("actalux.web.app.list_documents", return_value=[_FAKE_DOC])
    def test_browse_minutes_lists_documents(self, mock_list, mock_ent, mock_db) -> None:
        r = client.get("/mo/clayton/schools/browse/minutes")
        assert r.status_code == 200
        assert "Minutes" in r.text
        # title is homogenized at render (date-led), raw filename not shown
        assert "February 1, 2023 — Meeting Minutes" in r.text
        # browse rows open the document pane, they do not run a search
        assert "/document/195/pane" in r.text
        # the listing filters by document_type, not a keyword search
        assert mock_list.call_args.kwargs["document_type"] == "minutes"

    @patch("actalux.web.app._get_db")
    @patch("actalux.web.app.get_entity_by_path", return_value=_FAKE_ENTITY)
    @patch("actalux.web.app.list_documents", return_value=[])
    def test_browse_curriculum_maps_filters_by_filename(self, mock_list, mock_ent, mock_db) -> None:
        # Curriculum maps share document_type='other'; they are matched by filename.
        r = client.get("/mo/clayton/schools/browse/curriculum-maps")
        assert r.status_code == 200
        assert mock_list.call_args.kwargs["document_type"] is None
        assert mock_list.call_args.kwargs["source_file_like"] == "%curriculum%map%"

    @patch("actalux.web.app._get_db")
    @patch("actalux.web.app.get_entity_by_path", return_value=_FAKE_ENTITY)
    @patch("actalux.web.app.list_documents", return_value=[])
    def test_browse_facilities_plan_filters_by_type(self, mock_list, mock_ent, mock_db) -> None:
        r = client.get("/mo/clayton/schools/browse/facilities-plan")
        assert r.status_code == 200
        assert mock_list.call_args.kwargs["document_type"] == "facilities_plan"

    @patch("actalux.web.app._get_db")
    @patch("actalux.web.app.get_entity_by_path", return_value=_FAKE_ENTITY)
    def test_browse_unknown_kind_404(self, mock_ent, mock_db) -> None:
        r = client.get("/mo/clayton/schools/browse/nonsense")
        assert r.status_code == 404


_FAKE_VOLUME = {
    "id": 87,
    "meeting_title": "Volume1-ClaytonMasterPlan-Process-Priorities.pdf",
    "document_type": "facilities_plan",
    "meeting_date": "2026-04-11",
    "summary": "Volume I of the Clayton Long-Range Facilities Master Plan.",
}
_FAKE_PRESENTATION = {
    "id": 83,
    "meeting_title": "LRFMP_Board_Presentation_Feb2025.pdf",
    "document_type": "presentation",
    "meeting_date": "2025-02-01",
    "summary": "A February 2025 board presentation on the facilities master plan.",
}
_FAKE_FACILITIES_SECTIONS = [
    {
        "label": "What the plan is",
        "query": "comprehensive strategic document facilities",
        "results": [
            {
                "chunk_id": 1919,
                "document_id": 87,
                "document_type": "facilities_plan",
                "meeting_date": "2026-04-11",
                "meeting_title": "Volume1-ClaytonMasterPlan-Process-Priorities.pdf",
                "section": "",
                "hash_id": "#q077f",
                "content": (
                    "The School District of Clayton's Long-Range Master Facilities Plan "
                    "(LRFMP) is a comprehensive strategic document outlining the future "
                    "development, maintenance, and management of school facilities."
                ),
            }
        ],
    }
]


class TestFacilitiesPlanTopic:
    """The LRFMP topic page: curated plan documents plus cited quotes."""

    @patch("actalux.web.app._get_db")
    @patch("actalux.web.app.get_entity_by_path", return_value=_FAKE_ENTITY)
    @patch(
        "actalux.web.app.list_documents",
        side_effect=[[_FAKE_VOLUME], [_FAKE_PRESENTATION]],
    )
    @patch(
        "actalux.web.app._facilities_quote_sections",
        return_value=_FAKE_FACILITIES_SECTIONS,
    )
    def test_facilities_plan_renders_docs_and_quotes(
        self, mock_sections, mock_list, mock_ent, mock_db
    ) -> None:
        r = client.get("/mo/clayton/schools/facilities-plan")
        assert r.status_code == 200
        # Page chrome
        assert "Long-Range Facilities Master Plan" in r.text
        # Curated documents: the volume (by type) and the presentation (by filename)
        assert mock_list.call_args_list[0].kwargs["document_type"] == "facilities_plan"
        assert mock_list.call_args_list[1].kwargs["source_file_like"] == "%LRFMP%"
        assert "Volume1-ClaytonMasterPlan-Process-Priorities" in r.text
        # Cited-quote section uses the reader-facing label, not the raw query
        assert "What the plan is" in r.text
        # The definitional snippet renders, with query terms marked
        assert "<mark>strategic</mark>" in r.text
        assert "outlining the future" in r.text
        assert "#q077f" in r.text

    @patch("actalux.web.app._get_db")
    @patch("actalux.web.app.get_entity_by_path", return_value=_FAKE_ENTITY)
    @patch("actalux.web.app.list_documents", side_effect=[[_FAKE_VOLUME], [_FAKE_VOLUME]])
    @patch("actalux.web.app._facilities_quote_sections", return_value=_FAKE_FACILITIES_SECTIONS)
    def test_facilities_plan_dedupes_curated_documents(
        self, mock_sections, mock_list, mock_ent, mock_db
    ) -> None:
        # A document caught by both filters appears once, not twice.
        r = client.get("/mo/clayton/schools/facilities-plan")
        assert r.status_code == 200
        assert r.text.count("/document/87") == 1


class TestDocumentEndpoint:
    """Document view (mocked DB)."""

    @patch("actalux.web.app._get_db")
    @patch("actalux.web.app.get_document", return_value=None)
    def test_missing_document_returns_404(self, mock_doc, mock_db) -> None:
        response = client.get("/document/99999")
        assert response.status_code == 404

    @patch("actalux.web.app._get_db")
    @patch("actalux.web.app.get_entity", return_value=_FAKE_ENTITY)
    @patch("actalux.web.app.get_document", return_value=_FAKE_DOC)
    def test_document_pane_renders_without_citation(self, mock_doc, mock_ent, mock_db) -> None:
        r = client.get("/document/195/pane")
        assert r.status_code == 200
        assert "reader-summary" in r.text
        assert "pdf-frame" in r.text  # PDF source embeds in-window
        assert "cited-para" not in r.text  # browse has no cited passage

    @patch("actalux.web.app._get_db")
    @patch("actalux.web.app.get_document", return_value=None)
    def test_document_pane_404(self, mock_doc, mock_db) -> None:
        r = client.get("/document/99999/pane")
        assert r.status_code == 404


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
