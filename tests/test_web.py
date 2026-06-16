"""Tests for FastAPI web endpoints.

Tests static pages (no DB required) and verifies template rendering.
"""

from contextlib import contextmanager
from unittest.mock import patch

from fastapi.testclient import TestClient

from actalux.web.app import _render_citation_links, app, templates

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
    # source_url is the real Diligent origin; source_file is the storage key used
    # for PDF embeds.  These are kept separate (A1 fix).
    "source_url": "https://diligent.example.test/document/abc123",
    "source_file": "February 1, 2023 Business Meeting Minutes.pdf",
    "source_portal": "diligent",
    "video_id": "",
    "entity_id": 1,
}

# Fake stored_file_url function for tests: avoids loading ACTALUX_SUPABASE_URL.
_FAKE_STORED_FILE_URL = "https://storage.example.test/public/documents/minutes.pdf"


@contextmanager
def _mock_stored_file_url(return_value: str = _FAKE_STORED_FILE_URL):
    """Context manager: swap stored_file_url in the Jinja2 globals for one request.

    stored_file_url is registered as a Jinja2 global and filter at app-module-load
    time, so patching actalux.web.app.stored_file_url does not affect templates
    that have already captured the reference.  Replacing it in templates.env.globals
    (and env.filters) is the correct intercept point for render-time behaviour.
    """
    original = templates.env.globals["stored_file_url"]
    templates.env.globals["stored_file_url"] = lambda _f, cfg=None: return_value
    templates.env.filters["stored_file_url"] = lambda _f, cfg=None: return_value
    try:
        yield
    finally:
        templates.env.globals["stored_file_url"] = original
        templates.env.filters["stored_file_url"] = original


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
    def test_document_pane_renders_pdf_embed_from_stored_file(
        self, mock_doc, mock_ent, mock_db
    ) -> None:
        """PDF embed uses stored_file_url(source_file), not source_url."""
        with _mock_stored_file_url(_FAKE_STORED_FILE_URL):
            r = client.get("/document/195/pane")
        assert r.status_code == 200
        assert "reader-summary" in r.text
        # PDF iframe src is the storage URL (stored_file_url), not source_url.
        assert "pdf-frame" in r.text
        assert _FAKE_STORED_FILE_URL in r.text
        # source_url is the Diligent origin link, shown as "Open original ↗"
        assert "diligent.example.test" in r.text
        assert "cited-para" not in r.text  # browse has no cited passage

    @patch("actalux.web.app._get_db")
    @patch("actalux.web.app.get_entity", return_value=_FAKE_ENTITY)
    @patch(
        "actalux.web.app.get_document",
        return_value={**_FAKE_DOC, "source_url": "", "source_file": ""},
    )
    def test_document_pane_hides_origin_link_when_source_url_empty(
        self, mock_doc, mock_ent, mock_db
    ) -> None:
        """No "Open original" when source_url is absent."""
        with _mock_stored_file_url(""):
            r = client.get("/document/195/pane")
        assert r.status_code == 200
        assert "Open original" not in r.text
        assert "pdf-frame" not in r.text

    @patch("actalux.web.app._get_db")
    @patch("actalux.web.app.get_document", return_value=None)
    def test_document_pane_404(self, mock_doc, mock_db) -> None:
        r = client.get("/document/99999/pane")
        assert r.status_code == 404


_FAKE_VIDEO_DOC = {
    "id": 308,
    "meeting_title": "Board Meeting Oct 12 2023 transcript.txt",
    "document_type": "transcript",
    "meeting_date": "2023-10-12",
    "summary": "Transcript of the October 12, 2023 board meeting.",
    "source_url": "https://diligent.example.test/document/yt001",
    "source_file": "Board Meeting Oct 12 2023 transcript.txt",
    "source_portal": "youtube",
    "video_id": "AbCdEf123",
    "entity_id": 1,
    "content": "Opening remarks. The board called the meeting to order.",
}

_FAKE_CHUNK = {
    "id": 9001,
    "document_id": 195,
    "content": "The board approved the minutes.",
    "section": "Approval",
    "speaker": "",
    "chunk_index": 0,
    "start_seconds": None,
}

_FAKE_BUDGET_DOC = {
    **_FAKE_DOC,
    "id": 400,
    "document_type": "budget",
    "meeting_title": "2024-2025 Budget.pdf",
    "source_file": "2024-2025 Budget.pdf",
    "source_url": "https://diligent.example.test/document/budget2025",
    "content": "Total revenue: $24,000,000.",
}


class TestChunkSourceEndpoint:
    """Chunk source context (mocked DB)."""

    @patch("actalux.web.app._get_db")
    @patch("actalux.web.app.get_chunk_with_context", return_value={"chunk": None, "context": []})
    def test_missing_chunk_returns_404(self, mock_ctx, mock_db) -> None:
        response = client.get("/chunk/99999/source")
        assert response.status_code == 404


class TestOriginLinks:
    """Template behavior: origin vs storage links, shown/hidden per spec."""

    @patch("actalux.web.app._get_db")
    @patch("actalux.web.app.get_entity", return_value=_FAKE_ENTITY)
    @patch("actalux.web.app.get_document", return_value=_FAKE_DOC)
    def test_document_view_pdf_embed_uses_stored_file_url(
        self, mock_doc, mock_ent, mock_db
    ) -> None:
        """Full /document/{id} page: PDF docs embed via stored_file_url, not source_url."""
        with _mock_stored_file_url(_FAKE_STORED_FILE_URL):
            r = client.get("/document/195")
        assert r.status_code == 200
        # The storage URL appears as the iframe src.
        assert _FAKE_STORED_FILE_URL in r.text
        # The Diligent origin appears as the "Open original" link.
        assert "diligent.example.test" in r.text
        # The pdf-frame class should appear for a PDF doc.
        assert "pdf-frame" in r.text

    @patch("actalux.web.app._get_db")
    @patch("actalux.web.app.get_entity", return_value=_FAKE_ENTITY)
    @patch("actalux.web.app.get_document", return_value=_FAKE_VIDEO_DOC)
    def test_document_view_video_embed_no_pdf_frame(self, mock_doc, mock_ent, mock_db) -> None:
        """Video docs: show the YouTube embed, not a PDF iframe."""
        with _mock_stored_file_url(""):
            r = client.get("/document/308")
        assert r.status_code == 200
        # YouTube embed (via _video_embed.html partial uses video_id).
        assert "AbCdEf123" in r.text
        assert "pdf-frame" not in r.text

    @patch("actalux.web.app._get_db")
    @patch("actalux.web.app.get_entity", return_value=_FAKE_ENTITY)
    @patch("actalux.web.app.get_document", return_value=_FAKE_BUDGET_DOC)
    def test_document_view_budget_shows_budget_callout(self, mock_doc, mock_ent, mock_db) -> None:
        """Budget documents: a callout links to the structured Budget page."""
        with _mock_stored_file_url(_FAKE_STORED_FILE_URL):
            r = client.get("/document/400")
        assert r.status_code == 200
        # Callout pointing at the budget topic page.
        assert "Budget topic page" in r.text
        assert "/budget" in r.text

    @patch("actalux.web.app._get_db")
    @patch("actalux.web.app.get_entity", return_value=_FAKE_ENTITY)
    @patch(
        "actalux.web.app.get_document",
        return_value={**_FAKE_DOC, "source_url": "", "source_file": ""},
    )
    def test_document_view_hides_origin_link_when_empty(self, mock_doc, mock_ent, mock_db) -> None:
        """No 'Open original' when source_url is empty."""
        with _mock_stored_file_url(""):
            r = client.get("/document/195")
        assert r.status_code == 200
        assert "Open original" not in r.text

    @patch("actalux.web.app._get_db")
    @patch("actalux.web.app.get_entity", return_value=_FAKE_ENTITY)
    @patch("actalux.web.app.get_chunk_with_context")
    @patch("actalux.web.app.get_document", return_value=_FAKE_DOC)
    def test_reader_pane_origin_link_shown_when_non_empty(
        self, mock_doc, mock_ctx, mock_ent, mock_db
    ) -> None:
        """reader_pane.html: 'Source file' link shown when source_url is non-empty."""
        mock_ctx.return_value = {
            "chunk": _FAKE_CHUNK,
            "context": [_FAKE_CHUNK],
        }
        with _mock_stored_file_url(""):
            r = client.get("/chunk/9001/source?embed=1")
        assert r.status_code == 200
        # Origin link is present (shown because source_url is non-empty).
        assert "diligent.example.test" in r.text
        assert "Source file" in r.text

    @patch("actalux.web.app._get_db")
    @patch("actalux.web.app.get_entity", return_value=_FAKE_ENTITY)
    @patch("actalux.web.app.get_chunk_with_context")
    @patch(
        "actalux.web.app.get_document",
        return_value={**_FAKE_DOC, "source_url": ""},
    )
    def test_reader_pane_origin_link_hidden_when_empty(
        self, mock_doc, mock_ctx, mock_ent, mock_db
    ) -> None:
        """reader_pane.html: no 'Source file' link when source_url is empty."""
        mock_ctx.return_value = {
            "chunk": _FAKE_CHUNK,
            "context": [_FAKE_CHUNK],
        }
        with _mock_stored_file_url(""):
            r = client.get("/chunk/9001/source?embed=1")
        assert r.status_code == 200
        assert "Source file" not in r.text


class TestApiSourceUrl:
    """API _source_url: origin for non-video docs; YouTube for video docs."""

    @patch("actalux.web.api.get_config")
    @patch("actalux.web.api.get_db")
    @patch("actalux.web.api.get_entity_by_path", return_value=_FAKE_ENTITY)
    @patch("actalux.web.api.embed_query", return_value=[0.0])
    @patch("actalux.web.api.build_reranker", return_value=None)
    @patch("actalux.web.api.hybrid_search", return_value=[])
    @patch(
        "actalux.web.api.enrich_results",
        return_value=[
            {
                "chunk_id": 1,
                "hash_id": "#q0001",
                "content": "Budget approved.",
                "section": "",
                "speaker": "",
                "rrf_score": 0.01,
                "meeting_date": "2024-01-01",
                "meeting_title": "Jan 1 2024 Meeting Minutes",
                "document_id": 501,
                "document_type": "minutes",
                "summary": "",
            }
        ],
    )
    @patch(
        "actalux.web.api.get_documents",
        return_value={
            501: {
                "id": 501,
                "source_url": "https://diligent.example.test/document/guid001",
                "source_portal": "diligent",
                "video_id": "",
            }
        },
    )
    def test_api_search_returns_origin_source_url(
        self, m_docs, m_enrich, m_hybrid, m_rerank, m_embed, m_ent, m_db, m_cfg
    ) -> None:
        """API search: source_url in the hit is the real origin, not a storage URL."""
        from types import SimpleNamespace

        m_cfg.return_value = SimpleNamespace(
            api_key="", rate_limit_search_per_minute=30, rate_limit_api_per_minute=60
        )
        r = client.get("/api/v1/mo/clayton/schools/search", params={"q": "budget"})
        assert r.status_code == 200
        hit = r.json()["results"][0]
        # Returns the real Diligent origin, not a storage bucket URL.
        assert hit["source_url"] == "https://diligent.example.test/document/guid001"

    @patch("actalux.web.api.get_config")
    @patch("actalux.web.api.get_db")
    @patch("actalux.web.api.get_entity_by_path", return_value=_FAKE_ENTITY)
    @patch("actalux.web.api.embed_query", return_value=[0.0])
    @patch("actalux.web.api.build_reranker", return_value=None)
    @patch("actalux.web.api.hybrid_search", return_value=[])
    @patch(
        "actalux.web.api.enrich_results",
        return_value=[
            {
                "chunk_id": 2,
                "hash_id": "#q0002",
                "content": "The board opened the meeting.",
                "section": "",
                "speaker": "",
                "rrf_score": 0.01,
                "meeting_date": "2024-01-01",
                "meeting_title": "Jan 1 2024 transcript.txt",
                "document_id": 502,
                "document_type": "transcript",
                "summary": "",
            }
        ],
    )
    @patch(
        "actalux.web.api.get_documents",
        return_value={
            502: {
                "id": 502,
                "source_url": "https://storage.example.test/transcript.txt",
                "source_portal": "youtube",
                "video_id": "VidId999",
            }
        },
    )
    def test_api_search_youtube_doc_returns_youtube_url(
        self, m_docs, m_enrich, m_hybrid, m_rerank, m_embed, m_ent, m_db, m_cfg
    ) -> None:
        """API search: YouTube video doc returns the watch URL, never the .txt URL."""
        from types import SimpleNamespace

        m_cfg.return_value = SimpleNamespace(
            api_key="", rate_limit_search_per_minute=30, rate_limit_api_per_minute=60
        )
        r = client.get("/api/v1/mo/clayton/schools/search", params={"q": "board"})
        assert r.status_code == 200
        hit = r.json()["results"][0]
        assert hit["source_url"] == "https://www.youtube.com/watch?v=VidId999"


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
