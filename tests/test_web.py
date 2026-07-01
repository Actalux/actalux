"""Tests for FastAPI web endpoints.

Tests static pages (no DB required) and verifies template rendering.
"""

import threading
from contextlib import contextmanager
from dataclasses import replace
from unittest.mock import Mock, patch

from fastapi import HTTPException
from fastapi.testclient import TestClient

from actalux.config import load_config
from actalux.web.app import _embedder_ready, _pdf_available, _render_citation_links, app, templates

client = TestClient(app, raise_server_exceptions=False)

# The bare TestClient doesn't run the app lifespan (no embedder warm-up), so the
# readiness event stays unset and /healthz would report "warming". Tests exercise
# the normal warm, serving state — mark it ready. The warming path is tested
# explicitly (see test_healthz_reports_warming_until_ready) by patching a fresh event.
_embedder_ready.set()


def _open_api_cfg():
    """Real ``Config`` with the API open at default limits.

    The tier-aware API auth + limiters call ``cfg.tier(...)``, so a config stub
    must be a genuine ``Config`` (a bare namespace lacks that method). ``conftest``
    sets placeholder Supabase env, so ``load_config()`` is hermetic.
    """
    return replace(
        load_config(), api_key="", rate_limit_search_per_minute=30, rate_limit_api_per_minute=60
    )


_FAKE_ENTITY = {
    "id": 1,
    "body_slug": "schools",
    "type": "school_district",
    "display_name": "Clayton School District",
    "place": {"state": "mo", "slug": "clayton", "display_name": "Clayton"},
}

_FAKE_COUNCIL = {
    "id": 2,
    "body_slug": "council",
    "type": "city_council",
    "display_name": "Clayton City Council",
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

    def test_healthz_reports_warming_until_ready(self) -> None:
        # Before the embedder warm-up finishes, /healthz returns 503 so Fly keeps
        # the machine out of rotation (the first query then hits a warm model). A
        # fresh, unset event stands in for a still-warming process.
        from actalux.web import app as app_module

        with patch.object(app_module, "_embedder_ready", threading.Event()):
            response = client.get("/healthz")
        assert response.status_code == 503
        assert response.json() == {"status": "warming"}

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

    def test_privacy_page(self) -> None:
        # Privacy is site-wide and renders at the apex (no body scope, no DB).
        response = client.get("/privacy")
        assert response.status_code == 200
        assert "Privacy Policy" in response.text
        # The data-practice claims that must stay grounded in app behavior.
        assert "no cookies" in response.text.lower()
        assert "OpenAI" in response.text
        # The core reason this page exists: dossier eligibility.
        assert "private individual" in response.text.lower()

    def test_terms_page(self) -> None:
        response = client.get("/terms")
        assert response.status_code == 200
        assert "Terms of Use" in response.text
        # Actalux is an LLC, never described as a nonprofit/501(c)(3).
        assert "limited liability company" in response.text.lower()
        assert "not a nonprofit or 501(c)(3)" in response.text
        assert "Missouri" in response.text

    @patch("actalux.web.app._get_db")
    @patch("actalux.web.app.get_entity_by_path", return_value=_FAKE_ENTITY)
    def test_footer_links_are_apex(self, mock_ent, mock_db) -> None:
        # Site-wide pages must link to the apex, not the current body, from any page.
        response = client.get("/mo/clayton/schools/methodology")
        assert 'href="/privacy"' in response.text
        assert 'href="/terms"' in response.text
        assert 'href="/methodology"' in response.text
        assert "/mo/clayton/schools/privacy" not in response.text


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

    def test_apex_redirects_to_place_landing(self) -> None:
        r = client.get("/", follow_redirects=False)
        assert r.status_code == 307
        assert r.headers["location"] == "/mo/clayton"

    def test_legacy_search_redirects_preserving_query(self) -> None:
        r = client.get("/search?q=board+meeting", follow_redirects=False)
        assert r.status_code == 301
        assert r.headers["location"] == "/mo/clayton/schools/search?q=board+meeting"

    def test_legacy_budget_redirects(self) -> None:
        r = client.get("/budget", follow_redirects=False)
        assert r.status_code == 301
        assert r.headers["location"] == "/mo/clayton/schools/budget"

    def test_apex_methodology_renders(self) -> None:
        # /methodology is site-wide: it renders the generic page, not a redirect.
        r = client.get("/methodology", follow_redirects=False)
        assert r.status_code == 200
        assert "How Actalux works" in r.text

    def test_scoped_privacy_redirects_to_apex(self) -> None:
        r = client.get("/mo/clayton/schools/privacy", follow_redirects=False)
        assert r.status_code == 301
        assert r.headers["location"] == "/privacy"

    def test_scoped_terms_redirects_to_apex(self) -> None:
        r = client.get("/mo/clayton/council/terms", follow_redirects=False)
        assert r.status_code == 301
        assert r.headers["location"] == "/terms"

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
    def test_place_hub_renders_directory(self, mock_list, mock_db) -> None:
        r = client.get("/mo/clayton")
        assert r.status_code == 200
        assert "Clayton School District" in r.text  # body listed as a card
        assert "/mo/clayton/schools" in r.text  # card links to the body

    @patch("actalux.web.app._get_db")
    @patch("actalux.web.app.list_entities", return_value=[_FAKE_ENTITY, _FAKE_COUNCIL])
    @patch("actalux.web.app.get_entity_by_path", return_value=_FAKE_COUNCIL)
    def test_city_body_nav_is_entity_aware(self, mock_ent, mock_list, mock_db) -> None:
        r = client.get("/mo/clayton/council")
        assert r.status_code == 200
        assert "Clayton City Council" in r.text
        assert "Council Meetings" in r.text
        # school-only nav must not leak onto a city body
        assert "Curriculum maps" not in r.text
        assert "Facilities Master Plan" not in r.text
        # the jurisdiction switcher lists both bodies (place name dropped from labels)
        assert "Jurisdiction" in r.text
        assert ">City Council<" in r.text
        assert ">School District<" in r.text


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


def _canon(doc: dict | None, *, superseded: bool = False, requested_id: int = 0):
    """Wrap a doc in a CanonicalDocument for patching resolve_canonical_document.

    Document routes resolve supersession before rendering, so route tests mock
    ``resolve_canonical_document`` rather than the lower-level ``get_document``.
    """
    from actalux.db import CanonicalDocument

    rid = requested_id or (doc["id"] if doc else 0)
    return CanonicalDocument(document=doc, superseded=superseded, requested_id=rid)


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
        # A mocked stored file also "exists" — the route gates the embed on a HEAD
        # check (stored_file_exists), so mark it available or the embed degrades.
        with patch("actalux.web.app.stored_file_exists", return_value=True):
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


_FAKE_MEETING_ROWS = [
    {"id": 195, "meeting_title": "6/3/26 minutes", "document_type": "minutes",
     "meeting_date": "2026-06-03", "summary": "The board approved the budget.", "video_id": ""},
    {"id": 665, "meeting_title": "6/3/26 transcript", "document_type": "transcript",
     "meeting_date": "2026-06-03", "summary": "", "video_id": "O1EMWuCLTrc"},
    {"id": 660, "meeting_title": "5/1/26 transcript", "document_type": "transcript",
     "meeting_date": "2026-05-01", "summary": "Policy discussion.", "video_id": ""},
]  # fmt: skip

_FAKE_MEETING_RECORDS = [
    {"id": 665, "document_type": "transcript", "meeting_date": "2026-06-03",
     "meeting_title": "6/3/26 Board of Education Meeting",
     "summary": "A transcript of the June 3 board meeting covering the budget.",
     "video_id": "O1EMWuCLTrc",
     "chapters": [{"t": 50, "title": "Call to order"}, {"t": 600, "title": "Budget discussion"}],
     "content": "Good evening. The meeting is called to order. A motion carried. We adjourn."},
    {"id": 195, "document_type": "minutes", "meeting_date": "2026-06-03",
     "meeting_title": "6/3/26 minutes", "summary": "The board approved the budget.", "video_id": "",
     "chapters": None,
     "content": "The Board of Education met. The budget was approved. The meeting adjourned."},
]  # fmt: skip


class TestMeetingsTopic:
    """Board Meetings topic: a meetings list, each opening to its records."""

    @patch("actalux.web.app._get_db")
    @patch("actalux.web.app.get_entity_by_path", return_value=_FAKE_ENTITY)
    @patch("actalux.web.app.list_recent_meeting_documents", return_value=_FAKE_MEETING_ROWS)
    def test_list_groups_by_date_with_badges(self, mock_list, mock_ent, mock_db) -> None:
        r = client.get("/mo/clayton/schools/meetings")
        assert r.status_code == 200
        # one entry per date, newest first, with a human date
        assert "June 3, 2026" in r.text
        assert "May 1, 2026" in r.text
        assert "/meeting/2026-06-03" in r.text
        # 6/3 has minutes + a video transcript; 5/1 a transcript with no video
        assert ">Minutes<" in r.text
        assert "Video &amp; transcript" in r.text  # video badge (HTML-escaped &)
        assert ">Transcript<" in r.text
        # the listing is scoped to meeting record types, not every document
        assert list(mock_list.call_args.args[2]) == ["minutes", "transcript"]

    @patch("actalux.web.app._get_db")
    @patch("actalux.web.app.get_entity_by_path", return_value=_FAKE_ENTITY)
    @patch("actalux.web.app.get_meeting_records", return_value=_FAKE_MEETING_RECORDS)
    def test_detail_shows_transcript_and_minutes(self, mock_rec, mock_ent, mock_db) -> None:
        r = client.get("/mo/clayton/schools/meeting/2026-06-03")
        assert r.status_code == 200
        assert "June 3, 2026" in r.text
        assert "yt-facade" in r.text  # the video embed
        assert "Full transcript" in r.text
        assert "Machine-generated transcript" in r.text  # transcript accuracy label
        assert "Full minutes" in r.text
        assert "The board approved the budget." in r.text  # minutes summary shown
        # transcript summary (item 3) and topic chapters (item 5) are shown
        assert "covering the budget" in r.text
        assert "Budget discussion" in r.text  # chapter title
        assert ">0:50<" in r.text and ">10:00<" in r.text  # chapter clock times
        assert "/document/665" in r.text and "/document/195" in r.text

    @patch("actalux.web.app._get_db")
    @patch("actalux.web.app.get_entity_by_path", return_value=_FAKE_ENTITY)
    def test_detail_bad_date_404(self, mock_ent, mock_db) -> None:
        assert client.get("/mo/clayton/schools/meeting/not-a-date").status_code == 404

    @patch("actalux.web.app._get_db")
    @patch("actalux.web.app.get_entity_by_path", return_value=_FAKE_ENTITY)
    @patch("actalux.web.app.get_meeting_records", return_value=[])
    def test_detail_no_records_404(self, mock_rec, mock_ent, mock_db) -> None:
        assert client.get("/mo/clayton/schools/meeting/1999-01-01").status_code == 404

    @patch("actalux.web.app._get_db")
    @patch("actalux.web.app.get_entity_by_path", return_value=_FAKE_ENTITY)
    @patch("actalux.web.app.list_recent_meeting_documents", return_value=[])
    def test_nav_reorg(self, mock_list, mock_ent, mock_db) -> None:
        r = client.get("/mo/clayton/schools/meetings")
        assert r.status_code == 200
        # Board Meetings now lives under Topics; Resolutions moved under Documents.
        assert 'href="/mo/clayton/schools/meetings"' in r.text
        assert 'href="/mo/clayton/schools/browse/resolutions"' in r.text
        # the old standalone Meetings nav group is gone.
        assert 'id="sub-meetings"' not in r.text


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


class TestFacilitiesPlanTopic:
    """The LRFMP topic page: a structured, source-cited briefing (A7 rebuild)."""

    @patch("actalux.web.app._get_db")
    @patch("actalux.web.app.get_entity_by_path", return_value=_FAKE_ENTITY)
    @patch(
        "actalux.web.app.list_documents",
        side_effect=[[_FAKE_VOLUME], [_FAKE_PRESENTATION]],
    )
    @patch("actalux.web.app.resolve_source_anchor", return_value=1919)
    def test_facilities_plan_renders_structured_briefing(
        self, mock_anchor, mock_list, mock_ent, mock_db
    ) -> None:
        r = client.get("/mo/clayton/schools/facilities-plan")
        assert r.status_code == 200
        # Page chrome + lede stat tiles (figures, not vision filler).
        assert "Long-Range Facilities Master Plan" in r.text
        assert "$94,136,875" in r.text  # identified-need priority total
        # Cost-by-tier server-SVG bar (no JS chart lib).
        assert "tier-bars" in r.text
        # Cost-by-scope / cost-by-location tables with native <details> drill.
        assert "Identified need by assessment scope" in r.text
        assert "Identified need by location" in r.text
        assert "<details" in r.text
        # Future-development frame kept separate from the $94.1M.
        assert "Future-development options" in r.text
        assert "$137M to $178M" in r.text
        # Lede tiles ground the delivery date and consultant individually.
        assert "Feb 2025" in r.text
        assert "Paragon Architecture" in r.text
        # Funding facts each render their own figure + citation (grounded per-fact).
        assert "Current debt" in r.text
        assert "Up to $90M of bonds" in r.text
        # Timeline spans the full initiative (plan -> bond -> voter approval),
        # reframed away from a Feb-2025 endpoint, with each milestone cited.
        assert "From master plan to voter-approved bond" in r.text
        assert "Voters approve Proposition O" in r.text
        # The dropped ungrounded milestone must not reappear.
        assert "Board selects Paragon Architecture" not in r.text
        # The campaign-tainted district release (doc 505) is not cited anywhere.
        assert "Phased implementation of prioritized projects" not in r.text
        # CitedChunk milestones link to the verified bond / certified-result chunks,
        # independent of the anchor resolver mock.
        assert "/chunk/8140/source" in r.text
        assert "/chunk/8710/source" in r.text
        # Every resolved figure deep-links to its source chunk.
        assert "/chunk/1919/source" in r.text
        # Curated primary-source documents: volume (by type) + presentation (by filename).
        assert mock_list.call_args_list[0].kwargs["document_type"] == "facilities_plan"
        assert mock_list.call_args_list[1].kwargs["source_file_like"] == "%LRFMP%"
        assert "Primary-source documents" in r.text
        assert "Volume1-ClaytonMasterPlan-Process-Priorities" in r.text

    @patch("actalux.web.app._get_db")
    @patch("actalux.web.app.get_entity_by_path", return_value=_FAKE_ENTITY)
    @patch("actalux.web.app.list_documents", side_effect=[[_FAKE_VOLUME], [_FAKE_PRESENTATION]])
    @patch("actalux.web.app.resolve_source_anchor", return_value=1919)
    def test_facilities_plan_bond_approved_with_certified_citation(
        self, mock_anchor, mock_list, mock_ent, mock_db
    ) -> None:
        # Content-policy: the $135M bond is shown as APPROVED with the verbatim
        # certified vote totals, cited to the St. Louis County certified results
        # (chunk 8710); the $90M is relabelled as the Feb 2025 projection with the
        # tax-rate framing removed. No editorializing.
        r = client.get("/mo/clayton/schools/facilities-plan")
        assert r.status_code == 200
        assert "$135,000,000" in r.text
        assert "April 7, 2026" in r.text
        assert "Approved" in r.text
        assert "2,516 yes (89.25%)" in r.text
        # The result is cited to the certified county results, never left pending.
        assert "/chunk/8710/source" in r.text
        assert "certified-result citation pending" not in r.text
        # Official public-record citations (chunk ids 8140 / 1755 = #q1fcc / #q06db).
        assert "/chunk/8140/source" in r.text
        assert "/chunk/1755/source" in r.text
        # The plan's $90M is framed as a projection, never as the funding reality.
        assert "Feb 2025 projection" in r.text
        # The campaign tax-framing must never appear on the page.
        assert "without a tax increase" not in r.text.lower()
        assert "without increasing the property tax" not in r.text.lower()
        # The banned editorial phrase must never appear.
        assert "unspecified spending" not in r.text.lower()

    @patch("actalux.web.app._get_db")
    @patch("actalux.web.app.get_entity_by_path", return_value=_FAKE_ENTITY)
    @patch("actalux.web.app.list_documents", side_effect=[[_FAKE_VOLUME], [_FAKE_VOLUME]])
    @patch("actalux.web.app.resolve_source_anchor", return_value=1919)
    def test_facilities_plan_dedupes_curated_documents(
        self, mock_anchor, mock_list, mock_ent, mock_db
    ) -> None:
        # A document caught by both filters appears once in the document cards.
        r = client.get("/mo/clayton/schools/facilities-plan")
        assert r.status_code == 200
        assert r.text.count('href="/document/87"') == 1

    @patch("actalux.web.app._get_db")
    @patch("actalux.web.app.get_entity_by_path", return_value=_FAKE_ENTITY)
    @patch("actalux.web.app.list_documents", side_effect=[[_FAKE_VOLUME], [_FAKE_PRESENTATION]])
    @patch("actalux.web.app.resolve_source_anchor", return_value=None)
    def test_facilities_plan_renders_when_anchor_unresolved(
        self, mock_anchor, mock_list, mock_ent, mock_db
    ) -> None:
        # An unresolved anchor must not break the page, and it must NOT read as a
        # silently uncited figure: the figure still renders, but its citation slot
        # is visibly marked "source pending" (never linked to the wrong passage).
        # The bond's chunk-id citations are independent of anchor resolution.
        r = client.get("/mo/clayton/schools/facilities-plan")
        assert r.status_code == 200
        assert "$94,136,875" in r.text
        assert "source pending" in r.text
        assert "/chunk/8140/source" in r.text


class TestDocumentEndpoint:
    """Document view (mocked DB)."""

    @patch("actalux.web.app._get_db")
    @patch("actalux.web.app.resolve_canonical_document", return_value=_canon(None))
    def test_missing_document_returns_404(self, mock_doc, mock_db) -> None:
        response = client.get("/document/99999")
        assert response.status_code == 404

    @patch("actalux.web.app._get_db")
    @patch("actalux.web.app.get_entity", return_value=_FAKE_ENTITY)
    @patch("actalux.web.app.resolve_canonical_document", return_value=_canon(_FAKE_DOC))
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

    @patch("actalux.web.app.stored_file_exists", return_value=False)
    @patch("actalux.web.app._get_db")
    @patch("actalux.web.app.get_entity", return_value=_FAKE_ENTITY)
    @patch("actalux.web.app.resolve_canonical_document", return_value=_canon(_FAKE_DOC))
    def test_document_pane_degrades_when_pdf_object_missing(
        self, mock_doc, mock_ent, mock_db, mock_exists
    ) -> None:
        """A PDF whose stored object is missing shows a note, not a broken iframe."""
        r = client.get("/document/195/pane")
        assert r.status_code == 200
        assert "pdf-frame" not in r.text
        assert "too large to preview" in r.text
        # The origin link is still offered so the reader can reach the document.
        assert "diligent.example.test" in r.text

    @patch("actalux.web.app._get_db")
    @patch("actalux.web.app.get_entity", return_value=_FAKE_ENTITY)
    @patch(
        "actalux.web.app.resolve_canonical_document",
        return_value=_canon({**_FAKE_DOC, "source_url": "", "source_file": ""}),
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
    @patch("actalux.web.app.resolve_canonical_document", return_value=_canon(None))
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

    def test_rate_limit_blocks_before_db(self) -> None:
        """The chunk-source route enforces the per-IP cap (scope 'chunk') before it
        touches the DB — so a crawler flood is turned away without any lookup work."""
        enforce = Mock(side_effect=HTTPException(status_code=429))
        render = Mock()
        with (
            patch("actalux.web.app._enforce_rate", enforce),
            patch("actalux.web.app._chunk_source_render_context", render),
        ):
            r = client.get("/chunk/9001/source")
        assert r.status_code == 429
        render.assert_not_called()
        assert enforce.call_args.args[1] == "chunk"

    @patch("actalux.web.app._get_db")
    @patch("actalux.web.app.get_entity", return_value=_FAKE_ENTITY)
    @patch("actalux.web.app.get_document", return_value=_FAKE_DOC)
    @patch("actalux.web.app.get_chunk_with_context")
    def test_full_page_embeds_original_pdf_not_text_dump(
        self, mock_ctx, mock_doc, mock_ent, mock_db
    ) -> None:
        """The full /chunk/{ref}/source page leads with the embedded original PDF
        and tucks the cited passage behind a disclosure (source_pane treatment),
        instead of dumping extracted text — see DESIGN.md "Citations resolve to
        the original"."""
        mock_ctx.return_value = {"chunk": _FAKE_CHUNK, "context": [_FAKE_CHUNK]}
        with _mock_stored_file_url(_FAKE_STORED_FILE_URL):
            r = client.get("/chunk/9001/source")
        assert r.status_code == 200
        # Original PDF embedded in native form, cued via the storage URL.
        assert "pdf-frame" in r.text
        assert _FAKE_STORED_FILE_URL in r.text
        # The cited passage sits behind a disclosure, not as the leading content.
        assert "cited-disclosure" in r.text
        # The real origin is offered as "Open original".
        assert "Open original" in r.text


# A superseded version of _FAKE_DOC: replaces_id points at the canonical 196.
_FAKE_SUPERSEDED_DOC = {**_FAKE_DOC, "id": 195, "replaces_id": 196}
_FAKE_CANONICAL_DOC = {**_FAKE_DOC, "id": 196, "replaces_id": None}


class TestDocumentSupersession:
    """Superseded DOCUMENT deep-links redirect to the canonical version (A2)."""

    @patch("actalux.web.app._get_db")
    @patch("actalux.web.app.resolve_canonical_document")
    def test_document_view_redirects_to_canonical(self, mock_resolve, mock_db) -> None:
        from actalux.db import CanonicalDocument

        mock_resolve.return_value = CanonicalDocument(
            document=_FAKE_CANONICAL_DOC, superseded=True, requested_id=195
        )
        r = client.get("/document/195", follow_redirects=False)
        assert r.status_code == 301
        assert r.headers["location"] == "/document/196"

    @patch("actalux.web.app._get_db")
    @patch("actalux.web.app.resolve_canonical_document")
    def test_document_pane_redirects_to_canonical_pane(self, mock_resolve, mock_db) -> None:
        from actalux.db import CanonicalDocument

        mock_resolve.return_value = CanonicalDocument(
            document=_FAKE_CANONICAL_DOC, superseded=True, requested_id=195
        )
        r = client.get("/document/195/pane", follow_redirects=False)
        assert r.status_code == 301
        assert r.headers["location"] == "/document/196/pane"

    @patch("actalux.web.app._get_db")
    @patch("actalux.web.app.get_entity", return_value=_FAKE_ENTITY)
    @patch("actalux.web.app.resolve_canonical_document")
    def test_current_document_renders_without_redirect(
        self, mock_resolve, mock_ent, mock_db
    ) -> None:
        from actalux.db import CanonicalDocument

        mock_resolve.return_value = CanonicalDocument(
            document=_FAKE_CANONICAL_DOC, superseded=False, requested_id=196
        )
        with _mock_stored_file_url(_FAKE_STORED_FILE_URL):
            r = client.get("/document/196", follow_redirects=False)
        assert r.status_code == 200

    @patch("actalux.web.app._get_db")
    @patch("actalux.web.app.resolve_canonical_document")
    def test_missing_document_404(self, mock_resolve, mock_db) -> None:
        from actalux.db import CanonicalDocument

        mock_resolve.return_value = CanonicalDocument(
            document=None, superseded=False, requested_id=999
        )
        r = client.get("/document/999", follow_redirects=False)
        assert r.status_code == 404

    @patch("actalux.web.app._get_db")
    @patch("actalux.web.app.get_entity", return_value=_FAKE_ENTITY)
    @patch("actalux.web.app.resolve_canonical_document")
    def test_cycle_renders_in_place_no_redirect(self, mock_resolve, mock_ent, mock_db) -> None:
        """A cycle resolves to superseded=False, so the route renders (no 301 loop)."""
        from actalux.db import CanonicalDocument

        # The resolver reports not-superseded on a cycle; the route must render,
        # not redirect, so two superseded rows can't bounce a 301 loop.
        mock_resolve.return_value = CanonicalDocument(
            document=_FAKE_DOC, superseded=False, requested_id=195
        )
        with _mock_stored_file_url(_FAKE_STORED_FILE_URL):
            r = client.get("/document/195", follow_redirects=False)
        assert r.status_code == 200


class TestChunkSupersession:
    """Superseded CHUNK deep-links are NEVER blind-redirected (A2)."""

    _OLD_CHUNK = {
        "id": 9001,
        "document_id": 195,
        "content": "The board approved the minutes.",
        "section": "Approval",
        "speaker": "",
        "chunk_index": 0,
        "start_seconds": None,
    }
    _CANON_CHUNK = {
        "id": 9100,
        "document_id": 196,
        "content": "The board approved the minutes.",
        "section": "Approval",
        "speaker": "",
        "chunk_index": 0,
        "start_seconds": None,
    }

    @patch("actalux.web.app._get_db")
    @patch("actalux.web.app.get_entity", return_value=_FAKE_ENTITY)
    @patch("actalux.web.app.resolve_canonical_chunk")
    @patch("actalux.web.app.resolve_canonical_document")
    @patch("actalux.web.app.get_document")
    @patch("actalux.web.app.get_chunk_with_context")
    def test_superseded_chunk_shows_notice_not_redirect(
        self, mock_ctx, mock_doc, mock_resolve, mock_chunk, mock_ent, mock_db
    ) -> None:
        from actalux.db import CanonicalDocument

        # First get_chunk_with_context: the original (superseded-doc) chunk.
        # Second call (re-anchored): the canonical chunk's context.
        mock_ctx.side_effect = [
            {"chunk": self._OLD_CHUNK, "context": [self._OLD_CHUNK]},
            {"chunk": self._CANON_CHUNK, "context": [self._CANON_CHUNK]},
        ]
        mock_doc.return_value = _FAKE_SUPERSEDED_DOC
        mock_resolve.return_value = CanonicalDocument(
            document=_FAKE_CANONICAL_DOC, superseded=True, requested_id=195
        )
        mock_chunk.return_value = self._CANON_CHUNK

        with _mock_stored_file_url(""):
            r = client.get("/chunk/9001/source?embed=1", follow_redirects=False)
        # No redirect — citations never blind-jump.
        assert r.status_code == 200
        assert "superseded version" in r.text
        # The re-anchored canonical document is offered as the current version.
        assert "/document/196" in r.text

    @patch("actalux.web.app._get_db")
    @patch("actalux.web.app.get_entity", return_value=_FAKE_ENTITY)
    @patch("actalux.web.app.resolve_canonical_chunk", return_value=None)
    @patch("actalux.web.app.resolve_canonical_document")
    @patch("actalux.web.app.get_document")
    @patch("actalux.web.app.get_chunk_with_context")
    def test_superseded_chunk_no_match_keeps_old_passage(
        self, mock_ctx, mock_doc, mock_resolve, mock_chunk, mock_ent, mock_db
    ) -> None:
        from actalux.db import CanonicalDocument

        mock_ctx.return_value = {"chunk": self._OLD_CHUNK, "context": [self._OLD_CHUNK]}
        mock_doc.return_value = _FAKE_SUPERSEDED_DOC
        mock_resolve.return_value = CanonicalDocument(
            document=_FAKE_CANONICAL_DOC, superseded=True, requested_id=195
        )
        with _mock_stored_file_url(""):
            r = client.get("/chunk/9001/source?embed=1", follow_redirects=False)
        assert r.status_code == 200
        assert "superseded version" in r.text
        # No confident match: the original cited words are still shown verbatim.
        assert "The board approved the minutes." in r.text

    @patch("actalux.web.app._get_db")
    @patch("actalux.web.app.get_entity", return_value=_FAKE_ENTITY)
    @patch("actalux.web.app.get_document", return_value=_FAKE_DOC)
    @patch("actalux.web.app.get_chunk_with_context")
    def test_current_chunk_has_no_superseded_notice(
        self, mock_ctx, mock_doc, mock_ent, mock_db
    ) -> None:
        # _FAKE_DOC has no replaces_id -> not superseded -> no notice.
        mock_ctx.return_value = {"chunk": _FAKE_CHUNK, "context": [_FAKE_CHUNK]}
        with _mock_stored_file_url(""):
            r = client.get("/chunk/9001/source?embed=1", follow_redirects=False)
        assert r.status_code == 200
        assert "superseded version" not in r.text

    @patch("actalux.web.app._get_db")
    @patch("actalux.web.app.get_entity", return_value=_FAKE_ENTITY)
    @patch("actalux.web.app.resolve_canonical_document")
    @patch("actalux.web.app.get_document", return_value=_FAKE_SUPERSEDED_DOC)
    @patch("actalux.web.app.get_chunk_with_context")
    def test_broken_chain_keeps_original_no_notice(
        self, mock_ctx, mock_doc, mock_resolve, mock_ent, mock_db
    ) -> None:
        """A doc with replaces_id but an unresolved chain (superseded=False): no notice.

        resolve_canonical_document reports superseded=False on a broken chain and
        returns the requested row; the citation must keep its original document and
        show no (unreliable) 'current version' link.
        """
        from actalux.db import CanonicalDocument

        mock_ctx.return_value = {"chunk": self._OLD_CHUNK, "context": [self._OLD_CHUNK]}
        # Broken chain: requested row returned, superseded False.
        mock_resolve.return_value = CanonicalDocument(
            document=_FAKE_SUPERSEDED_DOC, superseded=False, requested_id=195
        )
        with _mock_stored_file_url(""):
            r = client.get("/chunk/9001/source?embed=1", follow_redirects=False)
        assert r.status_code == 200
        assert "superseded version" not in r.text


class TestOriginLinks:
    """Template behavior: origin vs storage links, shown/hidden per spec."""

    @patch("actalux.web.app._get_db")
    @patch("actalux.web.app.get_entity", return_value=_FAKE_ENTITY)
    @patch("actalux.web.app.resolve_canonical_document", return_value=_canon(_FAKE_DOC))
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
    @patch("actalux.web.app.resolve_canonical_document", return_value=_canon(_FAKE_VIDEO_DOC))
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
    @patch("actalux.web.app.resolve_canonical_document", return_value=_canon(_FAKE_BUDGET_DOC))
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
        "actalux.web.app.resolve_canonical_document",
        return_value=_canon({**_FAKE_DOC, "source_url": "", "source_file": ""}),
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
        m_cfg.return_value = _open_api_cfg()
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
        m_cfg.return_value = _open_api_cfg()
        r = client.get("/api/v1/mo/clayton/schools/search", params={"q": "board"})
        assert r.status_code == 200
        hit = r.json()["results"][0]
        assert hit["source_url"] == "https://www.youtube.com/watch?v=VidId999"


class TestReaderPanePortalAware:
    """reader_pane.html renders portal-aware: transcript reflow vs light clean_text."""

    # ── YouTube / transcript portal ────────────────────────────────────────
    _TRANSCRIPT_CHUNK = {
        "id": 7777,
        "document_id": 308,
        "content": (
            "[01:05]\n"
            "welcome and thank you for joining\n"
            "us tonight for this board meeting\n"
            "\n"
            "we will now hear from the superintendent"
        ),
        "section": "",
        "speaker": "",
        "chunk_index": 0,
        "start_seconds": 65,
    }

    @patch("actalux.web.app._get_db")
    @patch("actalux.web.app.get_entity", return_value=_FAKE_ENTITY)
    @patch("actalux.web.app.get_document", return_value=_FAKE_VIDEO_DOC)
    def test_transcript_chunk_has_caption_label(self, mock_doc, mock_ent, mock_db) -> None:
        """YouTube transcript reader pane shows the machine-transcript disclaimer."""
        mock_ctx = {"chunk": self._TRANSCRIPT_CHUNK, "context": [self._TRANSCRIPT_CHUNK]}
        with patch("actalux.web.app.get_chunk_with_context", return_value=mock_ctx):
            r = client.get("/chunk/7777/source?embed=1")
        assert r.status_code == 200
        assert "Machine-generated transcript" in r.text

    @patch("actalux.web.app._get_db")
    @patch("actalux.web.app.get_entity", return_value=_FAKE_ENTITY)
    @patch("actalux.web.app.get_document", return_value=_FAKE_VIDEO_DOC)
    def test_transcript_timestamps_stripped(self, mock_doc, mock_ent, mock_db) -> None:
        """Standalone timestamp markers are stripped from the transcript display."""
        mock_ctx = {"chunk": self._TRANSCRIPT_CHUNK, "context": [self._TRANSCRIPT_CHUNK]}
        with patch("actalux.web.app.get_chunk_with_context", return_value=mock_ctx):
            r = client.get("/chunk/7777/source?embed=1")
        assert r.status_code == 200
        # [01:05] is a standalone timestamp line — must not appear in output.
        assert "[01:05]" not in r.text

    @patch("actalux.web.app._get_db")
    @patch("actalux.web.app.get_entity", return_value=_FAKE_ENTITY)
    @patch("actalux.web.app.get_document", return_value=_FAKE_VIDEO_DOC)
    def test_transcript_cited_chunk_has_cited_class(self, mock_doc, mock_ent, mock_db) -> None:
        """The cited chunk carries the .cited span regardless of portal."""
        mock_ctx = {"chunk": self._TRANSCRIPT_CHUNK, "context": [self._TRANSCRIPT_CHUNK]}
        with patch("actalux.web.app.get_chunk_with_context", return_value=mock_ctx):
            r = client.get("/chunk/7777/source?embed=1")
        assert r.status_code == 200
        assert 'class="cited"' in r.text

    # ── Non-transcript portal (diligent / minutes) ─────────────────────────

    @patch("actalux.web.app._get_db")
    @patch("actalux.web.app.get_entity", return_value=_FAKE_ENTITY)
    @patch("actalux.web.app.get_document", return_value=_FAKE_DOC)
    def test_non_transcript_no_caption_label(self, mock_doc, mock_ent, mock_db) -> None:
        """Non-transcript reader pane does not show the machine-transcript label."""
        mock_ctx = {"chunk": _FAKE_CHUNK, "context": [_FAKE_CHUNK]}
        with patch("actalux.web.app.get_chunk_with_context", return_value=mock_ctx):
            r = client.get("/chunk/9001/source?embed=1")
        assert r.status_code == 200
        assert "Machine-generated transcript" not in r.text

    @patch("actalux.web.app._get_db")
    @patch("actalux.web.app.get_entity", return_value=_FAKE_ENTITY)
    @patch("actalux.web.app.get_document", return_value=_FAKE_DOC)
    def test_non_transcript_cited_chunk_has_cited_class(self, mock_doc, mock_ent, mock_db) -> None:
        """Non-transcript reader pane still shows the .cited highlight."""
        mock_ctx = {"chunk": _FAKE_CHUNK, "context": [_FAKE_CHUNK]}
        with patch("actalux.web.app.get_chunk_with_context", return_value=mock_ctx):
            r = client.get("/chunk/9001/source?embed=1")
        assert r.status_code == 200
        assert 'class="cited"' in r.text
        # The chunk content appears (whitespace-normalised).
        assert "The board approved the minutes" in r.text

    # ── Video embed in document.html ──────────────────────────────────────
    @patch("actalux.web.app._get_db")
    @patch("actalux.web.app.get_entity", return_value=_FAKE_ENTITY)
    @patch("actalux.web.app.resolve_canonical_document", return_value=_canon(_FAKE_VIDEO_DOC))
    def test_document_view_transcript_shows_video_embed(self, mock_doc, mock_ent, mock_db) -> None:
        """Full /document/{id} page for a transcript with video_id embeds the player."""
        with _mock_stored_file_url(""):
            r = client.get("/document/308")
        assert r.status_code == 200
        # The YouTube facade embed must appear.
        assert "yt-facade" in r.text
        assert "AbCdEf123" in r.text
        # No PDF iframe (it's a video/transcript).
        assert "pdf-frame" not in r.text


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


class TestPdfAvailable:
    """_pdf_available gates the PDF embed: non-PDFs never HEAD; PDFs delegate."""

    def test_non_pdf_is_available_without_check(self) -> None:
        with patch("actalux.web.app.stored_file_exists") as m:
            assert _pdf_available({"source_file": "Oct 12 transcript.txt"}) is True
            m.assert_not_called()

    def test_pdf_delegates_to_existence_check(self) -> None:
        with patch("actalux.web.app.stored_file_exists", return_value=False) as m:
            assert _pdf_available({"source_file": "huge.pdf"}) is False
            m.assert_called_once()

    def test_none_doc_is_available(self) -> None:
        assert _pdf_available(None) is True


class TestAskLatencyPhaseA:
    """Phase-A latency wins for /ask: fast condense model + embedder warm-up (task #19)."""

    def test_condense_model_is_a_fast_non_reasoning_model(self) -> None:
        from actalux.config import Config

        cfg = Config()
        # Condensing a follow-up into a standalone query is a mechanical rewrite,
        # not reasoning — it must not use the slow reasoning summary model.
        assert cfg.condense_model
        assert cfg.condense_model != cfg.summary_model
        # Non-reasoning model => _completion_kwargs won't attach reasoning_effort.
        assert not cfg.condense_model.split("/")[-1].lower().startswith(("gpt-5", "o1", "o3", "o4"))

    def test_warm_embedder_calls_load_model(self) -> None:
        from actalux.web.app import _warm_embedder

        with patch("actalux.web.app.load_model") as m:
            _warm_embedder()
        m.assert_called_once()

    def test_warm_embedder_swallows_load_errors(self) -> None:
        from actalux.web.app import _warm_embedder

        # A load failure must not propagate — the model just loads lazily later.
        with patch("actalux.web.app.load_model", side_effect=RuntimeError("boom")):
            _warm_embedder()


_MEMBER_ENTITY = {**_FAKE_COUNCIL, "place_id": 10}
_MEMBER = {
    "id": 5,
    "slug": "susan-buse",
    "canonical_name": "Susan Buse",
    "metadata": {"role": "Councilmember", "ward": 2},
    "start_date": "2020-06-23",
    "end_date": None,
}
_MEMBER_ROW = {
    "edge_type": "voted_aye_on",
    "document_id": 195,
    "meeting_date": "2023-02-01",
    "meeting_title": "February 1, 2023 — Meeting Minutes",
    "motion": "Approve the agenda as posted.",
    "result": "passed",
    "citation_id": "a3f91c08",
}

# A motion-only body (Plan Commission): no per-member roll calls, no term dates.
_PC_ENTITY = {
    "id": 3,
    "body_slug": "plan-commission",
    "type": "plan_commission",
    "display_name": "Clayton Plan Commission",
    "place_id": 10,
    "place": {"state": "mo", "slug": "clayton", "display_name": "Clayton"},
}
_PC_MEMBER = {
    "id": 31,
    "slug": "ron-reim",
    "canonical_name": "Ron Reim",
    "metadata": {"role": "Commissioner", "ward": None},
    "role": "Commissioner",
    "start_date": None,
    "end_date": None,
}
_PC_MOTION_ROW = {
    "edge_type": "moved",
    "document_id": 300,
    "meeting_date": "2017-05-15",
    "meeting_title": "May 15, 2017 — Plan Commission Minutes",
    "motion": "Recommend approval of the conditional use permit.",
    "result": "carried",
    "citation_id": "pc12ab34",
}


class TestMemberPages:
    @patch("actalux.web.app._get_db")
    @patch("actalux.web.app.get_entity_by_path", return_value=_MEMBER_ENTITY)
    @patch("actalux.web.app.body_members", return_value=[_MEMBER])
    def test_members_directory_renders(self, m_mem, m_ent, m_db) -> None:
        r = client.get("/mo/clayton/council/members")
        assert r.status_code == 200
        assert "Susan Buse" in r.text
        assert "/mo/clayton/council/member/susan-buse" in r.text

    @patch("actalux.web.app._get_db")
    @patch("actalux.web.app.get_entity_by_path", return_value=_MEMBER_ENTITY)
    @patch("actalux.web.app.member_by_slug", return_value=_MEMBER)
    @patch("actalux.web.app.member_records", return_value=[_MEMBER_ROW])
    def test_member_dossier_renders_cited(self, m_rec, m_by, m_ent, m_db) -> None:
        r = client.get("/mo/clayton/council/member/susan-buse")
        assert r.status_code == 200
        assert "Susan Buse" in r.text
        assert "Ward 2" in r.text
        # the verbatim motion and a citation that resolves to the original
        assert "Approve the agenda as posted." in r.text
        assert "/chunk/a3f91c08/source" in r.text

    @patch("actalux.web.app._get_db")
    @patch("actalux.web.app.get_entity_by_path", return_value=_MEMBER_ENTITY)
    @patch("actalux.web.app.member_by_slug", return_value=None)
    def test_member_unknown_404(self, m_by, m_ent, m_db) -> None:
        r = client.get("/mo/clayton/council/member/nobody")
        assert r.status_code == 404

    @patch("actalux.web.app._get_db")
    @patch("actalux.web.app.get_entity_by_path", return_value=_PC_ENTITY)
    @patch("actalux.web.app.member_by_slug", return_value=_PC_MEMBER)
    @patch("actalux.web.app.member_records", return_value=[_PC_MOTION_ROW])
    def test_motion_only_member_dossier(self, m_rec, m_by, m_ent, m_db) -> None:
        # PC/BoA record no per-member roll call: motions are the record, shown
        # directly (not in an empty "Voting record" section), with no vote badges
        # and the cited-record span in place of a fabricated term.
        r = client.get("/mo/clayton/plan-commission/member/ron-reim")
        assert r.status_code == 200
        assert "Ron Reim" in r.text
        assert "Commissioner" in r.text
        assert "Motions moved" in r.text
        assert "Recommend approval of the conditional use permit." in r.text
        assert "/chunk/pc12ab34/source" in r.text
        assert "Voting record" not in r.text
        assert "Aye " not in r.text  # no roll-call badges
        assert "in the record 2017" in r.text


_MATTER_SUBJECT = {
    "id": 200,
    "slug": "bill-7156",
    "canonical_name": "Bill No. 7156",
    "metadata": {"kind": "bill", "number": "7156", "title": "an Ordinance Amending Chapter 405"},
}
_MATTER_SUMMARY = {
    "subject_id": 200,
    "slug": "bill-7156",
    "canonical_name": "Bill No. 7156",
    "metadata": {"kind": "bill", "number": "7156", "title": "an Ordinance Amending Chapter 405"},
    "actions": 3,
    "latest_date": "2024-05-14",
}
_MATTER_TIMELINE_ROW = {
    "edge_id": 1,
    "subject_id": 200,
    "meeting_date": "2024-05-14",
    "meeting_title": "May 14, 2024 — Meeting Minutes",
    "motion": "Motion to pass Bill No. 7156, an Ordinance Amending Chapter 405.",
    "result": "passed",
    "result_basis": "stated",
    "vote_count_yes": 6,
    "vote_count_no": 1,
    "vote_count_abstain": 0,
    "citation_id": "b7156aa0",
}


_MATTER_ENTITY = {**_FAKE_COUNCIL, "place_id": 10}


class TestMatterPages:
    @patch("actalux.web.app._get_db")
    @patch("actalux.web.app.get_entity_by_path", return_value=_MATTER_ENTITY)
    @patch("actalux.web.app.body_matters", return_value=[_MATTER_SUMMARY])
    def test_matters_directory_renders(self, m_mat, m_ent, m_db) -> None:
        r = client.get("/mo/clayton/council/matters")
        assert r.status_code == 200
        assert "Bill No. 7156" in r.text
        assert "an Ordinance Amending Chapter 405" in r.text
        assert "/mo/clayton/council/matter/bill-7156" in r.text

    @patch("actalux.web.app._get_db")
    @patch("actalux.web.app.get_entity_by_path", return_value=_MATTER_ENTITY)
    @patch("actalux.web.app.matter_by_slug", return_value=_MATTER_SUBJECT)
    @patch("actalux.web.app.matter_records", return_value=[_MATTER_TIMELINE_ROW])
    def test_matter_timeline_renders_cited(self, m_rec, m_by, m_ent, m_db) -> None:
        r = client.get("/mo/clayton/council/matter/bill-7156")
        assert r.status_code == 200
        assert "Bill No. 7156" in r.text
        assert "Timeline" in r.text
        assert "Motion to pass Bill No. 7156" in r.text
        assert "/chunk/b7156aa0/source" in r.text

    @patch("actalux.web.app._get_db")
    @patch("actalux.web.app.get_entity_by_path", return_value=_MATTER_ENTITY)
    @patch("actalux.web.app.matter_by_slug", return_value=None)
    def test_matter_unknown_404(self, m_by, m_ent, m_db) -> None:
        r = client.get("/mo/clayton/council/matter/bill-9999")
        assert r.status_code == 404

    @patch("actalux.web.app._get_db")
    @patch("actalux.web.app.get_entity_by_path", return_value=_MATTER_ENTITY)
    @patch("actalux.web.app.matter_by_slug", return_value=_MATTER_SUBJECT)
    @patch("actalux.web.app.matter_records", return_value=[])
    def test_matter_no_actions_404(self, m_rec, m_by, m_ent, m_db) -> None:
        # a matter with no cited action in this body is withheld (404), not an empty page.
        r = client.get("/mo/clayton/council/matter/bill-7156")
        assert r.status_code == 404


# Council + PC entities for the global person page (career timeline across bodies).
_PERSON_COUNCIL_ENTITY = {
    "id": 2,
    "body_slug": "council",
    "type": "city_council",
    "display_name": "Clayton City Council",
    "place_id": 10,
    "place": {"state": "mo", "slug": "clayton", "display_name": "Clayton"},
}
_PERSON_PC_ENTITY = {
    "id": 3,
    "body_slug": "plan-commission",
    "type": "plan_commission",
    "display_name": "Clayton Plan Commission",
    "place_id": 10,
    "place": {"state": "mo", "slug": "clayton", "display_name": "Clayton"},
}
_PERSON_DOSSIER = {
    "person": {"slug": "susan-buse", "canonical_name": "Susan Buse"},
    "tenures": [
        {
            "subject_id": 1,
            "entity_id": 2,
            "role": "Councilmember",
            "start_date": "2020-06-23",
            "end_date": None,
            "actions": 12,
            "first_date": "2021-01-01",
            "last_date": "2023-05-05",
        },
        {
            "subject_id": 2,
            "entity_id": 3,
            "role": "Commissioner",
            "start_date": None,
            "end_date": None,
            "actions": 4,
            "first_date": "2025-02-02",
            "last_date": "2026-01-01",
        },
    ],
}
_PERSON_ENTITY_BY_ID = {2: _PERSON_COUNCIL_ENTITY, 3: _PERSON_PC_ENTITY}


class TestPersonPage:
    @patch("actalux.web.app._get_db")
    @patch("actalux.web.app.get_entity", side_effect=lambda _c, eid: _PERSON_ENTITY_BY_ID.get(eid))
    @patch("actalux.web.app.person_dossier", return_value=_PERSON_DOSSIER)
    def test_person_page_renders_career_across_bodies(self, m_dos, m_ent, m_db) -> None:
        r = client.get("/people/susan-buse")
        assert r.status_code == 200
        assert "Susan Buse" in r.text
        # each body served links to that body's per-board record by the PUBLIC person
        # slug (never an internal '--plan-commission' subject slug)
        assert "Clayton City Council" in r.text
        assert "Clayton Plan Commission" in r.text
        assert "/mo/clayton/council/member/susan-buse" in r.text
        assert "/mo/clayton/plan-commission/member/susan-buse" in r.text
        assert "12 cited actions" in r.text

    @patch("actalux.web.app._get_db")
    @patch("actalux.web.app.person_dossier", return_value=None)
    def test_unknown_person_404(self, m_dos, m_db) -> None:
        r = client.get("/people/nobody")
        assert r.status_code == 404
