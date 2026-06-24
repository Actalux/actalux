"""Tests for the read-only JSON API (v1)."""

from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest
from fastapi.testclient import TestClient

from actalux.config import load_config
from actalux.web.api import _reset_rate_limits
from actalux.web.app import app

client = TestClient(app, raise_server_exceptions=False)

BASE = "/api/v1/mo/clayton/schools"


def _cfg(**overrides):
    """A real ``Config`` with overrides, so ``cfg.tier(name)`` behaves for real.

    The tier-aware auth + limiters call ``get_config().tier(...)``, so test configs
    must be genuine ``Config`` instances rather than bare namespaces. ``conftest``
    sets placeholder Supabase env, so ``load_config()`` is hermetic here.
    """
    return replace(load_config(), **overrides)


def _db_with_authorize(*, valid: bool, tier: str = "developer", over_quota: bool = False):
    """A mock Supabase client whose ``rpc('api_key_authorize').execute().data`` is
    shaped like the migrate_026 RPC result (one ``{valid, tier, over_quota}`` row).
    """
    db = MagicMock()
    row = {"valid": valid, "tier": tier, "over_quota": over_quota}
    db.rpc.return_value.execute.return_value = SimpleNamespace(data=[row])
    return db


_FAKE_ENTITY = {
    "id": 1,
    "body_slug": "schools",
    "place": {"state": "mo", "slug": "clayton", "display_name": "Clayton"},
}

# Config with the API open (no key) and the default (generous) rate limits.
_OPEN_CFG = _cfg(
    api_key="",
    rate_limit_search_per_minute=30,
    rate_limit_api_per_minute=60,
)

_ENRICHED = [
    {
        "chunk_id": 1919,
        "hash_id": "#q077f",
        "content": "  Verbatim passage about\nthe facilities plan.  ",
        "section": "Overview",
        "speaker": "",
        "rrf_score": 0.0163,
        "meeting_date": "",
        "meeting_title": "Volume1-ClaytonMasterPlan.pdf",
        "document_id": 87,
        "document_type": "facilities_plan",
        "summary": "Volume I of the plan.",
    }
]
_DOCS = {
    87: {
        "id": 87,
        "source_url": "https://example.test/Volume1.pdf",
        "source_portal": "claytonschools",
        "video_id": "",
    }
}
_MEETING_ROW = {
    "id": 195,
    "meeting_title": "February 1, 2023 Business Meeting Minutes",
    "document_type": "minutes",
    "meeting_date": "2023-02-01",
    "summary": "Signed minutes from the February 1, 2023 meeting.",
    "source_url": "https://example.test/minutes.pdf",
    "source_portal": "diligent",
    "video_id": "",
}
_TRANSCRIPT_ROW = {
    "id": 308,
    "meeting_title": "Board Safety Meeting transcript.txt",
    "document_type": "transcript",
    "meeting_date": "2024-05-01",
    "summary": "Transcript of the safety meeting.",
    "source_url": "https://example.test/transcript.txt",
    "source_portal": "youtube",
    "video_id": "5eoLIM4PQEg",
}


@pytest.fixture(autouse=True)
def _isolate_rate_limits():
    """Each test starts with a clean rate-limit bucket map."""
    _reset_rate_limits()
    yield
    _reset_rate_limits()


def _patch_search(**overrides):
    """Stack the patches the search route needs; returns a list of patchers."""
    cfg = overrides.get("cfg", _OPEN_CFG)
    return [
        patch("actalux.web.api.get_config", return_value=cfg),
        patch("actalux.web.api.get_db"),
        patch("actalux.web.api.get_entity_by_path", return_value=_FAKE_ENTITY),
        patch("actalux.web.api.embed_query", return_value=[0.0]),
        patch("actalux.web.api.build_reranker", return_value=None),
        patch("actalux.web.api.hybrid_search", return_value=[]),
        patch("actalux.web.api.enrich_results", return_value=_ENRICHED),
        patch("actalux.web.api.get_documents", return_value=_DOCS),
    ]


def _do_search(headers=None, params=None):
    patchers = _patch_search()
    for p in patchers:
        p.start()
    try:
        return client.get(
            f"{BASE}/search",
            params=params or {"q": "facilities plan"},
            headers=headers or {},
        )
    finally:
        for p in reversed(patchers):
            p.stop()


class TestSearch:
    def test_search_returns_cited_passages(self) -> None:
        r = _do_search()
        assert r.status_code == 200
        body = r.json()
        assert body["entity"] == "mo/clayton/schools"
        assert body["query"] == "facilities plan"
        assert body["count"] == 1
        hit = body["results"][0]
        assert hit["hash_id"] == "#q077f"
        # text is verbatim but whitespace-normalized (no leading space / newline)
        assert hit["text"] == "Verbatim passage about the facilities plan."
        # non-date-led type -> title is the cleaned filename
        assert hit["title"] == "Volume1-ClaytonMasterPlan"
        assert hit["source_url"] == "https://example.test/Volume1.pdf"
        assert hit["html_url"] == "/chunk/1919/source"
        assert hit["citation"] == "Volume1-ClaytonMasterPlan [#q077f]"

    def test_search_requires_query(self) -> None:
        r = _do_search(params={"q": ""})
        assert r.status_code == 422  # min_length=1

    def test_search_rejects_bad_date(self) -> None:
        r = _do_search(params={"q": "x", "date_from": "not-a-date"})
        assert r.status_code == 400


class TestAuth:
    def _search_with_cfg(self, cfg, headers=None):
        # These configs leave the keyed path dormant (api_keys_enabled defaults to
        # False), so a presented non-matching key 401s at the gate with no DB call,
        # preserving the original "key set + wrong key -> 401" semantic. The
        # matching-global-key and no-key cases never reach the keyed path either.
        db = _db_with_authorize(valid=False)
        patchers = [
            patch("actalux.web.api.get_config", return_value=cfg),
            patch("actalux.web.api.get_db", return_value=db),
            patch("actalux.web.api.get_entity_by_path", return_value=_FAKE_ENTITY),
            patch("actalux.web.api.embed_query", return_value=[0.0]),
            patch("actalux.web.api.build_reranker", return_value=None),
            patch("actalux.web.api.hybrid_search", return_value=[]),
            patch("actalux.web.api.enrich_results", return_value=_ENRICHED),
            patch("actalux.web.api.get_documents", return_value=_DOCS),
        ]
        for p in patchers:
            p.start()
        try:
            return client.get(f"{BASE}/search", params={"q": "x"}, headers=headers or {})
        finally:
            for p in reversed(patchers):
                p.stop()

    def test_open_when_no_key_configured(self) -> None:
        r = self._search_with_cfg(_OPEN_CFG)
        assert r.status_code == 200

    def test_rejects_missing_key_when_configured(self) -> None:
        cfg = _cfg(api_key="s3cret", rate_limit_search_per_minute=30, rate_limit_api_per_minute=60)
        r = self._search_with_cfg(cfg)
        assert r.status_code == 401

    def test_rejects_wrong_key(self) -> None:
        cfg = _cfg(api_key="s3cret", rate_limit_search_per_minute=30, rate_limit_api_per_minute=60)
        r = self._search_with_cfg(cfg, headers={"X-API-Key": "nope"})
        assert r.status_code == 401

    def test_accepts_x_api_key(self) -> None:
        cfg = _cfg(api_key="s3cret", rate_limit_search_per_minute=30, rate_limit_api_per_minute=60)
        r = self._search_with_cfg(cfg, headers={"X-API-Key": "s3cret"})
        assert r.status_code == 200

    def test_accepts_bearer_token(self) -> None:
        cfg = _cfg(api_key="s3cret", rate_limit_search_per_minute=30, rate_limit_api_per_minute=60)
        r = self._search_with_cfg(cfg, headers={"Authorization": "Bearer s3cret"})
        assert r.status_code == 200


class TestRateLimit:
    def test_search_429_after_limit(self) -> None:
        cfg = _cfg(api_key="", rate_limit_search_per_minute=2, rate_limit_api_per_minute=60)
        patchers = [
            patch("actalux.web.api.get_config", return_value=cfg),
            patch("actalux.web.api.get_db"),
            patch("actalux.web.api.get_entity_by_path", return_value=_FAKE_ENTITY),
            patch("actalux.web.api.embed_query", return_value=[0.0]),
            patch("actalux.web.api.build_reranker", return_value=None),
            patch("actalux.web.api.hybrid_search", return_value=[]),
            patch("actalux.web.api.enrich_results", return_value=_ENRICHED),
            patch("actalux.web.api.get_documents", return_value=_DOCS),
        ]
        for p in patchers:
            p.start()
        try:
            ip = {"Fly-Client-IP": "203.0.113.7"}
            assert client.get(f"{BASE}/search", params={"q": "x"}, headers=ip).status_code == 200
            assert client.get(f"{BASE}/search", params={"q": "x"}, headers=ip).status_code == 200
            third = client.get(f"{BASE}/search", params={"q": "x"}, headers=ip)
            assert third.status_code == 429
            assert "Retry-After" in third.headers
        finally:
            for p in reversed(patchers):
                p.stop()


class TestMeetingBundle:
    @patch("actalux.web.api.get_config", return_value=_OPEN_CFG)
    @patch("actalux.web.api.get_db")
    @patch("actalux.web.api.get_entity_by_path", return_value=_FAKE_ENTITY)
    @patch("actalux.web.api.get_meeting_documents", return_value=[_MEETING_ROW, _TRANSCRIPT_ROW])
    def test_bundle_groups_meeting_documents(self, m_docs, m_ent, m_db, m_cfg) -> None:
        r = client.get(f"{BASE}/meetings/2023-02-01")
        assert r.status_code == 200
        body = r.json()
        assert body["date"] == "2023-02-01"
        assert body["count"] == 2
        first = body["documents"][0]
        assert first["title"] == "February 1, 2023 — Meeting Minutes"
        assert first["html_url"] == "/document/195"
        # the transcript's source is its YouTube video, not the derived .txt
        transcript = body["documents"][1]
        assert transcript["source_url"] == "https://www.youtube.com/watch?v=5eoLIM4PQEg"

    @patch("actalux.web.api.get_config", return_value=_OPEN_CFG)
    @patch("actalux.web.api.get_db")
    @patch("actalux.web.api.get_entity_by_path", return_value=_FAKE_ENTITY)
    def test_bundle_rejects_bad_date(self, m_ent, m_db, m_cfg) -> None:
        r = client.get(f"{BASE}/meetings/2023-13-99")
        assert r.status_code == 400

    @patch("actalux.web.api.get_config", return_value=_OPEN_CFG)
    @patch("actalux.web.api.get_db")
    @patch("actalux.web.api.get_entity_by_path", return_value=None)
    def test_unknown_jurisdiction_404(self, m_ent, m_db, m_cfg) -> None:
        r = client.get("/api/v1/zz/nowhere/schools/meetings/2023-02-01")
        assert r.status_code == 404


class TestRecentFeed:
    @patch("actalux.web.api.get_config", return_value=_OPEN_CFG)
    @patch("actalux.web.api.get_db")
    @patch("actalux.web.api.get_entity_by_path", return_value=_FAKE_ENTITY)
    @patch("actalux.web.api.list_recent_meeting_documents", return_value=[_MEETING_ROW])
    def test_recent_returns_meeting_documents(self, m_list, m_ent, m_db, m_cfg) -> None:
        r = client.get(f"{BASE}/recent", params={"since": "2023-01-01", "limit": 5})
        assert r.status_code == 200
        body = r.json()
        assert body["since"] == "2023-01-01"
        assert body["count"] == 1
        assert body["items"][0]["document_id"] == 195
        # the recency query is bounded by the meeting types and the since/limit
        assert m_list.call_args.kwargs["since"] == "2023-01-01"
        assert m_list.call_args.kwargs["limit"] == 5

    @patch("actalux.web.api.get_config", return_value=_OPEN_CFG)
    @patch("actalux.web.api.get_db")
    @patch("actalux.web.api.get_entity_by_path", return_value=_FAKE_ENTITY)
    @patch("actalux.web.api.list_recent_meeting_documents", return_value=[])
    def test_recent_without_since(self, m_list, m_ent, m_db, m_cfg) -> None:
        r = client.get(f"{BASE}/recent")
        assert r.status_code == 200
        assert r.json()["since"] is None
        assert m_list.call_args.kwargs["since"] is None


_VOTE_ROWS = [
    {
        "id": 7,
        "document_id": 195,
        "meeting_date": "2023-02-01",
        "motion": "Approve the agenda as posted.",
        "result": "passed",
        "result_basis": "stated",
        "vote_count_yes": 7,
        "vote_count_no": 0,
        "vote_count_abstain": 0,
        "details": {"moved_by": "Ms. Chris Win"},
        "chunk_id": 1919,
        "citation_id": "abc12345",
        "source_quote": "Approve the agenda as posted. ... Motion Carries 7-0",
    },
    {
        "id": 8,
        "document_id": 195,
        "meeting_date": "2023-02-01",
        "motion": "Adjourn the meeting.",
        "result": "passed",
        "result_basis": "derived",
        "vote_count_yes": 7,
        "vote_count_no": 0,
        "vote_count_abstain": 0,
        "details": None,
        "chunk_id": 1920,
        "citation_id": "",  # no stable id -> falls back to chunk_id for routing
        "source_quote": "Adjourn the meeting. ... Aye ...",
    },
]


class TestVotes:
    @patch("actalux.web.api.get_config", return_value=_OPEN_CFG)
    @patch("actalux.web.api.get_db")
    @patch("actalux.web.api.get_entity_by_path", return_value=_FAKE_ENTITY)
    @patch("actalux.web.api.get_entity_votes", return_value=_VOTE_ROWS)
    @patch("actalux.web.api.get_documents", return_value={195: _MEETING_ROW})
    def test_votes_returns_cited_records(self, m_docs, m_votes, m_ent, m_db, m_cfg) -> None:
        r = client.get(f"{BASE}/votes", params={"since": "2023-01-01", "limit": 10})
        assert r.status_code == 200
        body = r.json()
        assert body["entity"] == "mo/clayton/schools"
        assert body["since"] == "2023-01-01"
        assert body["count"] == 2

        stated = body["votes"][0]
        assert stated["motion"] == "Approve the agenda as posted."
        assert stated["result"] == "passed"
        assert stated["result_basis"] == "stated"
        assert stated["vote_count_yes"] == 7
        # stable citation_id drives the citation hash and deep link
        assert stated["citation"] == "February 1, 2023 — Meeting Minutes [#qabc12345]"
        assert stated["html_url"] == "/chunk/abc12345/source"
        assert stated["source_url"] == "https://example.test/minutes.pdf"

        derived = body["votes"][1]
        assert derived["result_basis"] == "derived"
        # no citation_id -> routes on the numeric chunk_id
        assert derived["html_url"] == "/chunk/1920/source"
        assert m_votes.call_args.kwargs["since"] == "2023-01-01"
        assert m_votes.call_args.kwargs["limit"] == 10

    @patch("actalux.web.api.get_config", return_value=_OPEN_CFG)
    @patch("actalux.web.api.get_db")
    @patch("actalux.web.api.get_entity_by_path", return_value=_FAKE_ENTITY)
    def test_votes_rejects_bad_since(self, m_ent, m_db, m_cfg) -> None:
        r = client.get(f"{BASE}/votes", params={"since": "nope"})
        assert r.status_code == 400


# Config with the API LOCKED behind a global key AND the keyed-DB path ENABLED, so
# any unmatched key takes the keyed (RPC) path and the global key takes the admin
# path. Generous flat limits so rate limiting never interferes with the
# tier-resolution assertions.
_KEYED_CFG = _cfg(
    api_key="globaladmin",
    api_keys_enabled=True,
    rate_limit_search_per_minute=30,
    rate_limit_api_per_minute=60,
)


class TestTiers:
    """Key-tier resolution: developer / invalid / over-quota / anonymous / admin.

    All exercise the search route (it is fully stubbable). The only live call on the
    mock db in the keyed path is ``rpc('api_key_authorize')``; search's own DB work
    is patched out by ``_patch_search``.
    """

    def _search(self, cfg, db, headers=None, ip="198.51.100.5"):
        """Run a search with a specific config + db mock, isolating the rate bucket
        per call via a distinct client IP so tier numbers don't bleed across tests.
        """
        patchers = [
            patch("actalux.web.api.get_config", return_value=cfg),
            patch("actalux.web.api.get_db", return_value=db),
            patch("actalux.web.api.get_entity_by_path", return_value=_FAKE_ENTITY),
            patch("actalux.web.api.embed_query", return_value=[0.0]),
            patch("actalux.web.api.build_reranker", return_value=None),
            patch("actalux.web.api.expand_and_embed", return_value=[]),
            patch("actalux.web.api.hybrid_search", return_value=[]),
            patch("actalux.web.api.enrich_results", return_value=_ENRICHED),
            patch("actalux.web.api.get_documents", return_value=_DOCS),
        ]
        for p in patchers:
            p.start()
        try:
            hdrs = {"Fly-Client-IP": ip, **(headers or {})}
            return client.get(f"{BASE}/search", params={"q": "x"}, headers=hdrs)
        finally:
            for p in reversed(patchers):
                p.stop()

    def test_valid_developer_key_authorized(self) -> None:
        db = _db_with_authorize(valid=True, tier="developer")
        r = self._search(_KEYED_CFG, db, headers={"X-API-Key": "ak_devkey"})
        assert r.status_code == 200
        # the call was authorized via the single-param RPC (period derived in SQL)
        call = db.rpc.call_args
        assert call.args[0] == "api_key_authorize"
        params = call.args[1]
        assert set(params) == {"p_key_hash"}  # no caller-set period param
        assert len(params["p_key_hash"]) == 64  # sha256 hex, not the raw key
        assert params["p_key_hash"] != "ak_devkey"

    def test_developer_tier_raises_the_search_limit(self) -> None:
        # Anonymous search limit is 1/min here; a developer key must lift it well
        # past that (developer search_per_min = 60), so a 2nd keyed call still 200s.
        cfg = _cfg(
            api_key="globaladmin",
            api_keys_enabled=True,
            rate_limit_search_per_minute=1,
            rate_limit_api_per_minute=1,
        )
        db = _db_with_authorize(valid=True, tier="developer")
        first = self._search(cfg, db, headers={"X-API-Key": "ak_devkey"}, ip="198.51.100.11")
        second = self._search(cfg, db, headers={"X-API-Key": "ak_devkey"}, ip="198.51.100.11")
        assert first.status_code == 200
        assert second.status_code == 200

    def test_anonymous_limit_unchanged_by_tier_table(self) -> None:
        # No key, no global key: anonymous tier reads the flat config limit (1/min),
        # so the 2nd call from the same IP is throttled exactly as before.
        cfg = _cfg(api_key="", rate_limit_search_per_minute=1, rate_limit_api_per_minute=60)
        db = MagicMock()
        first = self._search(cfg, db, ip="198.51.100.22")
        second = self._search(cfg, db, ip="198.51.100.22")
        assert first.status_code == 200
        assert second.status_code == 429
        # anonymous path makes NO api_key_authorize RPC call
        assert not any(c.args and c.args[0] == "api_key_authorize" for c in db.rpc.call_args_list)

    def test_invalid_key_401(self) -> None:
        db = _db_with_authorize(valid=False)
        r = self._search(_KEYED_CFG, db, headers={"X-API-Key": "ak_bogus"})
        assert r.status_code == 401

    def test_over_quota_429(self) -> None:
        db = _db_with_authorize(valid=True, tier="developer", over_quota=True)
        r = self._search(_KEYED_CFG, db, headers={"X-API-Key": "ak_exhausted"})
        assert r.status_code == 429

    def test_keyed_path_dormant_when_disabled(self) -> None:
        # api_keys_enabled=False (prod default): a presented non-global key is 401'd
        # immediately, with NO DB touch — neither get_db nor the authorize RPC fire.
        # Built without the _search helper so get_db's call count is observable here.
        cfg = _cfg(
            api_key="globaladmin",
            api_keys_enabled=False,
            rate_limit_search_per_minute=30,
            rate_limit_api_per_minute=60,
        )
        db = MagicMock()
        with (
            patch("actalux.web.api.get_config", return_value=cfg),
            patch("actalux.web.api.get_db", return_value=db) as m_get_db,
            patch("actalux.web.api.get_entity_by_path", return_value=_FAKE_ENTITY),
        ):
            r = client.get(
                f"{BASE}/search",
                params={"q": "x"},
                headers={"Fly-Client-IP": "198.51.100.33", "X-API-Key": "ak_devkey"},
            )
        assert r.status_code == 401
        # the auth gate rejected before any DB access
        m_get_db.assert_not_called()  # get_db() never invoked
        db.rpc.assert_not_called()

    def test_admin_via_global_key(self) -> None:
        # The global ACTALUX_API_KEY resolves to admin WITHOUT any RPC call.
        db = MagicMock()
        r = self._search(_KEYED_CFG, db, headers={"X-API-Key": "globaladmin"})
        assert r.status_code == 200
        assert not any(c.args and c.args[0] == "api_key_authorize" for c in db.rpc.call_args_list)

    def test_admin_via_bearer_global_key(self) -> None:
        db = MagicMock()
        r = self._search(_KEYED_CFG, db, headers={"Authorization": "Bearer globaladmin"})
        assert r.status_code == 200
