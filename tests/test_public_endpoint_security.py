"""Public abuse boundaries; all storage and paid services are mocked."""

from concurrent.futures import ThreadPoolExecutor
from contextlib import ExitStack
from dataclasses import replace
from unittest.mock import Mock, patch

import pytest
from fastapi import HTTPException
from fastapi.testclient import TestClient
from starlette.requests import Request

from actalux.config import load_config
from actalux.search.summarize import Summary
from actalux.web import api
from actalux.web import app as web


@pytest.fixture(autouse=True)
def isolated_limits():
    api._reset_rate_limits()
    web._ask_daily_count.clear()
    yield
    api._reset_rate_limits()
    web._ask_daily_count.clear()


@pytest.fixture
def services():
    cfg = replace(load_config(), openrouter_api_key="test-only", rate_limit_ask_per_minute=2)
    entity = {
        "id": 1,
        "body_slug": "schools",
        "type": "school_district",
        "display_name": "Test body",
        "place": {"state": "mo", "slug": "clayton", "display_name": "Test place"},
    }
    with ExitStack() as stack:
        mocks = {}
        values = {
            "_get_config": cfg,
            "_get_db": Mock(),
            "get_entity_by_path": entity,
            "_embed_query": [0.0],
            "_reranker": None,
            "assemble_evidence": ([], "text"),
            "generate_summary": Summary("No records.", 0, 0, 0),
            "insert_correction": None,
        }
        for name, value in values.items():
            mocks[name] = stack.enter_context(patch.object(web, name, return_value=value))
        yield cfg, mocks, TestClient(web.app, raise_server_exceptions=True)


def test_summary_shared_minute_and_daily_caps(services):
    cfg, mocks, client = services
    route = "/mo/clayton/schools/summarize"
    assert client.post(route, data={"q": "budget"}).status_code == 200
    assert sum(web._ask_daily_count.values()) == 1
    assert client.post(route, data={"q": "budget"}).status_code == 200
    blocked = client.post(route, data={"q": "budget"})
    assert blocked.status_code == 429 and "Retry-After" in blocked.headers
    assert mocks["generate_summary"].call_count == 2
    assert sum(web._ask_daily_count.values()) == 2
    api._reset_rate_limits()
    mocks["_get_config"].return_value = replace(cfg, ask_daily_message_cap=2)
    assert client.post(route, data={"q": "budget"}).status_code == 429
    assert mocks["generate_summary"].call_count == 2


@pytest.mark.parametrize("data", [{"q": "x" * 2001}, {"q": "budget", "date_from": "bad"}])
def test_summary_invalid_input_never_spends(services, data):
    _, mocks, client = services
    assert client.post("/mo/clayton/schools/summarize", data=data).status_code == 400
    mocks["_embed_query"].assert_not_called()
    mocks["generate_summary"].assert_not_called()
    assert not web._ask_daily_count


def test_summary_and_ask_share_ip_bucket(services):
    _, mocks, client = services
    request = Request({"type": "http", "headers": [], "client": ("testclient", 1)})
    api._enforce_rate(request, "ask", 2)
    api._enforce_rate(request, "ask", 2)
    assert client.post("/mo/clayton/schools/summarize", data={"q": "budget"}).status_code == 429
    mocks["generate_summary"].assert_not_called()


def test_corrections_hourly_limit_and_recovery(services):
    _, mocks, client = services
    data = {"chunk_id": "1", "description": "Please check this quote", "email": ""}
    with patch.object(api.time, "monotonic", return_value=100.0):
        for _ in range(5):
            assert client.post("/report-error", data=data).status_code == 200
        assert client.post("/report-error", data=data).status_code == 429
    with patch.object(api.time, "monotonic", return_value=3700.0):
        assert client.post("/report-error", data=data).status_code == 200
    assert mocks["insert_correction"].call_count == 6


@pytest.mark.parametrize(
    "field,value",
    [
        ("description", "x" * 4001),
        ("email", "x" * 255),
        ("chunk_id", "0"),
    ],
)
def test_invalid_correction_never_inserts(services, field, value):
    _, mocks, client = services
    data = {"chunk_id": "1", "description": "Please check", "email": ""}
    data[field] = value
    assert client.post("/report-error", data=data).status_code in (400, 422)
    mocks["insert_correction"].assert_not_called()


def test_limiter_admission_is_atomic():
    request = Request({"type": "http", "headers": [], "client": ("testclient", 1)})

    def attempt(_):
        try:
            api._enforce_rate(request, "parallel", 5)
            return True
        except HTTPException as exc:
            assert exc.status_code == 429
            return False

    with ThreadPoolExecutor(max_workers=20) as pool:
        assert sum(pool.map(attempt, range(100))) == 5


def test_full_limiter_preserves_active_limits_and_reclaims_expired():
    with patch.object(api, "_RATE_MAX_BUCKETS", 2), patch.object(api.time, "monotonic") as clock:
        clock.return_value = 100.0
        for name in ("one", "two"):
            req = Request({"type": "http", "headers": [], "client": (name, 1)})
            api._enforce_rate(req, "test", 1)
        req = Request({"type": "http", "headers": [], "client": ("three", 1)})
        with pytest.raises(HTTPException) as exc:
            api._enforce_rate(req, "test", 1)
        assert exc.value.status_code == 429
        assert len(api._rate_buckets) == 2
        clock.return_value = 161.0
        api._enforce_rate(req, "test", 1)
        assert len(api._rate_buckets) <= 2
