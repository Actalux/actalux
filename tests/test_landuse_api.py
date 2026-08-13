"""Tests for the land-use stealth API (actalux.web.landuse_api).

The property under test is stealth itself: to anyone without both a key and a
configured service client, these routes are indistinguishable from routes that
do not exist. A 401 here would be a leak — it confirms there is something to
authenticate against.
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from fastapi.testclient import TestClient

from actalux.web.app import app

client = TestClient(app)

URL = "/api/v1/mo/clayton/land-use/cases"

_CFG_WITH_ADMIN = SimpleNamespace(api_key="admin-key", api_keys_enabled=False)
_CFG_OPEN = SimpleNamespace(api_key="", api_keys_enabled=False)
_PLACE = {"id": 1, "state": "mo", "slug": "clayton"}


def _fake_db(rows: list[dict] | None = None, count: int = 0):
    """A chainable fake supabase client whose terminal execute() returns rows."""
    db = MagicMock()
    result = SimpleNamespace(data=rows or [], count=count)
    chain = db.table.return_value.select.return_value.eq.return_value
    chain.eq.return_value = chain
    chain.gte.return_value = chain
    chain.lte.return_value = chain
    chain.order.return_value.execute.return_value = result
    chain.order.return_value.range.return_value.execute.return_value = result
    chain.execute.return_value = result
    return db


class TestStealthGate:
    def test_no_key_is_404_not_401(self) -> None:
        with (
            patch("actalux.web.landuse_api.get_config", return_value=_CFG_WITH_ADMIN),
            patch("actalux.web.landuse_api._service_db", return_value=_fake_db()),
        ):
            r = client.get(URL)
        assert r.status_code == 404  # never 401: a 401 admits the route exists

    def test_wrong_key_is_404(self) -> None:
        with (
            patch("actalux.web.landuse_api.get_config", return_value=_CFG_WITH_ADMIN),
            patch("actalux.web.landuse_api._service_db", return_value=_fake_db()),
        ):
            r = client.get(URL, headers={"X-API-Key": "nope"})
        assert r.status_code == 404

    def test_admin_key_without_service_client_is_still_404(self) -> None:
        # Production today: no service key on the web host. Even the operator's
        # key gets a 404 there — turning the product on is a secrets decision.
        with (
            patch("actalux.web.landuse_api.get_config", return_value=_CFG_WITH_ADMIN),
            patch("actalux.web.landuse_api._service_db", return_value=None),
        ):
            r = client.get(URL, headers={"X-API-Key": "admin-key"})
        assert r.status_code == 404

    def test_open_public_api_does_not_open_these_routes(self) -> None:
        # The public API serves anonymous traffic when no global key is set;
        # the stealth routes never do, whatever the global toggles say.
        with (
            patch("actalux.web.landuse_api.get_config", return_value=_CFG_OPEN),
            patch("actalux.web.landuse_api._service_db", return_value=_fake_db()),
        ):
            r = client.get(URL)
        assert r.status_code == 404

    def test_admin_key_with_service_client_serves(self) -> None:
        rows = [{"id": 7, "address_raw": "42 N Central", "status": "approved"}]
        with (
            patch("actalux.web.landuse_api.get_config", return_value=_CFG_WITH_ADMIN),
            patch("actalux.web.landuse_api._service_db", return_value=_fake_db(rows, count=1)),
            patch("actalux.web.api.get_place_by_path", return_value=_PLACE),
            patch("actalux.web.api.get_db"),
        ):
            r = client.get(URL, headers={"X-API-Key": "admin-key"})
        assert r.status_code == 200
        body = r.json()
        assert body["count"] == 1
        assert body["cases"][0]["address_raw"] == "42 N Central"

    def test_not_in_openapi_schema(self) -> None:
        # include_in_schema=False: the OpenAPI document must not reveal the
        # surface either.
        assert not any("land-use" in path for path in app.openapi()["paths"])


class TestCaseDetail:
    def test_case_outside_the_place_404s(self) -> None:
        # Jurisdiction scoping is part of the lookup: a real case id under the
        # wrong place path must not resolve.
        db = _fake_db(rows=[], count=0)
        with (
            patch("actalux.web.landuse_api.get_config", return_value=_CFG_WITH_ADMIN),
            patch("actalux.web.landuse_api._service_db", return_value=db),
            patch("actalux.web.api.get_place_by_path", return_value=_PLACE),
            patch("actalux.web.api.get_db"),
        ):
            r = client.get(f"{URL}/999", headers={"X-API-Key": "admin-key"})
        assert r.status_code == 404
