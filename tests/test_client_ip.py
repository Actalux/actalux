"""Client-IP resolution for rate limiting, direct and behind the Cloudflare proxy."""

from __future__ import annotations

from starlette.requests import Request

from actalux.web.api import _CLOUDFLARE_RANGES, _client_ip

CF_EDGE_V4 = "172.64.1.1"  # inside 172.64.0.0/13
CF_EDGE_V6 = "2606:4700::1"  # inside 2606:4700::/32
VISITOR = "203.0.113.7"
NOT_CLOUDFLARE = "198.51.100.9"


def _request(headers: dict[str, str], peer: str = "10.0.0.1") -> Request:
    raw = [(k.lower().encode(), v.encode()) for k, v in headers.items()]
    return Request({"type": "http", "headers": raw, "client": (peer, 1234)})


def test_ranges_file_loads_both_families() -> None:
    assert any(net.version == 4 for net in _CLOUDFLARE_RANGES)
    assert any(net.version == 6 for net in _CLOUDFLARE_RANGES)


def test_direct_request_uses_fly_client_ip() -> None:
    assert _client_ip(_request({"fly-client-ip": VISITOR})) == VISITOR


def test_via_cloudflare_uses_cf_connecting_ip() -> None:
    req = _request({"fly-client-ip": CF_EDGE_V4, "cf-connecting-ip": VISITOR})
    assert _client_ip(req) == VISITOR


def test_via_cloudflare_ipv6_edge() -> None:
    req = _request({"fly-client-ip": CF_EDGE_V6, "cf-connecting-ip": VISITOR})
    assert _client_ip(req) == VISITOR


def test_spoofed_cf_header_from_outside_cloudflare_is_ignored() -> None:
    req = _request({"fly-client-ip": NOT_CLOUDFLARE, "cf-connecting-ip": VISITOR})
    assert _client_ip(req) == NOT_CLOUDFLARE


def test_cloudflare_edge_without_visitor_header_falls_back_to_edge() -> None:
    assert _client_ip(_request({"fly-client-ip": CF_EDGE_V4})) == CF_EDGE_V4


def test_malformed_fly_header_is_not_treated_as_cloudflare() -> None:
    req = _request({"fly-client-ip": "garbage", "cf-connecting-ip": VISITOR})
    assert _client_ip(req) == "garbage"


def test_no_fly_header_falls_back_to_forwarded_then_socket() -> None:
    assert _client_ip(_request({"x-forwarded-for": f"{VISITOR}, 10.0.0.2"})) == VISITOR
    assert _client_ip(_request({})) == "10.0.0.1"
