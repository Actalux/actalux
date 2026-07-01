"""Unit tests for the SEO sitemap + robots builders (no DB)."""

from __future__ import annotations

import xml.etree.ElementTree as ET
from unittest.mock import patch

from actalux.web import sitemap


def test_robots_txt_points_at_sitemap() -> None:
    txt = sitemap.build_robots_txt("https://actalux.org/")  # trailing slash tolerated
    assert "User-agent: *" in txt
    assert "Sitemap: https://actalux.org/sitemap.xml" in txt


def test_robots_txt_disallows_dynamic_and_fragment_paths() -> None:
    """Search-result and per-chunk citation pages are load-heavy and not canonical
    content; the sitemap lists the document/matter/member pages instead."""
    txt = sitemap.build_robots_txt("https://actalux.org")
    assert "Disallow: /*/search\n" in txt
    assert "Disallow: /chunk/\n" in txt


def test_collect_locs_builds_canonical_urls() -> None:
    entities = [{"id": 1, "body_slug": "council", "place": {"state": "mo", "slug": "clayton"}}]
    with (
        patch.object(sitemap, "list_entities", return_value=entities),
        patch.object(sitemap, "body_members", return_value=[{"slug": "jane-doe"}]),
        patch.object(
            sitemap,
            "body_matters",
            return_value=[{"slug": "bill-1", "meeting_date": "2026-06-01"}],
        ),
        patch.object(
            sitemap, "fetch_all_rows", return_value=[{"id": 7, "meeting_date": "2024-04-10"}]
        ),
    ):
        locs = sitemap.collect_locs(object(), "https://actalux.org")

    urls = {u for u, _ in locs}
    assert "https://actalux.org/mo/clayton" in urls  # place hub (once)
    assert "https://actalux.org/mo/clayton/council" in urls  # body hub
    assert "https://actalux.org/mo/clayton/council/members" in urls
    assert "https://actalux.org/mo/clayton/council/matters" in urls
    assert "https://actalux.org/mo/clayton/council/member/jane-doe" in urls
    assert "https://actalux.org/mo/clayton/council/matter/bill-1" in urls
    assert "https://actalux.org/document/7" in urls
    # lastmod is carried for matters + documents
    assert ("https://actalux.org/document/7", "2024-04-10") in locs


def test_collect_locs_skips_entity_with_incomplete_place() -> None:
    entities = [{"id": 1, "body_slug": "council", "place": {}}]  # missing state/slug
    with (
        patch.object(sitemap, "list_entities", return_value=entities),
        patch.object(sitemap, "fetch_all_rows", return_value=[]),
    ):
        locs = sitemap.collect_locs(object(), "https://actalux.org")
    assert locs == []


def test_build_sitemap_xml_is_wellformed() -> None:
    with (
        patch.object(sitemap, "list_entities", return_value=[]),
        patch.object(sitemap, "fetch_all_rows", return_value=[{"id": 1, "meeting_date": None}]),
    ):
        sitemap._cache.clear()
        xml = sitemap.build_sitemap_xml(object(), "https://actalux.org")
    root = ET.fromstring(xml)  # raises if not well-formed
    assert root.tag.endswith("urlset")
    assert "https://actalux.org/document/1" in xml
