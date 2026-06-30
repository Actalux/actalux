"""robots.txt + a DB-driven sitemap for actalux.org (SEO Phase 1).

Enumerates the canonical, server-rendered entity pages a crawler should index:
the place hub, each body hub (and its members/matters index), every publishable
member and matter detail page, and every current (non-superseded) document.
HTMX partial routes (`/pane`, `/stream`) are deliberately excluded — they are
fragments, not canonical content URLs.

The sitemap touches several tables, so the rendered XML is cached per base_url
for ``SITEMAP_TTL_SECONDS`` (sitemaps don't need to be real-time fresh).
"""

from __future__ import annotations

import time
from xml.sax.saxutils import escape

from supabase import Client

from actalux.db import fetch_all_rows, list_entities
from actalux.graph.store import body_matters, body_members

SITEMAP_TTL_SECONDS = 3600

# Rendered XML cached as {base_url: (xml, generated_at)}.
_cache: dict[str, tuple[str, float]] = {}


def _url(loc: str, lastmod: str | None = None) -> str:
    parts = [f"<loc>{escape(loc)}</loc>"]
    if lastmod:
        parts.append(f"<lastmod>{escape(lastmod)}</lastmod>")
    return "<url>" + "".join(parts) + "</url>"


def collect_locs(client: Client, base_url: str) -> list[tuple[str, str | None]]:
    """Return (absolute_url, lastmod) for every canonical page, in stable order."""
    base = base_url.rstrip("/")
    locs: list[tuple[str, str | None]] = []
    seen_places: set[str] = set()

    for entity in list_entities(client):
        place = entity.get("place") or {}
        state, pslug, bslug = place.get("state"), place.get("slug"), entity.get("body_slug")
        if not (state and pslug and bslug):
            continue
        place_path = f"/{state}/{pslug}"
        if place_path not in seen_places:
            seen_places.add(place_path)
            locs.append((base + place_path, None))
        body_path = f"{place_path}/{bslug}"
        locs.append((base + body_path, None))
        locs.append((base + body_path + "/members", None))
        locs.append((base + body_path + "/matters", None))
        for member in body_members(client, entity["id"]):
            if member.get("slug"):
                locs.append((base + f"{body_path}/member/{member['slug']}", None))
        for matter in body_matters(client, entity["id"]):
            if matter.get("slug"):
                locs.append(
                    (base + f"{body_path}/matter/{matter['slug']}", matter.get("meeting_date"))
                )

    # Every current document detail page (superseded versions excluded). Paged so
    # the newest documents aren't dropped once the corpus exceeds the row cap.
    docs = fetch_all_rows(
        lambda: client.table("documents").select("id,meeting_date").is_("replaces_id", "null"),
        order="id",
    )
    locs.extend((base + f"/document/{d['id']}", d.get("meeting_date")) for d in docs)
    return locs


def build_sitemap_xml(client: Client, base_url: str) -> str:
    """Render (and cache for SITEMAP_TTL_SECONDS) the urlset XML for ``base_url``."""
    now = time.time()
    cached = _cache.get(base_url)
    if cached and now - cached[1] < SITEMAP_TTL_SECONDS:
        return cached[0]
    body = "".join(_url(loc, lastmod) for loc, lastmod in collect_locs(client, base_url))
    xml = (
        '<?xml version="1.0" encoding="UTF-8"?>'
        '<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">'
        f"{body}</urlset>"
    )
    _cache[base_url] = (xml, now)
    return xml


def build_robots_txt(base_url: str) -> str:
    """robots.txt: allow crawling, point at the sitemap, keep crawlers off fragments."""
    base = base_url.rstrip("/")
    return (
        "User-agent: *\n"
        "Allow: /\n"
        "Disallow: /document/*/pane\n"
        "Disallow: /ask/stream\n"
        # Search-result pages are dynamic queries, not content to index. Pages link
        # documents/matters/meetings to a /search?q=<title>, so without this a crawler
        # follows one expensive hybrid search per item and saturates the database.
        "Disallow: /*/search\n"
        f"Sitemap: {base}/sitemap.xml\n"
    )
