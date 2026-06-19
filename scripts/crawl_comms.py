#!/usr/bin/env python3
"""Crawl the School District of Clayton's district-news communications.

The district publishes press releases, board/budget news, superintendent and
board messages, and its "Inside Clayton" newsletter as native HTML posts in one
stream at https://www.claytonschools.net/about-us/district-news. There is no RSS
or JSON feed, but the listing page server-renders every current post link, and
each post page exposes a clean headline, publication date, and body.

This downloads each post as a minimal HTML file (headline + body paragraphs) into
data/documents/ and writes a manifest for the standard ingester:

    python scripts/ingest.py --manifest data/documents/comms_manifest.json

Crawl scope + etiquette: only the two robots-allowed paths are fetched
(/about-us/district-news and /post-details/...), with the site's 5-second
crawl-delay honored between every request. The listing exposes a rolling ~8-month
window (currently ~35 posts); older posts are not reachable over plain HTTP, so
run this on a cadence to capture new posts before they roll off.

This script only downloads + writes the manifest; it does NOT ingest. Review the
manifest (and the content policy — district news can name individual students or
staff, and election/ballot coverage must stay nonpartisan) before ingesting.

Usage:
    python scripts/crawl_comms.py            # download all current posts
    python scripts/crawl_comms.py --limit 3  # download the newest 3 (a test run)
"""

from __future__ import annotations

import argparse
import html
import json
import logging
import re
import time
from pathlib import Path
from urllib.parse import urljoin

import httpx
from bs4 import BeautifulSoup

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

LISTING_URL = "https://www.claytonschools.net/about-us/district-news"
OUTPUT_DIR = Path("data/documents")
MANIFEST_PATH = OUTPUT_DIR / "comms_manifest.json"
SOURCE_PORTAL = "claytonschools"
# robots.txt sets Crawl-delay: 5 for this host; honored between every request.
CRAWL_DELAY_SECONDS = 5.0
USER_AGENT = "ActaluxBot/1.0 (+https://actalux.org; civic public-records archive)"
# Authoritative post date: <meta property="article:published" content="...Z">.
_PUBLISHED_META = {"property": "article:published"}


def _fetch(client: httpx.Client, url: str, *, delay: bool) -> httpx.Response | None:
    """GET a URL, honoring the crawl-delay. Returns the response or None on error."""
    if delay:
        time.sleep(CRAWL_DELAY_SECONDS)
    try:
        resp = client.get(url, follow_redirects=True)
        resp.raise_for_status()
        return resp
    except httpx.HTTPError as exc:
        logger.error("Fetch failed %s: %s", url, exc)
        return None


def discover_post_urls(client: httpx.Client) -> list[str]:
    """Return the post URLs linked from the district-news listing, in page order."""
    resp = _fetch(client, LISTING_URL, delay=False)
    if resp is None:
        return []
    soup = BeautifulSoup(resp.text, "lxml")
    urls = [urljoin(LISTING_URL, a["href"]) for a in soup.select("a.fsPostLink[href]")]
    return list(dict.fromkeys(urls))  # de-dupe, preserve order


def _post_date(soup: BeautifulSoup) -> str | None:
    """The post's publication date as YYYY-MM-DD, from article:published meta."""
    meta = soup.find("meta", attrs=_PUBLISHED_META)
    content = meta.get("content") if meta else None
    if not content:
        return None
    match = re.match(r"(\d{4}-\d{2}-\d{2})", content)
    return match.group(1) if match else None


def _post_body_paragraphs(soup: BeautifulSoup) -> list[str]:
    """Clean body paragraphs from the post's main content element.

    The post body is the <p> text of the largest <article> (falling back to the
    largest fsElementContent block) — the post-detail content region, excluding
    the page's nav/footer.
    """
    candidates = soup.select("article") or soup.select("div.fsElementContent")
    if not candidates:
        return []
    best = max(candidates, key=lambda el: len(el.get_text()))
    paras = [p.get_text(" ", strip=True) for p in best.find_all("p")]
    return [p for p in paras if p]


def parse_post(resp: httpx.Response) -> tuple[str, str, list[str]] | None:
    """Parse a post page into (title, date, paragraphs), or None if incomplete."""
    soup = BeautifulSoup(resp.text, "lxml")
    h1 = soup.find("h1")
    title = h1.get_text(strip=True) if h1 else ""
    date = _post_date(soup)
    paragraphs = _post_body_paragraphs(soup)
    if not title or not date or not paragraphs:
        return None
    return title, date, paragraphs


def slug_of(url: str) -> str:
    """The post's URL slug (its stable per-post identifier)."""
    return url.rstrip("/").rsplit("/", 1)[-1]


def render_html(title: str, paragraphs: list[str]) -> str:
    """A minimal, clean HTML document the standard parser can read verbatim.

    Only escaped text is emitted (no scraped markup, scripts, or links), and the
    headline is included so the post is retrievable by its title.
    """
    body = "".join(f"<p>{html.escape(p)}</p>" for p in paragraphs)
    return f"<!doctype html><html><body><h1>{html.escape(title)}</h1>{body}</body></html>"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--limit", type=int, default=0, help="download only the newest N posts (0 = all)"
    )
    args = parser.parse_args()

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    manifest: list[dict[str, str]] = []

    with httpx.Client(timeout=60.0, headers={"User-Agent": USER_AGENT}) as client:
        post_urls = discover_post_urls(client)
        if args.limit:
            post_urls = post_urls[: args.limit]
        logger.info("Discovered %d post(s) on the district-news listing.", len(post_urls))

        for url in post_urls:
            resp = _fetch(client, url, delay=True)
            if resp is None:
                continue
            parsed = parse_post(resp)
            if parsed is None:
                logger.warning("Skipping (no title/date/body): %s", url)
                continue
            title, date, paragraphs = parsed
            slug = slug_of(url)
            filename = f"comms_{slug}.html"
            (OUTPUT_DIR / filename).write_text(render_html(title, paragraphs), encoding="utf-8")
            manifest.append(
                {
                    "source_file": filename,
                    "source_url": url,
                    "source_portal": SOURCE_PORTAL,
                    "document_type": "communication",
                    "meeting_date": date,
                    "meeting_title": title,
                    "date_source": "content",
                }
            )
            logger.info("Saved %s — %s (%s)", filename, title, date)

    MANIFEST_PATH.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    logger.info("Crawl complete: %d post(s); manifest at %s", len(manifest), MANIFEST_PATH)
    logger.info("Review the manifest + content policy before ingesting with ingest.py --manifest.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
