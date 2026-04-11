#!/usr/bin/env python3
"""Extract text from Canva curriculum maps using headless browser.

Loads each Canva share URL, extracts DOM text, saves as .txt files,
and writes a manifest for ingestion.

Usage:
    python scripts/crawl_canva_maps.py

Requires gstack browse daemon to be running.
"""

from __future__ import annotations

import json
import logging
import re
import subprocess
import time
from pathlib import Path

import httpx
from bs4 import BeautifulSoup

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

OUTPUT_DIR = Path("data/documents")
MANIFEST_PATH = Path("data/documents/canva_manifest.json")

# Browse binary path
BROWSE_BIN = Path.home() / ".claude/skills/gstack/browse/dist/browse"

# Curriculum pages to scrape for Canva links
CURRICULUM_PAGES = [
    "https://www.claytonschools.net/curriculum-resources/fine-arts-curriculum",
    "https://www.claytonschools.net/curriculum-resources/humanities-curriculum",
    "https://www.claytonschools.net/curriculum-resources/pe-health-curriculum",
    "https://www.claytonschools.net/curriculum-resources/programs",
    "https://www.claytonschools.net/curriculum-resources/stem",
]

CANVA_URL_RE = re.compile(
    r"https://www\.canva\.com/design/([A-Za-z0-9_-]+)/([A-Za-z0-9_-]+)/view[^\"']*"
)


def discover_canva_urls() -> list[tuple[str, str, str]]:
    """Scrape curriculum pages for Canva links.

    Returns list of (title, design_id, full_url).
    """
    results: list[tuple[str, str, str]] = []
    seen_ids: set[str] = set()

    with httpx.Client(timeout=30.0) as client:
        for page_url in CURRICULUM_PAGES:
            logger.info("Scanning: %s", page_url)
            try:
                resp = client.get(page_url, follow_redirects=True)
                resp.raise_for_status()
            except httpx.HTTPError as exc:
                logger.error("Failed to fetch %s: %s", page_url, exc)
                continue

            soup = BeautifulSoup(resp.text, "lxml")

            for link in soup.find_all("a", href=True):
                href = link["href"]
                match = CANVA_URL_RE.search(href)
                if match:
                    design_id = match.group(1)
                    if design_id in seen_ids:
                        continue
                    seen_ids.add(design_id)

                    title = link.get_text(strip=True)
                    if not title or len(title) < 3:
                        title = f"Canva_{design_id}"

                    full_url = match.group(0)
                    results.append((title, design_id, full_url))

    logger.info("Discovered %d unique Canva designs", len(results))
    return results


def browse_cmd(cmd: str) -> str:
    """Run a browse command and return output."""
    result = subprocess.run(
        [str(BROWSE_BIN)] + cmd.split(),
        capture_output=True,
        text=True,
        timeout=30,
    )
    return result.stdout.strip()


def extract_canva_text(url: str) -> str:
    """Load a Canva URL in headless browser, extract text."""
    browse_cmd(f"goto {url}")
    time.sleep(2)  # wait for Canva JS to render
    raw_text = browse_cmd("text")

    # Strip the untrusted content markers
    lines = raw_text.split("\n")
    content_lines: list[str] = []
    for line in lines:
        if "BEGIN UNTRUSTED" in line or "END UNTRUSTED" in line:
            continue
        content_lines.append(line)

    text = "\n".join(content_lines).strip()

    # Remove Canva UI chrome text
    text = re.sub(
        r"(Screen reader content is loading|Page controls|Toolbar|"
        r"Previous page|Next page|Zoom in and out|More|"
        r"Enter full screen|Share|Create with Canva|\d+/\d+$)",
        "",
        text,
    )
    text = re.sub(r"Canva⁠\(opens in a new tab or window\)", "", text)

    return text.strip()


def sanitize_filename(title: str) -> str:
    """Convert title to safe filename."""
    clean = re.sub(r'[<>:"/\\|?*]', "", title)
    clean = re.sub(r"\s+", "_", clean.strip())
    return clean[:80]


def main() -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    if not BROWSE_BIN.exists():
        logger.error("Browse binary not found at %s", BROWSE_BIN)
        return

    # 1. Discover all Canva URLs from curriculum pages
    canva_designs = discover_canva_urls()

    if not canva_designs:
        logger.error("No Canva designs found")
        return

    # 2. Extract text from each design
    manifest: list[dict[str, str]] = []
    total_extracted = 0

    for title, design_id, url in canva_designs:
        logger.info("Extracting: %s (%s)", title, design_id)

        try:
            text = extract_canva_text(url)
        except Exception as exc:
            logger.error("Failed to extract %s: %s", title, exc)
            continue

        if len(text) < 20:
            logger.warning("Skipping %s: too little text (%d chars)", title, len(text))
            continue

        filename = f"canva_{sanitize_filename(title)}.txt"
        out_path = OUTPUT_DIR / filename
        out_path.write_text(text, encoding="utf-8")

        manifest.append(
            {
                "source_file": filename,
                "source_url": url,
                "source_portal": "claytonschools",
                "document_type": "curriculum",
            }
        )
        total_extracted += 1
        logger.info("  Saved: %s (%d chars)", filename, len(text))

    # 3. Write manifest
    MANIFEST_PATH.write_text(json.dumps(manifest, indent=2))
    logger.info(
        "Extraction complete: %d/%d designs extracted, manifest at %s",
        total_extracted,
        len(canva_designs),
        MANIFEST_PATH,
    )


if __name__ == "__main__":
    main()
