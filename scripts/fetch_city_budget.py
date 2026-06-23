"""Fetch the City of Clayton adopted budget book(s) from claytonmo.gov.

The adopted "Operating Budget and Capital Improvements Plan" PDFs live only on
claytonmo.gov, which is behind an Akamai bot-block (curl/headless all 403). The
working path is real Google Chrome (Playwright ``channel="chrome"`` + automation
masking) parked on a claytonmo.gov page to clear Akamai, then an in-page
``fetch()`` ferried out as base64 — the same mechanism the CivicPlus minutes
crawler uses (reused here via ``crawl_civicplus.fetch_pdf``). ``load_city_adopted_budget.py``
reads the FY2026 figures from the fetched PDF's verified extraction.

Requires real Chrome installed; run with playwright available, e.g.
  uv run --with playwright python scripts/fetch_city_budget.py
(CI installs it via ``playwright install --with-deps chrome``.)
"""

from __future__ import annotations

import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from crawl_civicplus import _STEALTH, _UA, fetch_pdf  # noqa: E402  (sibling crawler)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

OUT_DIR = Path(__file__).resolve().parent.parent / "data" / "city_budget"
# A claytonmo.gov page to park on so the in-page fetch reuses cleared Akamai cookies.
WARMUP_URL = "https://www.claytonmo.gov/government/finance/budgets"
# fiscal year -> the adopted "Operating Budget and Capital Improvements Plan" PDF.
BUDGET_BOOKS = {
    "2026": "https://www.claytonmo.gov/home/showpublisheddocument/8054/639089060510200000",
}


def fetch() -> None:
    from playwright.sync_api import sync_playwright

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=True,
            channel="chrome",
            args=["--disable-blink-features=AutomationControlled", "--no-sandbox"],
        )
        ctx = browser.new_context(
            user_agent=_UA, locale="en-US", viewport={"width": 1366, "height": 900}
        )
        ctx.add_init_script(_STEALTH)
        pg = ctx.new_page()
        resp = pg.goto(WARMUP_URL, wait_until="domcontentloaded", timeout=45000)
        logger.info("warmup %s -> %s", WARMUP_URL, resp.status if resp else "?")
        for fy, url in BUDGET_BOOKS.items():
            pdf = fetch_pdf(pg, url)
            if not pdf:
                logger.error("FY%s: no PDF from %s (Akamai block or wrong URL?)", fy, url)
                continue
            out = OUT_DIR / f"clayton_budget_FY{fy}.pdf"
            out.write_bytes(pdf)
            logger.info("FY%s -> %s (%d KB)", fy, out.name, len(pdf) // 1024)
        browser.close()


if __name__ == "__main__":
    fetch()
