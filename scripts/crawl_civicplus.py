#!/usr/bin/env python3
"""Crawl Clayton CivicPlus (MeetingsManager) agendas + minutes for a city body.

claytonmo.gov sits behind Akamai Bot Manager: only REAL Google Chrome (Playwright
``channel="chrome"``) plus automation-masking clears it — bundled Chromium is hard
-blocked ("Access Denied"). This works from both residential and GitHub-Actions
datacenter IPs (verified June 2026).

Enumeration (reverse-engineered from the meeting-archive page):
  * the archive page's ``MeetingTypes`` <select> filters by body category id
    (City Council=93, Plan Commission/ARB=121); ``maYears`` filters by year.
  * each listed meeting links to ``/Home/Components/MeetingsManager/MeetingAgenda/
    {agendaID}/1595``; that meeting page exposes the meeting date plus the
    "Final Agenda" and "Final [Draft] Minutes" documents as
    ``…/MeetingAgenda|MeetingMinutes/ShowPrimaryDocument/?agendaID=|minutesID=N``.

The script downloads those PDFs to ``data/documents/`` and writes a manifest that
``scripts/ingest.py`` consumes (``--manifest … --body <body>``). PDFs download
through the cleared browser session (a raw ``ctx.request`` fetch 403s).

Usage:
  doppler run --project mac --config dev -- \
    uv run --with playwright python scripts/crawl_civicplus.py --body council --limit 3
"""

from __future__ import annotations

import argparse
import base64
import json
import logging
import re
import time
from dataclasses import dataclass
from datetime import date
from pathlib import Path

from playwright.sync_api import Error as PWError
from playwright.sync_api import sync_playwright

from actalux.ingest.docket import extract_docket

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

ARCHIVE_URL = "https://www.claytonmo.gov/government/boards-and-commissions/meeting-archive"
ORIGIN = "https://www.claytonmo.gov"
OUTPUT_DIR = Path("data/documents")
MANIFEST_PATH = Path("data/documents/civicplus_manifest.json")
QUARANTINE_PATH = Path("data/documents/civicplus_quarantine.json")

_UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36"
)
_STEALTH = (
    "Object.defineProperty(navigator,'webdriver',{get:()=>undefined});window.chrome={runtime:{}};"
)

# Body short-key -> CivicPlus MeetingTypes category id (from the archive <select>).
CIVICPLUS_CATEGORY = {"council": "93", "plan-commission": "121"}

# A meeting-page document link.
_DOC_RE = re.compile(
    r"(MeetingAgenda|MeetingMinutes)/ShowPrimaryDocument/?\?(agendaID|minutesID)=(\d+)", re.I
)
_AGENDA_LINK_RE = re.compile(r"/MeetingAgenda/(\d+)/\d+")
_DATE_RE = re.compile(r"\b(\d{1,2})/(\d{1,2})/(\d{4})\b")


@dataclass(frozen=True)
class MeetingDoc:
    """One agenda or minutes document discovered for a meeting."""

    meeting_date: str  # ISO YYYY-MM-DD
    document_type: str  # "agenda" | "minutes"
    doc_url: str  # canonical ShowPrimaryDocument URL (carries the stable id)
    doc_id: str  # agendaID / minutesID


def safe_stem(body: str, iso: str, doc_type: str, doc_id: str) -> str:
    """Filesystem-safe, unique-per-document filename stem."""
    return f"{body}_{iso}_{doc_type}_{doc_id}"


def parse_iso(text: str) -> str | None:
    """First MM/DD/YYYY in ``text`` as ISO, or None."""
    m = _DATE_RE.search(text)
    if not m:
        return None
    mo, day, yr = (int(g) for g in m.groups())
    if 2000 <= yr <= 2100 and 1 <= mo <= 12 and 1 <= day <= 31:
        return f"{yr}-{mo:02d}-{day:02d}"
    return None


def _years_to_crawl(pg, since: date | None) -> list[str]:
    """Numeric years offered by the archive's ``maYears`` select, newest first."""
    opts = pg.eval_on_selector_all("#maYears option", "els=>els.map(e=>e.value)")
    years = sorted((o for o in opts if re.fullmatch(r"\d{4}", o or "")), reverse=True)
    if since:
        years = [y for y in years if int(y) >= since.year]
    return years


def enumerate_agenda_ids(pg, category: str, since: date | None, limit: int | None) -> list[str]:
    """Distinct agendaIDs for a body, newest-first, by driving the archive filters.

    Iterates the year dropdown so the full back-catalogue is reachable (the default
    view shows only the current year). Stops early once ``limit`` ids are collected.
    """
    pg.goto(ARCHIVE_URL, wait_until="domcontentloaded", timeout=60000)
    pg.wait_for_timeout(2500)
    years = _years_to_crawl(pg, since)
    seen: set[str] = set()
    ordered: list[str] = []
    for yr in years:
        pg.goto(ARCHIVE_URL, wait_until="domcontentloaded", timeout=60000)
        pg.wait_for_timeout(1500)
        pg.select_option("#MeetingTypes", category)
        pg.wait_for_timeout(2500)
        try:
            pg.select_option("#maYears", yr)
            pg.wait_for_timeout(2500)
        except PWError:
            logger.warning("year %s not selectable; using default view", yr)
        hrefs = pg.eval_on_selector_all("a", "els=>els.map(e=>e.getAttribute('href')||'')")
        new_this_year = 0
        for href in hrefs:
            m = _AGENDA_LINK_RE.search(href or "")
            if m and m.group(1) not in seen:
                seen.add(m.group(1))
                ordered.append(m.group(1))
                new_this_year += 1
        logger.info("year %s: +%d meetings (%d total)", yr, new_this_year, len(ordered))
        if limit and len(ordered) >= limit:
            break
    return ordered


def meeting_documents(pg, agenda_id: str) -> list[MeetingDoc]:
    """Visit one meeting page; return its agenda + minutes docs with the meeting date."""
    pg.goto(
        f"{ORIGIN}/Home/Components/MeetingsManager/MeetingAgenda/{agenda_id}/1595",
        wait_until="domcontentloaded",
        timeout=60000,
    )
    pg.wait_for_timeout(1500)
    iso = parse_iso(pg.inner_text("body"))
    if not iso:
        logger.warning("meeting %s: no parseable date; skipping", agenda_id)
        return []
    hrefs = pg.eval_on_selector_all("a", "els=>els.map(e=>e.getAttribute('href')||'')")
    docs: dict[str, MeetingDoc] = {}
    for href in hrefs:
        m = _DOC_RE.search(href or "")
        if not m:
            continue
        kind, param, doc_id = m.group(1), m.group(2).lower(), m.group(3)
        doc_type = "minutes" if param == "minutesid" else "agenda"
        q = f"{param.replace('id', 'ID')}={doc_id}&isPub=True"
        url = f"{ORIGIN}/Home/Components/MeetingsManager/{kind}/ShowPrimaryDocument/?{q}"
        docs.setdefault(doc_type, MeetingDoc(iso, doc_type, url, doc_id))
    return list(docs.values())


def fetch_pdf(pg, url: str) -> bytes | None:
    """Download a PDF via an in-page fetch; None if not a real PDF.

    A plain navigation to ShowPrimaryDocument returns Chrome's PDF-viewer stub
    (range-requested), and ``ctx.request`` is 403'd by Akamai. Running ``fetch()``
    inside the page — which the crawler keeps parked on a claytonmo.gov page —
    reuses the cleared cookies + the real browser fingerprint, so Akamai serves
    the bytes; we ferry them out as base64.
    """
    try:
        b64 = pg.evaluate(
            """async (u) => {
                const r = await fetch(u, {credentials: 'include'});
                if (!r.ok) return null;
                const bytes = new Uint8Array(await r.arrayBuffer());
                let s = '';
                for (let i = 0; i < bytes.length; i++) s += String.fromCharCode(bytes[i]);
                return btoa(s);
            }""",
            url,
        )
    except PWError as exc:
        logger.warning("download error %s: %s", url[-40:], exc)
        return None
    if not b64:
        return None
    data = base64.b64decode(b64)
    return data if data[:5] == b"%PDF-" else None


def crawl(
    body: str, *, limit: int | None, since: date | None, minutes_only: bool = False
) -> tuple[list[dict[str, str]], list[dict[str, object]]]:
    """Crawl a body's agendas + minutes; write files + return (manifest, quarantine).

    Minutes are ingested as their full (clean, small) PDF. Agendas are packets, so
    only the verbatim docket text is ingested (the full packet is linked via
    ``source_url``); a low-confidence docket extraction is quarantined — linked but
    not ingested — rather than silently clipped. ``minutes_only`` skips agendas.
    """
    category = CIVICPLUS_CATEGORY[body]
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    entries: list[dict[str, str]] = []
    quarantine: list[dict[str, object]] = []
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
        agenda_ids = enumerate_agenda_ids(pg, category, since, limit)
        logger.info("%s: %d meetings to inspect", body, len(agenda_ids))
        meetings_done = 0
        for agenda_id in agenda_ids:
            if limit and meetings_done >= limit:
                break
            docs = meeting_documents(pg, agenda_id)
            if minutes_only:
                docs = [d for d in docs if d.document_type == "minutes"]
            if since and docs and date.fromisoformat(docs[0].meeting_date) < since:
                continue
            wrote_any = False
            for doc in docs:
                pdf = fetch_pdf(pg, doc.doc_url)
                if pdf is None:
                    logger.warning("  %s %s: no PDF (skipped)", doc.meeting_date, doc.document_type)
                    continue
                pretty = f"{date.fromisoformat(doc.meeting_date):%B %-d, %Y}"
                if doc.document_type == "agenda":
                    result = extract_docket(pdf)
                    if result.confidence not in ("high", "medium"):
                        quarantine.append(
                            {
                                "meeting_date": doc.meeting_date,
                                "doc_url": doc.doc_url,
                                **result.metadata,
                            }
                        )
                        logger.warning(
                            "  %s agenda: docket %s — quarantined (link only): %s",
                            doc.meeting_date,
                            result.confidence,
                            "; ".join(result.metadata.get("warnings", [])),
                        )
                        continue
                    fname = f"{safe_stem(body, doc.meeting_date, 'agenda', doc.doc_id)}.txt"
                    (OUTPUT_DIR / fname).write_text(result.text)
                    entries.append(
                        {
                            "source_file": fname,
                            "source_url": doc.doc_url,
                            "source_portal": "civicplus",
                            "document_type": "agenda",
                            "meeting_date": doc.meeting_date,
                            "meeting_title": f"{pretty} — Agenda",
                            "date_source": "civicplus",
                        }
                    )
                    logger.info(
                        "  wrote %s (docket %d/%d pp, %s)",
                        fname,
                        result.metadata["docket_page_count"],
                        result.metadata["pdf_page_count"],
                        result.confidence,
                    )
                else:
                    stem = safe_stem(body, doc.meeting_date, doc.document_type, doc.doc_id)
                    fname = f"{stem}.pdf"
                    (OUTPUT_DIR / fname).write_bytes(pdf)
                    entries.append(
                        {
                            "source_file": fname,
                            "source_url": doc.doc_url,
                            "source_portal": "civicplus",
                            "document_type": doc.document_type,
                            "meeting_date": doc.meeting_date,
                            "meeting_title": f"{pretty} — {doc.document_type.title()}",
                            "date_source": "civicplus",
                        }
                    )
                    logger.info("  wrote %s (%d bytes)", fname, len(pdf))
                wrote_any = True
                time.sleep(1.5)  # be polite
            if wrote_any:
                meetings_done += 1
        browser.close()
    return entries, quarantine


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Crawl CivicPlus agendas + minutes for a city body."
    )
    parser.add_argument(
        "--body", required=True, choices=sorted(CIVICPLUS_CATEGORY), help="city body"
    )
    parser.add_argument("--limit", type=int, help="cap the number of meetings processed")
    parser.add_argument("--since", help="only meetings on/after this date (YYYY-MM-DD)")
    parser.add_argument(
        "--minutes-only", action="store_true", help="skip agenda packets; minutes only"
    )
    args = parser.parse_args()

    since = date.fromisoformat(args.since) if args.since else None
    entries, quarantine = crawl(
        args.body, limit=args.limit, since=since, minutes_only=args.minutes_only
    )
    MANIFEST_PATH.write_text(json.dumps(entries, indent=2))
    QUARANTINE_PATH.write_text(json.dumps(quarantine, indent=2))
    logger.info("staged %d document(s); manifest: %s", len(entries), MANIFEST_PATH)
    if quarantine:
        logger.warning(
            "quarantined %d agenda(s) (low-confidence docket; linked, not ingested): %s",
            len(quarantine),
            QUARANTINE_PATH,
        )


if __name__ == "__main__":
    main()
