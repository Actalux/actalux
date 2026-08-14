"""Crawl BoE meeting agendas from the Diligent portal's meetings service.

The document-library crawl (download_documents.py) walks the portal's folder
tree — but agendas never appear there. Diligent renders them from a separate
meetings surface: ``/Services/MeetingsService.svc/meetings?from=&to=`` lists
meetings, and ``/meetings/{id}/meetingDocuments`` returns each meeting's agenda
as HTML (DocumentType 1). Until this crawler existed the archive held zero true
BoE meeting agendas — every document classified "agenda" was a packet
attachment — which left each meeting's record transcript-only until the weekly
library crawl caught up, and agenda-less forever.

Writes one HTML file per agenda plus a manifest for scripts/ingest.py. The
meeting date comes from the service's own ``MeetingDate`` field
(``date_source="diligent"`` — portal metadata, same trust class as CivicPlus
dates), and the source URL is the human-facing MeetingInformation page so a
citation's "open original" lands somewhere a reader can use.

Run (no secrets needed to crawl; ingest needs the usual doppler prefix):
  uv run python scripts/crawl_diligent_meetings.py --days-back 90 --days-ahead 30
  doppler run --project mac --config dev -- uv run python scripts/ingest.py \\
      --manifest data/documents/diligent_meetings_manifest.json
"""

from __future__ import annotations

import argparse
import json
import logging
import re
from datetime import date, timedelta
from pathlib import Path

import httpx

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

BASE_URL = "https://claytonschools.community.diligentoneplatform.com"
OUTPUT_DIR = Path("data/documents/meetings")
MANIFEST_PATH = Path("data/documents/diligent_meetings_manifest.json")

# meetingDocuments DocumentType values: 1 = agenda, 2 = minutes. Minutes arrive
# through the document library as before; this crawl is the agenda gap only.
AGENDA_DOC_TYPE = 1

# The service also returns district-calendar noise (breaks, holidays) as
# pseudo-meetings with TypeId -100 and Published false; only real, published
# board meetings carry a positive MeetingTypeId.
_SLUG_RE = re.compile(r"[^a-z0-9]+")


def _slug(name: str) -> str:
    return _SLUG_RE.sub("-", name.lower()).strip("-")[:80]


def fetch_meetings(client: httpx.Client, start: date, end: date) -> list[dict]:
    """Real, published meetings in the window (calendar noise filtered out)."""
    resp = client.get(
        f"{BASE_URL}/Services/MeetingsService.svc/meetings",
        params={"from": start.isoformat(), "to": end.isoformat()},
    )
    resp.raise_for_status()
    return [
        m
        for m in resp.json()
        if m.get("Published") and (m.get("MeetingTypeId") or 0) > 0 and m.get("MeetingDate")
    ]


def fetch_agenda_html(client: httpx.Client, meeting_id: int) -> str | None:
    """The meeting's agenda as self-contained HTML, or None when none is posted."""
    resp = client.get(
        f"{BASE_URL}/Services/MeetingsService.svc/meetings/{meeting_id}/meetingDocuments"
    )
    resp.raise_for_status()
    for doc in resp.json().get("Documents") or []:
        if doc.get("DocumentType") == AGENDA_DOC_TYPE and doc.get("Html"):
            return doc["Html"]
    return None


def crawl(days_back: int, days_ahead: int) -> list[dict]:
    """Fetch agendas into OUTPUT_DIR; return manifest entries for ingest."""
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    today = date.today()
    entries: list[dict] = []
    with httpx.Client(
        timeout=30, follow_redirects=True, headers={"User-Agent": "Mozilla/5.0"}
    ) as client:
        meetings = fetch_meetings(
            client, today - timedelta(days=days_back), today + timedelta(days=days_ahead)
        )
        logger.info("%d published meetings in window", len(meetings))
        for m in meetings:
            html = fetch_agenda_html(client, m["Id"])
            if not html:
                logger.info("  no agenda posted yet: %s", m["CleanName"])
                continue
            fname = f"{m['MeetingDate']} {_slug(m['CleanName'])} agenda.html"
            (OUTPUT_DIR / fname).write_text(html)
            entries.append(
                {
                    # Relative to the manifest's directory: ingest resolves the
                    # file as data_dir / source_file, and the subfolder rides in
                    # the name so meeting agendas keep their own shelf on disk.
                    "source_file": f"meetings/{fname}",
                    # The human-facing meeting page, so "open original" works.
                    "source_url": f"{BASE_URL}/Portal/MeetingInformation.aspx?Id={m['Id']}",
                    "source_portal": "diligent",
                    "document_type": "agenda",
                    "meeting_date": m["MeetingDate"],
                    "date_source": "diligent",
                    "meeting_title": m["CleanName"],
                }
            )
            logger.info("  agenda: %s (%s)", m["CleanName"], m["MeetingDate"])
    return entries


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--days-back", type=int, default=90)
    parser.add_argument("--days-ahead", type=int, default=30)
    args = parser.parse_args()

    entries = crawl(args.days_back, args.days_ahead)
    MANIFEST_PATH.write_text(json.dumps(entries, indent=2))
    logger.info("Wrote %d agenda entries to %s", len(entries), MANIFEST_PATH)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
