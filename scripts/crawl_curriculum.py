#!/usr/bin/env python3
"""Crawl Clayton School District curriculum resources.

Downloads documents from three source types:
  - Finalsite resource-manager (UUID-based document hosting)
  - Google Docs/Slides (exported as PDF)
  - Direct PDF links on claytonschools.net

Writes a manifest JSON file for use with:
    python scripts/ingest.py --manifest data/curriculum_manifest.json

Usage:
    python scripts/crawl_curriculum.py
"""

from __future__ import annotations

import json
import logging
import re
from pathlib import Path

import httpx

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

OUTPUT_DIR = Path("data/documents")
MANIFEST_PATH = Path("data/documents/curriculum_manifest.json")

# --- Document sources ---

# Finalsite resource-manager documents (NWEA MAP + Literacy sub-pages)
RESOURCE_MANAGER_DOCS = [
    # NWEA MAP Assessment (/fs/pages/6894)
    ("RIT Reference Chart K-1", "1df12c8b-122c-4894-b47d-f2210703e4ad"),
    ("RIT Reference Chart 2-5", "22b347cd-1422-415d-bff6-d2d6607dfe93"),
    ("RIT Reference Chart 6-8", "6f0cc6da-88af-4eec-b179-b1d8770301dd"),
    ("Goal Area Descriptions K-1", "f3edf12c-4cf9-47c5-ba99-2a467ed139bd"),
    ("Goal Area Descriptions 2-5", "a289a8c1-b2ee-466c-a647-a02d7ed8fd86"),
    ("Goal Area Descriptions 6-12", "df44adff-bbdc-4ce5-ba73-88cdadd1a0b1"),
    ("Geometry K-1 and 2-5", "eb5e53f5-3df1-4e8f-afa4-eb3ffff838e8"),
    ("Measurement and Data K-1 and 2-5", "fb7a5f4e-aff7-4874-99d3-7a06bae8a178"),
    ("Numbers and Operations K-1 and 2-5", "9046203d-c8ef-4a35-ac8f-43310f72b1c5"),
    ("Operations and Algebraic Thinking K-1 and 2-5", "70f87387-096d-4a48-a320-59a0ed4b07f9"),
    ("Vocabulary K-1 and 2-5", "875a02d5-d120-48f6-a267-7ec00a47bcbe"),
    ("Geometry 6-12", "49d05107-eade-4c70-8830-a5c3bec8c21a"),
    ("Statistics and Probability 6-12", "40f42de2-2371-40c6-8cfd-e2e96b1d4d36"),
    ("Real and Complex Number System 6-12", "e79298c8-c22e-4158-a21f-08718743e641"),
    ("Operations and Algebraic Thinking 6-12", "1f820458-8a46-4963-8871-df4cc0d0b7fe"),
    ("RIT to Khan Academy 2-5", "2cfdbf17-6366-49d1-8c17-a23b4efa8803"),
    ("RIT to Khan Academy 6+", "0b5b7e4e-3ff1-4970-85d5-84742ea168ed"),
    # Literacy Assessments (/fs/pages/6893)
    ("Dyslexia Screening Tools", "a8cc0ac2-f908-4db7-8e45-3a0a25cbff38"),
]

# Google Docs/Slides (exported as PDF)
GOOGLE_DOCS = [
    # Programs - Counseling
    ("6-8 Counseling Curriculum Map", "14QV_N2fVl0DRp5lZEIIvJn8O6VDVaUCDsrNGhfNqi0U"),
    ("SEL Skills Presentation", "1Ub0CSqhsqIZY_R0DLARr-zc0b3niprsWLU-MS1ympiw"),
    # Programs - ELD Learning Targets
    ("Kindergarten ELD Learning Targets", "1Z8NIG6CGB0sz_Xg29x3P5FUBKXWUEg-b4gq7O6CpPl4"),
    ("Grades 1-2 ELD Learning Targets", "1aTTRc_y5VEgNEKeK8Wn0bukpDdPBXG0MleYolIru2XQ"),
    ("Grades 3-5 ELD Learning Targets", "1D7o03_oXsy2F-TjxighytZGjWdaDqNG2KYNujpnP9n4"),
    ("Grades 6-8 ELD Learning Targets", "1tTmfvDlSIZqc436CmK2mrqi7IgRqjgVjjEtdleAsmOQ"),
    ("Grades 9-12 ELD Learning Targets", "14PnY1jLJA1vPtgd6TJ8FWh8iQoFhL6fSJEFP36wYUs0"),
    ("Student Friendly Can-Dos K-12", "1krRTE0aLtm7LG9KbJMLZl8BJ0QvPCxNPn29VLmKdRAE"),
    # STEM - Math Placement
    ("CHS Math Placement Overview", "1yYRiqn6HWHUvxOySDEr5gbVE6MKfJszSCGFH5rekdqI"),
    ("Wydown Math Placement Overview", "1L0jrV74zCdgBs6EQkoAZtBwSGMp_YJA3cp5_iK9O5RA"),
]

# Google Slides (need /export?format=pdf with slides path)
GOOGLE_SLIDES = [
    ("SEL Skills Presentation", "1Ub0CSqhsqIZY_R0DLARr-zc0b3niprsWLU-MS1ympiw"),
]

# Direct PDFs on Clayton's servers
DIRECT_PDFS = [
    (
        "NWEA Parent Guide 2016",
        "https://www.claytonschools.net/cms/lib/MO01000419/Centricity/Domain/1106/NWEA_Brochure-2016.pdf",
    ),
]

# Home-School Reading Connections (/fs/pages/6826) — Google Docs
READING_CONNECTIONS = [
    # These are published Google Docs from the Humanities sub-page.
    # The recon agent found 20 of them but didn't list all URLs.
    # We'll crawl the sub-page directly to find them.
]


def sanitize_filename(title: str) -> str:
    """Convert a document title to a safe filename."""
    clean = re.sub(r'[<>:"/\\|?*]', "", title)
    clean = re.sub(r"\s+", "_", clean.strip())
    return clean


def download_resource_manager(
    client: httpx.Client,
    title: str,
    uuid: str,
) -> tuple[str, str] | None:
    """Download a Finalsite resource-manager document."""
    url = f"https://www.claytonschools.net/fs/resource-manager/view/{uuid}"
    try:
        resp = client.get(url, follow_redirects=True)
        resp.raise_for_status()
    except httpx.HTTPError as exc:
        logger.error("Failed to download %s: %s", title, exc)
        return None

    # Determine file extension from content-type
    content_type = resp.headers.get("content-type", "")
    ext = ".pdf" if "pdf" in content_type else ".html"
    if "image" in content_type:
        ext = ".png"

    filename = f"curriculum_{sanitize_filename(title)}{ext}"
    out_path = OUTPUT_DIR / filename
    out_path.write_bytes(resp.content)
    logger.info("Downloaded: %s (%d bytes)", filename, len(resp.content))
    return filename, url


def download_google_doc(
    client: httpx.Client,
    title: str,
    doc_id: str,
    is_slides: bool = False,
) -> tuple[str, str] | None:
    """Export a Google Doc or Slides as PDF."""
    if is_slides:
        url = f"https://docs.google.com/presentation/d/{doc_id}/export?format=pdf"
        source_url = f"https://docs.google.com/presentation/d/{doc_id}"
    else:
        url = f"https://docs.google.com/document/d/{doc_id}/export?format=pdf"
        source_url = f"https://docs.google.com/document/d/{doc_id}"

    try:
        resp = client.get(url, follow_redirects=True)
        resp.raise_for_status()
    except httpx.HTTPError as exc:
        logger.error("Failed to export %s: %s", title, exc)
        return None

    if "pdf" not in resp.headers.get("content-type", ""):
        logger.warning(
            "Export of %s did not return PDF, got %s", title, resp.headers.get("content-type")
        )
        return None

    filename = f"curriculum_{sanitize_filename(title)}.pdf"
    out_path = OUTPUT_DIR / filename
    out_path.write_bytes(resp.content)
    logger.info("Exported: %s (%d bytes)", filename, len(resp.content))
    return filename, source_url


def download_direct_pdf(
    client: httpx.Client,
    title: str,
    url: str,
) -> tuple[str, str] | None:
    """Download a direct PDF link."""
    try:
        resp = client.get(url, follow_redirects=True)
        resp.raise_for_status()
    except httpx.HTTPError as exc:
        logger.error("Failed to download %s: %s", title, exc)
        return None

    filename = f"curriculum_{sanitize_filename(title)}.pdf"
    out_path = OUTPUT_DIR / filename
    out_path.write_bytes(resp.content)
    logger.info("Downloaded: %s (%d bytes)", filename, len(resp.content))
    return filename, url


def crawl_reading_connections_page(
    client: httpx.Client,
) -> list[tuple[str, str, str]]:
    """Crawl the Home-School Reading Connections sub-page for Google Doc links."""
    from bs4 import BeautifulSoup

    url = "https://www.claytonschools.net/fs/pages/6826"
    try:
        resp = client.get(url, follow_redirects=True)
        resp.raise_for_status()
    except httpx.HTTPError as exc:
        logger.error("Failed to fetch reading connections page: %s", exc)
        return []

    soup = BeautifulSoup(resp.text, "lxml")
    results: list[tuple[str, str, str]] = []

    for link in soup.find_all("a", href=True):
        href = link["href"]
        if "docs.google.com/document" in href:
            doc_match = re.search(r"/d/([a-zA-Z0-9_-]+)", href)
            if doc_match:
                doc_id = doc_match.group(1)
                title = link.get_text(strip=True) or f"Reading Connection {doc_id[:8]}"
                results.append((title, doc_id, "document"))
        elif "docs.google.com/presentation" in href:
            doc_match = re.search(r"/d/([a-zA-Z0-9_-]+)", href)
            if doc_match:
                doc_id = doc_match.group(1)
                title = link.get_text(strip=True) or f"Presentation {doc_id[:8]}"
                results.append((title, doc_id, "presentation"))

    logger.info("Found %d Google Docs on reading connections page", len(results))
    return results


def main() -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    manifest: list[dict[str, str]] = []
    slides_ids = {doc_id for _, doc_id in GOOGLE_SLIDES}

    with httpx.Client(timeout=60.0) as client:
        # 1. Resource-manager documents
        logger.info("Downloading %d resource-manager documents...", len(RESOURCE_MANAGER_DOCS))
        for title, uuid in RESOURCE_MANAGER_DOCS:
            result = download_resource_manager(client, title, uuid)
            if result:
                filename, source_url = result
                manifest.append(
                    {
                        "source_file": filename,
                        "source_url": source_url,
                        "source_portal": "claytonschools",
                        "document_type": "curriculum",
                    }
                )

        # 2. Google Docs
        logger.info("Exporting %d Google Docs as PDF...", len(GOOGLE_DOCS))
        for title, doc_id in GOOGLE_DOCS:
            is_slides = doc_id in slides_ids
            result = download_google_doc(client, title, doc_id, is_slides)
            if result:
                filename, source_url = result
                manifest.append(
                    {
                        "source_file": filename,
                        "source_url": source_url,
                        "source_portal": "claytonschools",
                        "document_type": "curriculum",
                    }
                )

        # 3. Direct PDFs
        logger.info("Downloading %d direct PDFs...", len(DIRECT_PDFS))
        for title, url in DIRECT_PDFS:
            result = download_direct_pdf(client, title, url)
            if result:
                filename, source_url = result
                manifest.append(
                    {
                        "source_file": filename,
                        "source_url": source_url,
                        "source_portal": "claytonschools",
                        "document_type": "curriculum",
                    }
                )

        # 4. Reading Connections sub-page (discover + download)
        logger.info("Crawling reading connections sub-page...")
        reading_docs = crawl_reading_connections_page(client)
        for title, doc_id, doc_type in reading_docs:
            is_slides = doc_type == "presentation"
            result = download_google_doc(client, f"Reading_{title}", doc_id, is_slides)
            if result:
                filename, source_url = result
                manifest.append(
                    {
                        "source_file": filename,
                        "source_url": source_url,
                        "source_portal": "claytonschools",
                        "document_type": "curriculum",
                    }
                )

    # Write manifest
    MANIFEST_PATH.write_text(json.dumps(manifest, indent=2))
    logger.info(
        "Crawl complete: %d documents downloaded, manifest at %s",
        len(manifest),
        MANIFEST_PATH,
    )


if __name__ == "__main__":
    main()
