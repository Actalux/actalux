#!/usr/bin/env python3
"""Download Clayton school district documents from the Diligent Community portal.

Usage:
    python scripts/download_documents.py

Downloads all meeting minutes PDFs from the 2024-2025 and 2023-2024 school years
into data/documents/ organized by meeting date.
"""

from __future__ import annotations

import base64
import logging
import re
from pathlib import Path

import httpx

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

BASE_URL = "https://claytonschools.community.diligentoneplatform.com"

# Folder IDs from the Diligent portal
FOLDERS = {
    "2024-2025 Minutes": "8826a219-6b40-47bb-8b39-d0006eb6bf46",
    "2023-2024 Minutes": "5091d99e-9702-4d55-b2a5-9d8e809fa2f5",
    "District Finance": "47cca4ec-dce9-46f1-8956-26e586b09283",
}

DATE_RE = re.compile(r"(\w+ \d{1,2},? \d{4})")
OUTPUT_DIR = Path("data/documents")


def list_folder(client: httpx.Client, folder_id: str) -> list[dict]:
    """List documents in a Diligent Community folder."""
    resp = client.get(f"{BASE_URL}/api/documents", params={"id": folder_id, "type": "1"})
    resp.raise_for_status()
    return resp.json()


def download_document(client: httpx.Client, doc_guid: str) -> tuple[str, bytes]:
    """Download a document by GUID. Returns (filename, file_bytes)."""
    resp = client.get(f"{BASE_URL}/api/document/{doc_guid}/download")
    resp.raise_for_status()
    data = resp.json()
    filename = data["name"]
    file_bytes = base64.b64decode(data["document"])
    return filename, file_bytes


def sanitize_filename(name: str) -> str:
    """Make a filename safe for the filesystem."""
    return re.sub(r'[<>:"/\\|?*]', "_", name).strip()


def main() -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    with httpx.Client(timeout=60.0) as client:
        for folder_name, folder_id in FOLDERS.items():
            logger.info("Listing folder: %s", folder_name)
            items = list_folder(client, folder_id)

            # Handle subfolders recursively
            all_docs: list[dict] = []
            for item in items:
                if item.get("folder"):
                    logger.info("  Subfolder: %s — listing contents", item["title"])
                    sub_items = list_folder(client, item["guid"])
                    all_docs.extend(sub_items)
                else:
                    all_docs.append(item)

            logger.info("  Found %d documents in %s", len(all_docs), folder_name)

            for doc in all_docs:
                if not doc.get("canView"):
                    logger.warning("  Skipping (no view access): %s", doc["title"])
                    continue

                safe_name = sanitize_filename(doc["title"] + doc.get("extension", ""))
                out_path = OUTPUT_DIR / safe_name

                if out_path.exists():
                    logger.info("  Already downloaded: %s", safe_name)
                    continue

                logger.info("  Downloading: %s", doc["title"])
                try:
                    filename, file_bytes = download_document(client, doc["guid"])
                    out_path.write_bytes(file_bytes)
                    logger.info("  Saved: %s (%d bytes)", out_path, len(file_bytes))
                except Exception as exc:
                    logger.error("  Failed to download %s: %s", doc["title"], exc)

    # Summary
    pdfs = list(OUTPUT_DIR.glob("*.pdf"))
    logger.info("Download complete: %d PDF files in %s", len(pdfs), OUTPUT_DIR)


if __name__ == "__main__":
    main()
