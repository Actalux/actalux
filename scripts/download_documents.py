#!/usr/bin/env python3
"""Download Clayton school district documents from the Diligent Community portal.

Usage:
    python scripts/download_documents.py

Recursively crawls all public folders in the Diligent Community portal,
downloads documents, and writes a manifest for ingestion.
"""

from __future__ import annotations

import base64
import json
import logging
import re
from pathlib import Path

import httpx

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

BASE_URL = "https://claytonschools.community.diligentoneplatform.com"

# Root folder — contains all public content
ROOT_FOLDER = "16823c15-705e-49b3-b2b5-ee1fe9d381fb"

OUTPUT_DIR = Path("data/documents")
MANIFEST_PATH = Path("data/documents/diligent_manifest.json")


def list_folder(client: httpx.Client, folder_id: str) -> list[dict]:
    """List documents in a Diligent Community folder."""
    resp = client.get(
        f"{BASE_URL}/api/documents",
        params={"id": folder_id, "type": "1"},
    )
    resp.raise_for_status()
    return resp.json()


def walk_folders(
    client: httpx.Client,
    folder_id: str,
    path: str = "",
) -> list[dict]:
    """Recursively enumerate all documents under a folder.

    Returns list of items with an added 'folder_path' field.
    """
    items = list_folder(client, folder_id)
    docs: list[dict] = []

    for item in items:
        item_path = f"{path}/{item.get('title', '')}" if path else item.get("title", "")

        if item.get("folder"):
            logger.info("  Folder: %s", item_path)
            sub_docs = walk_folders(client, item["guid"], item_path)
            docs.extend(sub_docs)
        else:
            item["folder_path"] = item_path
            docs.append(item)

    return docs


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


def infer_doc_type(title: str, folder_path: str) -> str:
    """Infer document type from title and folder path."""
    combined = f"{folder_path} {title}".lower()
    if "minutes" in combined:
        return "minutes"
    if "agenda" in combined:
        return "agenda"
    if "calendar" in combined:
        return "calendar"
    if "budget" in combined or "finance" in combined:
        return "budget"
    if "resolution" in combined:
        return "resolution"
    if "strategic" in combined or "plan" in combined:
        return "plan"
    return "other"


def main() -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    manifest: list[dict[str, str]] = []

    with httpx.Client(timeout=60.0) as client:
        logger.info("Walking Diligent portal from root folder...")
        all_docs = walk_folders(client, ROOT_FOLDER)
        logger.info("Found %d documents total", len(all_docs))

        downloaded = 0
        skipped = 0

        for doc in all_docs:
            if not doc.get("canView"):
                logger.warning("  No view access: %s", doc.get("title", "unknown"))
                skipped += 1
                continue

            title = doc.get("title", "unknown")
            ext = doc.get("extension", "")
            # Avoid double extensions (e.g., "file.pdf" + ".pdf")
            if ext and not title.lower().endswith(ext.lower()):
                title = title + ext
            safe_name = sanitize_filename(title)
            out_path = OUTPUT_DIR / safe_name
            folder_path = doc.get("folder_path", "")
            source_url = f"{BASE_URL}/document/{doc['guid']}"

            if out_path.exists():
                logger.info("  Already on disk: %s", safe_name)
                # Still add to manifest so it gets ingested with provenance
                manifest.append(
                    {
                        "source_file": safe_name,
                        "source_url": source_url,
                        "source_portal": "diligent",
                        "document_type": infer_doc_type(title, folder_path),
                    }
                )
                continue

            logger.info("  Downloading: %s", title)
            try:
                _, file_bytes = download_document(client, doc["guid"])
                out_path.write_bytes(file_bytes)
                downloaded += 1
                logger.info("  Saved: %s (%d bytes)", safe_name, len(file_bytes))
                manifest.append(
                    {
                        "source_file": safe_name,
                        "source_url": source_url,
                        "source_portal": "diligent",
                        "document_type": infer_doc_type(title, folder_path),
                    }
                )
            except Exception as exc:
                logger.error("  Failed: %s — %s", title, exc)

    MANIFEST_PATH.write_text(json.dumps(manifest, indent=2))
    logger.info(
        "Download complete: %d new, %d skipped, %d in manifest, saved to %s",
        downloaded,
        skipped,
        len(manifest),
        MANIFEST_PATH,
    )


if __name__ == "__main__":
    main()
