#!/usr/bin/env python3
"""Download Clayton school district documents from the Diligent Community portal.

Usage:
    python scripts/download_documents.py

Recursively crawls all public folders in the Diligent Community portal, downloads
documents, and writes a manifest for ingestion.

The folder tree is not the whole public record: packets and agendas carry embedded
links to attachments/exhibits filed OUTSIDE the browsable tree (an audit found 115
distinct linked documents, none reachable by the tree walk). After the tree pass,
this crawler therefore scans every downloaded PDF's link annotations for same-portal
``/document/{guid}`` targets and follows them breadth-first (bounded depth), so
link-only documents land in the same manifest. A sidecar index maps already-fetched
linked GUIDs to filenames so re-runs stay incremental (a linked doc's filename is
only knowable after download).
"""

from __future__ import annotations

import base64
import json
import logging
import re
from pathlib import Path

import fitz
import httpx

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

BASE_URL = "https://claytonschools.community.diligentoneplatform.com"

# Root folder — contains all public content
ROOT_FOLDER = "16823c15-705e-49b3-b2b5-ee1fe9d381fb"

OUTPUT_DIR = Path("data/documents")
MANIFEST_PATH = Path("data/documents/diligent_manifest.json")
LINKED_INDEX_PATH = Path("data/documents/diligent_linked_index.json")

# Same-portal document links embedded in PDFs. The portal has answered on two hosts
# over its life (legacy diligent.community, current diligentoneplatform.com); both
# resolve to the same document GUIDs.
GUID_LINK = re.compile(
    r"https?://(?:claytonschools\.community\.diligentoneplatform\.com"
    r"|claytonschools\.diligent\.community)"
    r"/document/([0-9a-fA-F]{8}(?:-[0-9a-fA-F]{4}){3}-[0-9a-fA-F]{12})"
)

# Linked docs can themselves link onward (packet -> exhibit -> appendix). Depth 3
# covers every chain seen in the audit; the GUID dedup set is the real terminator.
MAX_LINK_DEPTH = 3

# Content policy (schools body): individual personnel, teachers, and students are never
# named. Board packets link per-employee appendix tables (hires/resignations/PTTE lists
# with names, positions, pay), which the browsable folder tree never exposed. Those stay
# out of the manifest entirely — held back pre-ingest, like the PII guard, so the weekly
# CI ingest cannot pick them up. Title-based and high-precision; a renamed personnel doc
# would slip past, so flag surprises in review rather than trusting this exhaustively.
PERSONNEL_HOLD_BACK = re.compile(
    r"personnel|employment|resignation|retirement|rehire|new hire|staffing"
    r"|pttes?\b|ptte_|job change|attendance award",
    re.IGNORECASE,
)


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


def extract_linked_guids(pdf_path: Path) -> set[str]:
    """GUIDs of same-portal documents referenced by a PDF's link annotations."""
    guids: set[str] = set()
    try:
        doc = fitz.open(pdf_path)
    except Exception:
        return guids  # not a PDF (or unreadable) — nothing to scan
    for page in doc:
        for link in page.get_links():
            m = GUID_LINK.match(link.get("uri") or "")
            if m:
                guids.add(m.group(1).lower())
    doc.close()
    return guids


def load_linked_index() -> dict[str, str]:
    """The sidecar ``{guid: filename}`` map of previously fetched link-only documents."""
    if LINKED_INDEX_PATH.exists():
        return json.loads(LINKED_INDEX_PATH.read_text())
    return {}


def follow_embedded_links(
    client: httpx.Client,
    scan_paths: list[Path],
    seen_guids: set[str],
    manifest: list[dict[str, str]],
) -> tuple[int, int]:
    """Fetch documents reachable only via embedded links; append them to the manifest.

    Breadth-first from the tree-walk downloads: scan each PDF's link annotations,
    download unseen same-portal targets, then scan those for further links, up to
    ``MAX_LINK_DEPTH``. Returns (downloaded, reused) counts.
    """
    linked_index = load_linked_index()
    downloaded = reused = 0
    frontier = scan_paths
    for depth in range(1, MAX_LINK_DEPTH + 1):
        targets: set[str] = set()
        for path in frontier:
            targets |= extract_linked_guids(path)
        targets -= seen_guids
        if not targets:
            break
        logger.info("Link depth %d: %d new linked documents", depth, len(targets))
        next_frontier: list[Path] = []
        for guid in sorted(targets):
            seen_guids.add(guid)
            cached_name = linked_index.get(guid)
            if cached_name and (OUTPUT_DIR / cached_name).exists():
                out_path = OUTPUT_DIR / cached_name
                reused += 1
            else:
                try:
                    filename, file_bytes = download_document(client, guid)
                except Exception as exc:
                    logger.warning("  Linked doc %s not fetchable — %s", guid, exc)
                    continue
                out_path = OUTPUT_DIR / sanitize_filename(filename)
                out_path.write_bytes(file_bytes)
                linked_index[guid] = out_path.name
                downloaded += 1
                logger.info("  Linked: %s (%d bytes)", out_path.name, len(file_bytes))
            if PERSONNEL_HOLD_BACK.search(out_path.name):
                logger.info("  Held back (personnel): %s", out_path.name)
                continue
            manifest.append(
                {
                    "source_file": out_path.name,
                    "source_url": f"{BASE_URL}/document/{guid}",
                    "source_portal": "diligent",
                    "document_type": infer_doc_type(out_path.name, ""),
                }
            )
            next_frontier.append(out_path)
        frontier = next_frontier
    LINKED_INDEX_PATH.write_text(json.dumps(linked_index, indent=2, sort_keys=True))
    return downloaded, reused


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
        held_back = 0
        scan_paths: list[Path] = []
        seen_guids = {doc["guid"].lower() for doc in all_docs if doc.get("guid")}

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
            if PERSONNEL_HOLD_BACK.search(title):
                logger.info("  Held back (personnel): %s", title)
                held_back += 1
                continue
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
                scan_paths.append(out_path)
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
                scan_paths.append(out_path)
            except Exception as exc:
                logger.error("  Failed: %s — %s", title, exc)

        logger.info("Following embedded document links...")
        linked_new, linked_reused = follow_embedded_links(client, scan_paths, seen_guids, manifest)

    MANIFEST_PATH.write_text(json.dumps(manifest, indent=2))
    logger.info(
        "Download complete: %d new, %d skipped, %d held back, %d linked (+%d reused), "
        "%d in manifest, saved to %s",
        downloaded,
        skipped,
        held_back,
        linked_new,
        linked_reused,
        len(manifest),
        MANIFEST_PATH,
    )


if __name__ == "__main__":
    main()
