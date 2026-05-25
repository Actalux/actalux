#!/usr/bin/env python3
"""Crawl claytonschools.net business-and-finance page for audited financial
reports and per-pupil expenditure summaries.

Skips monthly warrants / revenue / expenditure summaries by default — there
are ~400 of them and they need a separate ingest strategy. Pass --monthly
to include them.

Usage:
    uv run python scripts/crawl_clayton_finance.py
    uv run python scripts/crawl_clayton_finance.py --monthly
"""

from __future__ import annotations

import argparse
import json
import logging
import re
from pathlib import Path

import httpx

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

OUTPUT_DIR = Path("data/documents")
MANIFEST_PATH = Path("data/documents/clayton_finance_manifest.json")
RESOURCE_BASE = "https://www.claytonschools.net/fs/resource-manager/view/"

# Audited Financial Reports (7 years; 2018-19 through 2024-25)
AUDITS = [
    ("2024-2025 Audited Financial Report", "af53e9e8-2afb-4162-9273-405cee9d6aa9"),
    ("2023-2024 Audited Financial Report", "1b7ea57c-35c8-407a-96c8-8881cc2f9526"),
    ("2022-2023 Audited Financial Report", "339d70c2-9c52-4d8a-bfef-4b2b4e924820"),
    ("2021-2022 Audited Financial Report", "9ecf23c1-cd72-4c3a-899d-dd4000319e23"),
    ("2020-2021 Audited Financial Report", "deb0cd13-29e3-4a43-963a-721342c6230a"),
    ("2019-2020 Audited Financial Report", "678cfe73-d55f-4fd3-b4bf-aebba56b29c7"),
    ("2018-2019 Audited Financial Report", "f410ad19-c0f9-47ee-b829-73f8d8a077ab"),
]

# Per Pupil Building Expenditures (6 years)
PER_PUPIL = [
    ("2023-2024 Per Pupil Building Expenditures Summary", "8ddb333c-e524-4af7-8d18-419b3edfe7de"),
    ("2022-2023 Per Pupil Building Expenditures Summary", "34d7e7f0-725b-4dd6-a1ef-aea8fe887df9"),
    ("2021-2022 Per Pupil Building Expenditures Summary", "6814fb6a-85af-4bc5-84ae-81072433e722"),
    ("2020-2021 Per Pupil Building Expenditures Summary", "6e978197-1864-4f52-a91d-e973890f1389"),
    ("2019-2020 Per Pupil Building Expenditures Summary", "01e83ceb-75a4-433f-8d37-f39931e3bab7"),
    ("2018-2019 Per Pupil Building Expenditures Summary", "27e25838-e74f-4477-b1f2-39572758998e"),
]

# Adopted budgets (2020-21 through 2024-25). Several years already exist in
# the DB via Diligent — ingest dedup will skip identical content by hash.
BUDGETS = [
    ("2024-2025 Budget", "b6390998-cd02-45d4-9357-451fa2cc3122"),
    ("2023-2024 Budget", "8e23e619-17f2-4187-9e2a-5251de2e3090"),
    ("2022-2023 Budget", "791e51cc-ad72-456a-9009-af47a2f92c49"),
    ("2021-2022 Budget", "e74b9be7-bc2d-4830-830d-219d070eb649"),
    ("2020-2021 Budget", "fbd6211b-e241-408c-a60c-8dd8e382bfe4"),
]


def sanitize_filename(title: str) -> str:
    clean = re.sub(r'[<>:"/\\|?*]', "", title)
    return re.sub(r"\s+", "_", clean.strip())


def download(client: httpx.Client, title: str, uuid: str) -> tuple[str, str, int] | None:
    url = RESOURCE_BASE + uuid
    try:
        resp = client.get(url, follow_redirects=True)
        resp.raise_for_status()
    except httpx.HTTPError as exc:
        logger.error("FAIL %s: %s", title, exc)
        return None

    content_type = resp.headers.get("content-type", "")
    if "pdf" not in content_type and not resp.content[:4] == b"%PDF":
        logger.warning("Non-PDF response for %s (content-type=%s)", title, content_type)
        return None

    filename = f"finance_{sanitize_filename(title)}.pdf"
    out_path = OUTPUT_DIR / filename
    out_path.write_bytes(resp.content)
    logger.info("Downloaded: %s (%d bytes)", filename, len(resp.content))
    return filename, url, len(resp.content)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--monthly",
        action="store_true",
        help="Also crawl monthly warrants / revenue / expenditure summaries (~400 docs).",
    )
    parser.add_argument(
        "--budgets",
        action="store_true",
        help="Also re-download adopted budgets from claytonschools.net "
        "(most are already in DB via Diligent — dedup by hash will skip identical).",
    )
    args = parser.parse_args()

    if args.monthly:
        logger.warning(
            "--monthly not implemented yet. The monthly reports are highly redundant "
            "(warrants list vendors, expenditure/revenue summaries are per-fund roll-ups). "
            "Recommend extracting structured data instead of full-text indexing. "
            "Bailing for now."
        )
        return

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    manifest: list[dict[str, str]] = []
    targets: list[tuple[str, str]] = []
    targets.extend(AUDITS)
    targets.extend(PER_PUPIL)
    if args.budgets:
        targets.extend(BUDGETS)

    logger.info("Downloading %d documents...", len(targets))
    with httpx.Client(timeout=60.0) as client:
        for title, uuid in targets:
            result = download(client, title, uuid)
            if not result:
                continue
            filename, source_url, _ = result
            manifest.append(
                {
                    "source_file": filename,
                    "source_url": source_url,
                    "source_portal": "claytonschools",
                }
            )

    MANIFEST_PATH.write_text(json.dumps(manifest, indent=2))
    logger.info(
        "Crawl complete: %d/%d documents downloaded, manifest at %s",
        len(manifest),
        len(targets),
        MANIFEST_PATH,
    )


if __name__ == "__main__":
    main()
