"""Ingest the full City of Clayton ACFR PDFs as searchable documents.

Companion to ``load_city_budget.py``: that loader renders a clean digest of the
audited *figures* (the citation surface for the Budget page). This script ingests
the *full* audited ACFR PDFs (FY2020-FY2024) through the normal parse → chunk →
embed pipeline, so the report narrative — MD&A, the notes on pensions, debt, and
capital assets, the auditor's report — is searchable and answerable in Ask. The
two are distinct documents per year (digest = ``clayton_acfr_FYxxxx.md``, full
report = ``clayton_acfr_FYxxxx.pdf``); budget figures keep citing the digest.

Run fetch_city_acfr.py first (the PDFs + manifest must be on disk).

  doppler run --project mac --config dev -- uv run python scripts/ingest_city_acfr_fulltext.py
"""

from __future__ import annotations

import json
import logging
import sys
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from ingest import ingest_single_file, resolve_entity_id  # noqa: E402  (sibling script)

from actalux.config import load_config  # noqa: E402
from actalux.db import find_document_by_source, get_client  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

ACFR_DIR = Path(__file__).resolve().parent.parent / "data" / "city_acfr"
SOURCE_PORTAL = "auditor"
ENTITY_PATH = "mo/clayton/council"


def _delete_document(client, doc_id: int) -> None:
    """Delete a document and its chunks (no budget_line_items cite the full PDF)."""
    client.table("chunks").delete().eq("document_id", doc_id).execute()
    client.table("documents").delete().eq("id", doc_id).execute()


def main() -> int:
    config = load_config()
    client = get_client(config.supabase_url, config.supabase_service_key)
    entity_id = resolve_entity_id(client, ENTITY_PATH)
    manifest = json.loads((ACFR_DIR / "manifest.json").read_text())

    for entry in manifest:
        end_year = int(entry["fiscal_year"])
        pdf = ACFR_DIR / entry["file"]
        if not pdf.exists():
            raise SystemExit(f"{pdf} missing; run fetch_city_acfr.py first")
        prior = find_document_by_source(client, pdf.name, SOURCE_PORTAL)
        title = f"City of Clayton ACFR — year ended September 30, {end_year} (full report)"
        result = ingest_single_file(
            client=client,
            path=pdf,  # parse_file converts the PDF (pymupdf4llm + OCR on garbled pages)
            meeting_date=date(end_year, 9, 30),
            meeting_title=title,
            config=config,
            source_url=entry["source_url"],
            source_portal=SOURCE_PORTAL,
            document_type="financial_report",
            entity_id=entity_id,
            date_source="content",
        )
        if prior and prior["id"] != result["doc_id"]:
            _delete_document(client, prior["id"])
        logger.info(
            "FY%d: ingested full ACFR doc #%d (%d chunks)",
            end_year,
            result["doc_id"],
            result["chunks"],
        )
    logger.info("Ingested %d full ACFRs for full-text search.", len(manifest))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
