#!/usr/bin/env python3
"""Stage Sunshine-Law-obtained district records for ingest.

Sunshine records arrive as a private bundle of PDFs whose document type cannot be
read from the filename (an invoice/check/contract has no type keyword in its
name) and whose dates are scattered across filenames and document bodies. This
script is the reproducible record of exactly which files are ingested, with what
type and date, and why each date is trustworthy (``date_source``): it copies the
selected files into ``data/documents/sunshine/`` and writes a manifest that
``scripts/ingest.py --manifest`` consumes (source_portal='sunshine').

The source bundle path is supplied at runtime (``--src``) and is never committed;
``data/`` is gitignored, so the PDFs are not committed either. This file plus the
manifest it writes are the durable, reviewable provenance. Files held back from
ingest are recorded outside the repo (operator memory), not here.

Usage:
    uv run python scripts/stage_sunshine.py --src "/path/to/bundle"
    # then, after reviewing the printed report:
    doppler run --project mac --config dev -- \
        uv run python scripts/ingest.py --manifest data/documents/sunshine/sunshine_manifest.json
"""

from __future__ import annotations

import argparse
import json
import logging
import shutil
from pathlib import Path

from actalux.ingest import pii_guard
from actalux.ingest.chunker import chunk_document, validate_chunks
from actalux.ingest.parser import parse_file

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

SOURCE_PORTAL = "sunshine"

# Subfolders within the source bundle.
_SR1 = "2- sunshine request 1"
_P1 = "3- sunshine request 2/fwschooldistrictofclaytonrecordsrequestpart1"
_P2 = "3- sunshine request 2/fwschooldistrictofclaytonrecordsrequestpart2"
_P4 = "3- sunshine request 2/fwschooldistrictofclaytonrecordsrequestpart4"
_BM = "4- board meetings"

# Each row: (subdir, filename, document_type, meeting_date, date_source, title).
# date_source: 'filename' (date in the name) | 'content' (only date in the body) |
# 'manual' (no authoritative document date found — approximate, flagged for review).
INCLUDE: list[tuple[str, str, str, str, str, str]] = [
    # --- Paragon Architecture invoices (facilities/capital design) ---
    (
        _P2,
        "25-965-01 Invoice 2025-08-22.pdf",
        "invoice",
        "2025-08-22",
        "filename",
        "Paragon Architecture invoice 25-965-01",
    ),
    (
        _P2,
        "25-965-02 Invoice 2025-09-30.pdf",
        "invoice",
        "2025-09-30",
        "filename",
        "Paragon Architecture invoice 25-965-02",
    ),
    (
        _P2,
        "25-965-03 Invoice 2025-10-30.pdf",
        "invoice",
        "2025-10-30",
        "filename",
        "Paragon Architecture invoice 25-965-03",
    ),
    (
        _P2,
        "25-965-04 Invoice 2025-11-18.pdf",
        "invoice",
        "2025-11-18",
        "filename",
        "Paragon Architecture invoice 25-965-04",
    ),
    (
        _P2,
        "25-965-05 Invoice 2026-02-28.pdf",
        "invoice",
        "2026-02-28",
        "filename",
        "Paragon Architecture invoice 25-965-05",
    ),
    (
        _P2,
        "25-966-01 Invoice 2025-11-18.pdf",
        "invoice",
        "2025-11-18",
        "filename",
        "Paragon Architecture invoice 25-966-01",
    ),
    (
        _P2,
        "25-966-02 Invoice 2025-12-04.pdf",
        "invoice",
        "2025-12-04",
        "filename",
        "Paragon Architecture invoice 25-966-02",
    ),
    (
        _P2,
        "25-966-03 Invoice 2026-01-16.pdf",
        "invoice",
        "2026-01-16",
        "filename",
        "Paragon Architecture invoice 25-966-03",
    ),
    (
        _P2,
        "25-967-01 Invoice 2025-11-18.pdf",
        "invoice",
        "2025-11-18",
        "filename",
        "Paragon Architecture invoice 25-967-01",
    ),
    (
        _P2,
        "25-967-02 Invoice 2026-01-28.pdf",
        "invoice",
        "2026-01-28",
        "filename",
        "Paragon Architecture invoice 25-967-02",
    ),
    (
        _P2,
        "25-968-01 Invoice 2025-11-18.pdf",
        "invoice",
        "2025-11-18",
        "filename",
        "Paragon Architecture invoice 25-968-01",
    ),
    (
        _P2,
        "25-968-02 Invoice 2026-01-16.pdf",
        "invoice",
        "2026-01-16",
        "filename",
        "Paragon Architecture invoice 25-968-02",
    ),
    (
        _P2,
        "25-968-03 Invoice 2026-01-26.pdf",
        "invoice",
        "2026-01-26",
        "filename",
        "Paragon Architecture invoice 25-968-03",
    ),
    (
        _P2,
        "25-969-01 Invoice 2025-11-18.pdf",
        "invoice",
        "2025-11-18",
        "filename",
        "Paragon Architecture invoice 25-969-01",
    ),
    (
        _P2,
        "25-969-02 Invoice 2026-01-16.pdf",
        "invoice",
        "2026-01-16",
        "filename",
        "Paragon Architecture invoice 25-969-02",
    ),
    (
        _P2,
        "25-969-03 Invoice 2026-01-26.pdf",
        "invoice",
        "2026-01-26",
        "filename",
        "Paragon Architecture invoice 25-969-03",
    ),
    (
        _P2,
        "25-981-01 Invoice 2025-11-18.pdf",
        "invoice",
        "2025-11-18",
        "filename",
        "Paragon Architecture invoice 25-981-01",
    ),
    (
        _P2,
        "25-981-02 Invoice 2025-12-04.pdf",
        "invoice",
        "2025-12-04",
        "filename",
        "Paragon Architecture invoice 25-981-02",
    ),
    # --- BLDD Architects invoices (facilities/capital design) ---
    (
        _P2,
        "257EF01.400 Clatyon 11-25-2025 Inv 6431.pdf",
        "invoice",
        "2025-11-25",
        "filename",
        "BLDD Architects invoice #6431",
    ),
    (
        _P2,
        "257EF01.400 Clayton 1-31-2026 Inv 6517.pdf",
        "invoice",
        "2026-01-31",
        "filename",
        "BLDD Architects invoice #6517",
    ),
    (
        _P2,
        "257EF01.400 Clayton 10-31-2025 Inv 6321.pdf",
        "invoice",
        "2025-10-31",
        "filename",
        "BLDD Architects invoice #6321",
    ),
    (
        _P2,
        "257EF01.400 Clayton 12-31-2025 Inv 6485.pdf",
        "invoice",
        "2025-12-31",
        "filename",
        "BLDD Architects invoice #6485",
    ),
    (
        _P2,
        "257EF01.400 Clayton 9-30-2025 Inv 6298.pdf",
        "invoice",
        "2025-09-30",
        "filename",
        "BLDD Architects invoice #6298",
    ),
    # --- Architecture checks (district payments, capital projects fund) ---
    (
        _P2,
        "BLDD Check238580, 11.7.25.pdf",
        "check",
        "2025-11-07",
        "filename",
        "BLDD Architects check #238580",
    ),
    (
        _P2,
        "BLDD Check238726, 12.5.25.pdf",
        "check",
        "2025-12-05",
        "filename",
        "BLDD Architects check #238726",
    ),
    (
        _P2,
        "BLDD Check239103, 2.6.26.pdf",
        "check",
        "2026-02-06",
        "filename",
        "BLDD Architects check #239103",
    ),
    (
        _P2,
        "Paragon Architecture Check238601, 11.7.25.pdf",
        "check",
        "2025-11-07",
        "filename",
        "Paragon Architecture check #238601",
    ),
    (
        _P2,
        "Paragon Architecture Check238765, 12.5.25.pdf",
        "check",
        "2025-12-05",
        "filename",
        "Paragon Architecture check #238765",
    ),
    (
        _P2,
        "Paragon Architecture Check239093, 1.30.26.pdf",
        "check",
        "2026-01-30",
        "filename",
        "Paragon Architecture check #239093",
    ),
    # --- Architecture contract ---
    # Date is the AIA B133 "made as of" execution date (page 1), confirmed by the
    # signature page and the 10/28-10/30/2025 production/insurance dates.
    (
        _P2,
        "BLDDcontract-signed.pdf",
        "contract",
        "2025-10-29",
        "content",
        "BLDD Architects facilities-planning contract",
    ),
    # --- Communications-vendor spending ---
    (
        _P1,
        "SUSAN DOWNING 1.6.26 invoice for Check239032.pdf",
        "invoice",
        "2026-01-06",
        "filename",
        "Susan Downing communications invoice (#239032)",
    ),
    (
        _P1,
        "SUSAN DOWNING 12.8.25 invoice for Check238922.pdf",
        "invoice",
        "2025-12-08",
        "filename",
        "Susan Downing communications invoice (#238922)",
    ),
    (
        _P1,
        "SUSAN DOWNING 2.2.26 invoice for Check239220.pdf",
        "invoice",
        "2026-02-02",
        "filename",
        "Susan Downing communications invoice (#239220)",
    ),
    (
        _P1,
        "Susan Downing Check238922, 12.19.25.pdf",
        "check",
        "2025-12-19",
        "filename",
        "Susan Downing check #238922",
    ),
    (
        _P1,
        "Susan Downing Check239032, 1.15.26.pdf",
        "check",
        "2026-01-15",
        "filename",
        "Susan Downing check #239032",
    ),
    (
        _P1,
        "Susan Downing Check239220, 2.12.26.pdf",
        "check",
        "2026-02-12",
        "filename",
        "Susan Downing check #239220",
    ),
    (
        _P1,
        "ExcellenceK-12 Scope-cancelled.pdf",
        "proposal",
        "2025-08-27",
        "content",
        "Excellence K-12 scope authorization (cancelled)",
    ),
    (
        _P1,
        "ExcellenceK-12 Survey.pdf",
        "proposal",
        "2025-08-20",
        "content",
        "Excellence K-12 community survey authorization",
    ),
    (
        _P1,
        "DonavanGroupProposal2026.pdf",
        "proposal",
        "2026-01-01",
        "manual",
        "Donovan Group communications proposal",
    ),
    (
        _SR1,
        "Modern Litho PO2602589, 2.17.26.pdf",
        "invoice",
        "2026-02-17",
        "filename",
        "Modern Litho purchase order #2602589",
    ),
    (
        _SR1,
        "Modern Litho check#239424, 3.19.26.pdf",
        "check",
        "2026-03-19",
        "filename",
        "Modern Litho check #239424",
    ),
    # --- Board presentation + community letter ---
    (
        _P4,
        "2026-02-18-Clayton Board of Education Meeting.pdf",
        "presentation",
        "2026-02-18",
        "filename",
        "Facilities Master Plan presentation to the Board of Education",
    ),
    (
        _P4,
        "February Letter to the Clayton Community.pdf",
        "communication",
        "2026-02-01",
        "content",
        "February 2026 letter to the Clayton community",
    ),
    # --- Board agenda (folder 4). The Sept 3, 2025 meeting has no minutes in the
    # corpus (only video transcripts); this agenda is its only document-form record.
    # The June 4, 2025 agenda from the same folder is omitted as redundant — that
    # meeting's signed minutes are already ingested. ---
    (
        _BM,
        "Board of Education Meeting - Sep 03 2025 - Agenda.pdf",
        "agenda",
        "2025-09-03",
        "filename",
        "September 3, 2025 Board of Education Meeting agenda",
    ),
]


def stage(src_root: Path, out_dir: Path) -> list[dict[str, str]]:
    """Copy each included file into out_dir and return manifest entries."""
    out_dir.mkdir(parents=True, exist_ok=True)
    manifest: list[dict[str, str]] = []
    missing: list[str] = []
    for subdir, filename, doc_type, mdate, dsource, title in INCLUDE:
        src = src_root / subdir / filename
        if not src.exists():
            missing.append(str(src))
            continue
        shutil.copy2(src, out_dir / filename)
        manifest.append(
            {
                "source_file": filename,
                "source_url": "",
                "source_portal": SOURCE_PORTAL,
                "document_type": doc_type,
                "meeting_date": mdate,
                "date_source": dsource,
                "meeting_title": title,
            }
        )
    if missing:
        raise SystemExit("Missing source files:\n  " + "\n  ".join(missing))
    return manifest


def report(out_dir: Path, manifest: list[dict[str, str]]) -> None:
    """Parse/chunk/PII-scan each staged file (no DB, no embedding) and print a table.

    This is the pre-ingest dry run: it surfaces any file that yields no text
    (a scanned PDF the pipeline can't chunk) or trips the PII guard, before any
    write to Supabase.
    """
    print(f"\n{'TYPE':<13}{'DATE':<12}{'SRC':<9}{'CHARS':>7}{'CHK':>5}  PII  TITLE")
    print("-" * 100)
    empties: list[str] = []
    pii_hits: list[str] = []
    by_type: dict[str, int] = {}
    for entry in manifest:
        path = out_dir / entry["source_file"]
        text = parse_file(path)
        chars = len(text)
        chunks = validate_chunks(chunk_document(document_id=0, text=text), text) if text else []
        findings = pii_guard.scan_text(text) if text else []
        pii = pii_guard.summarize(findings) if findings else ""
        if not text or not chunks:
            empties.append(entry["source_file"])
        if findings:
            pii_hits.append(f"{entry['source_file']}: {pii}")
        by_type[entry["document_type"]] = by_type.get(entry["document_type"], 0) + 1
        print(
            f"{entry['document_type']:<13}{entry['meeting_date']:<12}"
            f"{entry['date_source']:<9}{chars:>7}{len(chunks):>5}  "
            f"{'!' if findings else ' ':<3}  {entry['meeting_title'][:48]}"
        )
    print("-" * 100)
    print(
        f"{len(manifest)} records | by type: "
        + ", ".join(f"{k}={v}" for k, v in sorted(by_type.items()))
    )
    if empties:
        print(
            f"\n!! {len(empties)} file(s) produced NO chunks (would fail ingest): "
            + ", ".join(empties)
        )
    if pii_hits:
        print("\n!! PII guard findings:\n  " + "\n  ".join(pii_hits))


def main() -> None:
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    ap.add_argument(
        "--src", required=True, help="root of the source bundle (private, not committed)"
    )
    ap.add_argument(
        "--out", default="data/documents/sunshine", help="staging dir (default: %(default)s)"
    )
    args = ap.parse_args()

    src_root = Path(args.src).expanduser()
    if not src_root.is_dir():
        raise SystemExit(f"--src is not a directory: {src_root}")
    out_dir = Path(args.out)

    manifest = stage(src_root, out_dir)
    manifest_path = out_dir / "sunshine_manifest.json"
    manifest_path.write_text(json.dumps(manifest, indent=2))
    logger.info("Staged %d files into %s", len(manifest), out_dir)
    logger.info("Wrote manifest: %s", manifest_path)
    report(out_dir, manifest)


if __name__ == "__main__":
    main()
