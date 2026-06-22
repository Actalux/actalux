"""Fetch the City of Clayton audited ACFRs from the Missouri State Auditor.

The State Auditor's political-subdivision repository (auditor.mo.gov) is the
authoritative, unblocked source for Clayton's audited Annual Comprehensive
Financial Reports. Each city files its full audited financial statements there
as a public PDF; the city's own site (claytonmo.gov) holds the same documents
but sits behind an Akamai bot-block, so the State Auditor copy is what we
ingest. The figures on the Budget page are transcribed from these PDFs by
``load_city_budget.py``.

Clayton's identity in that system: polysub "City of Clayton" (09-096-0018,
polysubId 1715), St. Louis County (096). The audited FINST report for each
fiscal year carries the full ACFR; the file ids below were resolved from the
repository's file-list API (``ViewReportFilesData?reportId=...``) and confirmed
to be the 120-160pp audited statements rather than the short single-audit
reports.

Idempotent: skips a file already present with the expected size unless --force.

Run:
  uv run python scripts/fetch_city_acfr.py
  uv run python scripts/fetch_city_acfr.py --force
"""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import urllib.request
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# Outside data/documents/ so a directory-scan ingest (ingest.py data/documents/)
# never picks these PDFs up; load_city_budget.py ingests them deliberately.
OUT_DIR = Path(__file__).resolve().parent.parent / "data" / "city_acfr"
VIEW_FILE = "https://auditor.mo.gov/LocalGov/ViewReportFile/"

# fiscal_year -> (audited-ACFR file id, parent report id). The audited financial
# statements PDF, not the acknowledgment letter or the separate single-audit report.
ACFRS: dict[str, dict[str, int]] = {
    "2020": {"file_id": 31760, "report_id": 40942},
    "2021": {"file_id": 40646, "report_id": 45505},
    "2022": {"file_id": 56150, "report_id": 53432},
    "2023": {"file_id": 61215, "report_id": 55978},
    "2024": {"file_id": 73150, "report_id": 62036},
}


def _sha256(path: Path) -> str:
    h = hashlib.sha256()
    h.update(path.read_bytes())
    return h.hexdigest()


def fetch(force: bool = False) -> Path:
    """Download every ACFR, write a provenance manifest, return the manifest path."""
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    manifest: list[dict] = []
    for fy, ref in ACFRS.items():
        url = f"{VIEW_FILE}{ref['file_id']}"
        out = OUT_DIR / f"clayton_acfr_FY{fy}.pdf"
        if out.exists() and out.stat().st_size > 0 and not force:
            logger.info("FY%s present (%d KB), skipping", fy, out.stat().st_size // 1024)
        else:
            logger.info("FY%s downloading %s", fy, url)
            urllib.request.urlretrieve(url, out)
            logger.info("  -> %s (%d KB)", out.name, out.stat().st_size // 1024)
        manifest.append(
            {
                "fiscal_year": fy,
                "fy_end": f"{fy}-09-30",  # Clayton's fiscal year ends September 30
                "file": out.name,
                "file_id": ref["file_id"],
                "report_id": ref["report_id"],
                "source_url": url,
                "source_portal": "auditor",
                "sha256": _sha256(out),
                "bytes": out.stat().st_size,
            }
        )
    manifest_path = OUT_DIR / "manifest.json"
    manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    logger.info("Wrote %s (%d ACFRs)", manifest_path, len(manifest))
    return manifest_path


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--force", action="store_true", help="re-download even if present")
    args = parser.parse_args()
    fetch(force=args.force)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
