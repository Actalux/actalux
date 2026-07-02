#!/usr/bin/env python3
"""Calibrate the voiceprint matcher's operating point (leakage-safe, read-only).

Reports the ``(threshold, margin)`` grid + the best operating point at a precision bar over
the stored gallery, using leave-one-**meeting**-out so a query is never scored against a
sample from its own recording (no version-sibling leakage). The matcher math lives in
``actalux.diarization.matching``; this CLI loads the gallery and prints the sweep.

For the full purity/label-gated recalibration with negatives + nested LOMO, see
``scripts/recalibrate_voiceprints.py``. Design: plan §5.

Usage:
    doppler run --project mac --config dev -- \\
      uv run python scripts/voiceprint_calibrate.py --precision-bar 0.98
"""

from __future__ import annotations

import argparse
import logging
import os
from typing import Any

from actalux.config import load_config
from actalux.db import fetch_all_rows, get_client
from actalux.diarization.matching import (
    DEFAULT_MARGINS,
    DEFAULT_THRESHOLDS,
    Metrics,
    OperatingPoint,
    Sample,
    as_vector,
    best_operating_point,
    cosine,
    enabled_officials,
    leave_one_meeting_out,
    nested_leave_one_meeting_out,
    person_scores,
    predict,
    score,
    select_operating_point,
    sweep,
)
from actalux.errors import ActaluxError

# Re-exported so existing callers/tests that import from this module keep working.
__all__ = [
    "DEFAULT_MARGINS",
    "DEFAULT_THRESHOLDS",
    "Metrics",
    "OperatingPoint",
    "Sample",
    "as_vector",
    "best_operating_point",
    "cosine",
    "enabled_officials",
    "leave_one_meeting_out",
    "load_samples",
    "nested_leave_one_meeting_out",
    "person_scores",
    "predict",
    "score",
    "select_operating_point",
    "sweep",
]

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def load_samples(client: Any) -> list[Sample]:
    """Build labeled gallery samples: gallery rows joined to their meeting ``video_id``."""
    rows = fetch_all_rows(
        lambda: client.table("subject_voiceprints").select("person_id,source_document_id,embedding")
    )
    if not rows:
        return []
    doc_ids = sorted({r["source_document_id"] for r in rows})
    docs = fetch_all_rows(
        lambda: client.table("documents").select("id,video_id").in_("id", doc_ids)
    )
    video_by_doc = {d["id"]: (d.get("video_id") or f"doc-{d['id']}") for d in docs}
    return [
        Sample(
            person_id=r["person_id"],
            meeting_key=video_by_doc.get(r["source_document_id"], f"doc-{r['source_document_id']}"),
            embedding=as_vector(r["embedding"]),
        )
        for r in rows
    ]


def main() -> None:
    parser = argparse.ArgumentParser(description="Calibrate the voiceprint matcher (read-only).")
    parser.add_argument("--precision-bar", type=float, default=0.98, help="macro-precision floor")
    parser.add_argument(
        "--aggregation",
        choices=("mean", "max"),
        default="mean",
        help="per-person score aggregation",
    )
    args = parser.parse_args()

    cfg = load_config()
    key = os.environ.get("ACTALUX_SUPABASE_SERVICE_KEY", "")
    if not key:
        raise ActaluxError(
            "ACTALUX_SUPABASE_SERVICE_KEY is required (subject_voiceprints is service-only)"
        )
    client = get_client(cfg.supabase_url, key)

    samples = load_samples(client)
    if not samples:
        logger.info("gallery is empty; enroll voiceprints first (scripts/enroll_voiceprints.py)")
        return
    persons = {s.person_id for s in samples}
    meetings = {s.meeting_key for s in samples}
    logger.info(
        "calibrating on %d samples / %d officials / %d meetings (aggregation=%s)",
        len(samples),
        len(persons),
        len(meetings),
        args.aggregation,
    )

    grid = sweep(samples, DEFAULT_THRESHOLDS, DEFAULT_MARGINS, aggregation=args.aggregation)
    logger.info("thr  margin  macroP  recall  preds")
    for t, m, mtr in grid:
        logger.info(
            "%.2f  %.2f    %.3f   %.3f   %d", t, m, mtr.macro_precision, mtr.recall, mtr.predictions
        )

    best = best_operating_point(grid, args.precision_bar)
    if best is None:
        logger.info(
            "no operating point reaches macro-precision >= %.2f; raise samples or lower bar",
            args.precision_bar,
        )
        return
    t, m, mtr = best
    logger.info(
        "RECOMMENDED: threshold=%.2f margin=%.2f -> macroP=%.3f recall=%.3f (%d predictions)",
        t,
        m,
        mtr.macro_precision,
        mtr.recall,
        mtr.predictions,
    )
    if mtr.confusions:
        logger.info("confusions at that point (true -> wrong pred): %s", mtr.confusions[:10])


if __name__ == "__main__":
    main()
