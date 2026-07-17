"""Freeze a body's linking operating point (migrate_048) — method + threshold + purity floor.

The operating point is the measured decision the proposer runs on: "score with METHOD, link at
THRESHOLD, hold purity at FLOOR, against the active COHORT". It comes out of the committed
measurements (``measure_calibrator.py`` / the proposer tradeoff), is approved by the operator, and
is stored per (place, body) so automation reads a decision — never a copy-pasted number.

``method=calibrated`` requires ``--calibrator-file`` (the ``measure_calibrator.py --out`` JSON);
the fitted weights are frozen with the threshold so propose-time refits cannot drift.

Dry-run by default — prints the row it would insert; ``--apply`` executes.

Run:
    doppler run --project mac --config dev -- \\
      uv run python scripts/linking/set_operating_point.py \\
      --state mo --place clayton --body schools \\
      --method calibrated --threshold 3.898 --purity-floor 0.95 \\
      --calibrator-file data/linking_cache/schools_calibrator.json \\
      --notes "..." --activate --apply
"""

from __future__ import annotations

import argparse
import json
import logging
import os
from pathlib import Path
from typing import Any

from supabase import Client

from actalux.config import load_config
from actalux.db import fetch_all_rows, get_client, get_place_by_path
from actalux.diarization.linking.calibration import Calibrator
from actalux.diarization.linking.cohort import active_cohort_row
from actalux.errors import ActaluxError

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def service_client() -> Client:
    cfg = load_config()
    key = os.environ.get("ACTALUX_SUPABASE_SERVICE_KEY", "")
    if not key:
        raise ActaluxError("ACTALUX_SUPABASE_SERVICE_KEY is required (service-only tables)")
    return get_client(cfg.supabase_url, key)


def load_calibrator_payload(path: str | None, method: str) -> dict[str, Any] | None:
    """The calibrator dict to freeze, validated by round-tripping through :class:`Calibrator`.

    Accepts either a bare ``Calibrator.to_dict()`` JSON or a ``measure_calibrator.py --out``
    summary (whose ``calibrator`` key holds it). Round-tripping at set time catches a stale feature
    layout here, not at propose time.
    """
    if method != "calibrated":
        if path:
            raise ActaluxError("--calibrator-file only makes sense with --method calibrated")
        return None
    if not path:
        raise ActaluxError("--method calibrated requires --calibrator-file")
    data = json.loads(Path(path).read_text())
    payload = data.get("calibrator", data)
    Calibrator.from_dict(payload)
    return payload


def run(args: argparse.Namespace) -> None:
    client = service_client()
    place = get_place_by_path(client, args.state, args.place)
    if not place:
        raise ActaluxError(f"no place {args.state}/{args.place}")
    place_id = place["id"]
    entities = fetch_all_rows(
        lambda: client.table("entities").select("id,body_slug").eq("place_id", place_id)
    )
    if not any(e.get("body_slug") == args.body for e in entities):
        raise ActaluxError(f"no entity for body {args.body!r} in place {place_id}")

    cohort = active_cohort_row(client, place_id)
    if cohort is None:
        raise ActaluxError(
            "no active cohort — the threshold was measured against one; freeze it "
            "first (build_cohort.py --activate)"
        )
    calibrator = load_calibrator_payload(args.calibrator_file, args.method)

    row = {
        "place_id": place_id,
        "body_slug": args.body,
        "cohort_id": cohort["id"],
        "method": args.method,
        "threshold": args.threshold,
        "purity_floor": args.purity_floor,
        "calibrator": calibrator,
        "notes": args.notes,
        "is_active": False,
    }
    logger.info(
        "operating point for %s/%s/%s: method=%s threshold=%.4f floor=%.2f cohort=%s(id=%d)%s",
        args.state,
        args.place,
        args.body,
        args.method,
        args.threshold,
        args.purity_floor,
        cohort.get("slug"),
        cohort["id"],
        " +calibrator" if calibrator else "",
    )
    if not args.apply:
        logger.info(
            "dry-run: no writes (re-run with --apply%s)", "" if args.activate else " --activate"
        )
        return

    inserted = client.table("linking_operating_points").insert(row).execute().data[0]
    logger.info("created linking_operating_points id=%d", inserted["id"])
    if args.activate:
        client.table("linking_operating_points").update({"is_active": False}).eq(
            "place_id", place_id
        ).eq("body_slug", args.body).execute()
        client.table("linking_operating_points").update({"is_active": True}).eq(
            "id", inserted["id"]
        ).execute()
        logger.info("activated operating point id=%d for %s", inserted["id"], args.body)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--state", required=True)
    parser.add_argument("--place", required=True)
    parser.add_argument("--body", required=True, help="body_slug, e.g. schools")
    parser.add_argument("--method", required=True, choices=("asnorm", "calibrated"))
    parser.add_argument(
        "--threshold",
        type=float,
        required=True,
        help="linkage threshold from the committed measurement (never invented)",
    )
    parser.add_argument(
        "--purity-floor",
        type=float,
        required=True,
        help="the operator-approved purity floor the threshold was selected at",
    )
    parser.add_argument(
        "--calibrator-file",
        help="measure_calibrator.py --out JSON (required for --method calibrated)",
    )
    parser.add_argument("--notes", help="provenance: which measurement, which decision")
    parser.add_argument("--activate", action="store_true", help="make this the body's active point")
    parser.add_argument("--apply", action="store_true", help="execute (default is dry-run)")
    run(parser.parse_args())


if __name__ == "__main__":
    main()
