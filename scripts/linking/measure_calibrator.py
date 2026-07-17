"""Measure the pair-score calibrator against AS-norm — the adoption gate for one body.

The calibrator (``linking/calibration.py``) is adopted per body ONLY if it beats plain AS-norm on
held-out proposer outcomes (the phase-2 adoption gate). This is the committed reproduction of that
measurement — the decision-driving numbers come from this script, not a REPL:

1. AS-norm proposer tradeoff -> best correct/wrong at the purity floor.
2. Calibrator fit in-sample -> same tradeoff (an optimistic upper bound: it saw the judged pairs).
3. Leave-one-cluster-out refit at each backend's fixed threshold -> the honest comparison.

``--out`` writes a JSON summary including the full-fit calibrator weights, ready to be frozen by
``set_operating_point.py --calibrator-file``.

Run (CACHE=data/linking_cache):
    doppler run --project mac --config dev -- \\
      uv run python scripts/linking/measure_calibrator.py \\
      --state mo --place clayton --body schools \\
      --target-cache $CACHE/mo_clayton_schools --purity-floor 0.95 \\
      --out data/linking_cache/schools_calibrator.json
"""

from __future__ import annotations

import argparse
import json
import logging
import os
from pathlib import Path

from supabase import Client

from actalux.config import load_config
from actalux.db import get_client, get_place_by_path
from actalux.diarization.enrollment import EMBED_MODEL
from actalux.diarization.linking.benchmark import (
    best_proposer_point,
    cannot_link_same_meeting,
    proposer_tradeoff,
)
from actalux.diarization.linking.calibration import (
    calibrated_matrix,
    fit_calibrator,
    labeled_pair_targets,
    loo_refit_outcomes,
    pair_features,
)
from actalux.diarization.linking.cohort import load_active_cohort
from actalux.diarization.linking.labels import fetch_person_labels
from actalux.diarization.linking.observations import embedding_matrix, load_observation_dir
from actalux.diarization.linking.scoring import asnorm_matrix
from actalux.errors import ActaluxError

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def service_client() -> Client:
    cfg = load_config()
    key = os.environ.get("ACTALUX_SUPABASE_SERVICE_KEY", "")
    if not key:
        raise ActaluxError("ACTALUX_SUPABASE_SERVICE_KEY is required (service-only tables)")
    return get_client(cfg.supabase_url, key)


def _fmt(point: dict[str, float] | None) -> str:
    if point is None:
        return "(no threshold clears the floor)"
    return (
        f"thr={point['threshold']:.3f} purity={point['purity']:.3f} "
        f"correct={point['correct']:.0f} wrong={point['wrong']:.0f} "
        f"ambiguous={point['ambiguous']:.0f} alone={point['alone']:.0f}"
    )


def run(args: argparse.Namespace) -> None:
    client = service_client()
    place = get_place_by_path(client, args.state, args.place)
    if not place:
        raise ActaluxError(f"no place {args.state}/{args.place}")

    obs = load_observation_dir(Path(args.target_cache))
    if not obs:
        raise ActaluxError(f"no cached observations under {args.target_cache}")
    labels = fetch_person_labels(client, place["id"], obs)
    true: list[object | None] = [labels.get((o.document_id, o.cluster_label)) for o in obs]
    cannot_link = cannot_link_same_meeting(obs)
    embeddings = embedding_matrix(obs)
    conditions = [o.acoustic_condition for o in obs]
    seconds = [o.speech_seconds for o in obs]

    cohort = load_active_cohort(client, place["id"], expected_model=EMBED_MODEL)
    if cohort.size == 0:
        raise ActaluxError("no active frozen cohort — build_cohort.py + activate one first")

    asn_rows = proposer_tradeoff(asnorm_matrix(embeddings, cohort), cannot_link, true)
    asn_best = best_proposer_point(asn_rows, purity_floor=args.purity_floor)
    print(f"\nasnorm best @floor {args.purity_floor}:      {_fmt(asn_best)}")

    feats, pairs = pair_features(embeddings, cohort, conditions, seconds)
    keep, y = labeled_pair_targets(true, pairs, exclude=cannot_link)
    calibrator = fit_calibrator(feats[keep], y)
    cal_scores = calibrated_matrix(embeddings, cohort, conditions, seconds, calibrator)
    cal_rows = proposer_tradeoff(cal_scores, cannot_link, true)
    cal_best = best_proposer_point(cal_rows, purity_floor=args.purity_floor)
    print(f"calibrated in-sample bound:      {_fmt(cal_best)}")

    loo = None
    if cal_best is not None:
        loo = loo_refit_outcomes(feats, pairs, true, cannot_link, threshold=cal_best["threshold"])
        print(f"calibrated LOO refit @thr {cal_best['threshold']:.3f}: {loo}")
        if asn_best is not None:
            verdict = "ADOPT" if loo["correct"] > asn_best["correct"] else "KEEP ASNORM"
            print(
                f"\nadoption gate: calibrated held-out correct={loo['correct']} vs "
                f"asnorm correct={asn_best['correct']:.0f} -> {verdict}"
            )

    if args.out:
        out = Path(args.out)
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(
            json.dumps(
                {
                    "body": f"{args.state}/{args.place}/{args.body}",
                    "target_cache": args.target_cache,
                    "purity_floor": args.purity_floor,
                    "asnorm_best": asn_best,
                    "calibrated_best_insample": cal_best,
                    "calibrated_loo_refit": loo,
                    "calibrator": calibrator.to_dict(),
                },
                indent=2,
            )
        )
        logger.info("wrote %s", out)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--state", required=True)
    parser.add_argument("--place", required=True)
    parser.add_argument("--body", required=True, help="body_slug being measured")
    parser.add_argument("--target-cache", required=True, help="[E] anchored cache dir")
    parser.add_argument(
        "--purity-floor",
        type=float,
        required=True,
        help="the operating purity floor the decision is judged at (an operator decision)",
    )
    parser.add_argument("--out", help="write the summary + fitted calibrator here as JSON")
    run(parser.parse_args())


if __name__ == "__main__":
    main()
