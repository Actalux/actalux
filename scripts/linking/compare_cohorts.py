"""Cohort bake-off — score one body's linking against several candidate AS-norm cohorts.

This is the tool that answers the open question before a cohort is frozen: WHICH background crowd
should the linker normalize against? The cohort only needs diverse voices, similar acoustic
conditions, and target-disjointness — not this town's residents specifically — so the candidates are
plug-ins, and the winner is decided by measurement rather than assumption
(docs/architecture/linking-backend-decision-2026-07-12.md, "Phase-2 build notes"):

- another body's meetings (in-domain, condition-matched, disjoint by construction),
- another municipality's public meetings (same public-record status; scales as Actalux expands),
- an open corpus (e.g. 3D-Speaker) re-embedded through the SAME model,
- the built-ins ``self`` (farthest-point sample of the trial set) and ``labeled-ceiling``
  (one vector per known official — the labeled reference the label-free options aim at).

Reports the purity/F1 frontier per cohort so the tradeoff is visible, not a single number.

Run (CACHE=data/linking_cache):
    doppler run --project mac --config dev -- \\
      uv run python scripts/linking/compare_cohorts.py \\
      --state mo --place clayton --body schools \\
      --target-cache $CACHE/mo_clayton_schools \\
      --cohort council=$CACHE/mo_clayton_council \\
      --cohort council+pc=$CACHE/mo_clayton_council,$CACHE/mo_clayton_plan-commission
"""

from __future__ import annotations

import argparse
import json
import logging
import os
from collections import defaultdict
from pathlib import Path

import numpy as np
from supabase import Client

from actalux.config import load_config
from actalux.db import get_client, get_place_by_path
from actalux.diarization.linking.benchmark import (
    best_at_floors,
    cannot_link_same_meeting,
    label_stats,
    sweep_backend,
)
from actalux.diarization.linking.labels import fetch_person_labels
from actalux.diarization.linking.observations import (
    VoiceObservation,
    embedding_matrix,
    load_observation_dir,
)
from actalux.diarization.linking.scoring import asnorm_matrix, cosine_matrix, diverse_cohort
from actalux.errors import ActaluxError

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

FLOORS = [0.99, 0.95, 0.90]
SELF_COHORT_SIZE = 32  # farthest-point sample size for the built-in `self` baseline


def service_client() -> Client:
    cfg = load_config()
    key = os.environ.get("ACTALUX_SUPABASE_SERVICE_KEY", "")
    if not key:
        raise ActaluxError("ACTALUX_SUPABASE_SERVICE_KEY is required (service-only tables)")
    return get_client(cfg.supabase_url, key)


def parse_cohort_arg(spec: str) -> tuple[str, list[str]]:
    """``NAME=DIR[,DIR...]`` -> ``(name, dirs)``."""
    name, _, dirs = spec.partition("=")
    if not name or not dirs:
        raise ActaluxError(f"--cohort expects NAME=DIR[,DIR...], got {spec!r}")
    return name, [d for d in dirs.split(",") if d]


def per_identity_cohort(client: Client, place_id: int, obs: list[VoiceObservation]) -> np.ndarray:
    """One L2-normalized centroid per known official — the LABELED reference cohort.

    Needs identity labels, so it is only a yardstick: it shows how much a label-free cohort gives up
    (in phase 1 the label-free external pool actually beat it). Never a production option.
    """
    labels = fetch_person_labels(client, place_id, obs)
    groups: dict[int, list[np.ndarray]] = defaultdict(list)
    for o in obs:
        pid = labels.get((o.document_id, o.cluster_label))
        if pid is not None:
            groups[pid].append(np.asarray(o.embedding, dtype=np.float64))
    vectors = []
    for vecs in groups.values():
        mean = np.mean(np.stack(vecs), axis=0)
        norm = float(np.linalg.norm(mean))
        vectors.append(mean / norm if norm > 0 else mean)
    return np.asarray(vectors, dtype=np.float64)


def _fmt(point: dict[str, float] | None) -> str:
    if point is None:
        return "(none clears this floor)"
    return (
        f"acrMtg={point['across_meeting_f1']:.3f} acrCond={point['across_condition_f1']:.3f} "
        f"recall={point['pair_recall']:.3f} bcubedF1={point['bcubed_f1']:.3f} "
        f"macroRec={point['macro_official_recall']:.3f}"
    )


def run(args: argparse.Namespace) -> None:
    client = service_client()
    place = get_place_by_path(client, args.state, args.place)
    if not place:
        raise ActaluxError(f"no place {args.state}/{args.place}")
    place_id = place["id"]

    obs = load_observation_dir(Path(args.target_cache))
    if not obs:
        raise ActaluxError(f"no cached observations under {args.target_cache}")
    labels = fetch_person_labels(client, place_id, obs)
    true: list[int | None] = [labels.get((o.document_id, o.cluster_label)) for o in obs]
    meeting_cond = [str(o.document_id) for o in obs]
    acoustic_cond = [o.acoustic_condition for o in obs]
    cannot_link = cannot_link_same_meeting(obs)
    embeddings = embedding_matrix(obs)
    stats = label_stats(true, acoustic_cond)
    logger.info(
        "target %s/%s/%s: %d clusters, %d officials (%d recurring, %d cross-condition)",
        args.state,
        args.place,
        args.body,
        len(obs),
        stats["officials"],
        stats["recurring_officials"],
        stats["cross_condition_officials"],
    )

    # cosine is the floor every cohort must beat to be worth freezing
    candidates: list[tuple[str, np.ndarray | None]] = [("cosine (no cohort)", None)]
    if args.include_self:
        candidates.append(("self (FPS)", diverse_cohort(embeddings, SELF_COHORT_SIZE)))
    if args.include_labeled_ceiling:
        candidates.append(("labeled-ceiling", per_identity_cohort(client, place_id, obs)))
    for spec in args.cohort or []:
        name, dirs = parse_cohort_arg(spec)
        pool: list[VoiceObservation] = []
        for d in dirs:
            loaded = load_observation_dir(Path(d))
            if not loaded:
                raise ActaluxError(f"cohort {name!r}: no observations under {d}")
            pool.extend(loaded)
        conditions = {o.acoustic_condition for o in pool}
        logger.info(
            "cohort %r: %d vectors from %d dir(s), conditions %s",
            name,
            len(pool),
            len(dirs),
            sorted(conditions),
        )
        candidates.append((name, embedding_matrix(pool)))

    results: dict[str, dict[str, dict | None]] = {}
    for name, cohort in candidates:
        scores = cosine_matrix(embeddings) if cohort is None else asnorm_matrix(embeddings, cohort)
        _, sweep = sweep_backend(
            scores, cannot_link, true, meeting_cond, acoustic_cond, purity_floor=0.0
        )
        frontier = best_at_floors(sweep, FLOORS)
        results[name] = {str(f): frontier[f] for f in FLOORS}
        print(f"\n=== {name} ===")
        for floor in FLOORS:
            print(f"  purity>={floor:.2f}: {_fmt(frontier[floor])}")

    if args.out:
        out = Path(args.out)
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(
            json.dumps(
                {
                    "body": f"{args.state}/{args.place}/{args.body}",
                    "target_cache": args.target_cache,
                    "label_stats": stats,
                    "floors": FLOORS,
                    "cohorts": results,
                },
                indent=2,
            )
        )
        logger.info("wrote %s", out)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--state", required=True)
    parser.add_argument("--place", required=True)
    parser.add_argument("--body", required=True, help="body_slug being scored (the target)")
    parser.add_argument("--target-cache", required=True, help="[E] cache dir for the target body")
    parser.add_argument(
        "--cohort",
        action="append",
        metavar="NAME=DIR[,DIR...]",
        help="a candidate cohort built from one or more cache dirs (repeatable)",
    )
    parser.add_argument(
        "--include-self",
        action="store_true",
        help="also score the built-in self/FPS cohort (transductive; a baseline, not shippable)",
    )
    parser.add_argument(
        "--include-labeled-ceiling",
        action="store_true",
        help="also score a one-vector-per-official cohort (needs labels; a reference only)",
    )
    parser.add_argument("--out", help="write the frontier table here as JSON")
    run(parser.parse_args())


if __name__ == "__main__":
    main()
