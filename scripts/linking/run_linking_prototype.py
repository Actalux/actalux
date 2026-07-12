"""Run the cross-meeting linking prototype on a cached body and report the go/no-go.

Loads the ``[E]`` embedding cache (build_embedding_cache.py), attaches ground-truth person labels
from the DB anchors, and measures whether calibrated scoring (AS-norm) links the same official's
clusters ACROSS meetings better than raw cosine — at a matched cluster-purity floor. The pure
measurement lives in ``actalux.diarization.linking.benchmark``; this script is the thin CLI (cache
load, the one DB label read, printing).

Design (docs/architecture/linking-prototype-phase1.md):

- **Ground truth** = the anchor's ``person_id`` per cached cluster (a cluster is only in the cache
  because it carried an anchor). Two clusters share a label iff they are the same official.
- **cannot_link** = clusters recorded in the same meeting are different people (structural, no label
  leakage). **must_link is empty** for this clean measurement: seeding from anchors would hand the
  linker the answer it is being tested on. The linker must recover cross-meeting identity from the
  audio alone.
- **Primary metric** = across-*meeting* pairwise F1, split on ``str(document_id)``. A secondary
  across-*condition* F1 (zoom-proxy) is reported but noisier (the condition label is
  precise-positive only). Within-meeting F1 is structurally ~0 (cannot_link forbids same-meeting
  merges) and omitted from the headline.
- **Operating point** = per backend, the threshold maximizing across-meeting F1 subject to
  ``purity >= --purity-floor``. **Go** iff AS-norm's across-meeting F1 exceeds cosine's there.

Run:
    doppler run --project mac --config dev -- \\
      uv run python scripts/linking/run_linking_prototype.py \\
      --state mo --place clayton --body schools --cache-dir data/linking_cache
"""

from __future__ import annotations

import argparse
import json
import logging
import os
from pathlib import Path

from supabase import Client

from actalux.config import load_config
from actalux.db import fetch_all_rows, get_client, get_place_by_path
from actalux.diarization.enrollment import select_enrollable
from actalux.diarization.linking.benchmark import (
    best_at_floors,
    cannot_link_same_meeting,
    label_stats,
    sweep_backend,
)
from actalux.diarization.linking.observations import (
    VoiceObservation,
    embedding_matrix,
    load_observation_dir,
)
from actalux.diarization.linking.scoring import asnorm_matrix, cosine_matrix, diverse_cohort
from actalux.errors import ActaluxError

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def service_client() -> Client:
    cfg = load_config()
    key = os.environ.get("ACTALUX_SUPABASE_SERVICE_KEY", "")
    if not key:
        raise ActaluxError("ACTALUX_SUPABASE_SERVICE_KEY is required (service-only tables)")
    return get_client(cfg.supabase_url, key)


def fetch_labels(
    client: Client, place_id: int, obs: list[VoiceObservation]
) -> dict[tuple[int, str], int]:
    """Map each cached ``(document_id, cluster_label)`` to its anchor's ``person_id`` (truth)."""
    doc_ids = sorted({o.document_id for o in obs})
    identities = fetch_all_rows(
        lambda: (
            client.table("speaker_identities")
            .select("id,document_id,cluster_label,subject_id,confidence,basis")
            .in_("document_id", doc_ids)
        )
    )
    subjects_by_id = {
        s["id"]: s
        for s in fetch_all_rows(
            lambda: (
                client.table("subjects")
                .select("id,person_id,publishable,canonical_name")
                .eq("place_id", place_id)
            )
        )
    }
    enrollable = select_enrollable(identities, subjects_by_id, confirmed_only=False)
    return {(ec.document_id, ec.cluster_label): ec.person_id for ec in enrollable}


def _fmt_point(point: dict[str, float] | None) -> str:
    """One-line summary of one operating point (or a miss)."""
    if point is None:
        return "(none clears this floor)"
    return (
        f"thr={point['threshold']:+.3f} nodes={int(point['n_nodes'])} "
        f"purity={point['purity']:.3f} recall={point['pair_recall']:.3f} "
        f"F1={point['pair_f1']:.3f} acrMtg={point['across_meeting_f1']:.3f} "
        f"acrCond={point['across_condition_f1']:.3f}"
    )


def run(args: argparse.Namespace) -> None:
    cache_dir = Path(args.cache_dir) / f"{args.state}_{args.place}_{args.body}"
    obs = load_observation_dir(cache_dir)
    if not obs:
        raise ActaluxError(f"no cached observations under {cache_dir}; run build_embedding_cache")

    client = service_client()
    place = get_place_by_path(client, args.state, args.place)
    if not place:
        raise ActaluxError(f"no place {args.state}/{args.place}")
    labels = fetch_labels(client, place["id"], obs)

    true: list[int | None] = [labels.get((o.document_id, o.cluster_label)) for o in obs]
    meeting_cond = [str(o.document_id) for o in obs]
    acoustic_cond = [o.acoustic_condition for o in obs]
    cannot_link = cannot_link_same_meeting(obs)
    unlabeled = sum(1 for t in true if t is None)

    stats = label_stats(true, acoustic_cond)
    logger.info(
        "%s/%s/%s: %d clusters (%d unlabeled), %d officials, %d recurring, %d cross-condition; "
        "purity floor %.2f",
        args.state,
        args.place,
        args.body,
        len(obs),
        unlabeled,
        stats["officials"],
        stats["recurring_officials"],
        stats["cross_condition_officials"],
        args.purity_floor,
    )

    embeddings = embedding_matrix(obs)
    cohort = diverse_cohort(embeddings, args.cohort_size)
    score_matrices = {
        "cosine": cosine_matrix(embeddings),
        # AS-norm needs a DIVERSE impostor cohort; a self/random cohort degenerates on a
        # speaker-imbalanced set (docs/architecture/linking-prototype-phase1.md, Build decisions).
        "asnorm": asnorm_matrix(embeddings, cohort),
    }
    floors = sorted({0.99, 0.95, 0.90, args.purity_floor}, reverse=True)
    frontier_by_backend: dict[str, dict[float, dict | None]] = {}
    for name, scores in score_matrices.items():
        _, sweep = sweep_backend(
            scores, cannot_link, true, meeting_cond, acoustic_cond, purity_floor=0.0
        )
        frontier = best_at_floors(sweep, floors)
        frontier_by_backend[name] = frontier
        print(f"\n=== {name} (cohort={len(cohort)}) ===")
        for floor in floors:
            print(f"  purity>={floor:.2f}: {_fmt_point(frontier[floor])}")

    pf = args.purity_floor
    cos, asn = frontier_by_backend["cosine"][pf], frontier_by_backend["asnorm"][pf]
    cos_f1 = cos["across_meeting_f1"] if cos else 0.0
    asn_f1 = asn["across_meeting_f1"] if asn else 0.0
    verdict = "GO" if asn_f1 > cos_f1 else "NO-GO"
    print(
        f"\n=== GO/NO-GO (across-meeting F1 @ purity>={pf}) ===\n"
        f"  cosine={cos_f1:.3f}   asnorm={asn_f1:.3f}   delta={asn_f1 - cos_f1:+.3f}   -> {verdict}"
    )

    if args.out:
        out = Path(args.out)
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(
            json.dumps(
                {
                    "body": f"{args.state}/{args.place}/{args.body}",
                    "purity_floor": pf,
                    "cohort_size": len(cohort),
                    "n_clusters": len(obs),
                    "label_stats": stats,
                    "verdict": verdict,
                    "frontier": {
                        name: {str(f): fr[f] for f in fr}
                        for name, fr in frontier_by_backend.items()
                    },
                },
                indent=2,
            )
        )
        logger.info("wrote %s", out)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--state", required=True)
    parser.add_argument("--place", required=True)
    parser.add_argument("--body", required=True)
    parser.add_argument("--cache-dir", default="data/linking_cache")
    parser.add_argument(
        "--cohort-size",
        type=int,
        default=32,
        help="AS-norm impostor cohort size (farthest-point sampled; tunable hyperparameter)",
    )
    parser.add_argument(
        "--purity-floor",
        type=float,
        default=0.95,
        help="min cluster purity an operating point must clear (fair comparison)",
    )
    parser.add_argument("--out", help="write results JSON here")
    run(parser.parse_args())


if __name__ == "__main__":
    main()
