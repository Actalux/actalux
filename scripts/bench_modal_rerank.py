"""Measure the self-hosted reranker's interactive latency, warm and cold.

The question this answers is not whether the model is good — it is the same
Apache-2.0 checkpoint the eval already measured at nDCG@10 0.889 — but whether it
can be served fast enough for live search once ZeroEntropy's endpoint shuts down.

Two numbers decide that, and they are very different:

* **Warm** — a container is already up with the model resident. This is the cost
  the median search pays and should sit in the same range as the hosted endpoint
  (~100-300 ms for a realistic pool).
* **Cold** — no container is up, so the request waits for a machine, the image,
  and a 1.7B checkpoint. ``search/rerank.py`` abandons the call at 10 s and falls
  back to RRF order, which is the failure that matters: the search still answers,
  just measurably worse, and nothing surfaces that it happened.

The gap between them is what a warm-container policy is buying, so it is measured
rather than assumed. Passages are synthetic and sized to the corpus's ~200-word
chunks — latency depends on pair length and count, not on passage content, and
this way the bench needs no DB.

Run (Modal tokens from Doppler ``actalux``):

    MODAL_TOKEN_ID="$(doppler secrets get MODAL_TOKEN_ID --plain --project actalux --config dev)" \
    MODAL_TOKEN_SECRET="$(doppler secrets get MODAL_TOKEN_SECRET --plain --project actalux --config dev)" \
    uv run --group diarization python scripts/bench_modal_rerank.py --depths 20 50 100
"""  # noqa: E501

from __future__ import annotations

import argparse
import statistics
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

QUERY = "how much did the district budget for facilities maintenance"

# ~200 words, matching a real chunk. Content is irrelevant to latency; length is not.
PASSAGE = (
    "The Board of Education reviewed the proposed operating budget for the coming "
    "fiscal year, including facilities maintenance, custodial services, utilities, "
    "and deferred capital projects across the district's elementary and secondary "
    "buildings. Administration presented a summary of expenditures by function and "
    "object, noting that the maintenance line reflects both routine upkeep and the "
    "scheduled replacement of aging mechanical systems identified in the long-range "
    "facilities plan. Members asked about the balance between deferred maintenance "
    "and new construction, the effect of enrollment projections on space needs, and "
    "whether the contingency reserve remains adequate given recent bid volatility. "
    "The superintendent noted that the figures presented are preliminary and will be "
    "revised before adoption, and that a public hearing will precede any final vote. "
    "Discussion followed regarding the timing of bond proceeds, the sequencing of "
    "projects across school sites, and the district's obligation to report actual "
    "expenditures against budget on a quarterly basis to the board and the public. "
) * 1


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--depths",
        type=int,
        nargs="+",
        default=[20, 50, 100],
        help="rerank pool sizes to time (production fuses a 100-candidate pool)",
    )
    parser.add_argument("--repeats", type=int, default=5, help="warm timings per depth")
    parser.add_argument(
        "--skip-cold",
        action="store_true",
        help="skip the cold-start measurement (which must idle out a container first)",
    )
    args = parser.parse_args()

    import modal

    from actalux.search.modal_rerank import APP_NAME

    Reranker = modal.Cls.from_name(APP_NAME, "Reranker")  # noqa: N806
    reranker = Reranker()

    print("=== health (forces a container up; this timing IS a cold start) ===")
    t0 = time.perf_counter()
    info = reranker.health.remote()
    cold_health = time.perf_counter() - t0
    print(f"  {info}")
    print(f"  first call (cold, model load included): {cold_health:.2f}s")

    print("\n=== warm latency ===")
    print(f"  {'depth':>6}  {'p50':>8}  {'min':>8}  {'max':>8}")
    results = {}
    for depth in args.depths:
        passages = [PASSAGE for _ in range(depth)]
        reranker.score.remote(QUERY, passages)  # prime, not timed
        timings = []
        for _ in range(args.repeats):
            t0 = time.perf_counter()
            scores = reranker.score.remote(QUERY, passages)
            timings.append(time.perf_counter() - t0)
            assert len(scores) == depth, f"expected {depth} scores, got {len(scores)}"
        results[depth] = timings
        print(
            f"  {depth:>6}  {statistics.median(timings) * 1000:>7.0f}ms"
            f"  {min(timings) * 1000:>7.0f}ms  {max(timings) * 1000:>7.0f}ms"
        )

    print("\n=== verdict ===")
    budget = 10.0  # search/rerank.py REQUEST_TIMEOUT
    worst_warm = max(max(t) for t in results.values())
    print(f"  search abandons a rerank at {budget:.0f}s and falls back to RRF order.")
    print(f"  worst warm call: {worst_warm:.2f}s  ({'within' if worst_warm < budget else 'OVER'})")
    print(
        f"  cold first call: {cold_health:.2f}s  ({'within' if cold_health < budget else 'OVER'})"
    )
    if cold_health >= budget:
        print(
            "  => a cold container blows the budget, so every search arriving cold is\n"
            "     silently served RRF order. Keeping a container warm (min_containers=1)\n"
            "     is not an optimisation here, it is what makes the reranker apply."
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
