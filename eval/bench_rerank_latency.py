"""Measure per-query reranking latency on CPU, to size the production rerank depth.

The reranker (zerank-1-small, 1.7B causal-LM cross-encoder) is the one piece of
the proposed search path whose interactive latency is unmeasured. Reranking adds
a forward pass over every (query, passage) pair in the candidate pool, so the
cost scales with how many candidates we rerank -- the "rerank depth". This bench
times that cost at several depths so we can pick a depth that keeps live search
responsive.

Run on an arm64 CPU (this repo's dev Mac, or the target Oracle A1 box). Apple
Silicon cores are faster per-core than Ampere A1, so a number measured here is a
*floor* -- the Oracle box will be somewhat slower. Thread count is pinned to
approximate the deploy box's core count (Oracle Always Free = 4 OCPUs).

    uv run python eval/bench_rerank_latency.py
    uv run python eval/bench_rerank_latency.py --threads 4 --depths 10 20 30 50

No secrets / DB needed: passages are synthetic, sized to the corpus's ~200-word
chunks, since latency depends on pair length and count, not passage content.
"""

from __future__ import annotations

import argparse
import statistics
import time

import torch

from actalux.eval.rerank import RERANKERS, _score_pairs, load_reranker

RERANKER = "zerank-1-small"  # the adopted model (Apache-2.0, self-hostable)
CHUNK_TARGET_WORDS = 200  # matches Config.chunk_target_words
WARMUP_PAIRS = 4  # first forward pays JIT/graph-build cost; exclude it
REPS = 3  # report the median of this many timed runs per depth
DEFAULT_DEPTHS = (10, 20, 30, 50)

# A realistic civic-records query and a paragraph of board-minutes-style prose.
# The passage is padded to ~CHUNK_TARGET_WORDS so each pair's token length
# matches what the model sees in production.
QUERY = "How did the board vote on the proposed operating budget for next year?"
_BASE_PASSAGE = (
    "The Board of Education convened in regular session to review the "
    "superintendent's recommended operating budget for the upcoming fiscal "
    "year. The finance committee presented projected revenues from local "
    "property taxes, state aid, and federal sources, alongside anticipated "
    "expenditures for instruction, student services, facilities, and "
    "administration. Several members raised questions about staffing levels "
    "and the assumptions used to forecast enrollment. After discussion, the "
    "board considered a motion to approve the budget as presented, with a "
    "scheduled public hearing to precede final adoption. The motion was "
    "seconded and the chair called for a roll-call vote, with the result "
    "entered into the official minutes of the meeting."
)


def _make_passage(index: int) -> str:
    """A ~200-word passage; the index nudges length so chunks vary like real data."""
    words = _BASE_PASSAGE.split()
    # Repeat the base text to reach the target word count, then trim with a
    # small per-index jitter so passages aren't all identical length.
    out: list[str] = []
    while len(out) < CHUNK_TARGET_WORDS + (index % 7) * 5:
        out.extend(words)
    return " ".join(out[: CHUNK_TARGET_WORDS + (index % 7) * 5])


def _time_depth(model, passages: list[str], chunk: int) -> float:
    """Median wall-clock seconds to rerank one query over `passages`."""
    times: list[float] = []
    for _ in range(REPS):
        start = time.perf_counter()
        _score_pairs(model, QUERY, passages, chunk)
        times.append(time.perf_counter() - start)
    return statistics.median(times)


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument(
        "--threads",
        type=int,
        default=4,
        help="torch CPU threads (default 4, ~Oracle A1 free-tier OCPUs)",
    )
    ap.add_argument(
        "--depths",
        type=int,
        nargs="+",
        default=list(DEFAULT_DEPTHS),
        help="rerank depths (pool sizes) to time",
    )
    args = ap.parse_args()

    torch.set_num_threads(args.threads)
    chunk = RERANKERS[RERANKER].predict_chunk
    max_depth = max(args.depths)
    passages = [_make_passage(i) for i in range(max_depth)]

    print(f"arch=arm64 threads={args.threads} model={RERANKER} chunk={chunk}")
    print("Loading model (first call lands on CPU; no GPU path in its code)...")
    model = load_reranker(RERANKER)

    # Warm up: the first forward builds the graph and pays one-time costs.
    _score_pairs(model, QUERY, passages[:WARMUP_PAIRS], chunk)

    print(f"\n{'depth':>6}  {'sec/query':>10}  {'ms/passage':>11}")
    for depth in sorted(args.depths):
        secs = _time_depth(model, passages[:depth], chunk)
        print(f"{depth:>6}  {secs:>10.2f}  {secs / depth * 1000:>11.1f}")


if __name__ == "__main__":
    main()
