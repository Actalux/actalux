# Retrieval evaluation

Measures Actalux search quality so any retrieval change — adding a reranker,
swapping the embedding model — can be judged on numbers, not intuition. Any
such change is a methods change; measure before/after rather than swapping
silently.

## Method

1. **Query set** (`queries.json`) — a fixed set of realistic records-search
   queries grounded in actual corpus content (finance, governance,
   curriculum). `expect_empty: true` marks integrity probes that *should*
   surface no relevant result (e.g. closed-session content is never
   published).

2. **Retrieval** — the production path, unchanged: bge-small query embedding →
   `hybrid_search` (pgvector + Postgres FTS, fused with RRF) → a candidate
   pool. The pool is retrieved 100 deep to leave room for a reranker arm.

3. **Relevance judging** (`judge.py`) — an LLM judge (Claude) grades each
   `(query, passage)` pair **0–3**, independently of which arm surfaced it:
   - 3 = directly answers; a citizen would cite it for this query
   - 2 = relevant/useful but partial
   - 1 = same topic area, doesn't address the query
   - 0 = unrelated or boilerplate

   Grading is **TREC-style depth pooling**: only the top 20 of each arm's
   ranking is judged. That fully covers the @10 metrics and bounds cost — a
   new arm only pays to grade the items it newly lifts into its top 20.
   Grades are cached in `judgments.json` keyed by `(query_id, chunk_id)`, so a
   passage is graded once and reused across arms and runs.

4. **Metrics** (`metrics.py`, "relevant" = grade ≥ 2), reported at k=10:
   - **nDCG@10** — ranking quality (ideal ordering of the same pool = 1.0)
   - **MRR** — reciprocal rank of the first relevant hit
   - **recall@10** — fraction of the pool's relevant items reaching the top 10
   - **relevant-in-pool** — how many relevant items the first stage surfaced
     at all (low values here, not low recall, mean the *retrieval* stage — not
     ordering — is the bottleneck, i.e. a signal to revisit embeddings)

5. **Spot-check** — `--spot-check N` prints a sample of cached grades to
   validate the judge against your own relevance sense before trusting the
   aggregate.

## Phases

- **Phase A:** the **RRF-only baseline** — no external retrieval API, only the
  LLM judge.
- **Phase B (here):** add self-hosted cross-encoder **reranker arms** over the
  same pool and judgments. The weights run locally via sentence-transformers —
  no API key, no per-token cost, no query-time network call — exactly as
  bge-small embeddings already do. The RRF ordering is always kept as the
  comparison anchor; each reranker reorders the same 100-candidate pool by
  cross-encoder relevance, so adding an arm only judges the items it newly
  lifts into the top `JUDGE_DEPTH`, reusing every prior grade. Two models:
  - `zerank-1-small` (`zeroentropy/zerank-1-small-reranker`, 1.7B,
    **Apache-2.0**) — self-hostable with no licensing constraint; the
    production candidate if it captures most of the gain. Loads with
    `trust_remote_code=True`; ships custom modeling code that prompts the
    Qwen3 base as a causal LM and reads the "Yes" logit (runs on CPU here, as
    its code has no MPS path). Needs `accelerate` (dev group) for its
    `device_map` load.
  - `zerank-2` (`zeroentropy/zerank-2-reranker`, 4B, **CC-BY-NC-4.0**) — newer,
    larger, with calibrated scores; a standard CrossEncoder (no remote code),
    runs on MPS. Eval comparison only; production use would need the
    non-commercial determination or a hosted-API agreement.

  Registry and load/score code: `src/actalux/eval/rerank.py`.

### One reranker per process

`zerank-1-small`'s custom code monkeypatches sentence-transformers'
`CrossEncoder` class **globally** and hardcodes its own weights path, so a
second reranker loaded in the same process silently scores with
zerank-1-small's weights. The CLI therefore allows **at most one reranker per
invocation**. Run each separately — each persists its per-query rankings to
`rankings.json` — then `--combined-report` scores every arm against the
**final** judgment union (the only correct way to compare recall@K across arms
that can't co-reside). The combined report is pure: no models, DB, or LLM.

## Running

All commands run under `doppler run --project mac --config dev -- uv run python …`
(the combined-report and spot-check commands need no secrets, but the prefix is
harmless):

```
# RRF baseline
scripts/eval_retrieval.py --no-judge --limit 3   # plumbing only, no LLM spend
scripts/eval_retrieval.py --limit 5              # small judged sample
scripts/eval_retrieval.py                        # full baseline → eval/results/

# self-hosted rerankers — ONE per process (first use downloads its weights)
scripts/eval_retrieval.py --rerankers zerank-2          # full run, persists rankings
scripts/eval_retrieval.py --rerankers zerank-1-small    # full run, persists rankings
scripts/eval_retrieval.py --combined-report             # merge → eval/results/combined_*.md

scripts/eval_retrieval.py --spot-check 20        # review cached grades
```

Reports land in `eval/results/` (`baseline_*.md`, `rerank_*.md`, `combined_*.md`).
`queries.json`, `judgments.json`, and `rankings.json` are committed (reproducible
labels + arm orderings); `results/` reports are regenerated, not committed.
