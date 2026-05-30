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

- **Phase A (here):** establish the **RRF-only baseline**. No external
  retrieval API; only the LLM judge.
- **Phase B:** add a `zerank-1-small` reranker arm over the same pool and
  judgments, behind a flag, with RRF as the fallback ordering. Needs an
  Actalux-scoped ZeroEntropy API key.

## Running

All commands run under `doppler run --project mac --config dev -- uv run python …`:

```
scripts/eval_retrieval.py --no-judge --limit 3   # plumbing only, no LLM spend
scripts/eval_retrieval.py --limit 5              # small judged sample
scripts/eval_retrieval.py                        # full baseline → eval/results/
scripts/eval_retrieval.py --spot-check 20        # review cached grades
```

`queries.json` and `judgments.json` are committed (reproducible labels);
`results/` reports are regenerated, not committed.
