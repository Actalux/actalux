# Retrieval evaluation

Measures Actalux search quality so any retrieval change — adding a reranker,
swapping the embedding model — can be judged on numbers, not intuition. Any
such change is a methods change; measure before/after rather than swapping
silently.

## Method

1. **Query set** (`queries.json`) — a fixed set of realistic records-search
   queries grounded in actual corpus content (finance, governance,
   curriculum). All entries are scored relevance queries. There are **no
   retrieval-time integrity probes**: the privacy guarantee (no individual
   student/personnel records; no closed-session deliberation content) is
   enforced at ingest (`src/actalux/ingest/pii_guard.py`), not at search. A
   relevance judge cannot tell public text *about* a private topic (e.g. the
   Missouri Sunshine Law, which itself lists what may be closed) from an actual
   private record — both look topically relevant — so a search-time "should
   return nothing" probe false-fails on legitimate public records.

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

# hosted-API reranker — the PRODUCTION path (zerank-1-small via ZeroEntropy);
# needs ZEROENTROPY_API_KEY; calls the same client search/hybrid.py uses
scripts/eval_retrieval.py --api-rerank                  # full run, persists rankings

scripts/eval_retrieval.py --spot-check 20        # review cached grades
```

Reports land in `eval/results/` (`baseline_*.md`, `rerank_*.md`, `combined_*.md`).
`queries.json`, `judgments.json`, and `rankings.json` are committed (reproducible
labels + arm orderings); `results/` reports are regenerated, not committed.

## Production reranker (decided)

The production retrieval path reranks the fused RRF pool with **zerank-1-small
via the ZeroEntropy hosted API** (`src/actalux/search/rerank.py`), wired into
`hybrid_search` behind `ACTALUX_RERANK=off|api` (default off; a reranker outage
falls back to RRF order). CPU self-hosting was ruled out on latency
(~244 ms/passage, see `bench_rerank_latency.py`); the hosted endpoint serves the
same Apache-2.0 weights GPU-backed in ~100-300 ms. The eval's `--api-rerank` arm
calls that exact client (same model, doc cap, and `latency="fast"` tier), so the
harness measures the production configuration, not a parallel implementation.

**Validated 2026-06-06** over the 24-query set (judge: claude-sonnet-4-6), RRF
baseline vs `zerank-1-small-api`:

| arm | nDCG@10 | MRR | recall@10 |
|---|---|---|---|
| rrf_only | 0.720 | 0.847 | 0.481 |
| zerank-1-small-api | 0.889 | 0.875 | 0.571 |

**+23% nDCG@10, +19% recall@10.** The lift is largest where RRF failed outright
(e.g. "per-pupil expenditure by building" 0.000 → 0.907). One query regressed
("bond ballot measure board action", 0.787 → 0.509); everything else held or
improved. This reproduces the earlier self-hosted finding, confirming the gain
transfers to the hosted API.

## Answer quality

A second eval (`answer_quality.py`, CLI `scripts/eval_answers.py`) scores the
generated *answer*, not retrieval. For each query it runs the production answer
path (`search/answer.py:assemble_evidence` → `generate_summary`) and an LLM judge
(claude-sonnet-4-6) grades the answer **0–3** on faithfulness, completeness, and
directness — each judged only against the quotes the answer was given, so the
score isolates synthesis from recall. Answers + grades cache per
`(query_id, model_id)`, so `--model` A/Bs reuse prior work.

### Structured-finance routing (decided)

A model A/B (Lever 1) showed no summary model fixes finance faithfulness — it sat
near 1/3 across gpt-5-mini, Haiku, and Gemini — because the answer was read out
of fragmented OCR'd budget-table chunks. The fix is not a better reader but a
better source: a figure-shaped finance query is routed to the structured
`budget_line_items` table (`search/finance.py`), where every figure is parsed,
audited, and carries a verbatim `source_quote` + the `chunk_id` it came from, so
the answer still cites a real source chunk. Function/fund-balance figures are
summed across funds per year (the same all-funds aggregation the public Budget
page uses) so the LLM reports a clean total rather than re-summing column
fragments. The router is deliberately conservative: per-pupil and tax-levy asks
have no structured figure and stay on the text path.

The eval's `--finance-routing` flag exercises the production decision (arm
labelled `<model>+finance`).

**Validated 2026-06-07** over the 8 finance queries (model gpt-5-mini, judge
claude-sonnet-4-6), text baseline vs `+finance`:

| arm | faithfulness | completeness | directness |
|---|---|---|---|
| gpt-5-mini (text) | 1.00 | — | — |
| gpt-5-mini+finance | 2.50 | 2.75 | 2.62 |

On the **6 queries that route to structured data**, mean faithfulness went
**0.67 → 2.83**; e.g. "operation of plant" 0/1/1 → 3/3/3, "instruction" 1/1/2 →
3/3/3, with zero citation drop. The 2 text-path queries (per-pupil, tax levy)
are unchanged within judge noise — the router correctly leaves them alone. This
is the structural gain Lever 1 predicted no model swap could deliver.
