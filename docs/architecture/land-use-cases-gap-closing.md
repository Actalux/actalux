# Land-Use Dataset: Gap-Closing Plan (v1.1)

The v1 backfill (docs/architecture/land-use-cases.md) produced 277 cases with a
97% staff-alignment rate — but the QA report says exactly where it is thin, and
this spec is the ordered plan for closing those gaps. Ground rule carried over
unchanged: every extracted field is verbatim-verified or rejected to QA, and
ambiguity goes to review, never into a guess.

## Current state (measured, 2026-08-11)

| gap | size | where recorded |
|---|---|---|
| BoA minutes unparsed | **46 of 56 docs** | `data/landuse_review.jsonl` |
| PC Zoom-era unparsed | 47 docs (2019–21 cluster) | same |
| cases typed `other` | **127 of 277** | QA report |
| party-quote rejections | 370 (parties sparse but safe) | QA report |
| link review queue | 8 appearances | same file |
| vote_id linkage | not attempted | schema column is null |

## G1 — LLM segmenter fallback (closes BoA + Zoom-era together)

**Why one mechanism:** the two gaps look different but fail the same way — no
regex-able header. BoA items are prose ("An appeal from Sanford Talley,
Applicant, on behalf of Dawn Kotva, Owner of 7451 Bland Avenue, for the
following variance … Section 405.330.A.5", doc 1796); Zoom-era minutes are
pipe-separated table cells (doc 1370). A third grammar would fit neither; an
LLM segmenter with the existing verification discipline fits both.

**Design:** `landuse/segment_llm.py::llm_segment_items(text, llm) ->
list[BusinessItem]`. The LLM returns, per item, the **exact opening sentence**
as a quote; the segmenter locates it with the whitespace-tolerant match already
in `extract.quote_in`, derives the span from match positions (each body runs to
the next located opening), and builds ordinary `BusinessItem`s so everything
downstream is unchanged. An opening quote that fails to locate kills that item
to review. The backfill routes to it only when `segment_items()` returns empty.

**BoA bonus fields, same pass:** the variance text carries the **cited code
section** ("Section 405.330.A.5") and the **quantified relief** ("A 200
square-foot variance from the maximum living area"). Extract both as optional
verbatim fields on the event (`code_section`, `relief_raw`) — for a variance
dataset these are the two columns a buyer asks for first. Additive migration
(migrate_050) adds the columns.

**Acceptance:** BoA unparsed ≤ 10 of 56; PC unparsed ≤ 15 of 283; every
LLM-segmented item's span verified verbatim; QA report gains a
`segmenter=regex|llm` breakdown so the two paths' rejection rates are
comparable forever after.

## G2 — application-type vocabulary tuning (the 127 `other` cases)

**Method: measure first, then map.** Dump the `(type_raw, subtype_raw)` pairs
for every `other` case, sorted by frequency; extend `_TYPE_MAP` for the real
patterns found (known already: legacy "PUBLIC HEARING – <real type>" headers
put the type in the subtype; BoA items will classify from "variance … Section"
once G1 lands). Anything still unmatched stays `other` — the vocabulary grows
by evidence, never by plausibility.

**Acceptance:** `other` ≤ 40 of ~330 cases (post-G1 count), and zero previously
classified cases change type (regression assert in the test suite, same
zero-change discipline as the filename-parser extension).

## G3 — party extraction recall (370 rejections)

**Diagnose before changing anything:** sample 30 rejections and classify —
(a) LLM paraphrased the quote, (b) quote crosses a PDF line-break pattern the
whitespace normalizer misses, (c) name genuinely absent. Fix accordingly:
(a) prompt tightening ("copy the quote character-for-character"), (b) extend
`_norm` for the observed artifact, (c) nothing — correct rejections stay
rejections. The gate itself does not weaken; `quote_in` stays the arbiter.

**Acceptance:** party rejection rate halves without any unverified name being
stored; spot-check of 20 stored parties against source PDFs shows 20/20 real.

## G4 — vote linkage (fills the null vote_id column)

630 PC + 54 BoA votes already exist with member-level tallies. Link an event to
a vote when **same document** and the vote's `motion` text appears inside the
item's body span (verbatim, whitespace-tolerant — the item body contains the
motion sentence in every sampled doc). One candidate → link; several → review
file; none → stays null. No fuzzy matching: a wrong vote attached to a case is
worse than no vote.

**Acceptance:** ≥ 70% of decisive PC events carry a vote_id; zero links where
the motion text is not verbatim-present in the item body.

## G5 — nightly maintenance hook

Once G1–G2 land, wire `build_land_use_cases.py --apply` into `crawl_minutes.yml`
after vote projection, so new minutes flow into cases within a day. The QA
summary prints into the workflow log; the run **fails** if the unparsed count
rises above its post-G1 baseline (same tripwire philosophy as
`check_meeting_dates.py`: catch regressions in the run that introduces them).
Full-rebuild stays acceptable at this corpus size; incremental rebuild is not
worth its drift risk yet.

## Order and dependencies

G1 → G2 (BoA items must exist before their types can be tuned) → G5 (hook only
after coverage is trustworthy). G3 and G4 are independent and can interleave.
Each gap is one PR with its acceptance criteria demonstrated in the PR body
from a fresh backfill run.

## Out of scope, still

Geocoding/parcels, cross-town schema, any public surface, any served aggregate
or ranking, and any second jurisdiction — Clayton correctness first, per the
council verdict and the operator's sequencing decision.
