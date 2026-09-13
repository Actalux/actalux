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

**Outcome (2026-09-01 → 09-12):** the measured `other` population was three
things — plat/boundary-adjustment items (→ subdivision), legacy ARB residence
fragments (→ arb, correctly dropped), and two clusters the locked vocabulary had
no type for: **Planned Unit Developments** and **Plan Commission coverage relief**
(impervious coverage, alternative compliance). The first two mapped by evidence
(PR #38: `other` 147 → 95). The last two waited on an operator decision, given
2026-09-12: PUDs get their own type, `planned_unit_development` (migrate_052;
advisory — doc 1304's motion is "recommend approval to the Board of Alderman"),
and coverage relief folds into `site_plan` (docs 1284/1293: PC-decided
site-development review). Map order matters and is pinned: PUD sits after
rezoning so "REZONING & PLANNED UNIT DEVELOPMENT" keeps its type, and coverage
sits after the ARB entries so the ARB's own alternative-compliance items stay
ARB work — both were caught by the full-corpus zero-regression check. Residual
`other` after this: conceptual reviews (no decision), public-comment headers the
legacy grammar mis-segments, and wrapped "Plan Commission – Major [Subdivision
Plat]" headers — a segmenter capture issue, not a vocabulary one.

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

**Outcome (2026-09-08):** precision holds (positional pairing, 15/15 sampled
links verified, every one of the corpus's 698 parsed votes positioned) but
coverage landed at **48% of decisive PC events**, not 70%. The binding
constraint is vote-parser recall, not linkage: the minutes record more motions
than the strict-format vote parsers extracted, and a vote row that does not
exist cannot be linked. Raising this number means extending votes_parser
recall (a separate, body-wide project with its own citation gates) — not
loosening the linker, which stays exact. Also learned: motion-in-body text
matching alone is unusable here — doc 1250 decides three different items with
byte-identical motion sentences, which is why linkage is positional (k-th
occurrence of a repeated motion belongs to the k-th vote in parse order).

## G5 — nightly maintenance hook

Once G1–G2 land, wire `build_land_use_cases.py --apply` into `crawl_minutes.yml`
after vote projection, so new minutes flow into cases within a day. The QA
summary prints into the workflow log; the run **fails** if the unparsed count
rises above its post-G1 baseline (same tripwire philosophy as
`check_meeting_dates.py`: catch regressions in the run that introduces them).
Full-rebuild stays acceptable at this corpus size; incremental rebuild is not
worth its drift risk yet.

**Outcome (2026-09-12):** wired into `crawl_minutes.yml` after the member-vote
projection, on the plan-commission and board-of-adjustment crons only (a council
crawl never touches land-use minutes), and only when that crawl's ingest log
reports ≥1 new or updated document — a dedup no-op day skips the ~40-minute LLM
rebuild. Tripwire: `--max-unparsed 7` (the post-G1 baseline; latest run 6). The
check runs **before** any write, so a tripped run leaves the previous dataset
standing. The rebuild's PostgREST writes retry transient failures (added after a
2026-09-08 ReadTimeout left the tables half-written), which is what makes an
unattended run trustworthy.

## Order and dependencies

G1 → G2 (BoA items must exist before their types can be tuned) → G5 (hook only
after coverage is trustworthy). G3 and G4 are independent and can interleave.
Each gap is one PR with its acceptance criteria demonstrated in the PR body
from a fresh backfill run.

## Out of scope, still

Geocoding/parcels, cross-town schema, any public surface, any served aggregate
or ranking, and any second jurisdiction — Clayton correctness first, per the
council verdict and the operator's sequencing decision.
