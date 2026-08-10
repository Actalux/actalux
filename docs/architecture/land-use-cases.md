# Land-Use Case Dataset

Stealth comparative dataset, v1 scope: Clayton entitlements. The strategic
grounding is the 2026-08 council session (`council/council-transcript-2026-08-09-*.md`):
sell **events, never conclusions**, metro-density before breadth, and validate on
Clayton before any comparative schema exists.

## Decisions (operator, 2026-08-08)

| fork | decision |
|---|---|
| Scope | **Entitlements only**: conditional use, site plan, variance, subdivision, rezoning, text amendment. ARB items (signage, homeowner exterior alterations) excluded from v1 — low buyer value, high resident-privacy surface. |
| Backfill | **Full archive 2015–2026** (~283 minutes docs, PC-ARB + BoA). Longitudinal depth is the product. |
| Extraction | **LLM + verbatim verification** for narrative fields; every extracted field carries a `source_quote` that must appear verbatim in the document or the row is rejected. Same discipline as vote extraction and summaries. |
| Access | **Keyed API only.** No public routes, no UI, no sitemap entries. Served through the dormant per-holder key path (`ACTALUX_API_KEYS`), which becomes the paid surface later without rework. |

## What the records contain (verified against the corpus)

Two header grammars across the decade:

- **2022–present** (`doc 2663` is the reference): numbered items,
  `N. <address> – <type> – <subtype> (HH:MM:SS)`, followed by a "Consideration of
  a request by <applicant>, on behalf of <owner/tenant>" sentence, a staff-report
  summary with an explicit recommendation, discussion, then motion + second +
  tally. The timestamp indexes into the meeting video/transcript we already host.
- **2015–2019** (`doc 1413` is the reference): unnumbered uppercase headings,
  **inverted order** `TYPE – SUBTYPE – ADDRESS`, no timestamps, staff narrative
  in prose. Motions still extract (the votes table already covers these years).
- **2020–2021, Zoom era** (37 docs; `doc 1370` is the reference): table-layout
  minutes whose PDF text extraction yields pipe-separated cells, not prose. No
  regex grammar — these route to the LLM segmenter with the same verbatim-quote
  verification. Also note the uppercase numbered hybrid ("1. 7401 CLAYTON ROAD –
  ARCHITECTURE REVIEW", doc 1319) and the "ARCHITECTURE"/"ARCHITECTURAL"
  spelling split, both handled by the modern grammar + classifier.

Continuances span meetings ("continue the application to a meeting date to be
determined", doc 2663 item 2), so the case — not the meeting item — is the unit.
Decision authority differs by type: the PC **recommends** CUPs/rezonings to the
City Council (advisory) but **decides** site plans; the BoA is **final** on
variances. A CUP case therefore chains into an existing council `matter`.

## Schema (migrate_0xx)

```sql
land_use_cases (
  id, place_id, entity_id,                -- jurisdiction-scoped, cardinal rule
  address_raw text,                       -- verbatim; no geocoding in v1
  application_type text,                  -- controlled vocab (see below)
  subtype_raw text,                       -- verbatim, never normalized
  status text,                            -- pending|approved|approved_with_conditions|
                                          -- denied|continued|withdrawn|recommended_to_council
  decision_role text,                     -- final|advisory
  staff_recommendation text,              -- approve|approve_with_conditions|deny|none_stated
  outcome_matches_staff boolean,          -- null until resolved; THE analytical column
  first_seen date, resolved_date date,
  council_matter_id fk nullable           -- CUP/rezoning chain into council matters
)

land_use_case_events (
  id, case_id fk,
  document_id fk, chunk_id fk, citation_id, video_timestamp,
  action text,                            -- heard|continued|approved|denied|recommended|withdrawn
  conditions_text text,                   -- verbatim, enumerated
  vote_id fk nullable,                    -- member-level tallies come free
  source_quote text not null              -- citation-first; no quote, no row
)

land_use_case_parties (
  id, case_id fk,
  role text,                              -- applicant|owner|tenant|architect|attorney|other
  name_raw text
  -- STRUCTURALLY NO subject_id. Private individuals are named per-record only;
  -- this table must never grow person-entity linkage for non-officials.
  -- Same enforcement pattern as transcript_speaker_names (tier-2 naming).
)
```

`application_type` vocab: `conditional_use | site_plan | variance | subdivision |
rezoning | text_amendment | other`. The v1 ingest filter keeps only these; ARB
rows are never created rather than created-then-hidden.

## Pipeline

1. **Segment** minutes into business items (two grammars above; deterministic).
2. **Classify** item → application_type; drop non-entitlements.
3. **Extract** narrative fields per item via LLM with mandatory `source_quote`
   post-verification (reuse the summarize-verify pattern).
4. **Link** items into cases: key (normalized address, application_type) within a
   rolling window; continuance actions extend the window. Deterministic;
   ambiguities go to a review file, never guessed.
5. **Resolve** status + `outcome_matches_staff` from the case's event sequence.
6. **Chain** advisory cases to council matters by address/CUP mention.

Idempotent per document (delete-then-insert by document_id, same as vote
projection); safe to re-run nightly after ingest.

## Non-goals for v1

No geocoding/parcels, no cross-town schema, no UI, no public access, no
rankings or "approval rate" derived stats in any served payload — the API
returns events with citations; interpretation happens on the buyer's side.
