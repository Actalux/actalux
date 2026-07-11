# Stylometry as an Evidence Family — spec (S0 MEASURED 2026-07-09: naming role NO-GO at
current corpus scale; contradiction-detector role viable — pending operator's Q4 call)

Decisions (operator, 2026-07-09): profiles from confirmed + inferred_high
roll-call/self-intro text; stylometry writes enrollable inferred_medium anchors like
discourse; contradictions flag-for-review only, never auto-reject.

## S0 results (2026-07-09, `scripts/analyze_stylometry.py`, 109 trusted clusters /
## 23 people / 198,593 words)

- **Attribution (the anchor-generator role): NO-GO.** Leave-one-cluster-out rank-1
  accuracy 0.46 overall (K=50/150/300 sweep: 0.456/0.466/0.408); even at 2,500+ words
  only 0.73–0.77. Same-person Delta (med 0.56) and different-person (med 0.77)
  distributions overlap badly; median win margin 0.04. Nowhere near naming-grade — an
  anchor generator at ~25% error on its BEST stratum would add noise, not evidence.
  The spec's own gate ("near-total separation above a word floor") is not met.
- **Contradiction detection: viable.** Both known acoustic aliens score as extreme
  style mismatches to their labeled person: Patel doc 2549 Delta-to-own 1.066
  (rank 16/23), Poole doc 2127 1.474 (rank 23/23, beyond the same-person max of 1.42
  at K=150). A review-queue flag at Delta-to-own above the same-person distribution's
  tail would have surfaced both — with zero automated data changes (Q3, already locked).
- Known-truth Wilson test: mixed. Of the 5 pre-tenure "Growe" clusters inside Wilson's
  actual term, 2 attribute rank-1 to Wilson (incl. the 4,557-word doc 2090 at 0.447),
  1 more has him top-3. The 3 clusters dated AFTER Wilson left (2021-04-21) correctly
  do NOT attribute to him — they are a third person's voice wearing Growe's label.

## Revised path (pending operator Q4 decision)

- DROP S2's naming half + S3 family wiring for now — signal is too weak at this corpus
  size. Revisit when trusted text grows (accuracy clearly rises with words: 0.08 at
  <250w → 0.73+ at 2500w+; every new confirmation feeds profiles), or with stronger
  features (character n-grams) — re-run S0 to re-decide; the gate stays falsifiable.
- KEEP the contradiction detector: score every existing anchor's cluster text against
  its labeled person's profile; Delta-to-own beyond the same-person tail -> cued review
  queue. Catches Patel/Poole-style aliens from text alone, complementing the acoustic
  vetting (which needs a calibration run to fire).

2026-07-09. Motivated by the Growe/Wilson finding: the acoustic layer correctly says "one
voice, two names" (collapse pairs at cosine 0.83–0.92), but nothing text-side can say WHICH
name is wrong — the discourse labeler is the only text signal, and it is the one that erred.
A per-person *style* signal (word choice, function-word habits) is a second, independent
text signal with a different error mode. It slots into the existing evidence-family
architecture: Gate A consensus already requires ≥2 independent families on one coherent
voice; stylometry becomes the fifth family (adjacency, vote, discourse, human, stylometry).

## 1. What it is (and is not)

- A **naming-evidence generator**: scores an unlabeled cluster's transcript text against
  per-person style profiles; a strong, unambiguous match writes a `speaker_identities`
  anchor with `basis='stylometry'` at `inferred_medium` — exactly the containment tier of
  `discourse` (never public display alone; errors contained by the acoustic gates +
  calibration; tenure guard + roster gating apply).
- A **contradiction detector**: scores EXISTING anchors against their labeled person's
  profile; a strong mismatch flags the anchor for human review (a queue, like the
  hygiene quarantines). It never auto-rejects (§7 Q3).
- **Not** a matcher feature: the voiceprint embedding stays purely acoustic. Measured
  discrimination is not the bottleneck (between-person cosine ≤0.38, within 0.82–0.92);
  label provenance is.

## 2. Method — Burrows's Delta (deterministic, no LLM)

Standard authorship-attribution baseline: z-scored relative frequencies of the top-K most
frequent words in the (per-body) corpus — dominated by function words, which are
topic-resistant (two members discussing the same agenda item share topic vocabulary but
not function-word habits). Distance = mean |Δz| over the K words (Burrows 2002; Evert et
al. 2017, "Understanding and explaining Delta measures for authorship attribution",
DSH 32(suppl 2)). Pure numpy; unit-testable; no model download.

Known confounds to measure, not assume (§3):
- **ASR laundering**: Whisper normalizes disfluencies and suppresses some fillers, which
  weakens style signal; ASR errors also correlate with speaker.
- **Register**: the same person chairs, presents, and chats — style drifts by role.
- **Length**: Delta is unreliable on short text; the family only fires above a per-cluster
  word floor chosen from S0's measurements (not invented).

## 3. Phase S0 — measure before wiring (GO/NO-GO gate)

Read-only script over the existing corpus (no schema, no writes):
1. Build profiles from TRUSTED text only (§7 Q1): per person, pooled turns of clusters
   with `confidence='confirmed'`, or `inferred_high` roll-call/self-intro anchors.
   Baseline volumes (measured 2026-07-09): 13 officials ≥3,000 trusted words; Growe has
   ZERO trusted words (all his anchors are unvetted discourse) — a Growe profile requires
   post-2022 confirmations first.
2. Leave-one-cluster-out over the trusted clusters: can Delta re-identify each held-out
   cluster among the roster? Report same-person vs different-person Delta distributions,
   rank-1 accuracy, and accuracy as a function of cluster word count -> pick the word
   floor and the score/margin thresholds FROM THIS DATA.
3. Score the known ground truth: the 8 pre-tenure "Growe" anchors (temporally impossible)
   and the 2 known alien anchors (Patel doc 2549, Poole doc 2127) should score as
   mismatches vs their labeled person's profile; Wilson's confirmed clusters should match
   Wilson. This is the falsification test — if Delta cannot separate these known cases,
   NO-GO (record why; fall back to long-term experiments).

GO criterion (proposed, §7 Q4): near-total separation of the same/different distributions
above the chosen word floor on trusted data, AND correct verdicts on the known-truth set.
The exact numeric bar is chosen by the operator from S0's report, not asserted here.

## 4. Phase S1 — module + tests

`src/actalux/identity/stylometry.py` (pure): corpus stats, profile build, Delta scoring,
threshold application. Mirrors `hygiene.py` in shape: receipts, no silent drops. Profiles
are place/body-scoped (jurisdiction cardinal); rebuilt from DB at run time, not persisted
(they derive entirely from existing rows — no new state to keep in sync).

## 5. Phase S2 — labeler + contradiction queue

`scripts/label_stylometry.py`:
- Names UNLABELED clusters: roster-gated (tenure-filtered via `members_active_on`),
  place-scoped; writes `basis='stylometry'` at `inferred_medium` only when the best
  person clears the S0 thresholds AND beats the runner-up by the S0 margin; one anchor
  per cluster; never overwrites an existing row (same guard discipline as the resolver).
- Flags CONTRADICTIONS: existing anchors whose cluster text strongly mismatches the
  labeled person's profile -> review queue (report + cued audit rows), no auto-reject.
- Anti-circularity: profiles never include stylometry-anchored text (the same principle
  as `basis='voiceprint'` never enrolling — no self-reinforcement loop).

## 6. Phase S3 — family wiring + recalibration

- `families.py`: `stylometry` becomes its own family.
- `enrollment.py`: add `stylometry` to `NAME_ANCHOR_BASES` + `_MEDIUM_ENROLLABLE_BASES`
  (same tier as discourse — that is what lets it produce calibration Samples and count
  in Gate A consensus; a family that produces no samples cannot corroborate anything).
- Re-run calibration: the deliverable metric is the enablement delta — how many
  unconfirmed officials the 2-family consensus now enables (today: zero at schools;
  everything enabled rides the confirmed waiver) and the trusted-recall movement.

## 7. Operator decisions (interview before S1)

- Q1 profile sources: confirmed-only, vs confirmed + inferred_high roll-call/self-intro
  (recommended — 13 officials clear 3k words vs far fewer confirmed-only).
- Q2 anchor tier: stylometry writes inferred_medium enrollable anchors like discourse
  (recommended — one mechanism), vs verification-only (cannot participate in consensus).
- Q3 contradictions: flag-for-review only (recommended), vs auto-reject at extreme
  mismatch.
- Q4 GO/NO-GO: operator picks the bar from S0's measured report.

## 8. Explicitly deferred (memory: long-term experiments, not now)

- Prosody/pacing features: low expected value while acoustic margins are ≥0.4 cosine.
- Alternate embedder A/B (ECAPA / ReDimNet via the existing `--embedders` harness):
  measurement-ready whenever wanted; today's numbers do not implicate the embedder.
