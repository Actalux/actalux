# Voiceprint Recalibration Plan — label-quality + purity gating, negatives, per-jurisdiction gate

Status: PROPOSED v2 (2026-07-02, codex-reviewed → blockers folded). Companion to
`voiceprint-speaker-id-plan.md`. Bounded scope: produce a *trustworthy* recalibration
verdict; do NOT build/enable the live matcher or publish any name.

## 1. Why — the evidence (root-cause re-diagnosis, 2026-07-02)

First calibration (full gallery, 81 samples / 16 officials / 71 meetings) gave max
macro-precision ~0.4 and the feature was gated as "not viable." Read-only diagnostics
against the stored vectors re-diagnosed the *cause*:

- **NOT far-field, NOT under-clustering.** pyannote finds a median of 16 speakers/
  meeting (7–38). Same-person cross-meeting cosine reaches **p90 = 0.937** — when the
  enrolled cluster is the right voice, wespeaker nails it.
- **The gallery is poisoned, and the poison is concentrated + is a LABEL problem.**
  Per-official median self-similarity: Steve Lichtenfeld 12 samples @ **0.912**
  (coherent) vs **Kami Waldman 23 @ 0.327** and **Bridget McAndrew 11 @ 0.228**
  (incoherent). Those two are 42% of samples but ~70% of same-person pairs, so they
  *are* the low median. Two clusters labeled the same person at cosine **−0.05** means
  the **name is on the wrong voice** — a label error, not just embedding noise.
- **Enrollment method compounds it.** Enrollment is 94% roll-call-anchored (80 of 85
  `inferred_high`; there are **0 confirmed and 0 vote_anchor** identities, and only 5
  self_intro — so we cannot sidestep roll call with a clean gold set). Each cluster is
  then embedded as a 180 s **concatenation** in one forward pass (`_embed_spans`),
  blending any crosstalk into one vector.
- **The resolver already anchors the responder, not the caller** (`identity/resolve.py`
  requires an affirmative "here/present" from a *different* cluster). So mislabels come
  from diarization contamination and the affirmative-heuristic misfiring — good design,
  but not sufficient on its own.
- **Ceiling (with caveats).** On a consensus-cleaned gallery, LOMO calibration hit
  macro-precision 1.000 at recall 0.82–0.96 — but that cleaning used cross-meeting
  label agreement (circular) and there were **zero negatives** (rejecting non-officials,
  the real risk, was untested). This plan removes both caveats.

## 2. Goal and scope

**Goal.** Produce a *trustworthy* verdict on whether the matcher can clear the
precision bar, per jurisdiction, by fixing enrollment with **two distinct gates**
(label-quality + embedding-purity), adding **negatives**, and using a **nested
leave-one-meeting-out** protocol so the operating point is not overfit.

**Done =** a per-place report that honestly states one of: (a) a usable operating
point exists at the macro-precision bar with negatives present and no circular
selection — marked **candidate** pending human confirmation; or (b) it does not, with
why. Plus the enrollment pipeline generalized so a new town avoids this failure.

**Explicitly out of scope (do NOT do):** build/enable the live matcher/predictor; any
auto-publish or name publication; mark a jurisdiction `cleared` (that is a human
decision on top of the candidate report); Fly/web deploy; storing/logging/artifacting
any negative (non-official) voiceprint; model swap.

**Honest limit up front.** We have **no confirmed ground truth** (0 human
confirmations). Every label is a heuristic anchor, some wrong. So the best autonomous
outcome is a **provisional/candidate** verdict; the real production unlock is the
deferred appointed-official confirmation tool (a human confirms a handful of voices).
The plan is written to get the strongest *honest* number and to say plainly how much
to trust it — never to manufacture a "cleared."

## 3. Gate A — label quality (does the vector belong to the named person?)

Purity alone proves a cluster is internally coherent, not that its *name* is right (a
clean vector of the clerk stored under a member is worse than noise). So before any
official sample is used as a positive, it passes label-quality checks:

1. **Basis trust order.** self_intro > rollcall(inferred_high) > rollcall(inferred_low).
   `basis='voiceprint'` never eligible (existing poison-loop guard).
2. **Cross-meeting coherence (per official).** An official must have a **coherent core**
   — a subset of their meetings whose pooled voiceprints mutually agree — or they are
   not enabled. Kami/Bridget fail this: their anchors disagree across meetings, which is
   the fingerprint of unreliable labels (caller/crosstalk). *This uses cross-meeting
   agreement, so it is applied ONLY inside training folds (§5), never to filter the
   held-out test sample — that is what keeps the metric non-circular.*
3. **Cross-official collapse detector.** If two clusters anchored to *different*
   person_ids have near-duplicate pooled vectors (cosine above a high, swept bound),
   that is one voice wearing several names (a roll-call caller labeled as many members)
   — flag both anchors as suspect and exclude from positives. This is independent of
   cross-meeting agreement.
4. **Report, don't hide.** Every dropped official/sample is logged with the reason and
   counts. Dropping is a disclosed policy ("only enable officials with a coherent
   core"), not silent data surgery.

## 4. Gate B — embedding purity (clean a correctly-labeled cluster)

For a cluster that passes Gate A, produce one robust voiceprint:

1. **Embed per turn** (GPU), turns ≥ `EMBED_MIN_SECONDS = 3.0` s (existing NaN-floor).
   GPU returns the per-turn vectors; pooling is local (CPU, pure, testable). Embed once;
   re-pool freely.
2. **Medoid + trim.** Medoid turn = highest mean cosine to the others; drop the bottom
   `trim_fraction` by cosine-to-medoid (trimmed-mean robustness); length-weight-average
   survivors; L2-normalize.
3. **Reject no-core.** If fewer than `min_coherent_turns` survive or their median
   cosine-to-medoid < `purity_floor`, do not emit a voiceprint. A crosstalk cluster
   self-eliminates. `trim_fraction`, `min_coherent_turns`, `purity_floor` are swept +
   reported (§5), never hardcoded.
4. **Record provenance:** `purity` (median cosine-to-medoid of kept turns), `n_turns`,
   `coherent_turns` stored with the vector.

Pure pooling math lives in `src/actalux/diarization/pooling.py` (numpy only, no `modal`,
no GPU) so it is unit-tested in the normal suite and shared by the enroller and the
recalibration harness (one mechanism, no drift).

*Note on the medoid-lands-on-contaminant risk (codex):* Gate B can be fooled if a
contaminant dominates a cluster. Gate A is the backstop — a dominated/mislabeled
cluster fails cross-meeting coherence or the collapse detector and is excluded as a
positive. The two gates are deliberately independent.

## 5. Recalibration harness — nested leave-one-meeting-out

One off-session GPU job (`scripts/recalibrate_voiceprints.py` + workflow), **per place**:

1. **Scope (place/body).** Resolve entities for the place (`documents.entity_id →
   entities.place_id`), exclude superseded docs, and gather: name-anchored official
   clusters + turn spans (positives) and a capped pool of non-official clusters + spans
   (negatives). Everything place-scoped — no global loads.
2. **Embed once (GPU).** Download each meeting's audio once; embed **per turn** for all
   clusters (officials + negatives). Per-turn vectors held in-process only.
3. **Nested LOMO (removes the lucky-operating-point circularity).** Outer loop holds
   out one meeting (`video_id`, so version siblings never leak). Using only the *other*
   meetings, select: which officials are enabled (Gate A coherent core), the pooling
   params `(trim_fraction, min_coherent_turns, purity_floor)`, and the matcher params
   `(threshold, margin, aggregation)`. Then score the held-out meeting's positives **and
   held-out negatives** as-is (no filtering in the test fold). A held-out sample whose
   label is actually wrong therefore *hurts* the score — the safe direction.
4. **Metric.** Macro precision by official (talkative officials can't dominate), recall,
   negative false-positive rate, and confusion pairs. Negatives that match any official
   count as FP (conservative — a negative could be an un-anchored real official, which
   would make the match correct; this *under*-estimates precision). Reported as
   **aggregate counts only** — official-level confusion tallies, never any negative
   cluster/doc/timestamp/speaker identifier (privacy, §11).
5. **Final operating point (nested-CV discipline).** The nested-LOMO numbers are the
   honest *performance estimate*. The single operating point that gets persisted is then
   chosen by running the **same selection procedure refit on ALL meetings** (standard
   after nested CV: inner folds estimate generalization; the deployed params are refit on
   the full data). Both are recorded — the estimate in the report, the refit point in the
   `voiceprint_calibration` row.
6. **Report** the nested-LOMO estimate + the refit operating point + a full sweep grid
   for insight, per place. This is the deliverable.
7. **Persist as CANDIDATE.** Re-enroll the clean official gallery (replace-per-meeting:
   delete a meeting's old rows, insert Gate-A/B survivors — idempotent, reversible),
   stamping each row with the `calibration_id` of this run (§8), and write a
   `voiceprint_calibration` row with `status='candidate'`. Do **not** write `cleared`; a
   human promotes candidate→cleared after reviewing the report. Because candidate-ness is
   carried structurally on each gallery row (via `calibration_id → status`), a future
   matcher cannot mistake a candidate voiceprint for a cleared one. Negatives are never
   persisted.

Because the GPU emits per-turn vectors, the whole nested sweep runs in one pass without
re-embedding.

## 6. Generalizability (a new town must not repeat this)

- **No hardcoded place/body/official.** The script takes a place (and optional body);
  officials, negatives, and meetings resolve per `place_id`/`entity_id` from DB + roster.
  (Current `enroll_voiceprints.py` / `voiceprint_calibrate.py` load globally — the new
  code must add the `entity_id → place_id` scope. Fixing this is part of the plan.)
- **The fix is in the algorithm, not the data.** Gate A + Gate B are the *default*
  enrollment behavior for all jurisdictions, so a new town's roll-call-heavy corpus is
  gated at enroll time — the poison never forms.
- **Calibration is a per-jurisdiction gate.** Operating point + status are calibrated
  and stored **per place** (audio differs by town/room). A town's officials become
  matchable only after *that town's* recalibration is reviewed to `cleared`.
- **reject-no-core + coherence gate are portable safeguards** — whatever a town's
  clerk/crosstalk pattern is, they exclude it structurally.

## 7. No invented constants (data-integrity cardinal)

- `EMBED_MIN_SECONDS = 3.0` — existing, sourced (embedder NaN-floor).
- `trim_fraction`, `min_coherent_turns`, `purity_floor`, coherence-core bound,
  collapse-cosine bound, `threshold`, `margin`, `aggregation` — **swept, reported, and
  the chosen values are selected per fold** (nested LOMO); nothing asserted as a magic
  number. Threshold grid extended to include 0.85/0.90 (diagnostic p90 = 0.937, so the
  old ≤0.80 grid stops too low).
- Pooling = medoid + trimmed length-weighted mean: standard robust aggregation, cited.
- Precision bar (default 0.98) — operator cardinal, unchanged.

## 8. Schema — `migrate_041` (additive, reversible, RLS service-only)

- New `voiceprint_calibration` — one row per run (create FIRST so it can be referenced):
  `id`, `place_id int NOT NULL`, `entity_id int NULL`, `precision_bar real`, `threshold`,
  `margin`, `aggregation`, `trim_fraction`, `min_coherent_turns`, `purity_floor`,
  `macro_precision`, `recall`, `fp_count int`, `n_officials`, `n_enabled_officials`,
  `n_negatives`, `gallery_size`, `model`, `status text` (`candidate|cleared|not_cleared`,
  default `candidate`), `report jsonb` (grid/provenance, aggregate only — no negative
  identifiers), `calibrated_at timestamptz`. The persisted `threshold/margin/aggregation/
  trim_fraction/min_coherent_turns/purity_floor` are the §5.5 full-data refit point.
- `subject_voiceprints`: add `purity real`, `n_turns int`, `coherent_turns int`
  (nullable provenance) + `calibration_id int NULL REFERENCES voiceprint_calibration(id)`.
  The FK makes candidate-vs-cleared **structural**: a future matcher only trusts rows
  whose `calibration_id` resolves to `status='cleared'`. Legacy rows (calibration_id
  NULL) are never auto-trusted.
- RLS service-only + REVOKE anon on both, matching the other voiceprint tables.

## 9. Deliverables + tests

- `src/actalux/diarization/pooling.py` — pure `pool_turn_embeddings(vectors, durations,
  *, trim_fraction, min_coherent_turns, purity_floor) -> Pooled | None`. Tests: clean →
  high purity; bimodal/contaminated → trims intruder or rejects; all-short → reject;
  single dominant → survives.
- `src/actalux/diarization/labelqa.py` (or in the harness) — pure Gate-A helpers:
  `coherent_core(samples)`, `collapse_pairs(clusters)`, `enabled_officials(...)`. Tested.
- `modal_runner.py` — `embed_clusters_remote` returns per-turn vectors per cluster;
  `ModalRunner.embed_cluster_turns(...)` local accessor. Dormant whole-cluster path left
  intact.
- `scripts/voiceprint_calibrate.py` — add negatives support; **nested LOMO**; conservative
  tie-break (on ties prefer **higher** threshold then **higher** margin); extended grid.
  Update `test_best_operating_point_*` accordingly.
- `scripts/recalibrate_voiceprints.py` — the §5 harness (place-scoped GPU embed + nested
  LOMO + report + candidate persist + calibration row). Pure bits unit-tested.
- `scripts/enroll_voiceprints.py` — enroll via Gate A + `pooling`, place-scoped,
  replace-per-meeting, write purity columns.
- `.github/workflows/recalibrate_voiceprints.yml` — off-session (WARP + Modal + Supabase
  service), `workflow_dispatch`, place/body inputs, `concurrency: transcribe`.
- `scripts/migrate_041_voiceprint_purity.sql` (+ apply).

## 10. Execution order + gates

1. Branch off master (`voiceprint-recalibration`); FF-merge at the end.
2. `migrate_041` — write, apply to prod (`apply_migrations.py`), confirm `--check`.
3. Implement §9 with the per-step protocol: build → tests + `ruff` → codex review (high,
   read-only) → fold → conventional commit (author Actalux, no AI attribution).
4. `modal deploy` the diarization app (per-turn return).
5. Dispatch `recalibrate_voiceprints.yml` off-session; read the report via
   `gh run view --log`.
6. Update memory + HANDOFF with the verdict; report to operator with a go/no-go
   recommendation for Phase 4. **Stop at the candidate verdict** — do not enable the
   matcher, do not mark `cleared`, do not publish.

Operator pre-authorized execution **through recalibration** (code, commits, migration
apply, Modal deploy, workflow dispatch, candidate gallery re-enroll).

## 11. Risks

- **No ground truth.** Mitigated by honest `candidate` status + naming human
  confirmation as the real unlock; verdict is provisional by construction.
- **Label-QA/metric entanglement.** Mitigated by nested LOMO: Gate A selection happens
  in training folds only; test-fold samples are scored unfiltered.
- **Medoid-on-contaminant.** Mitigated by independent Gate A (coherence + collapse).
- **Negatives labeling.** Conservative FP counting; report **aggregate counts only**
  (official-level tallies), never any negative cluster/doc/timestamp/speaker identifier.
- **Small-gallery overfit.** Nested LOMO + conservative tie-break; treat numbers as
  provisional until more officials/towns/confirmations exist.
- **Short-turn noise.** Medoid+trim absorbs it; `purity_floor` rejects where it can't.
- **GPU cost.** One L4 pass over ~71 meetings + negatives, a few dollars.
- **Privacy.** Negatives may pass Modal → the CI process, but are NEVER written to DB,
  workflow artifacts, logs, or the report payload; only aggregate FP counts survive.
