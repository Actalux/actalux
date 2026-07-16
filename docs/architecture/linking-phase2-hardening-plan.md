# Phase-2 linker review + hardening plan (2026-07-14)

A code review of the eight phase-2 commits (`49c0e0f..c68d20b`, pushed 2026-07-14) with the
resulting engineering plan. **This document is the work order for the implementing session** —
each item names the files, the design, and the acceptance test. Nothing here changes the phase-2
architecture (cosine + frozen-cohort AS-norm + light calibrator + quarantined proposer, per
docs/architecture/linking-backend-decision-2026-07-12.md); it fixes coherence gaps and evaluation
blind spots found on review, all BEFORE the gated rollout runs anything for real.

## Review verdict

The safety envelope is structurally sound and verified: the proposer writes only
`inferred_medium`/`voiceprint` (below the public RLS gate), `select_enrollable` refuses
`basis='voiceprint'` (no self-enrollment loop), the ambiguity guard refuses two-official nodes,
the confirmed/rejected protection triggers hold, and the cohort tables are service-only with no
person/subject FK. Tests are green (1668) and everything is jurisdiction-general.

But three findings are **rollout blockers** (P0): the dual-prototype feature is dead code
(nothing writes `acoustic_condition`), the all-cluster cache can silently mix with the anchored
cache (shared directory + resume-skip), and the proposal writer can clobber name-anchored
identity rows it cannot see (it fetches `confidence` but not `basis`). Two more (P1) undermine
the evaluation the rollout threshold depends on: the LOO harness reports threshold *spread* but
never held-out *performance*, and the poison simulation's "first 50 pairs in index order"
sampling means nearly every trial shares cluster 0 — it measures poisoning around one official.

## Findings

### P0 — rollout blockers

**F1. Anchored and all-cluster caches share a directory → silent mixed cache.**
`scripts/linking/build_embedding_cache.py:233` computes
`out_dir = <out-dir>/<state>_<place>_<body>` in both modes, and the resume check (`:262`) skips
any `doc_<id>.npz` already present. Running `--include-unanchored` over an existing anchored
cache silently keeps the anchored-only files for those meetings — the "all-cluster" cache is
partial, and the proposer would treat missing citizen clusters as nonexistent. No error, no
warning.

**F2. `subject_voiceprints.acoustic_condition` has no writer and no backfill → dual prototypes
are dead code.** Verified: `voiceprint_row()` (src/actalux/diarization/enrollment.py:156) does
not emit the column; neither `scripts/enroll_voiceprints.py` nor `scripts/recalibrate_voiceprints.py`
mentions it; migrate_047 adds the column with no `UPDATE` backfill and no `CHECK`. Every gallery
row will have NULL condition, so `load_gallery_prototypes()` collapses each official to a single
`"unknown"` prototype — exactly the blurred average dual prototypes were meant to replace.

**F3. The proposal writer can clobber name-anchored rows.**
`propose_identities._write_proposals` fetches only `document_id,cluster_label,confidence` and
skips only `PROTECTED_CONFIDENCE` (`inferred_high`/`confirmed`/`rejected`). But an
`inferred_medium` row with basis `rollcall`/`self_intro` is *not* enrollable at medium
(`_MEDIUM_ENROLLABLE_BASES` excludes them), so it is invisible to the anchor set — and the upsert
then overwrites its basis to `voiceprint` (and possibly its subject). Same for any `inferred_low`
row. A weaker biometric guess must never replace a name-derived attribution of any tier.

**F4. Cohort loader ignores the embedding model.** `load_active_cohort()` selects `model` from
`linking_cohorts` but never checks it. A cohort embedded with a different model would silently
normalize against mismatched geometry — the exact failure the `model` column exists to prevent.

### P1 — evaluation validity (the rollout threshold depends on these)

**F5. `loo_threshold_ci` measures stability, not generalization — and is ~21× slower than
needed.** It re-selects the best threshold per held-out official and reports the threshold
spread (with ≤21 folds, the 2.5/97.5 percentile band is effectively min/max — a spread band, not
a CI). It never evaluates the held-out official at the fold's chosen threshold, so we get no
estimate of how the operating point performs on an unseen official. Also, clustering depends
only on `(scores, threshold)`, never on labels — yet `sweep_backend` re-clusters identically in
every fold.

**F6. Poison simulation sampling is degenerate.** `poison_blast_radius` takes the *first*
`max_trials` cross-official pairs from `combinations(labeled, 2)` — i.e. pairs
`(0,1),(0,2),…(0,~50)`: nearly all trials poison around cluster 0. Additionally,
`_false_enrollments` uses a majority-vote enrollment model, but the actual proposer *refuses*
nodes containing two anchored officials — so the reported radius is an unlabeled upper bound
disconnected from proposer semantics.

**F7. Calibrator training hygiene.** `pair_features`/`labeled_pair_targets` include same-meeting
pairs — structurally-negative pairs that clustering excludes via `cannot_link`, injecting free
easy negatives into the fit. `min_seconds` enters raw (heavy right tail; hundreds→thousands of
seconds), and the heavy same/different pair imbalance is unweighted.

### P2 — operational / consistency

**F8. The cohort bake-off tool only ever existed in a session scratchpad** (`ext_cohort_measure.py`,
now volatile/gone). The committed runner can only compare `self` vs `frozen` — it cannot compare
candidate cohort *sources*, which is the decision the operator still has to make.

**F9. CI cannot run the new modes.** `linking_prototype.yml` has no `include_unanchored` input
(all-cluster embeds are multi-hour and must run in CI — no local WARP), and no cohort/proposer
steps.

**F10. `build_cohort.py` gaps:** never verifies target-disjointness (caller-trust only), never
populates `source_entity_id`, writes `n_vectors` before the vector inserts run (a partial failure
leaves a wrong count), and the migration comment says "condition-balanced" while nothing
balances (the measured winner was *diverse*, not balanced — fix the wording, not the code).

**F11. Gallery-prototype trust is place-scoped, not body-scoped.**
`load_gallery_prototypes` accepts any `cleared` calibration for the *place*; a PC-cleared
calibration would unlock schools prototypes. Should accept only calibrations for the target
body's entity (or `entity_id IS NULL` = place-wide).

**F12. Minor:** evidence rows omit `alternatives` and `target_seconds` (schema has both;
reviewers want them); `_parse_embedding` is a private symbol imported cross-module; the new eval
functions aren't exported from the package `__init__`.

## Work plan

Ordered so each phase is independently commit-able and the pure-library work lands first. All of
it precedes (and does not require) the gated rollout actions.

### Phase A — evaluation library (pure, no DB; F5, F6, F7, F12)

**A1. Rework `loo_threshold_ci` → `loo_operating_point`** in
`src/actalux/diarization/linking/benchmark.py`:
- Precompute once: `thresholds = candidate_thresholds(scores)`, and
  `preds = {thr: constrained_complete_linkage(scores, threshold=thr, cannot_link=…)}`.
- Per held-out official `h`: on labels-minus-`h`, pick `thr*` maximizing across-meeting F1
  subject to the purity floor (evaluating the *precomputed* preds — no re-clustering); then with
  FULL labels evaluate `h` at `thr*`: `h`'s per-official pair recall, and a false-merge flag
  (any of `h`'s clusters in a node whose majority official differs).
- Return: per-fold list `{official, threshold, heldout_recall, false_merge}` + summary
  `{mean_heldout_recall, n_false_merge_folds, mean_threshold, threshold_spread_lo/hi}`. Name the
  band a *spread band* in the docstring, not a CI.
- Keep a thin `loo_threshold_ci` wrapper only if other callers exist (there are none today —
  delete it; it's unreleased).
- Tests: fold count, held-out recall on the two-official fixture, the no-re-cluster path (assert
  identical results to the old implementation on a fixture), a false-merge fixture.

**A2. Fix poison sampling + add guard catch-rate** in `benchmark.py`:
- Stratified deterministic sampling: group candidate poison pairs by unordered official pair,
  round-robin one pair per official-pair (sorted order) until `max_trials`.
- Report additionally `ambiguity_caught` — the fraction of poisoned runs where the forced merge
  produces a node containing ≥2 distinct labeled officials (the node the proposer refuses).
  Keep `_false_enrollments` as the explicit upper bound; docstring states the relationship to
  proposer semantics.
- Tests: stratification (no single cluster dominates the trials), catch-rate = 1.0 on the
  all-anchored fixture.

**A3. Calibrator hygiene** in `src/actalux/diarization/linking/calibration.py`:
- `labeled_pair_targets(true, pairs, *, exclude: set[frozenset[int]] | None = None)` — drop
  cannot-link pairs from the FIT (prediction still fills the full matrix; linkage ignores those
  entries anyway).
- `min_seconds` feature → `log1p(min_seconds)` (rename in `FEATURE_NAMES` to `log_min_seconds`).
- Optional balanced sample weighting in `_fit_logistic` (`sample_weight` multiplying grad/Hessian
  contributions; default = balance pos/neg mass).
- Tests: exclusion drops the right pairs; monotonicity test still passes; weighting shifts the
  bias, not the sign of the score weight.

**A4. Exports + naming (F12):** export `bcubed_prf`, `macro_recall_by_official`, and the new
benchmark entry points from `linking/__init__.py`; rename `cohort._parse_embedding` →
`parse_pgvector` (public; used by `propose_identities`).

### Phase B — schema + scripts coherence (F1, F2, F3, F4, F10, F11, F12-evidence)

**B1. Cache namespacing (F1)** in `scripts/linking/build_embedding_cache.py`:
- All-cluster mode writes to `<out-dir>/<state>_<place>_<body>_all/`.
- Write a `manifest.json` (`{"mode": "anchored"|"all", "min_seconds": …, "model": …}`) into the
  cache dir on first write; on resume, hard-error (`ActaluxError`) if the manifest mode (or
  min_seconds) mismatches the invocation. A missing manifest in a non-empty dir = legacy anchored
  cache: error in all-cluster mode, accept in anchored mode.
- `propose_identities.py` defaults its cache path to the `_all` dir and errors if the manifest
  says `anchored` (an all-cluster proposer run on an anchored cache silently sees no citizens —
  that must be loud).
- Tests: manifest round-trip; mode-mismatch error (tmp_path, no DB).

**B2. Make `acoustic_condition` real (F2).** `scripts/migrate_047_…sql` is **unapplied** — verify
via `apply_migrations.py --check` (047 absent from `schema_migrations`), then edit it in place
(the never-edit rule binds applied migrations only). If it turns out applied, put the same
changes in a new `migrate_048` instead:
- `CHECK (acoustic_condition IN ('zoom','in_person') OR acoustic_condition IS NULL)` on BOTH new
  columns (widen later by constraint-swap, the `speaker_identities_basis_check` precedent).
- Idempotent backfill:
  `UPDATE subject_voiceprints sv SET acoustic_condition = CASE WHEN EXISTS (SELECT 1 FROM
  speaker_identities si WHERE si.document_id = sv.source_document_id AND si.basis='screen_name')
  THEN 'zoom' ELSE 'in_person' END WHERE sv.acoustic_condition IS NULL;`
- Writer path: factor the zoom-doc derivation into a shared helper
  `zoom_document_ids(identities: list[dict]) -> set[int]` in
  `src/actalux/diarization/enrollment.py`; reuse it in `build_embedding_cache._load_anchored`,
  `enroll_voiceprints.py`, and `recalibrate_voiceprints.py`. Add
  `acoustic_condition: str | None = None` to `voiceprint_row()` and stamp it at both enrollment
  writers.
- Tests: helper unit test; `voiceprint_row` includes the key.

**B3. Writer overwrite policy (F3)** in `propose_identities._write_proposals`:
- Fetch `basis` alongside `confidence`. Skip (with a logged reason) any existing row whose
  `confidence ∈ PROTECTED_CONFIDENCE` **or** whose `basis` is a name-anchor basis
  (`enrollment.NAME_ANCHOR_BASES`) at any confidence. A voiceprint proposal may only fill rows
  that are absent, basis-NULL, or basis='voiceprint'.
- Add `alternatives` (top-3 `[{person_id, score}]` from the proposal's other-official anchor
  scores) and `target_seconds` (the cluster's `speech_seconds`) to the evidence insert. Extend
  `proposer.Proposal` with `alternatives: tuple[tuple[int, float], ...]` computed in
  `build_proposals` (it already scans other officials' anchors for the margin); pass
  `speech_seconds` via a `(doc, label) → seconds` map in the script.
- Tests: proposer alternatives on the existing fixtures; a pure policy-function test
  (`_should_skip(existing_row) -> reason | None`) extracted so it's testable without a DB.

**B4. Cohort model validation (F4):** `load_active_cohort(client, place_id, *, expected_model:
str)` raises `ActaluxError` on mismatch. Define the model string once —
`EMBED_MODEL = "pyannote/wespeaker-voxceleb-resnet34-LM"` in `enrollment.py` (light import) — and
consume it from `build_cohort.py`, `propose_identities.py`, and `run_linking_prototype.py`
instead of their private string copies.

**B5. `build_cohort.py` hardening (F10):**
- `--verify-disjoint-body BODY` (repeatable): resolve the body's entity ids, fetch
  `documents.entity_id` for all source doc ids, hard-error on overlap. The rollout invocation for
  schools MUST pass `--verify-disjoint-body schools`.
- Populate `source_entity_id` from the same doc→entity map.
- Insert header with `n_vectors=0`, insert vectors, then `UPDATE n_vectors` to the actual count
  (and only then `--activate`).
- Fix the migrate_047 comment wording: "condition-diverse", not "condition-balanced".

**B6. Body-scoped gallery trust (F11):** in `load_gallery_prototypes`, accept a cleared
calibration only when `entity_id IS NULL` or `entity_id ∈` the target body's entities.

### Phase C — tooling + CI (F8, F9)

**C1. `scripts/linking/compare_cohorts.py`** — the productionized bake-off (replaces the dead
scratchpad script): `--target-cache DIR` plus repeatable `--cohort NAME=DIR[,DIR…]` (and the
built-ins `self` / `labeled-ceiling`); loads target observations + labels (share
`fetch_labels`/label logic with `run_linking_prototype` via a small `scripts/linking/common.py`),
scores each cohort, prints the frontier table at floors {0.99, 0.95, 0.90} with across-meeting +
across-condition F1 (and the new B-cubed/macro columns), writes an optional results JSON. This is
what the operator runs to choose in-domain vs other-town vs open-corpus before freezing.

**C2. CI:** add `include_unanchored` (boolean, default false) input to `linking_prototype.yml`,
pass it through to the embed step, and suffix the artifact name (`…-all`) so anchored and
all-cluster artifacts never collide. A proposer workflow (dry-run) can wait until the rollout
gate opens.

### Explicit non-goals (unchanged from the decision doc)

No changes to `scoring.py` / AS-norm math, `POOL_PARAMS`, or complete-linkage; no multi-
sub-centroid enrollment (deferred hardening); no PLDA/CORAL/TAS-norm escalation; calibrator
adoption stays measure-gated; no new dependencies.

## Verification (whole plan)

1. `uv run python -m pytest tests/` — green (≥1668 + the new tests) and
   `uv run ruff check . && uv run ruff format --check .` clean.
2. Frontier regression: `run_linking_prototype.py --cohort-source self` on the schools cache
   reproduces the decision-doc frontier (Phase A must not move the sweep — only add columns and
   fix the LOO/poison layers above it).
3. `build_embedding_cache.py --include-unanchored` against a tmp copy of an anchored cache dir
   errors loudly (manifest guard) instead of silently mixing.
4. `propose_identities.py --dry-run` on a synthetic mini-cache exercises the skip policy: a
   fixture row with basis `rollcall`@`inferred_medium` is skipped with a logged reason.
5. `--help` smoke on all four `scripts/linking/` CLIs.

## Verification results (2026-07-16 — plan implemented)

Implemented across three commits (Phase A eval, Phase B schema/scripts, Phase C tooling/CI). Suite
**1703 passing** (+35), ruff clean over the touched tree.

**1. Frontier regression — PASSES, bit-identical.** `run_linking_prototype.py --cohort-source self`
on the real 169-cluster schools cache (84 meetings, 21 officials) reproduces the decision doc
exactly: cosine @0.95 `acrMtg=0.540 / acrCond=0.302`, diverse-self AS-norm `0.569 / 0.443`, and
@0.90 cosine `0.891 / 0.883`. Phase A added columns and fixed the layers *above* the sweep without
moving it.

**2. The reworked LOO produces what the old one could not** (asnorm/self cohort, floor 0.95, real
benchmark): 21/21 folds resolve; **mean held-out recall 0.407** against an in-sample recall of 0.416
— the operating threshold generalizes to an official it never saw, with little overfit. Threshold
spread `[3.329, 4.437]`, mean 3.661 (the in-sample pick, 3.805, sits inside it).

**3. A finding the per-official metrics surfaced: 8 of 21 folds show a false merge.** Pairwise purity
0.953 sounds comfortable, but ~5% of 169 clusters being misplaced spreads across **8 distinct
officials** — i.e. at the 0.95 floor, more than a third of the roster has at least one cluster
sitting in someone else's node. Arithmetically consistent with the purity (not a defect), and
exactly the framing the reviewers wanted macro/per-official metrics for. **Carry this into the
rollout:** the purity floor alone is not a per-official safety guarantee.

**4. Poisoning is contained.** At the mean LOO threshold: 50 stratified trials, mean blast radius
0.84, **max 1.0** — a forced cross-official merge does not cascade (complete-linkage's precision
bias holds), and `ambiguity_caught = 1.0` confirms the proposer's guard would refuse every one.

**5. `cannot_link_audit`: 0 suspicious same-meeting pairs** at that threshold — no detectable
diarization fragmentation undermining the cannot-link assumption on this body.

**6. Manifest guard + writer policy** are covered by unit tests (legacy-cache refusal, mode/
min_seconds/model mismatch, and `rollcall@inferred_medium` skipped with a logged reason). The full
`propose_identities --dry-run` remains gated: it needs an all-cluster cache and an active cohort,
neither of which exists until migrate_047 is applied.

## Sequenced rollout (after the plan lands; each step operator-gated, unchanged)

1. Push the hardening commits.
2. Apply migrate_047 (now with CHECK + backfill) via `apply_migrations.py`.
3. Re-embed/refresh caches as needed in CI; run `compare_cohorts.py` (in-domain council+PC vs any
   external candidate) → operator picks the cohort source.
4. `build_cohort.py --verify-disjoint-body <target> … --activate`.
5. All-cluster embed for the target body in CI (`include_unanchored`).
6. `loo_operating_point` on the anchored benchmark → operating threshold + held-out recall.
7. `propose_identities.py` dry-run → operator review → `--write` → `review_identities.py` /
   `confirm_speaker.py`.
