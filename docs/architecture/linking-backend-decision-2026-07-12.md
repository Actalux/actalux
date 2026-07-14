# Linking-backend decision — cohort, calibration, cross-condition drift (2026-07-12)

Phase-1 measured whether calibrated scoring beats raw cosine for cross-meeting speaker linking
(docs/architecture/linking-prototype-phase1.md). This records the schools result, three
independent expert reviews (codex `gpt-5.6-sol`, grok-4.5, and an HF/academic SOTA survey), their
convergence, and the phase-2 backend plan. It does **not** change code beyond the phase-1 tooling
already shipped; it sets the direction.

## The schools measurement (169 clusters, 21 officials, 17 recurring, 84 meetings)

Pairwise F1 over same-official cross-meeting pairs, at a cluster-purity (precision) floor. Best
operating point per floor, threshold grid = 80 percentiles (a coarse 30-grid understated both
backends — the original "NO-GO" artifact):

| Backend | @purity≥0.95 across-meeting | @0.95 across-**condition** | @purity≥0.90 |
|---|---|---|---|
| cosine | 0.540 | 0.302 | 0.89 (recall 0.86) |
| AS-norm, self-cohort | 0.00 (degenerate) | 0.00 | 0.89 |
| **AS-norm, FPS diverse cohort (label-free)** | **0.569–0.591** | **0.44–0.46** | 0.89 |
| AS-norm, 1-per-official cohort (labeled ceiling) | 0.632 | 0.460 | 0.89 |

Findings: (1) the wespeaker embeddings carry strong cross-meeting signal — at 0.90 purity plain
cosine already links 86% of pairs; (2) the high-purity (≥0.95) regime — the only one safe for a
poison-sensitive gallery — is where calibration matters, and there a **diverse-cohort AS-norm
beats cosine, especially on the cross-condition (Zoom↔in-person) metric**; (3) the AS-norm
degeneracy was self-cohort contamination: a few officials appear in 15–28 meetings, so their own
sibling clusters poison the impostor cohort and suppress true matches. A random cohort fails the
same way; a farthest-point-sampled diverse cohort recovers most of the labeled ceiling.

## Three-source convergence

All three reviewers independently agreed on:

1. **Don't ship cosine@0.90 into a self-updating gallery** — 0.90 purity is too poison-prone.
   Anything feeding enrollment lives at ≥0.95.
2. **Not PLDA yet** — 21 officials / 84 meetings is too little; PLDA overfits the imbalance and the
   fixed 256-d embeddings gain little over good score normalization. Keep spherical/shrinkage PLDA
   (CORAL+/CORAL++ adaptation) as a later challenger.
3. **Diverse-cohort AS-norm is principled** (it is metric k-center / Gonzalez 1985; and clustering-
   based cohort selection is an established technique — Apsingekar & De Leon 2011), **not a hack —
   but use it as a cross-condition score *adapter*, not the global backend.**
4. **Freeze a diverse, external, per-identity, condition-balanced impostor cohort built offline.**
   The self/trial-set cohort makes scoring transductive (adding a meeting silently changes old
   identities) and re-introduces the imbalance. Standard wespeaker/VoxSRC practice builds the
   cohort from **per-speaker averaged embeddings** (one vector per impostor identity — the
   imbalance fix baked in) + adaptive top-N ≈ 200–400. Canonical reference: Matejka et al.,
   Interspeech 2017. Our FPS is the unsupervised stand-in for "per-identity + diverse" when we
   cannot label identities.
5. **Separate linking from enrollment.** Auto-linked clusters are *quarantined candidates*; only
   name-anchored or independently corroborated clusters update the canonical gallery, with
   provisional-vs-confirmed tiers and human review on first enrollment. (This is already Actalux's
   candidate→cleared + Option B rule — the reviewers independently validated our safety model.)
6. **Highest-leverage cross-condition fix = condition-awareness**, without retraining the embedder:
   condition-stratified scoring with dual per-condition prototypes/thresholds, plus a **light**
   condition-aware calibrator. codex → a QMF-style logistic calibrator (cosine + AS-norm +
   condition-pair + duration/dispersion); grok → a thin affine/logistic condition calibrator +
   AS-norm only cross-condition + session-mean subtraction; SOTA → condition-aware LR calibration
   with a QMF side-feature (Mandasari 2013 → IDLab VoxSRC-20 Thienpondt 2021). All three: keep it
   light given weak in-domain ground truth.

Shared caveats to honor in phase 2:
- **`cannot_link` (same-meeting = different people) can be wrong** if diarization fragments/merges
  a speaker within a meeting — monitor within-meeting cluster purity before trusting it.
- **A single centroid hides contamination** — duration-weighted median-trim pooling, min-duration
  gate (drop <3–5 s turns), reject diarization-leakage turns, consider 2–3 sub-centroids per
  enrollment rather than one residue.
- **Complete-linkage** is the right precision-biased choice; keep it. Its recall decays as an
  identity accumulates meetings (one bad cluster becomes a permanent veto) — fix via pooling
  hygiene, not by switching linkage.
- **Threshold on 21 speakers is high-variance** — fit it with leave-one-official-out / bootstrap
  over meetings and report a confidence interval; never retune on the same anchors that define GT.
- **Pairwise F1 overweights prolific officials quadratically** — also report macro per-official
  recall, B-cubed P/R (Amigó et al. 2009), worst-cluster purity, and a *poisoning simulation*
  (inject one bad merge, measure downstream false enrollments).
- **Anchor-derived truth is selected truth** (biased to frequently-named chairs/clerks) — keep an
  explicit "unlinked / guest" sink; never force every cluster into a gallery identity.
- **Freeze cohort + calibrator + threshold + gallery together, versioned.**

## Decision — phase-2 backend

Ship a synthesis, not a single lettered option:

> **cosine baseline + diverse-cohort AS-norm as a cross-condition adapter (frozen, external,
> per-identity, condition-balanced cohort) + a light condition-aware logistic calibrator, all
> feeding a quarantine → human-confirm gallery with dual per-condition prototypes.**

This reconciles grok's "cosine + thin adapter" and codex's "light QMF calibrator": AS-norm is the
adapter, the LR calibrator is light and condition-aware, and neither is trusted as a standalone
global backend. PLDA/CORAL/TAS-norm stay as escalation options, not the first build.

## Sequenced plan (cheapest + most diagnostic first — SOTA-recommended)

1. **External per-identity condition-balanced cohort.** Build the AS-norm cohort from *other Clayton
   bodies'* officials (council / plan-commission — disjoint from schools officials by construction,
   so zero sibling leakage), one vector per identity, balanced Zoom/in-person; FPS/medoid on those.
   Re-run schools. If it recovers the labeled ceiling → done, no training. (This repurposes the
   council/PC embed we already planned; needs those bodies cached.)
2. **If it plateaus cross-condition:** add a light condition-aware LR calibrator (cosine [+AS-norm]
   + a Zoom/in-person indicator [+ duration]) fit on our anchor-derived same/different pairs. Adopt
   **wespeaker's** Apache-2.0 `score_norm` + `calibration` harness rather than hand-rolling (per the
   upstream-native rule).
3. **If still short:** escalate to CORAL+ PLDA adaptation (ASV-Subtools) or TAS-norm (trainable
   AS-norm with a margin penalty against sibling selection — Choi et al. 2025), and/or session-mean
   subtraction + enrollment-hygiene pooling.
4. Throughout: dual per-condition prototypes, purity-≥0.95 gallery gate with a lower-confidence-
   bound threshold, and the richer eval metrics above.

## Tooling + licenses (all commercial-OK)

- **wespeaker** (Apache-2.0, active): AS-norm + PLDA + quality-aware LR calibration — the best
  single fit; adopt its scoring/calibration rather than reimplementing.
- **3D-Speaker** (Apache-2.0): AS-norm code + the **3D-Speaker multi-device/multi-distance corpus**
  (arXiv 2306.15354) — the closest public analogue to our Zoom↔room shift; a ready external
  cross-condition benchmark + cohort top-up. ReDimNet = a stronger second embedder if we ever need
  to confirm the drift is scoring-side, not embedder-side.
- **ASV-Subtools** (Apache-2.0): several PLDA domain-adaptation variants in one place.
- **SpeechBrain** (Apache-2.0): pip-installable two-covariance PLDA for a first PLDA back-end.
- **License note (LLC):** our embedder `pyannote/wespeaker-voxceleb-resnet34-LM` ships **CC-BY-4.0**
  weights (attribution, commercial-OK); the underlying VoxCeleb data is research-oriented — a
  one-line legal note is prudent. All toolkit *code* above is Apache-2.0 with no dataset lineage.
- **No turnkey cross-meeting linker exists** (confirmed again) — the linking orchestration
  (constrained clustering, condition-aware scoring, keep-official-anchored) is ours to assemble; the
  scoring/calibration *components* are off-the-shelf.

## Step-1 result (2026-07-13) — external cohort validated, ceiling exceeded

Council (92 meetings) and plan-commission (91) were embedded and used as schools' external
impostor cohort. Schools linking @purity≥0.95 (across-meeting F1 / across-condition F1):

| Cohort | across-meeting | across-condition |
|---|---|---|
| cosine baseline | 0.540 | 0.302 |
| diverse-self (32) | 0.569 | 0.443 |
| council-only, all-cluster | 0.604 | 0.465 |
| **council+PC, all-cluster (419, condition-diverse, disjoint)** | **0.649** | **0.528** |
| labeled 1-per-official ceiling (reference) | 0.632 | 0.460 |

The external, condition-diverse, target-disjoint cohort **beats cosine by +0.11 across-meeting and
+0.226 across-condition (+75% rel.)** and **exceeds the labeled ceiling** — confirming the plan with
**no training required**. Sub-finding: the *all-cluster* external cohort beats the *per-identity*
one (0.649/0.528 vs 0.586/0.435) — for a target-disjoint pool there are no siblings to dedup, so a
larger diverse pool gives richer impostor statistics (matches Matejka's "diverse pool + adaptive
top-N"). **Recipe locked:** freeze a large, condition-balanced, cross-body (target-disjoint) cohort;
score AS-norm top-N against it. Step 2 (light condition-aware calibration) is now optional headroom,
not a necessity. Measured with the cached npz (no re-embed) via `ext_cohort_measure.py`; recall at
0.95 is ~0.50 (precision-first, correct for gallery safety — the rest is recovered as purity relaxes
to 0.90 or via the evidence ledger over more meetings). Caveats still apply (point estimate on 21
officials → phase 2 needs leave-one-official-out + CI; pairwise F1 overweights prolific officials →
add B-cubed / poisoning sim).

## Key references

- Matejka et al., "Analysis of Score Normalization in Multilingual Speaker Recognition," Interspeech 2017 — the AS-norm cohort study (diverse multi-channel pool; per-identity; adaptive top-N).
- Sturim & Reynolds, "Speaker Adaptive Cohort Selection for Tnorm," ICASSP 2005 — origin of adaptive cohort selection.
- Nautsch et al., Interspeech 2015 — condition-matched cohort selection; Nautsch PhD thesis 2019 (tuprints 9199) — QMF + cohort + calibration under mismatch, one place.
- Thienpondt et al., "IDLab VoxSRC-20: Quality-Aware Score Calibration," ICASSP 2021; "Tackling the Score Shift in Cross-Lingual Speaker Verification," ICASSP 2022 — modern QMF LR calibration + condition score-shift.
- Lee et al., CORAL+ (ICASSP 2019, arXiv 1812.10260); Li et al., CORAL++ (ICASSP 2022) — unsupervised PLDA domain adaptation.
- Choi et al., "Trainable Adaptive Score Normalization (TAS-norm)," ICASSP 2025 (arXiv 2504.04512) — learned margin against sibling selection = our contamination fix, learned.
- Ferras & Bourlard, "Speaker Diarization and Linking of Meeting Data," IEEE/ACM TASLP 2016; Ghaemmaghami et al., Computer Speech & Language 2016 — the closest academic precedents to cross-meeting linking (PLDA-linking; no maintained code).
- Amigó et al., 2009 — B-cubed clustering evaluation. Ghaemmaghami et al., Interspeech 2011 — complete-linkage in cross-recording attribution.
