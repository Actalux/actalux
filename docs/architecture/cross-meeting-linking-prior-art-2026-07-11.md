# Cross-meeting speaker linking — prior-art triage (2026-07-11)

Companion to `voice-first-identity-resolution.md` §6. Scope: the SOTA and open source
for **cross-recording speaker linking** — deciding which anonymous per-recording
diarization clusters, across many separate meeting recordings, are the same physical
person (a global voice-identity graph). Distinct from single-recording diarization
(which pyannote already does well for us) and from representation learning.

## Headline findings

1. **This is a named research problem** — "speaker linking" / "longitudinal linking"
   (Cambridge/BBC) / "cross-show diarization" / "speaker retrieval in the wild." It has
   a well-defined SOTA recipe. We are not inventing an unstudied task.
2. **No maintained open-source turnkey implementation exists.** pyannote, WeSpeaker,
   3D-Speaker, and VBx all stop at single-file diarization + embeddings. The
   cross-recording agglomeration, drift-robust score calibration, and constraint
   handling are glue we assemble from permissively-licensed components.
3. **The fix for our measured 0.85–0.95 cross-condition false-matches is score
   calibration, not a new embedder.** Replace raw cosine at the linking stage with
   **AS-norm (adaptive symmetric score normalization)** and/or a **PLDA / sph-PLDA
   backend**. The score-normalization literature is explicit that normalization is
   unnecessary under ideal conditions and *essential* under enrollment-vs-runtime
   condition mismatch — exactly the Zoom-vs-in-person case.
4. **Officials-only transient retention is easier with a build-it approach** than with
   any enroll-everyone product: cluster all voices transiently, keep only nodes that hit
   an official anchor, discard the rest before any voiceprint persists. Option B is
   structurally easy here.
5. **No public labeled benchmark for US municipal audio** — we will need a small
   hand-labeled linking set (measure cluster purity / coverage) to tune thresholds.

## The SOTA recipe for our exact problem

Given **condition drift + anchor seeds + within-meeting cannot-link**, the literature
converges on a two-stage, backend-calibrated, constrained-agglomeration pipeline:

1. **Per-recording diarization → pooled cluster embeddings** (have this). Pool multiple
   segments per cluster (multi-enrollment centroid), not one segment — sph-PLDA's gains
   come specifically from multi-enrollment, which is the recurring-officials case.
2. **Score the cross-recording cluster×cluster matrix with a calibrated backend, not raw
   cosine.** Two interchangeable, cheap options:
   - **AS-norm** against an impostor cohort drawn from our own corpus (VBx / 3D-Speaker
     recipes; theory: Swart & Brümmer 2017). A far-field impostor that spuriously hits
     0.9 gets pulled down because it also scores high against the cohort.
   - **PLDA / sph-PLDA** replacing cosine (online-speaker-clustering, MIT) — calibrated
     and multi-enrollment-aware.
3. **Constrained complete-linkage agglomeration** over the normalized matrix:
   - **cannot-link** = two distinct clusters in the same meeting (hard, blocks merge);
   - **must-link** = self-intro / Zoom-label anchors (hard seeds);
   - **complete-linkage** (farthest-neighbor) not average/single — Ghaemmaghami 2015
     shows it is the robust choice for linking; add cluster-voting over multiple
     segmentations for stability.
4. **Retain only official-anchored nodes**; discard the rest before any voiceprint persists.

### Prototype order (phase-1, standalone, measured)

1. **AS-norm at the linking stage** (cohort from our corpus). Highest leverage, cheapest,
   directly targets the measured cross-condition false-highs. Lift from VBx's recipe.
2. **sph-PLDA/PSDA backend** from online-speaker-clustering (MIT) as the cosine
   replacement, exploiting multi-enrollment for recurring officials.
3. **Constrained complete-linkage** seeded by anchors + cannot-link — constraint
   formulation from Cheng 2023; linkage/voting choice from Ghaemmaghami 2015.
4. **Second-embedder agreement gate** (orthogonal): merge only when a second embedder's
   normalized score agrees (ReDimNet-MIT or ERes2NetV2 from 3D-Speaker-Apache vs wespeaker).

## Reusable components (license is decisive — we are a for-profit LLC)

| Resource | Role | License | Link |
|---|---|---|---|
| **online-speaker-clustering** (Sholokhov, ICASSP'23) | sph-PLDA/PSDA backend, multi-enrollment scoring — most directly relevant | **MIT** | github.com/sholokhovalexey/online-speaker-clustering |
| **VBx** (BUT Speech@FIT) | AS-norm + calibrated AHC reference recipes | license UNVERIFIED — confirm before shipping | github.com/BUTSpeechFIT/VBx |
| **3D-Speaker** (Alibaba/CMU) | ERes2NetV2 (short-utterance-robust) 2nd embedder | **Apache-2.0** | github.com/modelscope/3D-Speaker |
| **ReDimNet** (IDRnD) | top-EER small embedder, 2nd-embedder gate | **MIT** (verify each checkpoint) | github.com/IDRnD/redimnet |
| **speechbrain/spkrec-ecapa-voxceleb** | ECAPA-TDNN 2nd-embedder candidate | **Apache-2.0** | hf.co/speechbrain/spkrec-ecapa-voxceleb |
| **nvidia titanet_large** | 192-d cross-domain embedder | **CC-BY-4.0** (content licence on code — sanity-check) | hf.co/nvidia/speakerverification_en_titanet_large |
| **wespeaker resnet34-LM** | our current 256-d embedder | **CC-BY-4.0** (sanity-check for load-bearing use) | hf.co/Wespeaker/wespeaker-voxceleb-resnet34-LM |
| **pyannoteAI Precision-2** | commercial enrollment→ID API — the *resolution* side, NOT unsupervised linking | commercial (Dev €19/mo; voiceprints €0.015 ea; 30-day trial 150h+10 voiceprints) | docs.pyannote.ai |
| **OpenTranscribe** | only OSS system w/ cross-video voiceprint persistence — but naive cosine, no drift calibration | **AGPL-3.0 — BLOCKER for a closed product; read for architecture only** | github.com/attevon-llc/OpenTranscribe |

## Adapt-from (Tier 2)

- **Joint Pairwise Constraint Propagation** — Cheng et al. 2023, arXiv 2309.10456
  (3D-Speaker team): must-link/cannot-link constraint propagation over speaker
  embeddings — the mechanism for our anchors + within-meeting cannot-link. Written for
  within-meeting; re-target to cross-recording centroids.
- **EEND-vector-clustering** — Kinoshita et al. 2021, arXiv 2105.09040
  (github.com/nttcslab-sp/EEND-vector-clustering): robust constrained clustering module.
- **Multimodal constrained-optimization diarization** — Cheng et al. 2024, arXiv
  2408.12102: audio+visual+semantic pairwise constraints (relevant if Zoom labels become
  must-link constraints rather than just evidence).

## Key reference papers (Tier 3 — define the recipe, no drop-in code)

- **Karanasou et al. 2015, ASRU** — "Speaker diarisation and longitudinal linking in
  multi-genre broadcast data" (doi:10.1109/asru.2015.7404859). *Closest match* —
  "longitudinal linking" = same speakers across many episodes of a series = our recurring
  officials across meetings.
- **Ghaemmaghami et al. 2015, ICASSP** — "A cluster-voting approach for speaker
  diarization and linking of broadcast news" (doi:10.1109/icassp.2015.7178888) —
  complete-linkage + cluster-voting; farthest-neighbor beats average-linkage for linking.
- **Loweimi, Qian, Knill, Gales 2025** — "Speaker Retrieval in the Wild" (arXiv
  2504.18950) — modern restatement: retrieve a speaker across a large archive under
  real-world distortions = our condition-drift concern. No code; read for framing.
- **Swart & Brümmer 2017** — "A Generative Model for Score Normalization" (arXiv
  1709.09868) — why AS-norm works precisely under enrollment/runtime dataset shift.
- **Viñals et al. 2019** — "Unsupervised adaptation of PLDA models for broadcast
  diarization" (doi:10.1186/s13636-019-0167-7) — adapt the backend to target audio
  without in-domain labels.
- **Kauffman et al. 2018** — "Multimodal Speaker Identification in Legislative Discourse"
  (Digital Democracy, CA legislature): voice+face+text fusion beats any single modality —
  existence proof for the multimodal direction. Closed tooling.

## Caveats

- The claim that AS-norm cleanly resolves *our specific* 0.85–0.95 far-field false-highs
  is a **hypothesis to test**, synthesized from the score-norm literature applied to our
  measurements — not a result anyone published on municipal audio. It is the first thing
  to prototype, not a proven outcome.
- VBx license not verified this pass — confirm before it becomes load-bearing.
- CC-BY-4.0 on wespeaker/titanet weights is commercial-usable with attribution, but CC-BY
  targets content not software — a quick legal sanity-check before it is load-bearing.
