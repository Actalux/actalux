# Prior art: cross-recording speaker attribution (named-person ID) for civic meeting archives

Research survey, 2026-07-09 (web-verified same day; agent-produced, links checked at
source). Companion to `docs/architecture/stylometry-evidence-family.md` and the
voiceprint recalibration plan.

## TL;DR

The existing architecture (embedder + per-cluster pooling + text anchors + per-place
calibration) IS the state of the art for this problem — nothing found replaces it
wholesale. The two genuinely adoptable things: (1) pyannoteAI's Precision-2 voiceprint
identification API — the only commercial product found with native enrollment +
cross-recording ID mapping onto officials-only voiceprints (big clouds are retreating:
Azure Speaker Recognition retired 9/2025, AWS Voice ID retiring 5/2026); (2) a second
permissive embedder (TitaNet-L or ERes2NetV2) as an agreement gate — cheap, directly
serves never-wrong-name. Speakerbox (Council Data Project) is the closest civic prior
art but is a CLOSED-SET per-council classifier, dormant since 2023 — adapt ideas, don't
adopt. Text attribution at 20-30 candidates has no evidence of naming-grade precision on
conversational/ASR text — matches our own Burrows-Delta S0 NO-GO; keep text as a
contradiction flag.

## Tier 1 — directly usable

| Resource | Type | Notes | License |
|---|---|---|---|
| pyannoteAI Precision-2 identification | commercial API | diarize + match vs pre-enrolled voiceprints (≤30s clean audio each), per-candidate confidence; free trial 150h + 10 voiceprints | commercial |
| nvidia/speakerverification_en_titanet_large | model | 192-d embeddings; the model AssemblyAI's own FAQ recommends for DIY cross-file ID | CC-BY-4.0 |
| speechbrain/spkrec-ecapa-voxceleb | model | ECAPA-TDNN + verify_files API; 44.7M downloads | Apache-2.0 |
| 3D-Speaker (ERes2NetV2, CAM++) | toolkit | ERes2NetV2 = 0.61% EER VoxCeleb1-O @17.8M params, tuned for short utterances | Apache-2.0 |
| WeSpeaker (current) | toolkit | active — w2v-bert2 checkpoint added 2025-12, worth a bump test (SSL-pretrained transfers better to far-field audio) | Apache-2.0 |

## Tier 2 — adapt / fine-tune-from

- **Speakerbox** (CouncilDataProject, MIT, JOSS 2023): the exact civic use case —
  few-shot fine-tuned audio transformer per council, ~0.937 accuracy from ~60 min
  annotated audio. Gaps: closed-set (no principled abstain → conflicts with
  precision-first + no-citizen-enrollment), last release 3/2023, per-body retraining.
  Its annotation protocol is reusable as-is.
- **Picovoice Eagle**: true enrollment→recognition SDK, on-device (audio never leaves
  our storage); proprietary engine + license-server phone-home; licensing review needed.
- **LUAR** (Apache-2.0, active): modern authorship embeddings, far stronger than
  Burrows-Delta. Trained on Reddit; uneven transfer to ASR speech text per its own
  paper. ~1 day to score rank-1 on our existing 23-person stylometry benchmark.
- **Active-speaker detection** (TalkNet-ASD, Light-ASD / LR-ASD [IJCV 2025, active]):
  for fixed-camera chambers, a visual who-is-talking signal orthogonal to voice/text —
  could break doc-2088-class cluster collapses. Licenses NOT verified; face-model
  downstream needed.

## Tier 3 — reference

- **Digital Democracy** (Cal Poly IATPP / CalMatters, relaunched 3/2024): production
  speaker ID for the CA legislature via text + voice + FACIAL recognition fusion.
  Closed tooling — but the strongest existence proof that face+voice fusion is what a
  funded team chose for exactly this problem shape.
- **LocalView** (Sci Data 2023) + Holman et al. 2025 (~100k school-board videos): big
  civic corpora with transcripts, NO named-speaker attribution — nobody has published a
  solved version of this task at scale; possible external eval corpora.
- **SpeakerLM** (2025) / **SE-DiCoW** (1/2026): research frontier — enrollment-conditioned
  end-to-end diarization+recognition. Watch, don't build on.
- **Embedding alignment** (Amazon 2024, arXiv 2401.12440): keeping old enrollments valid
  across embedder upgrades — relevant the day we bump wespeaker without re-enrolling.
- **InsightFace**: code MIT but the pretrained buffalo packs are
  non-commercial-research-only — **Actalux is an LLC: buffalo_l is NOT usable as-is**
  (issue #2587); commercial-safe face path = AWS Rekognition face collections (closed
  allowlist; not verified in detail) or a licensed model.
- **Commercial landscape**: Azure Speaker Recognition retired 9/30/2025; AWS Voice ID
  end-of-support 5/20/2026 (AWS points at Pindrop); AssemblyAI documents DIY
  TitaNet+vector-DB (i.e., recommends the architecture we already built);
  Deepgram/Speechmatics/Rev/Google = per-file diarization only. Phonexia = serious
  on-prem forensic vendor (poor fit); Pindrop = call-center fraud (poor fit). The
  big-cloud retreat from voice biometrics validates the officials-only enrollment
  posture as the defensible one.

## Gap analysis

What doesn't exist anywhere: an open-source OPEN-SET enrollment/identification harness
with calibrated abstention (gallery management, per-jurisdiction thresholds,
precision-at-abstention curves). Every open toolkit stops at embeddings+cosine; every
civic project went closed-set (Speakerbox) or closed-source (Digital Democracy). The
nested-LOMO calibration layer here is the novel part — no drop-in substitute. Also
missing publicly: any labeled benchmark for named-speaker attribution on US local-gov
meetings.

## Top 3 by leverage (survey's recommendation)

1. **Trial pyannoteAI Precision-2 on one jurisdiction** (free tier: 150h + 10
   voiceprints): enroll the schools gallery, run the already-scored meetings, compare
   confidence-thresholded precision/recall to ours. External benchmark either way.
2. **Second embedder as an agreement gate** (TitaNet-L or ERes2NetV2): only auto-name
   when BOTH embedders' galleries agree above threshold. ~a day inside the existing
   Modal + `--embedders` A/B harness; directly targets never-wrong-name.
3. **Spike LR-ASD active-speaker detection + face tracks on collapsed-cluster meetings**
   — orthogonal signal for the doc-2088 class; resolve face-model licensing first
   (InsightFace pretrained = non-commercial; LLC blocker).

Full source list: see the links inline above (all fetched 2026-07-09).
