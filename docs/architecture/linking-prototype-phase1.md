# Phase 1 — cross-meeting linking prototype (plan + contract)

Implements phase 1 of `voice-first-identity-resolution.md`: prototype the cross-meeting
linker (Stage C) **standalone**, validate on ground-truth meetings, and produce a go/no-go
on cross-condition bridging. **No production change; no writes to production tables.**
Jurisdiction-general (per the locked decision): no place/body constants in the library;
place is a CLI argument.

## Data flow

```
corpus meetings (per place/body)
   → [E] Modal embed each diarization cluster's turns → pooled cluster centroid   (heavy, cached)
   → voice_observation cache artifact (per-cluster centroid + metadata)            (scratch, reusable)
   → [S] calibrated score matrix (cosine baseline | AS-norm | sph-PLDA)
   → [C] constrained complete-linkage (cannot-link=same-meeting, must-link=anchors)
   → voice_node assignment
   → [V] evaluate vs benchmark (purity / coverage / pairwise P·R·F1)
```

`[E]` is the only heavy/GPU/cost step and is **cached** so `[S]`/`[C]`/`[V]` iterate freely
— this is the piece the current recalibration lacks (it re-embeds every run). `[S]`,`[C]`,`[V]`
are pure (numpy only), no DB/Modal, fully unit-tested.

## Modules (`src/actalux/diarization/linking/`)

- **`observations.py`** — `VoiceObservation` (one cluster: `document_id, cluster_label,
  embedding (np.ndarray), speech_seconds, acoustic_condition, meeting_date`) and
  `VoiceNode` (a set of observations judged one voice). Cache load/save (npz + jsonl
  sidecar). The Modal producer is a thin caller of the existing `modal_runner` embed path
  (wired in the build step, not the library core).
- **`scoring.py`** — `cosine_matrix` (baseline) and `asnorm_matrix` (the drift fix): AS-norm
  normalizes each pair score by the two clusters' score distributions against an impostor
  cohort. Interface leaves room for a `plda_matrix` backend later.
- **`cluster.py`** — `constrained_complete_linkage`: agglomerative, complete-linkage
  (farthest-neighbor), honoring `cannot_link` (never merge) and `must_link` (pre-merged)
  constraints, cutting at a threshold. Returns cluster → node assignment.
- **`evaluate.py`** — `purity`, `coverage`, and `pairwise_prf` (precision/recall/F1 over
  same-node pairs) against a ground-truth labeling; plus a per-condition-pair breakdown.
- **`ledger.py`** — the transparent weighted evidence ledger (locked decision): an
  `EvidenceLedger` per voice-node accumulating `(channel, weight, source)` observations,
  combined **family-aware** (diminishing returns within a channel-family, per
  `families.py`) into a per-candidate score. This is the resolution-side scaffold; phase 1
  uses it only to score how well linked nodes resolve to the roster, not to write anything.

## Constraints derived from our data (jurisdiction-general)

- **cannot-link**: two distinct clusters in the SAME document are different people (hard).
- **must-link**: clusters carrying a very-high-confidence anchor (self-intro or Zoom
  active-tile label) for the same roster slug are the same person (hard seed).
- These come from `speaker_identities` (confirmed / self_intro / screen_name tile rows) and
  from document co-membership — loaded per place, not hardcoded.

## Benchmark (ground truth)

No public municipal linking benchmark exists, so build one from what we already trust:
`scripts/linking/build_linking_benchmark.py` extracts, per place/body, the set of
(document_id, cluster_label) → roster-slug assignments that carry a **very-high-confidence**
anchor (human-`confirmed`, `self_intro`, or `screen_name` tile-mode). Two clusters with the
same slug across meetings are a ground-truth SAME_VOICE link; same slug is impossible within
one meeting (that would be the cannot-link violated). This yields a labeled linking set
skewed toward the recurring officials — exactly the population the linker must get right.
Report coverage of the benchmark honestly (it is not the whole corpus).

## Metrics + go/no-go

- **Cluster purity** (do nodes mix people?) and **coverage** (are one person's clusters
  gathered into one node?), plus **pairwise F1** over same-node pairs.
- **Per-condition-pair breakdown** is the decisive view: does linking hold ACROSS
  conditions (Zoom↔in-person), not just within? This is where cosine is expected to fail
  and AS-norm is expected to help.
- **Go/no-go for phase 2**: AS-norm (or PLDA) must materially beat the cosine baseline on
  *cross-condition* pairwise F1 at a fixed purity floor, on the benchmark. If it does not,
  revise the linking approach here before building the evidence model downstream.

## Out of scope for phase 1

Production tables, the resolver's write path, retiring the name-first flow — all later
phases. Phase 1 is measurement only: does drift-robust linking work on our audio?

## Build decisions (2026-07-11) — what shipped vs. the plan above

The scaffold above described the intent; three things resolved differently once the data was
in hand, and the code reflects these (docstrings cite them):

- **`acoustic_condition` is derived, not a DB field.** No `documents` column records
  Zoom-vs-in-person, and the Z1 probe's per-frame `mode` (`tile`/`fullframe`/`none`) proved too
  noisy for a clean per-doc label (`tile` detections scatter across 2017–2025; `fullframe` fires
  on ~all docs). So the cache uses a **precise-positive proxy**: a meeting is `"zoom"` iff it
  produced a `screen_name` identity (a Zoom gallery tile was OCR'd), else `"in_person"` (the
  uncertain bucket). High precision on the positive, low recall — treated accordingly below.
- **Primary metric is across-*meeting* F1, not across-*condition*.** Because the condition label
  is only precise-positive, the headline go/no-go is the fundamental linking axis — pairwise F1
  over cross-*meeting* pairs — obtained by passing `str(document_id)` as the categorical to the
  library's general `per_condition_pair_f1`. Across-*condition* F1 (the zoom proxy) is reported
  as a secondary, explicitly-noisier view. Within-meeting F1 is structurally ~0 (cannot-link
  forbids same-meeting merges) and omitted from the headline.
- **No separate `build_linking_benchmark.py`; `must_link` is empty for the clean run.** Benchmark
  construction (person-id labels from DB anchors via `select_enrollable`; `cannot_link` from
  same-meeting co-occurrence) is a set of pure functions in `linking/benchmark.py` plus the one DB
  read in the runner — no serialized intermediate to drift. The clean measurement seeds **no**
  `must_link`: seeding from the same anchors that form the ground truth would hand the linker its
  answer. The library still supports `must_link` for the production (coverage-seeking) path.

**Feasibility (DB inventory, 2026-07-11):** cross-condition officials (anchored in both a zoom-
and a non-zoom-proxy meeting) — **schools 12 / plan-commission 9**; council is in-person-only by
the proxy (a within-condition control at best); board-of-adjustment too thin. Schools + PC carry
the measurement.

**Pipeline shipped:** `scripts/linking/build_embedding_cache.py` (`[E]`, resumable per-doc `.npz`,
reuses the recalibration embed→pool path) and `scripts/linking/run_linking_prototype.py` (thin CLI
over `linking/benchmark.py`). Not yet run — the `[E]` pass is ~175 meetings of YouTube download +
Modal GPU (hours, bot-check-gated), pending an operator go and a pilot on the cross-condition
officials first.
