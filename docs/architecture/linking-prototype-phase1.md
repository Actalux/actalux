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
