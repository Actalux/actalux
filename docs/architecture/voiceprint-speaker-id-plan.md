# Build plan — Cross-meeting voiceprints for speaker identification (Phase 2)

Status: **EXECUTING — Phase 0 done, Phase 1 schema written (unapplied).** codex-reviewed
2026-07-01, findings folded in. Extends the shipped speaker-attribution system
(`docs/architecture/speaker-attribution.md`, §"Cross-meeting voiceprints"). **Cardinal
outcome of review: a voiceprint-only match NEVER auto-publishes a name — it maxes at
`inferred_medium` and routes to human review. Publication requires a human confirmation or
an independent same-meeting name anchor.**

## Progress

- **Phase 0 (embedding spike) — DONE 2026-07-01.** `src/actalux/diarization/modal_embedding_spike.py`
  (throwaway harness) ran on Modal **L4** over a real Clayton meeting. Frozen values:
  - `VECTOR(256)`; embedding model `pyannote/wespeaker-voxceleb-resnet34-LM`
    (the embedding half of `speaker-diarization-3.1`), **pyannote.audio 4.0.5**, torch 2.12.1+cu130.
  - The unpinned `>=3.1` resolved to **4.x**, which returns a `DiarizeOutput` and **ignores
    `return_embeddings`** — so we extract embeddings ourselves via `PretrainedSpeakerEmbedding`
    over each cluster's own speech (version-stable across 3.x/4.x, and gives us aggregation control).
  - Embeddings are **NOT L2-normalized** by the model (norms ~3–4) → normalize before storing so
    cosine == dot product. **Repeatability = 1.0** across all 38 clusters (deterministic).
  - Production consequence: the Phase 1 embedding-producing GPU function pins pyannote 4.0.5 +
    the wespeaker model and runs on **L4** (operator directive).
- **Phase 1 schema — APPLIED to prod 2026-07-01.** `scripts/migrate_040_voiceprints.sql`
  (`VECTOR(256)`, person_id-keyed gallery + officials-only trigger + evidence audit + RLS +
  basis-CHECK extension). Both tables verified empty + queryable via the service client.
- **Phase 1 code — DONE, tested, UNPUSHED / UNDEPLOYED (1200 tests pass).**
  - `backend.py`: `ClusterEmbedding` + `SpeakerTimeline.embeddings` + tolerant `from_remote`
    (accepts the old bare-list wire shape AND the new `{segments,embeddings}` dict, so client
    code never breaks against a not-yet-redeployed function).
  - `modal_runner.py`: `diarize_remote(..., return_embeddings=False)` extracts per-cluster
    L2-normalized embeddings ON DEMAND only; pinned pyannote 4.0.5 / torch 2.12.1; **L4**.
    `ModalRunner.run/spawn` + the `DiarizationBackend` Protocol thread `return_embeddings`.
- **STORAGE DECISION — Option B (operator, 2026-07-01): officials-only, re-extract.** A raw
  voiceprint is NEVER persisted for an un-confirmed speaker. Enrollment re-runs the GPU
  embedder for one meeting and stores only the confirmed official's vector in the gallery;
  matching runs in-memory and persists only the RESULT (`person_id` + score), never a
  citizen's vector. **Guarantee: a private citizen's voiceprint is never written to disk.**
  Consequence: the transcribe-time persistence plumbing (a `SpeakerLayer`/sidecar embedding
  carry) was trimmed as dead code; extraction is on-demand via the flag above.
- **BOOTSTRAP DECISION — name-anchored first (operator, 2026-07-01).** 0 human-`confirmed`
  identities exist and no confirmation tool does, so the initial gallery seeds from the
  name-anchored `inferred_high` rows (rollcall/self_intro/vote_anchor) — deterministic
  name→voice facts, source-linked + invalidatable (the plan's allowed source), which also
  serve as the leave-one-meeting-out calibration ground truth. Appointed-official
  confirmation tooling is the next step (Phase 2b-later).
- **Phase 2 code — DONE, tested, UNPUSHED / UNDEPLOYED (1208 tests pass).**
  - `modal_runner.py`: refactored shared helpers (`_decode_16k_mono`, `_load_embedder`,
    `_embed_spans`); new `embed_clusters_remote` + `ModalRunner.embed_clusters` embed a
    cluster's STORED `diarization_turns` spans (robust to re-diarization renumbering — the
    reason enrollment can't just re-diarize), one GPU load per meeting.
  - `scripts/enroll_voiceprints.py` (dry-run default, `--apply`): selects enrollable
    official clusters (confirmed OR name-anchored high; never `basis='voiceprint'`), embeds
    each on demand, upserts to the gallery; skips superseded docs, docs without `video_id`,
    and clusters below `--min-seconds` (default 10); prunes superseded samples.
  - **Dry-run verified on prod data: would enroll 81 samples / 16 officials / 74 meetings**
    (Waldman 23, Lichtenfeld 12, McAndrew 11, Hummell 9, Buse 7 …); 4 sub-10s single-word
    roll-call clusters correctly skipped; 0 to prune.
  - Next gated steps: (1) `modal deploy` the updated diarization app (adds
    `embed_clusters_remote`; also flips normal diarization to pinned 4.0.5 / L4); (2)
    `enroll_voiceprints.py --apply` to populate the gallery.

## 1. Why

Today's resolver (`src/actalux/identity/resolve.py`) is **per-meeting and name-anchored**:
it maps a diarization cluster → a roster subject only when a name is *spoken* — a roll-call
("Buse — aye"), a vote, or a self-introduction. That structurally **cannot name appointed
officials** (City Manager, CFO, directors, counsel) who speak often but are never in a roll
call. A **voiceprint** is the compounding cross-meeting signal we lack: once a cluster is
confirmed to an official, store its voice embedding; in later meetings, match anonymous
clusters against the gallery of known official voiceprints → a *candidate* identity before
any name is spoken. Better with every confirmed meeting.

## 2. Guardrails (non-negotiable — from the spec + review)

- **Voiceprint-only never auto-publishes.** `basis='voiceprint'` writes at most
  `inferred_medium` (below the `inferred_high`/`confirmed` public-display RLS gate in
  `migrate_034`). A name goes public only via human confirmation or an independent
  same-meeting name anchor. Rationale: `inferred_high` is anon-readable
  (`migrate_034_speaker_attribution.sql:113`) — a thresholded biometric match would become
  a *public fact*, misattributing quotes/clips into transcript UI, speaker-filtered search,
  person pages, and Ledger consumers. A wrong public name is the cardinal failure.
- **Corroborating signal, not a verdict.** Voiceprint raises confidence and routes to
  review; a name-spoken anchor stays the gold standard for `basis` and `confirmed`.
- **Officials only — never private citizens** — and **enforced in the DB** (trigger), not
  only in code. The gallery is built ONLY from publishable official subjects. A recurring
  public commenter must never be auto-fingerprinted (surveillance-shaped; violates the
  no-PII / "private individuals get no dossier" ethos).
- **Never invent a name.** Below threshold or weak margin → no proposal / review queue,
  never a guess.

## 3. Schema — `migrate_040_voiceprints.sql` (new; written AFTER the §4 spike)

Keyed on **`person_id`** (Model B: global `persons` + per-body `subjects` via
`subjects.person_id`/`entity_id`, `migrate_036`). One human has one voice across bodies;
storing by `subject_id` would duplicate or miss the same person. Matches map back to the
body-appropriate current subject at resolve time.

```sql
-- Gallery: per-sample voice embeddings for confirmed official speakers.
CREATE TABLE subject_voiceprints (
    id                     SERIAL PRIMARY KEY,
    person_id              INT NOT NULL REFERENCES persons(id) ON DELETE CASCADE,
    source_subject_id      INT REFERENCES subjects(id) ON DELETE SET NULL,   -- provenance
    source_document_id     INT NOT NULL REFERENCES documents(id) ON DELETE CASCADE,
    source_identity_id     INT REFERENCES speaker_identities(id) ON DELETE CASCADE, -- the confirmed row it came from
    cluster_label          TEXT NOT NULL,
    embedding              VECTOR(256) NOT NULL,  -- DIM frozen by the §4 spike
    source_basis           TEXT NOT NULL,          -- rollcall|vote_anchor|self_intro|manual
    model                  TEXT NOT NULL,          -- embedding model id + version
    seconds                REAL,                   -- speech behind this sample (quality)
    created_at             TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (person_id, source_document_id, cluster_label)
);
CREATE INDEX subject_voiceprints_embedding_hnsw
    ON subject_voiceprints USING hnsw (embedding vector_cosine_ops);
CREATE INDEX idx_subject_voiceprints_person ON subject_voiceprints (person_id);

-- Officials-only enrollment, ENFORCED (not just app code): a BEFORE INSERT/UPDATE trigger
-- rejects a row whose person lacks a publishable official subject. (App-side filtering
-- alone is what the review flagged — resolve.py:311 only filters memberships+publishable.)
CREATE TRIGGER trg_voiceprint_officials_only BEFORE INSERT OR UPDATE ON subject_voiceprints
    FOR EACH ROW EXECUTE FUNCTION enforce_voiceprint_official();

-- Audit every voiceprint DECISION (the current tables can't — speaker_identities stores
-- only subject/confidence/basis, migrate_034:77). Internal, service-only.
CREATE TABLE voiceprint_match_evidence (
    id                  SERIAL PRIMARY KEY,
    document_id         INT NOT NULL REFERENCES documents(id) ON DELETE CASCADE,
    cluster_label       TEXT NOT NULL,
    proposed_person_id  INT REFERENCES persons(id) ON DELETE SET NULL,
    score               REAL NOT NULL,   -- aggregated cosine to the winner
    margin              REAL NOT NULL,    -- winner − runner-up
    model               TEXT NOT NULL,
    threshold_version   TEXT NOT NULL,    -- which calibrated operating point produced this
    aggregation         TEXT NOT NULL,    -- mean|best-k|...
    target_seconds      REAL,
    alternatives        JSONB,            -- top-N {person_id, score}
    created_at          TIMESTAMPTZ DEFAULT NOW()
);

ALTER TABLE subject_voiceprints ENABLE ROW LEVEL SECURITY;      -- service-only, no anon
ALTER TABLE voiceprint_match_evidence ENABLE ROW LEVEL SECURITY;-- service-only, no anon
REVOKE ALL ON subject_voiceprints, voiceprint_match_evidence FROM anon;

-- Extend the basis CHECK to admit the new signal. The migrate_034 constraint is
-- inline + unnamed, so Postgres named it speaker_identities_basis_check.
ALTER TABLE speaker_identities DROP CONSTRAINT IF EXISTS speaker_identities_basis_check;
ALTER TABLE speaker_identities ADD CONSTRAINT speaker_identities_basis_check
    CHECK (basis IN ('rollcall','vote_anchor','self_intro','manual','voiceprint'));
```

- **Derived + rebuildable.** `subject_voiceprints` is never the record of who a speaker is
  (that stays `speaker_identities`); it can be dropped and re-enrolled.
- **Current-version cleanup (review finding).** `migrate_035` protects `confirmed` from
  UPDATE-downgrade but NOT from re-transcribe DELETE — `persist_speaker_layer`
  (`pipeline.py:198`) clears a document's `speaker_identities` wholesale on re-transcode.
  So voiceprints MUST be pruned/re-enrolled for superseded documents: a prune step drops
  samples whose `source_document_id` is superseded (`replaces_id IS NOT NULL`) or whose
  `source_identity_id` no longer exists — mirroring the edge/mention prune. The
  `source_identity_id` FK + `ON DELETE CASCADE` handles the wholesale-clear case
  automatically; the prune handles supersession.

## 4. Phase 0 — embedding-extraction SPIKE (BEFORE the migration)

The review is right that this is not a return-shape tweak: `DiarizationBackend` returns only
`SpeakerTimeline` turns (`backend.py:24`), Modal returns only `{speaker,start,end}`
(`modal_runner.py:54`), sidecars serialize no embeddings (`pipeline.py:64`), and
`pyannote.audio>=3.1` is unpinned (`modal_runner.py:41`). A `VECTOR(<DIM>)` migration
cannot be written until the dimension is known. Spike deliverables:

- Prove the exact embedding source in the Modal image: reuse pyannote 3.1's internal
  `wespeaker-voxceleb-resnet34-LM` per-cluster embedding if cleanly exposable, else a
  pinned dedicated model (ECAPA-TDNN). **Pin the version.**
- Confirm: dimension, extraction API, L2-normalization, and **repeatability** (same audio
  → same vector within tolerance across runs/image rebuilds).
- Decide per-cluster aggregation (pyannote's own vs mean of per-turn embeddings) and record
  `seconds`.
- Output: the frozen model id + `<DIM>` for the migration, behind the abstract
  `DiarizationBackend` seam so tests still use fakes (no GPU).

## 5. Enrollment (build the gallery)

Enroll a cluster as a sample for person P when ALL hold:
- the cluster's `speaker_identities` row is **`confirmed` (human)** — the initial gallery
  is human-confirmed ONLY (review finding: enrolling from auto `inferred_high` risks a
  poison→propagate loop). Auto name-anchored `inferred_high` may be added later, only once
  source-linked + easy to invalidate.
- P has a publishable official subject (**DB-enforced**, §3 trigger);
- cluster speech ≥ `MIN_ENROLL_SECONDS`.
- Distinguish "human listened/corroborated" from "operator accepted a score": a
  confirmation of a *voiceprint proposal* that wasn't independently corroborated must NOT
  become a new enrolled sample (else the model self-trains). Track this on the
  `speaker_identities`/evidence provenance.

Idempotent (`UNIQUE (person_id, source_document_id, cluster_label)`); `source_identity_id`
links each sample to the decision it came from. `scripts/enroll_voiceprints.py` (dry-run
default, `--apply`).

## 6. Matching (the new signal — writes review proposals only)

Per **anonymous** cluster (no name-anchored identity) in a meeting:
1. Embedding (seconds ≥ `MIN_MATCH_SECONDS`, else skip).
2. Cosine-match against the gallery **restricted to officials eligible for this body/date**
   (join to active memberships), aggregate per person (mean / best-K) → ranked candidates.
3. Accept top person only if `sim ≥ TOP_THRESHOLD` AND `sim − sim_2nd ≥ MARGIN`; else no
   proposal (borderline band → review queue).
4. Write `speaker_identities` at **`inferred_medium` max** with `basis='voiceprint'`, and a
   `voiceprint_match_evidence` row (score, margin, model, threshold_version, aggregation,
   target_seconds, alternatives). Route to the review queue for human confirmation.

Composes with the name-anchored resolver: name-anchored always wins; voiceprint fills only
clusters left anonymous; never overwrite a name-anchored or `confirmed` identity. **No
`voiceprint → inferred_high` path ships in v1.**

## 7. Calibration (leakage-safe — do NOT ship an unvalidated threshold)

Held-out validation with strict leakage control (review finding):
- **Leave-one-meeting-out** (or temporal train/test): to score a meeting's known clusters,
  exclude from the gallery all samples from that document, its **version-chain siblings**
  (same video / superseded copies), and ideally the entire same meeting.
- **Macro precision by official** (not micro) so a few talkative officials or same-channel
  repeats don't inflate the number; stratify by **channel/body** (Zoom vs in-room).
- **Negative cases:** include non-official speakers / public commenters — the matcher must
  reject them (no false enroll, no false name).
- Sweep `TOP_THRESHOLD`/`MARGIN`; pick the operating point at the operator's precision bar
  (precision >> recall). Emit the confusion pairs (which officials collide).
- `scripts/voiceprint_calibrate.py` — dry-run report, writes nothing; stamps a
  `threshold_version` used by matching.

## 8. Phasing (reordered per review)

0. **Embedding spike** (§4) — freeze model + `<DIM>`. No DB yet.
1. **Schema** (§3) — `migrate_040`: `subject_voiceprints` (person_id + provenance),
   `voiceprint_match_evidence`, officials-only trigger, basis CHECK, RLS. Extraction seam
   emits embeddings; pure assembly in `pipeline.py` (fakes in tests).
2. **Enrollment dry-run → apply** (§5) — human-confirmed gallery + prune of superseded.
3. **Leakage-safe calibration** (§7) — operator sets the precision bar; thresholds frozen.
4. **Matching pass** (§6) — writes `inferred_medium` + evidence + review queue ONLY.
   Wire into transcribe/backfill after the name-anchored pass.
5. **Review + (later) publication** — operator confirms proposals → `confirmed` (publishes,
   and enrolls a corroborated sample). Only after the gallery matures + calibration holds
   is any auto-publication path even reconsidered — a separate, gated decision, not v1.
- API/UI need no change — labels flow through `speaker_identities` → `diarization_turns`;
  a *confirmed* identity simply appears.

## 9. Open decisions (for operator)

1. **Embedding model + dimension** (§4 spike output) — freezes the migration. Recommend
   wespeaker-256 if cleanly exposable from pyannote 3.1; else pinned ECAPA-192.
2. **Precision bar** for calibration (§7) — e.g. macro-precision ≥ 0.98 on held-out. Sets
   the operating point.
3. **`MIN_ENROLL_SECONDS` / `MIN_MATCH_SECONDS`** — minimum reliable speech.
4. **Gallery representation** — per-sample + query-time aggregation (recommended, keep) vs
   materialized centroid; per-person sample cap / pruning policy.
5. **Whether an auto-publish path is EVER enabled** (post-v1) — default per review: **no**;
   human confirmation is the publication path. Revisit only with mature gallery + validated
   precision, as a separate gated decision.

(Resolved by review, no longer open: voiceprint-only is non-public in v1; schema keys on
person_id; officials-only + cleanup are DB-enforced; calibration is leakage-safe.)

## 10. Risks

- **Misattribution → wrong public name** (cardinal). Mitigated: voiceprint-only never
  publishes (v1); precision-first threshold + margin; officials-only (DB-enforced);
  enroll-from-confirmed-only (no drift loop); never override name-anchored; review gate.
- **Gallery poisoning / feedback loop** — human-confirmed-only initial gallery,
  source-linked + invalidatable, corroborated-vs-accepted distinction (§5).
- **Superseded samples lingering** — prune on supersession + `source_identity_id` cascade
  (§3).
- **Voice/channel variation** (Zoom vs in-room) — surfaced by stratified calibration;
  margin test guards ambiguous cases.
- **Cost** — extraction rides the existing diarization GPU pass; matching is a pgvector
  cosine query.

## 11. Test surface

- Pure matcher: cosine + `TOP_THRESHOLD` + `MARGIN` + officials-only + never-override +
  caps-at-inferred_medium (fakes, no GPU) — mirrors `tests/test_identity_resolve.py`.
- Enrollment gate: only human-confirmed officials with ≥ min seconds enroll; idempotency;
  officials-only trigger rejects a non-official.
- Supersession prune: samples on superseded docs removed; cascade on identity delete.
- Resolver composition: voiceprint fills only anonymous clusters; name-anchored wins.
- Calibration harness: leave-one-meeting-out excludes version-chain siblings; macro-precision
  math; non-official negatives rejected.
