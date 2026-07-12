# Voice-first speaker identity resolution — design spec (DRAFT for interview)

**Status:** proposal, not approved. Written 2026-07-11 after the cal23→cal25 cycle,
which exposed the structural weakness this design fixes. Do NOT implement before the
operator signs off — this is a spec-first interview document (open questions in §12).

**Supersedes the *ordering* of:** `docs/architecture/voiceprint-recall-orchestration.md`,
`speaker-attribution.md` (identity-resolution half), and the enroll→calibrate control
flow in `scripts/{enroll_voiceprints,recalibrate_voiceprints}.py`. It does **not**
change the acoustic front end (pyannote diarization, WhisperX, wespeaker embeddings)
or the content policy.

**Builds on:** `docs/Actalux_Entity_Resolution_Architecture.md` — this is that document's
philosophy (separate extraction from resolution; mentions vs canonical entities;
multi-signal weighted scoring; deterministic identifiers first; reversible merges)
applied to the acoustic layer, where we currently do not follow it.

---

## 1. The problem with what we have (name-first)

Today the pipeline is **name-first**: a text labeler reads a transcript, decides a
diarization cluster is a named person, and writes that decision as a `speaker_identities`
anchor. That anchor then *becomes* a voiceprint in the enrollment gallery; calibration
hangs off names that originated in text.

This welds two problems that should be separate:

- **(a) acoustic structure** — how many distinct voices a meeting has, which turns are
  the same person, and which voices recur meeting-to-meeting. No names required.
- **(b) identity** — which real person each voice is.

Because they are welded, a *naming* error becomes an *acoustic* error: a wrong label
poisons the voiceprint gallery. Every failure of the last week is this one bug —
Gruber↔Doherty confusion, the systematic "Jason→Growe" mislabels, the cal23 regression
where a full-corpus re-label minted 71 anchors and dragged the calibration from 6
enabled to 0. The entire hygiene apparatus we have built (alien-positive quarantine,
collapse-pair detection, confirmed-centroid vetting, the F1–F4 passes) exists to clean
up messes this ordering creates. We are paying, continuously, to sweep up after an
avoidable coupling.

## 2. The inversion (voice-first)

Establish the acoustic structure **first, with no names**, then attach identity as
accumulating, weighted, reversible evidence — names come **last**.

1. Diarize each meeting → anonymous per-meeting voice observations (have this).
2. Embed each observation (have this: wespeaker 256-d, Modal).
3. **Link observations across meetings into voice-nodes** — an unsupervised acoustic
   identity graph. The recurring high-degree nodes are, structurally, the persistent
   officials (clerk, superintendent, board members). *This is the missing layer.*
4. Attach identity **evidence** to voice-nodes from many channels (self-intro, being
   introduced/addressed by name, roll-call/vote alignment, agenda-topic/role fit,
   tenure, Zoom on-screen label). Each channel is an *observation*, never a direct edit.
5. **Resolve** each recurring voice-node to a roster member via a multi-signal weighted
   score, with confidence tiers and reversible proposed/confirmed/rejected states.
6. **Gate + persist** under the content policy: only recurring voice-nodes that resolve
   to a tracked official are enrolled/persisted; everything else stays transient.

Under this ordering a wrong name-cue is no longer poison — it is one weak vote on a
voice-node whose accumulated evidence can outweigh it. The acoustic layer is never
corrupted by a text mistake, because names never write into it.

## 3. Principles (carried in, non-negotiable)

- **Precision cardinal.** Never publish a wrong public name. The precision bar and
  zero-false-positive-on-citizens requirement survive unchanged (§9).
- **Option B.** A private citizen's voiceprint is NEVER persisted. The global voice
  map is transient; only confirmed-official nodes persist (§8).
- **Named-in-transcript ≠ tracked entity** (CLAUDE.md content policy). This design makes
  the distinction *structural*: a resolved tracked official → persistent voiceprint;
  a per-document name for a public participant → transcript attribution, no voiceprint.
- **Human promotes candidate→cleared.** No auto-promotion, no live matcher, no published
  name without an explicit go. candidate≠cleared stays a structural FK gate.
- **Jurisdiction-general.** No Clayton-specific logic. Rosters, aliases, tenure windows
  load per place; the linking + resolution algorithms are place-agnostic.
- **Reversible over destructive.** Every link and every resolution is a
  proposed/confirmed/rejected row, never an in-place merge (per the entity-res doc).
- **Separate extraction from resolution.** Labelers emit evidence observations; a
  distinct resolution step reads them. An LLM never writes the canonical graph directly.

## 4. Data model

Four new concepts (names provisional; reconcile with existing tables in §10):

- **`voice_observation`** — one diarization cluster in one meeting: `(document_id,
  cluster_label, embedding_ref, total_speech_seconds, acoustic_condition)`. This is the
  atom. Already exists as diarization clusters + pooled embeddings; formalize it.
- **`voice_node`** — an acoustic identity: a set of observations judged the same voice
  across meetings. Transient by default (`scope='run'`); promoted to persistent only
  when resolved to a tracked official. Carries `n_meetings`, `n_seconds`, `condition_span`.
- **`voice_link`** — a `SAME_VOICE` edge between two observations (or observation↔node)
  with a similarity score and `status ∈ {proposed, confirmed, rejected}`. Confirmed
  links are the must-link seeds; rejected are cannot-link.
- **`identity_evidence`** — one observation that a voice-node (or observation) is a
  particular roster candidate: `(voice_ref, candidate_subject_id, channel, weight,
  source_document_id, evidence_location, tenure_ok)`. This is the acoustic analogue of
  `entity_mention` in the entity-res doc. Never a direct name; always a scored vote.

Existing tables are reused: `diarization_turns` (input), `subjects`/`entities` (roster),
`subject_voiceprints` (the persistent official gallery — the *output* for confirmed
nodes only), `speaker_identities` (becomes a *derived projection* of resolution, or is
retired — §10).

## 5. Pipeline stages

### Stage A — Observations (reuse)
pyannote diarization + WhisperX turns (existing). Each cluster → one `voice_observation`
with a pooled wespeaker embedding and an **acoustic-condition tag** (Zoom-gallery /
Zoom-share / in-person room-cam / phone) inferred from the Z0-style frame probe and
audio features. The condition tag is load-bearing for Stage C.

### Stage B — Embedding (reuse)
wespeaker-voxceleb-resnet34-LM, 256-d, on Modal. Optionally a second embedder for an
agreement gate (the ECAPA A/B harness already supports this). No change.

### Stage C — Cross-meeting linking (NEW — the crux, §6)
Cluster observations across the whole corpus into `voice_node`s. This is the hard,
make-or-break layer and gets prototyped and validated **before** the rest is built.

### Stage D — Evidence extraction (reframe + extend)
Every existing text labeler is reframed from "write a name anchor" to "emit an
`identity_evidence` observation." Add the channels the operator named (§7). Each
observation is tenure-gated (a vote for someone off the board that date is dropped).

### Stage E — Resolution (NEW scoring, §7)
For each recurring voice-node, aggregate its evidence across all member meetings into a
weighted, family-aware score per roster candidate. Emit a resolution with a confidence
tier and a reversible status.

### Stage F — Gating + persistence (§8, §9)
Only voice-nodes that (i) recur above a threshold AND (ii) resolve to a *tracked
official* with sufficient multi-family evidence are enrolled into `subject_voiceprints`.
The precision cardinal is measured by a held-out calibration (§9) before any node is
promotable. Humans promote candidate→cleared.

## 6. Cross-meeting linking — the hard part, designed honestly

Pure agglomerative clustering on cosine would work if audio conditions were uniform.
They are not: the same person over 2021 Zoom and in a 2024 in-person room-cam may not
link, and our own collapse pairs show *different* people at 0.85–0.95 in bad audio. So
cosine-threshold-alone is insufficient, and this is where the design earns or loses its
keep. Proposed approach:

1. **Stratify by acoustic condition.** Link confidently *within* a condition first
   (Zoom-gallery clusters link cleanly; in-person room-cam is noisier). Within-condition
   similarity distributions are tighter and give a trustworthy threshold.
2. **Bridge conditions via anchored officials.** A board member attends across eras. An
   observation carrying a *very-high* evidence channel (self-intro, Zoom active-tile
   label) is a near-certain identity; those anchored observations span conditions and
   let us learn the cross-condition similarity offset — i.e., the officials themselves
   are the bridges, exactly the operator's "voices in common meeting to meeting."
3. **Seed with must-link / cannot-link.** Confirmed `voice_link`s (from strong evidence
   or human confirmation) are must-links; rejected are cannot-links; within one meeting,
   two distinct clusters are cannot-link (they are simultaneously present). Constrained
   clustering respects these.
4. **Leave uncertainty explicit.** A borderline join is `proposed`, not `confirmed`. An
   unresolved recurring voice is a first-class object ("Speaker A, unidentified, appears
   in 40 meetings") — useful for within-corpus quote consistency and citizen-safe (no
   name, and non-officials never persist, §8).
5. **Validate empirically before building downstream.** Measure link precision/recall on
   the meetings where we DO have ground truth (self-intro + Zoom-labeled clusters), by
   condition pair. If cross-condition bridging is unreliable, the design falls back to
   within-condition nodes + evidence resolution per stratum — still better than today.

**This stage is prototyped first as a standalone experiment** (§11 phase 1). The rest of
the spec assumes it clears a measured bar; if it does not, we revise here, not downstream.

## 7. Evidence taxonomy (channels, weights, failure modes)

Each channel emits `identity_evidence`. Weights are relative priors to be *calibrated*,
not hand-final. Independence follows the existing `families.py` insight: two observations
count as independent only if their *mechanism* differs (diminishing returns within a
channel/meeting).

| Channel | Strength | Mechanism / how extracted | Failure mode |
|---|---|---|---|
| Self-introduction ("my name is X") | very high | text; deterministic phrase near a turn | rare; misattributed turn boundary |
| Zoom active-tile label (Z1/Z2) | very high | platform-rendered name on speaker's own tile | account-feed streaming a room (guarded) |
| Roll-call / vote-sequence alignment | high | existing `vote_anchor` | clerk reads the roll (voice≠member) |
| Introduced by chair ("I'll turn it over to X") | high | text; directed handoff | handoff to someone who then defers |
| Named in reply / directed address ("thanks, X") | medium-high | text; 2nd-person naming adjacent to a turn | naming a *different* person present |
| Agenda-topic / role fit (CFO ↔ budget item) | medium (prior) | agenda × role × speech content | two officials on one topic |
| Tenure / roster active-on-date | gate + weak prior | roster window | — (used as a hard filter, not a vote) |
| Voice similarity to a *confirmed* official | linking signal | wespeaker cosine | unlabeled officials in pool (the D2 trap) |

Key rules:
- **Tenure is a gate, not a vote.** A candidate off the board on the meeting date is
  removed from the vote set entirely (this is the fix for the Wilson-tenure class of bug,
  now enforced structurally rather than by a labeler staying correct).
- **Voice similarity names nothing on its own.** It *links* observations into nodes;
  identity comes from the non-acoustic channels attached to the node. This avoids the
  D2 self-reinforcement where an unlabeled official in the negative pool looks like a
  match.
- **Multimodal is just more rows.** Zoom labels are one channel today; face-ID (AWS
  Rekognition is the commercial-safe path; InsightFace packs are non-commercial and
  blocked for the LLC) would be another, added without changing the model.

## 8. Option B and content-policy compliance (structural)

The global voice map touches every voice, including citizens — so persistence must be
gated, by construction:

- **The cross-meeting map is transient** (`scope='run'`): built per analysis run, used
  for linking and within-corpus quote consistency, not a persisted biometric store.
- **Recurrence is the officials-vs-everyone filter.** A citizen appears once; a board
  member appears in dozens of meetings. "Recurs above threshold" is the first gate; the
  evidence layer resolving the node to a *tracked official* is the second. A voice-node
  persists to `subject_voiceprints` **only if both hold**.
- **A recurring non-official** (e.g. a serial public commenter) recurs but does not
  resolve to a tracked official → it is **never persisted**; at most it is a per-document
  named public participant (existing name-the-public-record path: transcript attribution,
  no voiceprint, no tracking).
- **Protected classes win first** (schools students/personnel/teachers): never named
  even on a self-intro, and never persisted, regardless of recurrence or evidence.
- Net effect: Option B and the tracked-vs-named distinction stop depending on a code path
  staying correct and become properties of *what is allowed to persist*.

## 9. Precision & calibration (the cardinal survives)

The nested-LOMO harness's *purpose* survives: prove, on held-out meetings, that the
system recognizes officials without false-attributing citizens, without overfitting. Its
*object* changes — from "does the enrolled gallery match" to "does resolution correctly
map recurring voice-nodes to roster members, leave-one-meeting-out." Specifically:

- Hold out one meeting; resolve its voice-nodes using evidence from the *other* meetings
  only; measure whether known-official turns are correctly named and whether any citizen
  turn is falsely named. Precision at the bar, FP-on-citizens = 0, as today.
- The precision cardinal and candidate≠cleared FK gate are unchanged.
- Much of F1–F4 retires: alien-positive quarantine and collapse-pair cleanup exist to
  repair name-first poisoning; voice-first does not create it. What remains is the honest
  held-out precision measurement.

## 10. Keep / retire / reframe (explicit inventory)

- **Keep unchanged:** pyannote diarization; WhisperX; wespeaker + Modal; the Z0/Z1/Z2
  Zoom probe (becomes an evidence + condition-tag source); the roster/tenure loading; the
  precision cardinal + calibration FK gate; `families.py` independence concept.
- **Reframe:** the text labelers (`rollcall`, `discourse`, `self_intro`, `presenter_intro`,
  `vote_anchor`) — stop writing name anchors, start emitting `identity_evidence`. The Z2
  writer likewise emits evidence, not a `screen_name` anchor.
- **New:** `voice_observation` / `voice_node` / `voice_link` / `identity_evidence` model;
  the Stage-C cross-meeting linker; the Stage-E weighted resolver.
- **Retire or demote:** most of `enrollment.py`'s anchor-tier logic and the F1–F4 hygiene
  passes in `recalibrate_voiceprints.py` (their job disappears). `speaker_identities`
  becomes a *derived projection* of confirmed resolutions (for the search/reader surface)
  rather than the source of truth — or is dropped in favor of reading resolutions directly.
- **Data migration:** the 61 Zoom anchors + existing confirmed rows convert to
  `identity_evidence` observations (their basis becomes a channel); nothing is lost, and
  the human confirmations become high-weight evidence.

## 11. Migration path (phased, not big-bang)

1. **Prototype the linker (Stage C) standalone** and validate on ground-truth meetings by
   condition pair. Go/no-go on cross-condition bridging. *No production change.*
2. **Stand up the evidence model** (`identity_evidence`) and convert existing labelers +
   the 61 Zoom anchors into evidence emitters. Read-only; runs alongside the current gallery.
3. **Build the resolver (Stage E)** and run it against the transient voice-node graph;
   compare its schools/PC resolutions to the current calibration output, honestly.
4. **Cut the calibration harness over** to measure resolution (Stage F), precision-first.
5. **Switch the search/reader surface** to read confirmed resolutions; retire the
   name-first enroll/calibrate flow once parity is shown.

Each phase is measurable and reversible; the current system keeps running until a phase
proves out.

## 12. Open questions for interview (resolve before building)

1. **Linking strategy** — pure unsupervised global clustering (cleanest, the operator's
   framing) vs. anchored/seeded linking from strong-evidence observations (more robust to
   audio drift). Recommendation: hybrid — seed from anchors, cluster within condition,
   bridge via anchored officials. Confirm.
2. **Evidence combination** — transparent weighted ledger (entity-res doc's suggestion,
   recommended for v1) vs. a probabilistic posterior. Start weighted?
3. **First-cut scope** — prove on schools only, or design the general multi-body/multi-town
   version up front. Scalability cardinal says general; cost says one body first.
   Recommendation: general *model*, first *validation* on schools + PC.
4. **`speaker_identities` fate** — derived projection vs. full retirement. Affects the
   search/reader/API surface; needs a compatibility check.
5. **Unresolved recurring voices** — do we surface "Speaker A, unidentified, N meetings"
   anywhere in the product (citizen-safe, unnamed), or keep it purely internal to linking?
6. **Effort appetite** — this is an inversion, not a rewrite, but Stage C + E + the harness
   cutover is real work across several sessions. Confirm the appetite before phase 1.

## 13. Risks

- **Cross-condition linking may not bridge reliably** (§6). Mitigation: validate first;
  fall back to within-condition nodes if needed. This is the primary technical risk.
- **The evidence resolver can still be confidently wrong** — but errors no longer poison
  the acoustic layer, and the held-out precision gate + human promotion catch them before
  publication.
- **Scope creep into general entity resolution.** Keep this scoped to voices; reuse the
  entity-res doc's model, don't rebuild it.
- **Migration drift.** Run old and new in parallel and cut over only at measured parity.
