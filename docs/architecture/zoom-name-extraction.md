# Zoom on-screen name extraction — spec (DRAFT 2026-07-09; recon CONFIRMED)

Operator insight 2026-07-09: COVID-era meetings are Zoom recordings with participant
names printed on every tile — the platform already attributed the speakers and rendered
it into the public video. Reading a displayed name is NOT biometric processing (no face
or voice template of anyone), so it is policy-cheap in exactly the way face-ID is not.

## Recon (verified by frame inspection, 2026-07-09)

- doc 2528 / 2020-05-13 (17H_l0CU6-M @20min): full Zoom gallery, name label on EVERY
  tile ("Adam Jaffe", "Kristin Redington", "Sean Doherty", "Stacy Siwak", "Joe Miller",
  "Amy Rubin", "Gary Pierson", "lily raymond", "jasonwilson"), and **the active
  speaker's tile carries a green border** (Sean Doherty at that instant).
- doc 2531 / 2021-02-10 (NcZxWmRoSE4 @20min): same layout; green border on the
  "jasonwilson" tile — frame-accurate ground truth inside Wilson's term.
- doc 2101 / 2022-01-19 (roUDWBmyHK0 @20min): screen-share mode — but the speaker
  thumbnail (top-right) still shows the label "Nisha Patel". Share mode is readable too.
- doc 2536 / 2021-11-10 (9qLqGr9XYb8 @20min): in-person room camera — no tiles. Coverage
  is era-dependent; a per-video probe is required. City bodies unprobed (check COVID-era
  council/PC/BoA videos the same way).

Frame extraction is cheap and full-download-free: `yt-dlp -g` for the stream URL +
`ffmpeg -ss <t> -i <url> -frames:v 1` (ranged request, <2 s/frame locally).

## Design sketch (phases; spec-first interview before build)

1. **Z0 coverage probe**: 3 sampled frames per video across the corpus -> classify
   {zoom-gallery | share-with-tile | room-cam | other} per video (+ per-segment later:
   meetings switch modes). Output: the coverage map per body/era. Cheap; no writes.
2. **Z1 turn-aligned reading**: for target clusters (unlabeled or disputed), sample
   frames at several of the cluster's TURN midpoints (timestamps from
   diarization_turns); detect the active-speaker tile (green-border color threshold —
   classic CV, deterministic) or the share-mode speaker thumbnail; OCR the label.
3. **Z2 anchors**: if the OCR'd display name maps to exactly one roster member (alias
   matching via the existing lexicon; display names are informal — "jasonwilson") and
   the SAME person reads out across >= N distinct turns of the cluster (majority with a
   floor — Zoom's active-speaker highlight can flick to crosstalk), write a
   `speaker_identities` anchor with a new basis `screen_name`. Evidence strength:
   platform-rendered attribution — proposed tier inferred_high (like roll call; decide
   in interview). New basis needs: CHECK-constraint migration + families.py mapping
   (its own family — independent error mode from adjacency/discourse/vote) +
   NAME_ANCHOR_BASES.
4. **Z3 non-roster names**: tiles for non-officials ("lily raymond", "Kaitlyn Tran" =
   likely student reps/public) are name-the-public-record tier-2 candidates
   (per-document, never tracked) — a later phase; the protected-class rule (schools
   personnel/students never named) applies BEFORE any write, so Z3 needs the
   review-queue path, not auto.

## Why this is high-leverage

The COVID era (~2020-04 -> 2022) is exactly where the label debt lives: Wilson's whole
term, the pre-tenure "Growe" mess, the gap-era mystery voice, and the sparsest
discourse-anchor coverage. It can also mint anchors for people with ZERO trusted text
(Growe-era members), feeding voiceprint enrollment AND stylometry profiles. OCR of a
rendered name is deterministic, auditable (keep the frame as the receipt), and
citizen-safe (reading a published name is not enrollment).

## Open questions (interview before Z1)

- Tier for `screen_name` anchors: inferred_high (roll-call grade) vs inferred_medium?
- Turn-consistency floor N and the crosstalk-flick tolerance (choose from Z0/Z1 data).
- OCR engine: tesseract (local, deterministic) vs macOS Vision — pick by measured
  accuracy on the small white-on-dark labels at 640px; may need the 720p/1080p stream.
- Keep sampled frames as receipts? (data/ gitignored dir, like audit sheets.)

## Z2 write policy — DECIDED 2026-07-11 (operator-approved)

Z1 measured two rendering modes with different error profiles, so the tier is decided
**per verdict by mode**, not globally:

1. **Basis**: new `speaker_identities.basis` value `screen_name`
   (migrate_046; also `subject_voiceprints.source_basis`). Own evidence family
   `screen` in `families.py` — a platform-rendered name is a visual mechanism,
   independent of adjacency/discourse/vote. Added to `NAME_ANCHOR_BASES` and to
   `_MEDIUM_ENROLLABLE_BASES` in `enrollment.py`.
2. **Tier by mode**:
   - every supporting frame is **gallery tile** mode (green border on the speaker's
     own labeled tile) → `inferred_high` — the label is physically attached to the
     active speaker; roll-call grade.
   - any supporting frame is **full-frame speaker-view** → `inferred_medium` — the
     bottom-left label usually tracks the active speaker but is subject to the
     account-feed trap below, so it stays under the public-display gate and earns
     enablement only through cross-meeting family agreement.
   - `cluster_verdict` already requires ≥2 agreeing frames (min_agree).
3. **Feed guards (no write when tripped)**:
   - per-doc slug cap (`feed_label_slugs`, >2 clusters won ⇒ account label); AND
   - **full-frame diversity rule** (added after doc 2363): a slug that wins ≥2
     clusters via full-frame frames in a document whose readable full-frame frames
     contain NO other slug is the streaming account's label, not the speaker
     (doc 2363: every readable frame = "Ryan Helle" across two diarized voices,
     contradicting a roll-call anchor; contrast doc 2340 where full-frame labels
     read five different names as the speaker view switches).
4. **Against existing anchors** (`UNIQUE (document_id, cluster_label)` = one anchor
   per cluster, which is correct — a second row at the same cluster would count the
   SAME voice sample twice):
   - **AGREE** (existing anchor names the same person): NO write. The frame receipt
     is audit value; independent-family corroboration happens across meetings
     (discourse anchor in doc A + screen anchor in doc B = 2 families), never by
     stacking rows on one sample.
   - **CONFLICT** (different person): NO write, surface for review — never
     auto-reject (standing rule).
   - **Rejected row at that cluster**: sticky; skip and surface.
   - **No row**: insert the `screen_name` anchor at the mode-decided tier.
5. **Non-roster names**: never written by Z2 (no subject exists). City bodies may
   later feed the tier-2 name-the-public-record path; schools tiles include likely
   students — the protected-class rule applies before ANY use. Z3 remains gated.
6. **Writer**: `scripts/apply_zoom_verdicts.py` — reads a Z1 evidence JSON,
   applies rules 2-5, dry-run by default, `--apply` to write; idempotent (a
   `screen_name` row already present at (doc, cluster) is left alone).
