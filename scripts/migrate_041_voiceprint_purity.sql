-- Migration 041: purity provenance on voiceprints + per-jurisdiction calibration record.
-- Run in the Supabase SQL editor (or via apply_migrations.py).
--
-- Design: docs/architecture/voiceprint-recalibration-plan.md (codex-reviewed).
-- Extends migrate_040 (subject_voiceprints gallery).
--
-- WHY: the first calibration failed because the gallery was poisoned by roll-call-
-- anchored, whole-cluster embeddings concentrated in a few mislabeled officials --
-- not by the model. The recalibration re-enrolls each cluster via two gates (label
-- quality + embedding purity) and evaluates with negatives under nested leave-one-
-- meeting-out. This migration adds (1) the purity provenance each re-enrolled sample
-- now carries, and (2) a per-place calibration record that stores the chosen operating
-- point and a candidate/cleared status. Calibration is a PER-JURISDICTION gate: a
-- town's voiceprints become trustworthy only after that town's calibration is reviewed
-- to 'cleared' -- so a new town cannot inherit Clayton's (or anyone's) operating point.
--
-- Additive + idempotent (CREATE ... IF NOT EXISTS; ADD COLUMN IF NOT EXISTS;
-- DROP POLICY then CREATE).

-- 1. Per-run calibration record (create FIRST so subject_voiceprints can reference it).
--    status starts 'candidate'; a human promotes candidate -> cleared after reviewing the
--    report. The persisted operating point is the full-data refit (plan §5.5); the nested-
--    LOMO estimate lives in `report`. report is aggregate-only -- never any negative
--    (non-official) cluster/doc/timestamp identifier (privacy).
CREATE TABLE IF NOT EXISTS voiceprint_calibration (
    id                  SERIAL PRIMARY KEY,
    place_id            INT NOT NULL REFERENCES places(id) ON DELETE CASCADE,
    entity_id           INT REFERENCES entities(id) ON DELETE SET NULL,  -- null = all bodies
    precision_bar       REAL NOT NULL,
    threshold           REAL,
    margin              REAL,
    aggregation         TEXT,
    trim_fraction       REAL,
    min_coherent_turns  INT,
    purity_floor        REAL,
    macro_precision     REAL,   -- nested-LOMO estimate
    recall              REAL,   -- nested-LOMO estimate
    fp_count            INT,    -- negatives matched to an official (nested-LOMO)
    n_officials         INT,
    n_enabled_officials INT,
    n_negatives         INT,
    gallery_size        INT,
    model               TEXT NOT NULL,
    status              TEXT NOT NULL DEFAULT 'candidate'
                          CHECK (status IN ('candidate', 'cleared', 'not_cleared')),
    report              JSONB,  -- grid/provenance, aggregate only
    calibrated_at       TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_vp_calibration_place
    ON voiceprint_calibration (place_id, entity_id);

-- 2. Purity provenance + the structural candidate/cleared link on each gallery sample.
--    calibration_id makes candidate-vs-cleared STRUCTURAL: a future matcher trusts a
--    voiceprint only when its calibration_id resolves to status='cleared'. Legacy rows
--    (calibration_id NULL) are never auto-trusted.
ALTER TABLE subject_voiceprints
    ADD COLUMN IF NOT EXISTS purity          REAL,
    ADD COLUMN IF NOT EXISTS n_turns         INT,
    ADD COLUMN IF NOT EXISTS coherent_turns  INT,
    ADD COLUMN IF NOT EXISTS calibration_id  INT REFERENCES voiceprint_calibration(id)
                              ON DELETE SET NULL;

-- 3. RLS: the calibration record is internal (operating points + status). Service key
--    only; no anon/authenticated read. (subject_voiceprints already service-only in 040.)
ALTER TABLE voiceprint_calibration ENABLE ROW LEVEL SECURITY;
REVOKE ALL ON voiceprint_calibration FROM anon, authenticated;
REVOKE ALL ON SEQUENCE voiceprint_calibration_id_seq FROM anon, authenticated;
