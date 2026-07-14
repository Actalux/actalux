-- Migration 047: frozen linking cohort (AS-norm background) + acoustic condition on voiceprints.
-- Run in the Supabase SQL editor (or via apply_migrations.py).
--
-- Design: docs/architecture/linking-backend-decision-2026-07-12.md. Extends the linking prototype
-- (linking-prototype-phase1.md) and the voiceprint gallery (migrate_040/041).
--
-- WHY (cohort): the cross-meeting linker scores AS-norm against a diverse, target-disjoint impostor
-- cohort. That cohort must be FROZEN and EXTERNAL: a self/trial-set cohort is transductive (adding a
-- meeting silently re-scores old identities) and re-introduces speaker imbalance. The cohort is an
-- UNLABELED statistical background — a yardstick with no identity attached — persisted service-only.
--   Operator decision (2026-07-13): a nameless background cohort is not a "tracked voiceprint", so it
--   may include non-official (resident) vectors, unlabeled. This is an explicit, auditable reading of
--   the Option-B rule (no citizen voiceprints) — see the decision doc. There is NO person/subject
--   link on these rows BY DESIGN, so a cohort vector can never be re-attached to an identity.
--
-- WHY (acoustic_condition): dual per-condition prototypes — an official carries separate Zoom and
-- room-mic gallery centroids instead of one averaged vector, so a query cluster is scored against the
-- condition-matched prototype (the cross-condition drift fix). A document is a single condition, so
-- the value is derivable from source_document_id; storing it makes the split structural + indexable.
--
-- Additive + idempotent (CREATE ... IF NOT EXISTS; ADD COLUMN IF NOT EXISTS; DROP then CREATE).

-- 1. Versioned cohort header. place_id NULL = a shared / cross-jurisdiction / open-corpus cohort
--    (the source is a plug-in chosen by measurement, not hardcoded to one town's meetings).
CREATE TABLE IF NOT EXISTS linking_cohorts (
    id                SERIAL PRIMARY KEY,
    slug              TEXT NOT NULL UNIQUE,
    place_id          INT REFERENCES places(id) ON DELETE CASCADE,   -- NULL = shared / open-corpus
    model             TEXT NOT NULL,             -- embedding model id (MUST match the gallery model)
    source            TEXT NOT NULL,             -- provenance: 'clayton-council-pc' | '3d-speaker' | ...
    n_vectors         INT NOT NULL DEFAULT 0,
    condition_balance JSONB,                     -- {"zoom": n, "in_person": n} for audit
    notes             TEXT,
    is_active         BOOLEAN NOT NULL DEFAULT FALSE,
    created_at        TIMESTAMPTZ DEFAULT NOW()
);
-- At most one active cohort per scope (COALESCE folds the shared NULL-place cohort into one slot).
CREATE UNIQUE INDEX IF NOT EXISTS uq_linking_cohorts_one_active
    ON linking_cohorts (COALESCE(place_id, -1)) WHERE is_active;

-- 2. The cohort vectors. UNLABELED BY DESIGN: no person_id/subject_id column exists, so a vector
--    can never be re-attached to an identity. source_* are coarse provenance for audit/rebuild only.
CREATE TABLE IF NOT EXISTS linking_cohort_vectors (
    id                 SERIAL PRIMARY KEY,
    cohort_id          INT NOT NULL REFERENCES linking_cohorts(id) ON DELETE CASCADE,
    embedding          VECTOR(256) NOT NULL,     -- same 256-d wespeaker space as the gallery
    acoustic_condition TEXT,                     -- 'zoom' | 'in_person' | NULL (unknown)
    source_entity_id   INT REFERENCES entities(id) ON DELETE SET NULL,    -- which body (audit)
    source_document_id INT REFERENCES documents(id) ON DELETE SET NULL,   -- which meeting (audit)
    created_at         TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_linking_cohort_vectors_cohort
    ON linking_cohort_vectors (cohort_id);

-- 3. Dual per-condition prototypes: tag each gallery sample with its meeting's acoustic condition so
--    an official's Zoom and room-mic samples aggregate into separate centroids. Nullable, backfilled
--    from the source document's condition. A document is one condition, so the existing
--    UNIQUE(person_id, source_document_id, cluster_label) on subject_voiceprints is unaffected.
ALTER TABLE subject_voiceprints
    ADD COLUMN IF NOT EXISTS acoustic_condition TEXT;

-- 4. RLS: the cohort tables are internal (background embeddings + headers). Service key only; no
--    anon/authenticated read. subject_voiceprints is already service-only (migrate_040).
ALTER TABLE linking_cohorts ENABLE ROW LEVEL SECURITY;
REVOKE ALL ON linking_cohorts FROM anon, authenticated;
REVOKE ALL ON SEQUENCE linking_cohorts_id_seq FROM anon, authenticated;

ALTER TABLE linking_cohort_vectors ENABLE ROW LEVEL SECURITY;
REVOKE ALL ON linking_cohort_vectors FROM anon, authenticated;
REVOKE ALL ON SEQUENCE linking_cohort_vectors_id_seq FROM anon, authenticated;
