-- Migration 048: per-body linking operating points (frozen threshold + method + calibrator).
-- Run in the Supabase SQL editor (or via apply_migrations.py).
--
-- WHY: the linker's operating point — "score with METHOD, link at THRESHOLD, hold purity at FLOOR,
-- against COHORT" — is a measured, per-body decision (leave-one-out eval + the correct-vs-wrong
-- proposer tradeoff). Until now it lived in CLI arguments and docs, which fails at scale two ways:
-- a second town inherits the first town's threshold by copy-paste, and automation has nothing to
-- read. Storing it per (place, body) makes the decision structural, versioned, and auditable
-- (CLAUDE.md: structural over procedural; per-place config, not constants).
--
-- Keyed by body_slug, not entity id: a body can span several entities (plan commission + ARB) and
-- every linking CLI identifies its target by (state, place, body_slug).
--
-- Additive + idempotent (CREATE ... IF NOT EXISTS).

CREATE TABLE IF NOT EXISTS linking_operating_points (
    id           SERIAL PRIMARY KEY,
    place_id     INT NOT NULL REFERENCES places(id) ON DELETE CASCADE,
    body_slug    TEXT NOT NULL,
    -- the frozen cohort the threshold was measured against; a threshold is meaningless on a
    -- different score distribution, so the proposer refuses to run when the active cohort differs
    cohort_id    INT NOT NULL REFERENCES linking_cohorts(id) ON DELETE CASCADE,
    method       TEXT NOT NULL CHECK (method IN ('asnorm', 'calibrated')),
    threshold    DOUBLE PRECISION NOT NULL,
    purity_floor DOUBLE PRECISION NOT NULL CHECK (purity_floor > 0 AND purity_floor <= 1),
    -- fitted calibrator (weights/mean/std/feature_names) for method='calibrated', frozen so
    -- propose-time refits cannot drift as anchors accrue; NULL for method='asnorm'
    calibrator   JSONB,
    notes        TEXT,
    is_active    BOOLEAN NOT NULL DEFAULT FALSE,
    created_at   TIMESTAMPTZ DEFAULT NOW(),
    CONSTRAINT linking_operating_points_calibrated_needs_weights
        CHECK (method <> 'calibrated' OR calibrator IS NOT NULL)
);

-- at most one active operating point per body
CREATE UNIQUE INDEX IF NOT EXISTS uq_linking_operating_points_one_active
    ON linking_operating_points (place_id, body_slug) WHERE is_active;

-- Service-only, mirroring linking_cohorts (migrate_047): internal tuning state, never public.
ALTER TABLE linking_operating_points ENABLE ROW LEVEL SECURITY;
REVOKE ALL ON linking_operating_points FROM anon, authenticated;
REVOKE ALL ON SEQUENCE linking_operating_points_id_seq FROM anon, authenticated;
