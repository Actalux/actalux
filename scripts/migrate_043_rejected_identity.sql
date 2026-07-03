-- Migration 043: admit a 'rejected' speaker-identity tier + protect it like 'confirmed'.
-- Run in the Supabase SQL editor (or via apply_migrations.py).
--
-- Design: docs/architecture/voiceprint-recalibration-plan.md (Phase 2, lever B).
-- Extends migrate_034 (speaker_identities.confidence) + migrate_035 (confirmed guard).
--
-- WHY: the operator confirm/deny CLI (scripts/confirm_speaker.py) records a human DENIAL
-- of a hypothesized official label ("this cluster is NOT that official"). A denial must
-- survive the automatic resolver's re-passes so the wrong name is never re-proposed for
-- that (document_id, cluster_label) -- exactly the durability 'confirmed' already has, in
-- the opposite direction. It stores NOTHING about who the voice actually is (Option B): the
-- row keeps the OFFICIAL subject_id it was proposed under and only moves to 'rejected'.
--
-- 'rejected' sits BELOW the public display gate (migrate_034's RLS policy exposes only
-- inferred_high / confirmed), so a rejected row never renders, and select_enrollable never
-- enrolls it -- both verified in code.
--
-- Additive + idempotent (DROP ... IF EXISTS then ADD / CREATE OR REPLACE).

-- 1. Widen the confidence CHECK to admit 'rejected'. The original constraint (migrate_034)
--    is inline + unnamed, so Postgres named it speaker_identities_confidence_check. The
--    separate table-level "displayable rows must name a subject" CHECK is left untouched:
--    'rejected' is not a displayable tier, so it imposes no subject_id requirement.
ALTER TABLE speaker_identities DROP CONSTRAINT IF EXISTS speaker_identities_confidence_check;
ALTER TABLE speaker_identities ADD CONSTRAINT speaker_identities_confidence_check
    CHECK (confidence IN
      ('unknown', 'inferred_low', 'inferred_medium', 'inferred_high', 'confirmed', 'rejected'));

-- 2. Extend the human-decision guard (migrate_035) to protect 'rejected' the same way it
--    protects 'confirmed'. A human decision -- confirm OR deny -- is a locked tier: the
--    automatic resolver's ON CONFLICT DO UPDATE (which only ever writes inferred_* rows)
--    must not move a locked row off its tier. Each tier is preserved unless the update keeps
--    it in that exact tier; the resolver's upsert therefore becomes a no-op on locked rows
--    instead of clobbering them. To un-decide, DELETE the row (not blocked here) -- the next
--    auto pass re-proposes it. persist_identities keeps its own read-then-write guard for the
--    retract (DELETE) path this BEFORE UPDATE trigger does not cover (see migrate_035).
--
-- CREATE OR REPLACE reuses migrate_035's function name so applying this upgrades the guard
-- in place; the trigger is re-created idempotently.
CREATE OR REPLACE FUNCTION protect_confirmed_speaker_identity()
RETURNS TRIGGER AS $$
BEGIN
    -- IS DISTINCT FROM so a malformed NULL update is treated as a downgrade too.
    IF OLD.confidence = 'confirmed' AND NEW.confidence IS DISTINCT FROM 'confirmed' THEN
        RETURN NULL;  -- skip: the confirmed row is preserved
    END IF;
    IF OLD.confidence = 'rejected' AND NEW.confidence IS DISTINCT FROM 'rejected' THEN
        RETURN NULL;  -- skip: the denied row is preserved (never re-proposed as inferred_*)
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_protect_confirmed_speaker_identity ON speaker_identities;
CREATE TRIGGER trg_protect_confirmed_speaker_identity
    BEFORE UPDATE ON speaker_identities
    FOR EACH ROW
    EXECUTE FUNCTION protect_confirmed_speaker_identity();
