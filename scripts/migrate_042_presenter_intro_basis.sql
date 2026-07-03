-- Migration 042: admit the 'presenter_intro' name-anchor basis.
-- Run in the Supabase SQL editor (or via apply_migrations.py).
--
-- Design: docs/architecture/voiceprint-recalibration-plan.md (Phase 2, lever A).
-- Extends migrate_040 (speaker_identities.basis + subject_voiceprints.source_basis).
--
-- WHY: the resolver (identity/resolve.py) gains a third deterministic name anchor,
-- 'presenter_intro' -- a member introduced in handoff position immediately followed by
-- a different cluster taking the floor with sustained speech. Like roll call and
-- self-introduction it is a spoken-name anchor (a name-anchor basis, enrollable), so
-- both CHECK constraints that enumerate the allowed bases must admit it:
--   1. speaker_identities.basis     -- the resolver writes 'presenter_intro' here;
--   2. subject_voiceprints.source_basis -- an enrolled presenter_intro cluster carries
--      its source basis into the gallery, so the gallery constraint must allow it too
--      (otherwise enrollment of the new anchor is rejected at insert time).
--
-- Both original constraints (migrate_034 / migrate_040) are inline + unnamed, so
-- Postgres named them <table>_<column>_check.
--
-- Additive + idempotent (DROP CONSTRAINT IF EXISTS then ADD).

-- 1. speaker_identities.basis (last set in migrate_040:112-114).
ALTER TABLE speaker_identities DROP CONSTRAINT IF EXISTS speaker_identities_basis_check;
ALTER TABLE speaker_identities ADD CONSTRAINT speaker_identities_basis_check
    CHECK (basis IN
      ('rollcall', 'vote_anchor', 'self_intro', 'manual', 'voiceprint', 'presenter_intro'));

-- 2. subject_voiceprints.source_basis (set inline in migrate_040:47-48).
ALTER TABLE subject_voiceprints DROP CONSTRAINT IF EXISTS subject_voiceprints_source_basis_check;
ALTER TABLE subject_voiceprints ADD CONSTRAINT subject_voiceprints_source_basis_check
    CHECK (source_basis IN
      ('rollcall', 'vote_anchor', 'self_intro', 'manual', 'presenter_intro'));
