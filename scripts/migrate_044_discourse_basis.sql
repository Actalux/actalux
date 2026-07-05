-- Migration 044: admit the 'discourse' name-anchor basis.
-- Run in the Supabase SQL editor (or via apply_migrations.py).
--
-- Design: docs/architecture/voiceprint-scale-design.md (family 3, the LLM discourse labeler).
-- Extends migrate_042 (which added 'presenter_intro' to the same two CHECK constraints).
--
-- WHY: the identity layer gains a second, independent evidence family
-- (identity/discourse.py) — a language model reads how a meeting addresses its speakers
-- (chair recognitions, gratitude handoffs, role-claims, directed questions) and proposes
-- cluster -> member labels. Like presenter_intro it is a name-anchor basis held at
-- inferred_medium (below the public-display gate) yet enrollable, so both CHECK constraints
-- that enumerate the allowed bases must admit it:
--   1. speaker_identities.basis     -- the discourse labeler writes 'discourse' here;
--   2. subject_voiceprints.source_basis -- an enrolled discourse cluster carries its source
--      basis into the gallery, so the gallery constraint must allow it too (otherwise
--      enrollment of the new anchor is rejected at insert time).
--
-- Both original constraints (migrate_034 / migrate_040, last widened in migrate_042) are
-- named <table>_<column>_check by Postgres.
--
-- Additive + idempotent (DROP CONSTRAINT IF EXISTS then ADD).

-- 1. speaker_identities.basis (last set in migrate_042).
ALTER TABLE speaker_identities DROP CONSTRAINT IF EXISTS speaker_identities_basis_check;
ALTER TABLE speaker_identities ADD CONSTRAINT speaker_identities_basis_check
    CHECK (basis IN
      ('rollcall', 'vote_anchor', 'self_intro', 'manual', 'voiceprint', 'presenter_intro',
       'discourse'));

-- 2. subject_voiceprints.source_basis (last set in migrate_042).
ALTER TABLE subject_voiceprints DROP CONSTRAINT IF EXISTS subject_voiceprints_source_basis_check;
ALTER TABLE subject_voiceprints ADD CONSTRAINT subject_voiceprints_source_basis_check
    CHECK (source_basis IN
      ('rollcall', 'vote_anchor', 'self_intro', 'manual', 'presenter_intro', 'discourse'));
